#!/usr/bin/env python3
"""Normalize ECHOREPO soil texture/structure from Flutter Dart translations.

Sources: data/app_translations/<locale>.dart (keys are English canonical choices).
Outputs: PostgreSQL samples.*_en; optional local canonical CSV/ZIP;
         static/soil_classifications_i18n.json for map.js.

Usage examples:
  python3 tools/normalize_soil_classifications.py --dry-run
  python3 tools/normalize_soil_classifications.py
  python3 tools/normalize_soil_classifications.py --upload-latest
  python3 tools/normalize_soil_classifications.py --no-postgres --no-local-canonical

The script does NOT modify *_orig, Firebase, or dated MinIO snapshots.
"""
from __future__ import annotations
import argparse
import csv
import json
import logging
import os
import re
import sys
import tempfile
import unicodedata
import zipfile
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOG = logging.getLogger('soil_classifications')
TEXTURE = ('Sandy', 'Sandy Loam', 'Sandy Clay', 'Silty', 'Silty Clay', 'Clayey', 'Clay Loam', 'Other *')
STRUCTURE = ('FRIABLE', 'INTACT', 'FIRM', 'COMPACT', 'HIGH COMPACT')
KEYS = {'texture': TEXTURE, 'structure': STRUCTURE}
COLS = {'texture': ('soil_texture_orig', 'soil_texture_en'), 'structure': ('soil_structure_orig', 'soil_structure_en')}
JSON_NAME = 'soil_classifications_i18n.json'
# Strictly parse line-oriented Dart map string entries, including backslash-escaped quotes.
ENTRY = re.compile(r'''^\s*(?P<kq>['"])(?P<key>(?:\\.|.)*?)(?P=kq)\s*:\s*(?P<vq>['"])(?P<value>(?:\\.|.)*?)(?P=vq)\s*,?\s*(?://.*)?$''')
ESCAPE = re.compile(r'\\(u\{[0-9a-fA-F]+\}|u[0-9a-fA-F]{4}|x[0-9a-fA-F]{2}|.)', re.S)


def unescape_dart(s: str) -> str:
    def decode(m):
        v = m.group(1)
        if v.startswith('u{'):
            try: return chr(int(v[2:-1], 16))
            except ValueError: return m.group(0)
        if v.startswith('u') and len(v) == 5: return chr(int(v[1:], 16))
        if v.startswith('x') and len(v) == 3: return chr(int(v[1:], 16))
        return {'n':'\n', 'r':'\r', 't':'\t', '$':'$', '\\':'\\', "'":"'", '"':'"'}.get(v, m.group(0))
    return ESCAPE.sub(decode, s)


def norm(s: object) -> str:
    """Normalize case, spaces, Unicode width; don't strip meaningful accents."""
    return ' '.join(unicodedata.normalize('NFKC', str(s or '')).split()).casefold()


def english(key: str) -> str:
    # Canonical lowercase, except Other as requested.
    return 'Other' if key == 'Other *' else key.lower()


def display_en(key: str) -> str:
    return 'Other' if key == 'Other *' else key.title() if key in STRUCTURE else key


def load_env(path: Path):
    # Match pull_and_enrich_samples.py: an existing exported env wins.
    try:
        from dotenv import load_dotenv
        load_dotenv(path)
    except ImportError:
        LOG.warning('python-dotenv not installed: only exported environment variables are available')


def build_catalog(directory: Path) -> tuple[dict, dict, dict]:
    dart_files = sorted(directory.glob('*.dart'))
    if not dart_files:
        raise FileNotFoundError(f'No Dart translation files found at {directory}')

    locales: dict[str, dict] = {}
    reverse: dict[str, dict[str, set[str]]] = {
        field: defaultdict(set) for field in KEYS
    }
    # Key names also directly match, irrespective of which languages are present.
    for field, keys in KEYS.items():
        for key in keys:
            reverse[field][norm(key)].add(key)
            reverse[field][norm(english(key))].add(key)
            if key == 'Other *':
                reverse[field][norm('Other')].add(key)

    for source in dart_files:
        locale = source.stem
        translations: dict[str, str] = {}
        for line in source.read_text(encoding='utf-8-sig').splitlines():
            match = ENTRY.match(line)
            if match:
                key = unescape_dart(match.group('key'))
                if key in TEXTURE or key in STRUCTURE:
                    value = unescape_dart(match.group('value')).strip()
                    if value:
                        translations[key] = value
        missing = [k for k in (*TEXTURE, *STRUCTURE) if k not in translations]
        if missing:
            LOG.warning('%s: missing %s', source.name, ', '.join(missing))
        locales[locale] = {field: {k: translations[k] for k in keys if k in translations}
                           for field, keys in KEYS.items()}
        for field, keys in KEYS.items():
            for key in keys:
                if key in translations:
                    reverse[field][norm(translations[key])].add(key)

    ambiguous = {
        field: {value: sorted(keys) for value, keys in reverse[field].items() if len(keys) > 1}
        for field in KEYS
    }
    for field, cases in ambiguous.items():
        for value, keys in sorted(cases.items()):
            LOG.warning('Ambiguous %s value %r: %s (will NOT auto-normalize)', field, value, keys)
    clean_reverse = {
        field: {value: next(iter(keys)) for value, keys in reverse[field].items() if len(keys) == 1}
        for field in KEYS
    }
    catalog = {
        'schema_version': 1,
        'english': {field: {key: display_en(key) for key in keys} for field, keys in KEYS.items()},
        'locales': locales,
        'reverse': clean_reverse,
    }
    return catalog, clean_reverse, ambiguous


def resolve(value: object, field: str, reverse: dict) -> str | None:
    if value is None or norm(value) in ('', 'none', 'null', 'nan'):
        return None
    key = reverse[field].get(norm(value))
    return english(key) if key else None


def atomic_write_text(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f'.{path.name}.', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='') as f:
            f.write(content)
        if path.exists():
            os.chmod(tmp, path.stat().st_mode & 0o777)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)


def translate_record(row: dict, reverse: dict, stats: Counter, unknown: dict):
    result = dict(row)
    for field, (src, dest) in COLS.items():
        raw = result.get(src, '')
        converted = resolve(raw, field, reverse)
        if converted is None:
            if norm(raw):
                stats[f'{field}_unmatched'] += 1
                unknown[field][str(raw).strip()] += 1
            continue  # Preserve existing translation if input unrecognized.
        stats[f'{field}_recognized'] += 1
        if str(result.get(dest) or '').strip() != converted:
            stats[f'{field}_changed'] += 1
            result[dest] = converted
    return result


def normalize_pg(reverse: dict, dry_run: bool, stats: Counter, unknown: dict):
    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except ImportError as exc:
        raise RuntimeError('Postgres update requires psycopg2 (pip install psycopg2-binary)') from exc
    # Use the host-side connection env, matching the existing ingestion pipeline.
    with psycopg2.connect(
        host=os.getenv('DB_HOST_OUTSIDE') or os.getenv('DB_HOST') or 'localhost',
        port=int(os.getenv('DB_PORT_OUTSIDE') or os.getenv('DB_PORT') or '5432'),
        dbname=os.getenv('DB_NAME', 'echorepo'),
        user=os.getenv('DB_USER', 'echorepo'),
        password=os.getenv('DB_PASSWORD', ''),
    ) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT sample_id, soil_texture_orig, soil_texture_en, soil_structure_orig, soil_structure_en FROM samples')
            entries = cur.fetchall()
            stats['pg_rows_scanned'] = len(entries)
            updates = []
            for sid, texture, texture_en, structure, structure_en in entries:
                d = {'soil_texture_orig': texture, 'soil_texture_en': texture_en,
                     'soil_structure_orig': structure, 'soil_structure_en': structure_en}
                changed = translate_record(d, reverse, stats, unknown)
                if changed['soil_texture_en'] != texture_en or changed['soil_structure_en'] != structure_en:
                    updates.append((sid, changed['soil_texture_en'], changed['soil_structure_en']))
            stats['pg_rows_to_update'] = len(updates)
            if updates and not dry_run:
                execute_values(cur, '''
                    UPDATE samples AS s SET
                      soil_texture_en = v.soil_texture_en,
                      soil_structure_en = v.soil_structure_en
                    FROM (VALUES %s) AS v(sample_id, soil_texture_en, soil_structure_en)
                    WHERE s.sample_id = v.sample_id
                      AND (s.soil_texture_en IS DISTINCT FROM v.soil_texture_en
                        OR s.soil_structure_en IS DISTINCT FROM v.soil_structure_en)
                ''', updates, page_size=500)
                # execute_values with 500 batch size and 9k rows, commits on context exit.
            if dry_run:
                conn.rollback()
            LOG.info('PostgreSQL: scanned=%d rows_to_update=%d dry_run=%s', len(entries), len(updates), dry_run)


def normalize_local_canonical(csv_path: Path, reverse: dict, dry_run: bool, stats: Counter, unknown: dict) -> bool:
    if not csv_path.is_file():
        LOG.warning('Local canonical CSV not found: %s (skipped)', csv_path)
        return False
    with csv_path.open('r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or not all(c in reader.fieldnames for pair in COLS.values() for c in pair):
            raise ValueError(f'CSV missing canonical texture/structure columns: {csv_path}')
        fields = list(reader.fieldnames)
        records = [translate_record(row, reverse, stats, unknown) for row in reader]
    stats['csv_rows_scanned'] = len(records)
    # Atomic rewrite (only on real run) preserves all original non-translation cells.
    if not dry_run:
        from io import StringIO
        out = StringIO(newline='')
        writer = csv.DictWriter(out, fieldnames=fields, lineterminator='\n')
        writer.writeheader()
        writer.writerows(records)
        old_text = csv_path.read_text(encoding='utf-8-sig')
        if out.getvalue() != old_text:
            atomic_write_text(csv_path, out.getvalue())
            LOG.info('Updated local canonical CSV: %s', csv_path)
            return True
    return False


def refresh_local_zip(zip_path: Path, samples_csv: Path, dry_run: bool):
    if not zip_path.exists():
        LOG.info('No local all.zip; skipped')
        return
    with zipfile.ZipFile(zip_path, 'r') as old:
        names = old.namelist()
        if 'samples.csv' not in names:
            raise RuntimeError(f'{zip_path} contains no samples.csv')
        if not dry_run:
            fd, tmp = tempfile.mkstemp(prefix=f'.{zip_path.name}.', suffix='.zip.tmp', dir=zip_path.parent)
            os.close(fd)
            try:
                with zipfile.ZipFile(tmp, 'w') as new:
                    for item in old.infolist():
                        payload = samples_csv.read_bytes() if item.filename == 'samples.csv' else old.read(item.filename)
                        new.writestr(item, payload)
                os.replace(tmp, zip_path)
                LOG.info('Refreshed local all.zip: %s', zip_path)
            finally:
                if os.path.exists(tmp): os.unlink(tmp)


def upload_latest(csv_path: Path, zip_path: Path):
    try:
        from minio import Minio
    except ImportError as exc:
        raise RuntimeError('MinIO publishing requires the minio Python package') from exc
    endpoint = os.getenv('MINIO_ENDPOINT_OUTSIDE', 'localhost:9000')
    secure = endpoint.startswith('https://')
    endpoint = endpoint.removeprefix('https://').removeprefix('http://')
    user = os.getenv('MINIO_ACCESS_KEY') or os.getenv('MINIO_ROOT_USER')
    password = os.getenv('MINIO_SECRET_KEY') or os.getenv('MINIO_ROOT_PASSWORD')
    if not user or not password:
        raise RuntimeError('MinIO credentials missing')
    bucket = os.getenv('MINIO_BUCKET', 'echorepo-uploads')
    client = Minio(endpoint, access_key=user, secret_key=password, secure=secure)
    # Never rewrite historical dated snapshots.
    if zip_path.is_file():
        client.fput_object(bucket, 'canonical/latest/all.zip', str(zip_path), content_type='application/zip')
        LOG.info('MinIO updated canonical/latest/all.zip')
    client.fput_object(bucket, 'canonical/latest/samples.csv', str(csv_path), content_type='text/csv')
    LOG.info('MinIO updated canonical/latest/samples.csv')


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--translations', type=Path, default=ROOT/'data/app_translations')
    ap.add_argument('--json-output', type=Path, default=ROOT/'static/soil_classifications_i18n.json')
    ap.add_argument('--canonical-csv', type=Path, default=ROOT/'data/canonical/samples.csv')
    ap.add_argument('--no-postgres', action='store_true')
    ap.add_argument('--no-local-canonical', action='store_true')
    ap.add_argument('--no-json', action='store_true')
    ap.add_argument('--upload-latest', action='store_true', help='Overwrite ONLY latest MinIO samples.csv/all.zip')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    load_env(ROOT/'.env')
    try:
        catalog, reverse, ambiguous = build_catalog(args.translations)
        LOG.info('Parsed Dart translations from %s', args.translations)
    except FileNotFoundError:
        # Production can use the version-controlled JSON generated in development.
        if not args.json_output.is_file():
            raise
        catalog = json.loads(args.json_output.read_text(encoding='utf-8'))
        if catalog.get('schema_version') != 1 or not catalog.get('reverse'):
            raise ValueError(f'Invalid static translations catalog: {args.json_output}')
        reverse = catalog['reverse']
        ambiguous = {'texture': {}, 'structure': {}}
        LOG.info('No Dart files; using existing static JSON catalog: %s', args.json_output)
    LOG.info('Available %d locales, %d texture keys and %d structure keys',
             len(catalog['locales']), len(TEXTURE), len(STRUCTURE))
    stats = Counter()
    unknown = {'texture': Counter(), 'structure': Counter()}
    if not args.no_json and not args.dry_run:
        atomic_write_text(args.json_output, json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=True)+'\n')
        LOG.info('Wrote map translations: %s', args.json_output)
    if not args.no_postgres:
        normalize_pg(reverse, args.dry_run, stats, unknown)
    csv_changed = False
    if not args.no_local_canonical:
        csv_changed = normalize_local_canonical(args.canonical_csv, reverse, args.dry_run, stats, unknown)
        if csv_changed:
            refresh_local_zip(args.canonical_csv.parent/'all.zip', args.canonical_csv, args.dry_run)
    if args.upload_latest:
        if args.dry_run:
            LOG.info('Dry run: not publishing to MinIO')
        elif args.no_local_canonical:
            raise RuntimeError('--upload-latest requires local canonical CSV processing')
        elif not args.canonical_csv.exists():
            raise RuntimeError('Cannot upload latest: local canonical samples.csv not found')
        else:
            upload_latest(args.canonical_csv, args.canonical_csv.parent/'all.zip')
    LOG.info('Stats: %s', json.dumps(dict(sorted(stats.items())), ensure_ascii=False))
    for field in KEYS:
        LOG.info('Unrecognized %s examples: %s', field, unknown[field].most_common(20))
    LOG.info('Ambiguous labels skipped: %s', {k: len(v) for k,v in ambiguous.items()})
    return 0

if __name__ == '__main__':
    try: sys.exit(main())
    except Exception:
        LOG.exception('Soil classification normalization failed')
        sys.exit(1)
