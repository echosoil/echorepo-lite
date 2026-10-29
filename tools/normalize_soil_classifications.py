#!/usr/bin/env python3
"""Normalize ECHOREPO soil texture/structure from Flutter Dart translations.

Sources: data/app_translations/<locale>.dart and reviewed soil_classification_overrides.json.
Generated JSON is an output, not a source of manual corrections.
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
    """Case-insensitive, Unicode-safe normalization of mobile-app choice labels.

    Differences in capitalization, whitespace, dash/hyphen types, and spacing
    around a trailing asterisk are formatting variants, not new classes.
    Accents are preserved: do not merge unrelated words automatically.
    """
    if s is None:
        return ''
    value = unicodedata.normalize('NFKC', str(s)).casefold()
    value = re.sub(r'[\u2010-\u2015\u2212\uFE58\uFE63\uFF0D-]', ' ', value)
    value = ' '.join(value.split())
    value = re.sub(r'\s*\*\s*$', ' *', value)
    return value


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


def load_overrides(path: Path) -> dict:
    """Load durable, reviewed corrections; generated JSON must not be edited by hand."""
    if not path.is_file():
        LOG.info('No overrides file at %s (using Dart translations only)', path)
        return {'forward_overrides': {}, 'aliases': {}}
    data = json.loads(path.read_text(encoding='utf-8-sig'))
    if data.get('schema_version') != 1:
        raise ValueError(f'Unsupported overrides schema in {path}')
    for locale, fields in data.get('forward_overrides', {}).items():
        for field, translations in fields.items():
            if field not in KEYS:
                raise ValueError(f'Unknown override field: {field}')
            for key in translations:
                if key not in KEYS[field]:
                    raise ValueError(f'Unknown canonical {field} key {key!r} in {locale}')
    for field, entries in data.get('aliases', {}).items():
        if field not in KEYS:
            raise ValueError(f'Unknown aliases field: {field}')
        for label, key in entries.items():
            if key not in KEYS[field]:
                raise ValueError(f'Alias {label!r} targets unknown {field} key: {key}')
    return data


def catalog_from_locales(locales: dict, overrides: dict) -> tuple[dict, dict, dict]:
    """Build a clean reverse index from effective locale values and reviewed aliases.

    Forward overrides are applied BEFORE computing ambiguity, so corrected
    Flutter strings cannot leave obsolete collisions in the reverse mapping.
    """
    locales = {loc: {field: dict(values.get(field, {})) for field in KEYS}
               for loc, values in locales.items()}
    for locale, fields in overrides.get('forward_overrides', {}).items():
        if locale not in locales:
            LOG.warning('Locale override %s has no matching Dart/JSON locale', locale)
            continue
        for field, translations in fields.items():
            for key, text in translations.items():
                old = locales[locale][field].get(key)
                if old != text:
                    LOG.info('Locale override %s/%s/%s: %r -> %r', locale, field, key, old, text)
                locales[locale][field][key] = str(text).strip()

    reverse_sets: dict[str, dict[str, set[str]]] = {
        field: defaultdict(set) for field in KEYS
    }
    for field, keys in KEYS.items():
        for key in keys:
            reverse_sets[field][norm(key)].add(key)
            reverse_sets[field][norm(english(key))].add(key)
    for values in locales.values():
        for field in KEYS:
            for key, value in values[field].items():
                if key not in KEYS[field]:
                    continue
                if norm(value):
                    reverse_sets[field][norm(value)].add(key)
                    # Some Flutter labels append the English key as a hint,
                    # e.g. "Hiekkainen savi (sandy clay)". The unadorned
                    # version is a safe alias *only* when the hint matches.
                    hint = re.fullmatch(r'(.*?)\s*\(\s*([^()]*)\s*\)\s*', str(value))
                    if hint and norm(hint.group(2)) in {norm(key), norm(english(key))}:
                        reverse_sets[field][norm(hint.group(1))].add(key)

    # Explicit synonyms are reviewed declarations, not fuzzy guessing.
    # If an alias conflicts with another translated label, fail visibly rather
    # than overriding the data with an arbitrary canonical category.
    for field, labels in overrides.get('aliases', {}).items():
        for label, key in labels.items():
            nlabel = norm(label)
            if not nlabel:
                raise ValueError(f'Empty synonym in field {field}')
            existing = reverse_sets[field].get(nlabel, set())
            if existing and existing != {key}:
                raise ValueError(
                    f'Conflicting {field} synonym {label!r} -> {key}; '
                    f'existing categories: {sorted(existing)}'
                )
            reverse_sets[field][nlabel].add(key)

    ambiguous = {
        field: {label: sorted(keys) for label, keys in vals.items() if len(keys) > 1}
        for field, vals in reverse_sets.items()
    }
    for field, cases in ambiguous.items():
        for value, keys in sorted(cases.items()):
            LOG.warning('Ambiguous %s value %r: %s (will NOT auto-normalize)', field, value, keys)
    clean_reverse = {
        field: {label: sorted(keys)[0] for label, keys in vals.items() if len(keys) == 1}
        for field, vals in reverse_sets.items()
    }
    catalog = {
        'schema_version': 1,
        'english': {field: {key: display_en(key) for key in keys} for field, keys in KEYS.items()},
        'locales': locales,
        'reverse': clean_reverse,
    }
    return catalog, clean_reverse, ambiguous


def build_catalog(directory: Path, overrides: dict) -> tuple[dict, dict, dict]:
    dart_files = sorted(directory.glob('*.dart'))
    if not dart_files:
        raise FileNotFoundError(f'No Dart translation files found at {directory}')
    locales = {}
    for source in dart_files:
        translations: dict[str, str] = {}
        for line in source.read_text(encoding='utf-8-sig').splitlines():
            match = ENTRY.match(line)
            if match:
                key = unescape_dart(match.group('key'))
                if key in TEXTURE or key in STRUCTURE:
                    value = unescape_dart(match.group('value')).strip()
                    if value:
                        translations[key] = value
        missing = [key for key in (*TEXTURE, *STRUCTURE) if key not in translations]
        if missing:
            LOG.warning('%s: missing %s', source.name, ', '.join(missing))
        locales[source.stem] = {
            field: {key: translations[key] for key in keys if key in translations}
            for field, keys in KEYS.items()
        }
    return catalog_from_locales(locales, overrides)


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
            if norm(raw) not in ('', 'none', 'null', 'nan'):
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
    ap.add_argument('--overrides', type=Path, default=ROOT/'data/app_translations/soil_classification_overrides.json',
                    help='Reviewed forward corrections and synonyms; never generated automatically')
    ap.add_argument('--canonical-csv', type=Path, default=ROOT/'data/canonical/samples.csv')
    ap.add_argument('--no-postgres', action='store_true')
    ap.add_argument('--no-local-canonical', action='store_true')
    ap.add_argument('--no-json', action='store_true')
    ap.add_argument('--upload-latest', action='store_true', help='Overwrite ONLY latest MinIO samples.csv/all.zip')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--unknown-report', type=Path, default=None,
                    help='Optional CSV of all unrecognized labels and counts (also written during dry-run)')
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    load_env(ROOT/'.env')
    overrides = load_overrides(args.overrides)
    try:
        catalog, reverse, ambiguous = build_catalog(args.translations, overrides)
        LOG.info('Parsed Dart translations from %s', args.translations)
    except FileNotFoundError:
        # Production checkout may not include Dart files. Rebuild reverse from
        # published locale strings + overrides instead of trusting stale keys.
        if not args.json_output.is_file():
            raise
        existing = json.loads(args.json_output.read_text(encoding='utf-8'))
        if existing.get('schema_version') != 1 or not existing.get('locales'):
            raise ValueError(f'Invalid static translations catalog: {args.json_output}')
        catalog, reverse, ambiguous = catalog_from_locales(existing['locales'], overrides)
        LOG.info('No Dart files; rebuilt mapping from %s', args.json_output)
    LOG.info('Available %d locales, %d texture keys and %d structure keys',
             len(catalog['locales']), len(TEXTURE), len(STRUCTURE))
    stats = Counter()
    unknown = {'texture': Counter(), 'structure': Counter()}
    if not args.no_json and not args.dry_run:
        payload = json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=True)+'\n'
        if not args.json_output.exists() or args.json_output.read_text(encoding='utf-8') != payload:
            atomic_write_text(args.json_output, payload)
            LOG.info('Wrote map translations: %s', args.json_output)
        else:
            LOG.info('Map translations already up-to-date: %s', args.json_output)
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
    if args.unknown_report is not None:
        from io import StringIO
        report = StringIO(newline='')
        writer = csv.writer(report, lineterminator='\n')
        writer.writerow(['field', 'unrecognized_value', 'count'])
        for field in KEYS:
            for raw, count in unknown[field].most_common():
                writer.writerow([field, raw, count])
        atomic_write_text(args.unknown_report, report.getvalue())
        LOG.info('Wrote unknown-label report: %s', args.unknown_report)
    return 0

if __name__ == '__main__':
    try: sys.exit(main())
    except Exception:
        LOG.exception('Soil classification normalization failed')
        sys.exit(1)
