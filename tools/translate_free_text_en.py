#!/usr/bin/env python3
"""Backfill only missing English free-text fields in ECHOREPO PostgreSQL."""
import argparse
import os
import time
from collections import Counter
from pathlib import Path

import psycopg2
from psycopg2 import sql
import requests
from dotenv import load_dotenv

FIELDS = (
    ('samples', ('sample_id',), 'contamination_other'),
    ('samples', ('sample_id',), 'observations'),
    ('samples', ('sample_id',), 'metals_info'),
    ('sample_images', ('sample_id', 'image_id'), 'image_description'),
)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--env-file', default='.env')
    p.add_argument('--db-host', default='127.0.0.1')
    p.add_argument('--db-port', type=int, required=True)
    p.add_argument('--endpoint', default='http://127.0.0.1:5001')
    p.add_argument('--apply', action='store_true', help='Write successful translations; default is count-only')
    p.add_argument('--limit', type=int, help='Maximum candidates per field (for a pilot)')
    p.add_argument('--timeout', type=int, default=90)
    args = p.parse_args()
    if args.limit is not None and args.limit < 1:
        p.error('--limit must be positive')
    env = Path(args.env_file)
    if not env.is_file():
        p.error(f'Environment file not found: {env}')
    load_dotenv(env)
    name = os.getenv('DB_NAME', 'echorepo')
    user = os.getenv('DB_USER', 'echorepo')
    password = os.getenv('DB_PASSWORD') or os.getenv('POSTGRES_PASSWORD')
    if not password:
        p.error('DB_PASSWORD or POSTGRES_PASSWORD is required')
    print(f'DB: {args.db_host}:{args.db_port}/{name} as {user}; apply={args.apply}', flush=True)
    endpoint = args.endpoint.rstrip('/')
    session = requests.Session()
    cache = {}
    totals = Counter()
    conn = psycopg2.connect(host=args.db_host, port=args.db_port, dbname=name, user=user, password=password)
    try:
        conn.set_session(readonly=not args.apply)
        with conn.cursor() as cur:
            cur.execute('SELECT current_database(), current_user, inet_server_addr(), inet_server_port()')
            print('Connected:', cur.fetchone(), flush=True)
        conn.commit()
        if args.apply:
            r = session.get(endpoint + '/languages', timeout=10)
            r.raise_for_status()
            langs = r.json()
            if not any(x.get('code') == 'en' for x in langs):
                raise RuntimeError('LibreTranslate has no English model')
            r = session.post(endpoint + '/translate', json={'q': 'Bonjour tout le monde', 'source': 'fr', 'target': 'en', 'format': 'text'}, timeout=args.timeout)
            r.raise_for_status()
            probe = r.json().get('translatedText')
            if not isinstance(probe, str) or not probe.strip() or probe.strip() == 'Bonjour tout le monde':
                raise RuntimeError('LibreTranslate French-to-English probe failed')
        for table, keys, field in FIELDS:
            table_id = sql.Identifier(table)
            orig, en = map(sql.Identifier, (field + '_orig', field + '_en'))
            key_ids = list(map(sql.Identifier, keys))
            key_list = sql.SQL(', ').join(key_ids)
            missing = sql.SQL("NULLIF(btrim({orig}), '') IS NOT NULL AND NULLIF(btrim({en}), '') IS NULL").format(orig=orig, en=en)
            with conn.cursor() as cur:
                cur.execute(sql.SQL('SELECT count(*) FROM {} WHERE {}').format(table_id, missing))
                count = cur.fetchone()[0]
                print(f'{table}.{field}_en: {count} missing', flush=True)
                totals['missing'] += count
                if not args.apply or count == 0:
                    conn.commit()
                    continue
                query = sql.SQL('SELECT {keys}, {orig}, country_code FROM {table} WHERE {missing} ORDER BY {keys}').format(keys=key_list, orig=orig, table=table_id, missing=missing)
                if args.limit is not None:
                    cur.execute(query + sql.SQL(' LIMIT %s'), (args.limit,))
                else:
                    cur.execute(query)
                rows = cur.fetchall()
            conn.commit()  # never hold a DB transaction open while calling LibreTranslate
            where_keys = sql.SQL(' AND ').join(sql.SQL('{} = %s').format(k) for k in key_ids)
            update = sql.SQL("UPDATE {table} SET {en} = %s WHERE {keys} AND {orig} IS NOT DISTINCT FROM %s AND country_code IS NOT DISTINCT FROM %s AND NULLIF(btrim({en}), '') IS NULL").format(table=table_id, en=en, keys=where_keys, orig=orig)
            stats = Counter()
            consecutive_errors = 0
            for row in rows:
                ids, raw, country = row[:-2], row[-2], row[-1]
                source = raw.strip()
                if source in cache:
                    translated = cache[source]
                else:
                    try:
                        r = session.post(endpoint + '/translate', json={'q': source, 'source': 'auto', 'target': 'en', 'format': 'text'}, timeout=args.timeout)
                        r.raise_for_status()
                        result = r.json()
                        candidate = result.get('translatedText')
                        detected = (result.get('detectedLanguage') or {}).get('language')
                        if not isinstance(candidate, str) or not candidate.strip():
                            raise ValueError('Empty translation')
                        translated = candidate.strip()
                        if len(translated) > max(200, 10 * len(source)):
                            raise ValueError('Implausibly long translation')
                        consecutive_errors = 0
                        if translated == source and detected != 'en':
                            stats['unchanged_unconfirmed'] += 1
                            continue
                        cache[source] = translated
                    except (requests.RequestException, ValueError, TypeError, AttributeError) as exc:
                        stats['failed'] += 1
                        consecutive_errors += 1
                        # Do not log participant free text or response bodies.
                        print(f'  Translation failed ({type(exc).__name__}); left missing', flush=True)
                        if consecutive_errors >= 5:
                            raise RuntimeError('Five consecutive translation failures; stopping; prior commits retained') from None
                        time.sleep(1)
                        continue
                with conn.cursor() as cur:
                    cur.execute(update, (translated, *ids, raw, country))
                    stats['updated' if cur.rowcount else 'changed_since_read'] += 1
                conn.commit()
                if sum(stats.values()) % 50 == 0:
                    print(f'  {dict(stats)}', flush=True)
            print(f'  {dict(stats)}', flush=True)
            totals.update(stats)
        print('TOTAL:', dict(totals), flush=True)
    finally:
        conn.close()
        session.close()


if __name__ == '__main__':
    main()
