#!/usr/bin/env python3
"""Backfill missing English free text only, without changing soil categories."""

import argparse
import os
from collections import Counter
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2 import sql

FIELDS = (
    ("samples", ("sample_id",), "contamination_other"),
    ("samples", ("sample_id",), "observations"),
    ("samples", ("sample_id",), "metals_info"),
    ("sample_images", ("sample_id", "image_id"), "image_description"),
)


def translate_text(session, endpoint, source, timeout):
    response = session.post(
        endpoint + "/translate",
        json={"q": source, "source": "auto", "target": "en", "format": "text"},
        timeout=timeout,
    )
    response.raise_for_status()
    result = response.json()
    candidate = result.get("translatedText")
    detected = (result.get("detectedLanguage") or {}).get("language")
    if not isinstance(candidate, str) or not candidate.strip():
        raise ValueError("Empty translation")
    candidate = candidate.strip()
    if len(candidate) > max(200, 10 * len(source)):
        raise ValueError("Implausibly long translation")
    if candidate == source and detected != "en":
        return None
    return candidate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--db-host", default="127.0.0.1")
    parser.add_argument("--db-port", type=int, required=True)
    parser.add_argument("--endpoint", default="http://127.0.0.1:5001")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, help="Maximum candidates per field")
    parser.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be positive")
    if not Path(args.env_file).is_file():
        parser.error("Environment file not found")
    load_dotenv(args.env_file, override=True)
    password = os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
    if not password:
        parser.error("DB_PASSWORD or POSTGRES_PASSWORD is required")
    name = os.getenv("DB_NAME", "echorepo")
    user = os.getenv("DB_USER", "echorepo")
    print(f"DB: {args.db_host}:{args.db_port}/{name}; apply={args.apply}", flush=True)
    endpoint = args.endpoint.rstrip("/")
    cache = {}
    totals = Counter()
    conn = psycopg2.connect(
        host=args.db_host, port=args.db_port, dbname=name, user=user, password=password
    )
    session = requests.Session()
    session.trust_env = False
    try:
        conn.set_session(readonly=not args.apply)
        if args.apply:
            probe = translate_text(session, endpoint, "Bonjour tout le monde", args.timeout)
            if not probe or probe == "Bonjour tout le monde":
                raise RuntimeError("LibreTranslate readiness probe failed")
        for table, keys, field in FIELDS:
            table_id = sql.Identifier(table)
            orig, en = map(sql.Identifier, (field + "_orig", field + "_en"))
            key_ids = list(map(sql.Identifier, keys))
            key_list = sql.SQL(", ").join(key_ids)
            missing = sql.SQL(
                "NULLIF(btrim({orig}), '') IS NOT NULL "
                "AND lower(btrim({orig})) NOT IN ('nan', 'none', 'null', '<na>') "
                "AND NULLIF(btrim({en}), '') IS NULL"
            ).format(orig=orig, en=en)
            with conn.cursor() as cur:
                cur.execute(sql.SQL("SELECT count(*) FROM {} WHERE {}").format(table_id, missing))
                count = cur.fetchone()[0]
                print(f"{table}.{field}_en: {count} missing", flush=True)
                totals["missing"] += count
                if not args.apply or not count:
                    conn.commit()
                    continue
                query = sql.SQL(
                    "SELECT {keys}, {orig}, country_code FROM {table} "
                    "WHERE {missing} ORDER BY {keys}"
                ).format(keys=key_list, orig=orig, table=table_id, missing=missing)
                if args.limit is not None:
                    cur.execute(query + sql.SQL(" LIMIT %s"), (args.limit,))
                else:
                    cur.execute(query)
                rows = cur.fetchall()
            conn.commit()
            where_keys = sql.SQL(" AND ").join(
                sql.SQL("{} = %s").format(key) for key in key_ids
            )
            update = sql.SQL(
                "UPDATE {table} SET {en} = %s WHERE {keys} "
                "AND {orig} IS NOT DISTINCT FROM %s "
                "AND country_code IS NOT DISTINCT FROM %s "
                "AND NULLIF(btrim({en}), '') IS NULL"
            ).format(table=table_id, en=en, keys=where_keys, orig=orig)
            stats = Counter()
            errors = 0
            for row in rows:
                ids, raw, country = row[:-2], row[-2], row[-1]
                source = raw.strip()
                if source not in cache:
                    try:
                        cache[source] = translate_text(session, endpoint, source, args.timeout)
                        errors = 0
                    except (requests.RequestException, ValueError, TypeError, AttributeError) as exc:
                        stats["failed"] += 1
                        errors += 1
                        print(f"Translation failed ({type(exc).__name__}); left missing", flush=True)
                        if errors >= 5:
                            raise RuntimeError("Five consecutive failures; prior commits retained") from None
                        continue
                translated = cache[source]
                if translated is None:
                    stats["unchanged_unconfirmed"] += 1
                    continue
                with conn.cursor() as cur:
                    cur.execute(update, (translated, *ids, raw, country))
                    stats["updated" if cur.rowcount else "changed_since_read"] += 1
                conn.commit()
                if sum(stats.values()) % 50 == 0:
                    print(dict(stats), flush=True)
            print(dict(stats), flush=True)
            totals.update(stats)
        print("TOTAL:", dict(totals), flush=True)
        return 1 if totals["failed"] else 0
    finally:
        conn.close()
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
