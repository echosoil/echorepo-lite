#!/usr/bin/env python3
"""Backfill or explicitly replace English free text, never soil categories."""

import argparse
import csv
import os
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2 import sql

from tools.retranslation_journal import RetranslationJournal

FIELDS = (
    ("samples", ("sample_id",), "contamination_other"),
    ("samples", ("sample_id",), "observations"),
    ("samples", ("sample_id",), "metals_info"),
    ("sample_images", ("sample_id", "image_id"), "image_description"),
)


# Ordered language priors, not assertions about individual participants.
# Include multilingual countries; the first entry is the default when evidence is weak.
COUNTRY_LANGUAGES = {
    "AT": ("de",),
    "BE": ("nl", "fr", "de"),
    "BG": ("bg",),
    "CH": ("de", "fr", "it", "rm"),
    "CY": ("el", "tr"),
    "CZ": ("cs",),
    "DE": ("de",),
    "DK": ("da",),
    "EE": ("et",),
    "EL": ("el",),
    "ES": ("es", "ca", "eu", "gl"),
    "FI": ("fi", "sv"),
    "FR": ("fr",),
    "GB": ("en", "cy"),
    "GR": ("el",),
    "HR": ("hr",),
    "HU": ("hu",),
    "IE": ("en", "ga"),
    "IS": ("is",),
    "IT": ("it",),
    "LI": ("de",),
    "LT": ("lt",),
    "LU": ("lb", "fr", "de"),
    "LV": ("lv",),
    "MT": ("mt", "en"),
    "NL": ("nl",),
    "NO": ("nb", "nn", "no"),
    "PL": ("pl",),
    "PT": ("pt",),
    "RO": ("ro",),
    "RU": ("ru",),
    "SE": ("sv",),
    "SI": ("sl",),
    "SK": ("sk",),
    "TR": ("tr",),
    "UA": ("uk",),
    "UK": ("en", "cy"),
}
STRATEGIES = ("auto", "country", "hybrid")


def country_key(country):
    return (country or "").strip().upper()


def available_sources(session, endpoint, timeout):
    response = session.get(endpoint + "/languages", timeout=timeout)
    response.raise_for_status()
    rows = response.json()
    if not isinstance(rows, list):
        raise ValueError("Invalid /languages response")
    if not any(row.get("code") == "en" for row in rows):
        raise ValueError("English model unavailable")
    return frozenset(
        row["code"]
        for row in rows
        if row.get("code") == "en" or "en" in (row.get("targets") or [])
    )


def detect_languages(session, endpoint, source, timeout):
    response = session.post(endpoint + "/detect", json={"q": source}, timeout=timeout)
    response.raise_for_status()
    rows = response.json()
    if not isinstance(rows, list):
        raise ValueError("Invalid /detect response")
    candidates = {}
    for row in rows:
        code = row.get("language")
        confidence = float(row.get("confidence", 0))
        if isinstance(code, str) and 0 <= confidence <= 100:
            candidates[code] = max(candidates.get(code, 0), confidence)
    return sorted(candidates.items(), key=lambda item: (-item[1], item[0]))


@dataclass(frozen=True)
class LanguageChoice:
    source: str | None
    reason: str
    detected: str = ""
    confidence: float = 0.0


def choose_source(text, country, strategy, supported, candidates=()):
    """Scores are detector heuristics (0–100), not calibrated probabilities."""
    priors = COUNTRY_LANGUAGES.get(country_key(country), ())
    # Do not replace an unavailable primary with an unrelated minority language.
    primary = priors[0] if priors and priors[0] in supported else None
    detected, confidence = candidates[0] if candidates else ("", 0.0)
    second = candidates[1][1] if len(candidates) > 1 else 0.0
    margin = confidence - second
    short = len(text.split()) <= 3 or sum(ch.isalpha() for ch in text) < 15

    def choice(source, reason):
        return LanguageChoice(source, reason, detected, confidence)

    if strategy == "auto":
        return choice("auto", "auto")
    if strategy == "country" and primary:
        return choice(primary, "forced_country")
    # Strong contrary evidence overrides the prior; stronger for short strings.
    threshold = 95 if short else 85
    strong = bool(detected) and confidence >= threshold and margin >= 15
    if strong:
        if detected not in supported:
            return choice(None, "unsupported_detected")
        return choice(detected, "strong_detection")
    # A different national language can win on slightly weaker evidence.
    if (
        detected in priors
        and detected != primary
        and confidence >= (85 if short else 75)
        and margin >= 15
    ):
        if detected not in supported:
            return choice(None, "unsupported_detected")
        return choice(detected, "multilingual_detection")
    if primary and primary != "en":
        return choice(primary, "country_prior")
    # An English country alone does not prove that an unchanged string is English.
    if primary == "en" and detected == "en" and confidence >= 85 and margin >= 15:
        return choice("en", "english_detection")
    if detected and detected not in supported and confidence >= 75 and margin >= 15:
        return choice(None, "unsupported_detected")
    return choice("auto", "no_supported_prior")


def translate_text(session, endpoint, source, timeout, source_language="auto", supported=None):
    response = session.post(
        endpoint + "/translate",
        json={"q": source, "source": source_language, "target": "en", "format": "text"},
        timeout=timeout,
    )
    response.raise_for_status()
    result = response.json()
    candidate = result.get("translatedText")
    detected = (result.get("detectedLanguage") or {}).get("language")
    if source_language == "auto" and supported is not None and detected and detected not in supported:
        return None
    if not isinstance(candidate, str) or not candidate.strip():
        raise ValueError("Empty translation")
    candidate = candidate.strip()
    if len(candidate) > max(200, 10 * len(source)):
        raise ValueError("Implausibly long translation")
    if candidate == source and source_language != "en" and detected != "en":
        return None
    return candidate


class Translator:
    """Cache detection and country-aware decisions; never cache failed HTTP calls."""

    def __init__(self, session, endpoint, timeout, supported):
        self.session = session
        self.endpoint = endpoint
        self.timeout = timeout
        self.supported = supported
        self.detections = {}
        self.results = {}
        self.translations = {}

    def translate(self, text, country, strategy):
        key = (text, country_key(country), strategy)
        if key in self.results:
            return self.results[key]
        candidates = ()
        priors = COUNTRY_LANGUAGES.get(country_key(country), ())
        forced = strategy == "country" and priors and priors[0] in self.supported
        if strategy != "auto" and not forced:
            if text not in self.detections:
                self.detections[text] = detect_languages(
                    self.session, self.endpoint, text, self.timeout
                )
            candidates = self.detections[text]
        decision = choose_source(text, country, strategy, self.supported, candidates)
        translated = None
        if decision.source == "en":
            translated = text
        elif decision.source:
            translation_key = (text, decision.source)
            if translation_key not in self.translations:
                self.translations[translation_key] = translate_text(
                    self.session,
                    self.endpoint,
                    text,
                    self.timeout,
                    source_language=decision.source,
                    supported=self.supported,
                )
            translated = self.translations[translation_key]
        self.results[key] = (translated, decision)
        return self.results[key]


def comparison_rows(conn, per_country, countries):
    # All source strings (including already translated ones), deduplicated across fields.
    parts = []
    for table, _, field in FIELDS:
        orig = sql.Identifier(field + "_orig")
        parts.append(
            sql.SQL(
                "SELECT upper(btrim(coalesce(country_code, ''))) AS country, "
                "btrim({orig}) AS original, {field} AS field FROM {table} "
                "WHERE NULLIF(btrim({orig}), '') IS NOT NULL "
                "AND lower(btrim({orig})) NOT IN ('nan', 'none', 'null', '<na>')"
            ).format(orig=orig, field=sql.Literal(field), table=sql.Identifier(table))
        )
    query = sql.SQL("""
        WITH sources AS ({sources}), unique_texts AS (
            SELECT country, original, string_agg(DISTINCT field, ', ' ORDER BY field) AS fields
            FROM sources WHERE (%s OR country = ANY(%s::text[]))
            GROUP BY country, original
        ), ranked AS (
            SELECT *, row_number() OVER (
                PARTITION BY country ORDER BY md5(original), original
            ) AS rn FROM unique_texts
        )
        SELECT country, original, fields FROM ranked WHERE rn <= %s
        ORDER BY country, rn
    """).format(sources=sql.SQL(" UNION ALL ").join(parts))
    with conn.cursor() as cur:
        cur.execute(query, (countries is None, countries or [], per_country))
        rows = cur.fetchall()
    conn.commit()
    return rows


def spreadsheet_safe(value):
    """Prevent spreadsheet formula interpretation of participant text."""
    value = str(value or "")
    if value.lstrip().startswith(("=", "+", "-", "@")) or value.startswith(("\t", "\r", "\n")):
        return "'" + value
    return value


def write_comparison(conn, translator, path, per_country, countries):
    rows = comparison_rows(conn, per_country, countries)
    # Original observations may contain sensitive text. Never include IDs or emails.
    # Exclusive creation prevents overwriting files; 0600 limits local access.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    stats = Counter()
    with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "country_code",
                "fields",
                "original",
                "auto_en",
                "country_en",
                "hybrid_en",
                "hybrid_source",
                "hybrid_reason",
                "detected_language",
                "confidence_pct",
                "auto_status",
                "country_status",
                "hybrid_status",
            ]
        )
        for country, original, fields in rows:
            results = {}
            statuses = {}
            hybrid = LanguageChoice(None, "error")
            for strategy in STRATEGIES:
                try:
                    translation, decision = translator.translate(original, country, strategy)
                    results[strategy] = translation
                    statuses[strategy] = "ok" if translation is not None else "unconfirmed"
                    if strategy == "hybrid":
                        hybrid = decision
                except (requests.RequestException, ValueError, TypeError, AttributeError):
                    results[strategy] = None
                    statuses[strategy] = "error"
                stats[statuses[strategy]] += 1
            writer.writerow(
                [
                    spreadsheet_safe(country),
                    fields,
                    spreadsheet_safe(original),
                    *(spreadsheet_safe(results[s]) for s in STRATEGIES),
                    spreadsheet_safe(hybrid.source),
                    hybrid.reason,
                    spreadsheet_safe(hybrid.detected),
                    hybrid.confidence,
                    *(statuses[s] for s in STRATEGIES),
                ]
            )
            if sum(stats.values()) % 150 == 0:
                print(f"Comparison: {sum(stats.values()) // 3}/{len(rows)} rows", flush=True)
    print(f"Comparison: {len(rows)} rows; {dict(stats)}; saved to {path}", flush=True)
    return 1 if stats["error"] else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--db-host", default="127.0.0.1")
    parser.add_argument("--db-port", type=int, required=True)
    parser.add_argument("--endpoint", default="http://127.0.0.1:5001")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--compare-output", type=Path, help="Read-only comparison CSV (new file)")
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--retranslate-all", action="store_true", help="Include existing English values")
    scope.add_argument("--retranslate-country", action="append", help="Repeat for selected countries")
    parser.add_argument("--backup-output", type=Path, help="Private write-ahead CSV journal")
    parser.add_argument("--resume", action="store_true", help="Resume using the existing backup CSV")
    parser.add_argument("--language-strategy", choices=STRATEGIES, default="hybrid")
    parser.add_argument("--compare-per-country", type=int, default=50)
    parser.add_argument("--compare-country", action="append", help="Repeat for selected countries")
    parser.add_argument("--preflight", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--limit", type=int, help="Maximum candidates per field")
    parser.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()
    retranslate = bool(args.retranslate_all or args.retranslate_country)
    countries = sorted({country_key(code) for code in (args.retranslate_country or [])})
    if any(len(code) != 2 or not code.isascii() or not code.isalpha() for code in countries):
        parser.error("--retranslate-country requires two-letter country codes")
    if retranslate and args.compare_output:
        parser.error("Retranslation and comparison are separate modes")
    if args.apply and retranslate and not args.backup_output:
        parser.error("Retranslation writes require --backup-output")
    if args.backup_output and (not retranslate or not args.apply):
        parser.error("--backup-output requires --apply and a retranslation scope")
    if args.resume and not args.backup_output:
        parser.error("--resume requires --backup-output")
    if args.backup_output:
        if not args.backup_output.parent.is_dir():
            parser.error("Backup output directory does not exist")
        if args.backup_output.exists() != args.resume:
            parser.error("Use a new backup path, or --resume with an existing backup")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be positive")
    if args.compare_per_country < 1:
        parser.error("--compare-per-country must be positive")
    if args.compare_output and args.compare_output.exists():
        parser.error("Comparison output already exists")
    if args.compare_output and not args.compare_output.parent.is_dir():
        parser.error("Comparison output directory does not exist")
    if args.compare_country and not args.compare_output:
        parser.error("--compare-country requires --compare-output")
    if args.compare_output and args.limit is not None:
        parser.error("Use --compare-per-country rather than --limit for comparison")
    if not Path(args.env_file).is_file():
        parser.error("Environment file not found")
    load_dotenv(args.env_file, override=True)
    password = os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
    if not password:
        parser.error("DB_PASSWORD or POSTGRES_PASSWORD is required")
    name = os.getenv("DB_NAME", "echorepo")
    user = os.getenv("DB_USER", "echorepo")
    print(
        f"DB: {args.db_host}:{args.db_port}/{name}; "
        f"apply={args.apply and not args.preflight}; strategy={args.language_strategy}",
        flush=True,
    )
    endpoint = args.endpoint.rstrip("/")
    apply = args.apply and not args.preflight
    compare = None if args.preflight else args.compare_output
    totals = Counter()
    conn = psycopg2.connect(
        host=args.db_host, port=args.db_port, dbname=name, user=user, password=password
    )
    session = requests.Session()
    session.trust_env = False
    journal = None
    try:
        conn.set_session(readonly=not apply)
        if apply or compare:
            supported = available_sources(session, endpoint, args.timeout)
            print("English source models:", ", ".join(sorted(supported)), flush=True)
            probe = translate_text(
                session, endpoint, "Bonjour tout le monde", args.timeout,
                source_language="fr" if "fr" in supported else "auto", supported=supported,
            )
            if not probe or probe == "Bonjour tout le monde":
                raise RuntimeError("LibreTranslate readiness probe failed")
            translator = Translator(session, endpoint, args.timeout, supported)
        if compare:
            countries = (
                [country_key(code) for code in args.compare_country]
                if args.compare_country
                else None
            )
            return write_comparison(
                conn, translator, compare, args.compare_per_country, countries
            )
        # Count and write modes exclude protected values once the migration is deployed.
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('manual_en_overrides') IS NOT NULL")
            manual_protection = cur.fetchone()[0] is True
        conn.commit()
        if retranslate:
            print(
                f"Retranslation scope: {','.join(countries) or 'ALL'}; "
                "includes existing and missing English values",
                flush=True,
            )
        if apply and retranslate:
            journal = RetranslationJournal(
                args.backup_output,
                {
                    "version": 1,
                    "host": args.db_host,
                    "port": args.db_port,
                    "database": name,
                    "user": user,
                    "strategy": args.language_strategy,
                    "countries": countries,
                },
                resume=args.resume,
            )
            print(f"Recovery journal: {args.backup_output}", flush=True)
        for table, keys, field in FIELDS:
            table_id = sql.Identifier(table)
            orig, en = map(sql.Identifier, (field + "_orig", field + "_en"))
            key_ids = list(map(sql.Identifier, keys))
            key_list = sql.SQL(", ").join(key_ids)
            missing = sql.SQL(
                "NULLIF(btrim({orig}), '') IS NOT NULL "
                "AND lower(btrim({orig})) NOT IN ('nan', 'none', 'null', '<na>')"
            ).format(orig=orig)
            params = []
            if not retranslate:
                missing += sql.SQL(" AND NULLIF(btrim({}), '') IS NULL").format(en)
            if retranslate and countries:
                missing += sql.SQL(" AND upper(btrim(country_code)) = ANY(%s)")
                params.append(countries)
            if manual_protection:
                missing += sql.SQL(
                    " AND NOT EXISTS (SELECT 1 FROM manual_en_overrides m "
                    "WHERE m.field = {field} AND m.country = upper(btrim(coalesce({table}.country_code, ''))) "
                    "AND m.original_hash = md5({table}.{orig}) AND m.original = {table}.{orig})"
                ).format(field=sql.Literal(field), table=table_id, orig=orig)
            with conn.cursor() as cur:
                count_query = sql.SQL("SELECT count(*) FROM {} WHERE {}").format(table_id, missing)
                cur.execute(count_query, params)
                count = cur.fetchone()[0]
                label = "eligible" if retranslate else "missing"
                print(f"{table}.{field}_en: {count} {label}", flush=True)
                totals[label] += count
                if not apply or not count:
                    conn.commit()
                    continue
                query = sql.SQL(
                    "SELECT {keys}, {orig}, country_code, {en} FROM {table} "
                    "WHERE {missing} ORDER BY {keys}"
                ).format(keys=key_list, orig=orig, en=en, table=table_id, missing=missing)
                if args.limit is not None and not retranslate:
                    cur.execute(query + sql.SQL(" LIMIT %s"), [*params, args.limit])
                else:
                    cur.execute(query, params)
                rows = cur.fetchall()
            conn.commit()
            where_keys = sql.SQL(" AND ").join(sql.SQL("{} = %s").format(key) for key in key_ids)
            update = sql.SQL(
                "UPDATE {table} SET {en} = %s WHERE {keys} "
                "AND {orig} IS NOT DISTINCT FROM %s "
                "AND country_code IS NOT DISTINCT FROM %s "
                "AND {en} IS NOT DISTINCT FROM %s"
            ).format(table=table_id, en=en, keys=where_keys, orig=orig)
            if manual_protection:
                update += sql.SQL(
                    " AND NOT EXISTS (SELECT 1 FROM manual_en_overrides m "
                    "WHERE m.field = {field} AND m.country = upper(btrim(coalesce({table}.country_code, ''))) "
                    "AND m.original_hash = md5({table}.{orig}) AND m.original = {table}.{orig}) "
                    "RETURNING {en}"
                ).format(field=sql.Literal(field), table=table_id, orig=orig, en=en)
            stats = Counter()
            errors = 0
            attempted = 0
            for row in rows:
                ids, raw, country, previous = row[:-3], row[-3], row[-2], row[-1]
                if journal:
                    status = journal.resume_status(table, field, ids, raw, country, previous)
                    if status != "retry":
                        stats["already_completed" if status == "done" else "resume_conflict"] += 1
                        continue
                if retranslate and args.limit is not None and attempted >= args.limit:
                    break
                attempted += 1
                if attempted % 50 == 0:
                    print(f"Attempted {attempted}/{len(rows)}: {dict(stats)}", flush=True)
                source = raw.strip()
                try:
                    translated, decision = translator.translate(
                        source, country, args.language_strategy
                    )
                    errors = 0
                except (requests.RequestException, ValueError, TypeError, AttributeError) as exc:
                    stats["failed"] += 1
                    errors += 1
                    print(f"Translation failed ({type(exc).__name__}); previous value retained", flush=True)
                    if errors >= 5:
                        raise RuntimeError("Five consecutive failures; prior commits retained") from None
                    continue
                totals["source:" + (decision.source or "unsupported")] += 1
                totals["decision:" + decision.reason] += 1
                if translated is None:
                    stats["unsupported" if decision.source is None else "unchanged_unconfirmed"] += 1
                    continue
                if journal and translated == previous:
                    journal.record("unchanged", table, field, ids, raw, country, previous, translated)
                    stats["unchanged"] += 1
                    continue
                if journal:
                    # Persist the exact previous and proposed value before touching PostgreSQL.
                    journal.record("pending", table, field, ids, raw, country, previous, translated)
                with conn.cursor() as cur:
                    cur.execute(update, (translated, *ids, raw, country, previous))
                    changed = bool(cur.rowcount)
                    if changed and manual_protection:
                        changed = cur.fetchone()[0] == translated
                conn.commit()
                if changed and journal:
                    journal.record("committed", table, field, ids, raw, country, previous, translated)
                stats["updated" if changed else "changed_since_read"] += 1
            print(dict(stats), flush=True)
            totals.update(stats)
        print("TOTAL:", dict(totals), flush=True)
        conflicts = totals["resume_conflict"] + totals["changed_since_read"]
        return 1 if totals["failed"] or (retranslate and conflicts) else 0
    finally:
        try:
            if journal is not None:
                journal.close()
        finally:
            conn.close()
            session.close()


if __name__ == "__main__":
    raise SystemExit(main())
