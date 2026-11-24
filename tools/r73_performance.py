#!/usr/bin/env python3
"""Conservative sequential, opt-in HTTP response measurements for R7-3."""
from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import re
import statistics
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import requests

from tools.validate_echorepo_bundle import EXPECTED_SCHEMAS

PROD_HOST = "echorepo.quanta-labs.com"
FIELDS = ("sample_id", "country_code", "lat", "lon")
MIB = 1024 * 1024


def utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def approve_origin(base: str, environment: str, ack: str, allowed_host: str = "") -> str:
    p = urlsplit(base)
    if p.scheme not in {"http", "https"} or not p.hostname or p.username or p.password or p.query or p.fragment or p.path not in {"", "/"}:
        raise ValueError("Supply an origin only, without credentials, path, query or fragment")
    if environment == "production-readonly":
        if ack != "yes-production-readonly" or p.scheme != "https" or p.hostname != PROD_HOST or p.port not in (None, 443):
            raise ValueError("Production requires explicit acknowledgement and the official HTTPS origin")
    elif environment == "staging":
        if ack != "yes-staging-readonly" or p.hostname == PROD_HOST or (p.hostname in {"localhost", "127.0.0.1", "::1"} and p.port == 8001):
            raise ValueError("Staging acknowledgement required; known production target blocked")
        if p.hostname not in {"localhost", "127.0.0.1", "::1"} and (p.hostname != allowed_host or p.scheme != "https"):
            raise ValueError("Remote staging requires explicit host approval and HTTPS")
    else:
        raise ValueError("Unknown environment")
    return f"{p.scheme}://{p.netloc}"


def operations(country: str, public_only: bool, include_zip: bool) -> list[dict]:
    fields = ",".join(FIELDS)
    ops = [
        {"id": "ping", "path": "/api/v1/ping", "params": {}, "auth": False, "kind": "ping", "target_ms": 500},
        {"id": "explore", "path": "/explore", "params": {}, "auth": False, "kind": "explore", "target_ms": 2000},
        {"id": "filtered_search", "path": "/search", "params": {"country_code": country, "ph_min": 5, "ph_max": 8, "page": 1}, "auth": False, "kind": "search", "target_ms": 2000},
    ]
    if not public_only:
        ops += [
            {"id": "sample_count", "path": "/api/v1/canonical/samples/count", "params": {}, "auth": True, "kind": "count", "target_ms": 1000},
            {"id": "samples_page", "path": "/api/v1/canonical/samples", "params": {"format": "json", "limit": 50, "offset": 0, "fields": fields, "order": "sample_id", "dir": "asc"}, "auth": True, "kind": "samples", "target_ms": 2000},
            {"id": "compact_map", "path": "/api/v1/canonical/map.geojson", "params": {"country": country, "limit": 200, "offset": 0, "compact": 1}, "auth": True, "kind": "map", "target_ms": 3000},
            {"id": "bounded_csv", "path": "/api/v1/canonical/samples", "params": {"format": "csv", "limit": 100, "offset": 0, "fields": fields, "order": "sample_id", "dir": "asc"}, "auth": True, "kind": "csv", "target_ms": None},
        ]
    if include_zip:
        ops.append({"id": "filtered_zip", "path": "/api/v1/canonical/all.zip", "params": {"country": country}, "auth": True, "kind": "zip", "target_ms": None, "repetitions": 1})
    return ops


def valid_payload(op: dict, media: str, body: bytes) -> bool:
    kind = op["kind"]
    try:
        if kind == "explore":
            return media == "text/html" and b'id="map"' in body
        if kind == "search":
            return media == "text/html" and b'name="country_code"' in body and b'name="ph_min"' in body
        if kind == "csv":
            if media not in {"text/csv", "application/csv"}:
                return False
            reader = csv.reader(io.StringIO(body.decode("utf-8-sig")))
            return tuple(next(reader, [])) == FIELDS and sum(1 for _ in reader) <= op["params"]["limit"]
        if kind == "zip":
            if media not in {"application/zip", "application/x-zip-compressed", "application/octet-stream"}:
                return False
            with zipfile.ZipFile(io.BytesIO(body)) as archive:
                return sorted(archive.namelist()) == sorted(EXPECTED_SCHEMAS)
        if media not in {"application/json", "application/geo+json"}:
            return False
        obj = json.loads(body)
        if not isinstance(obj, dict):
            return False
        if kind == "ping":
            return obj.get("ok") is True
        if kind == "count":
            return type(obj.get("count")) is int and obj["count"] >= 0
        if kind == "samples":
            meta, rows = obj.get("meta"), obj.get("data")
            return isinstance(meta, dict) and meta.get("limit") == op["params"]["limit"] and isinstance(rows, list) and len(rows) <= op["params"]["limit"] and all(isinstance(r, dict) and set(r) == set(FIELDS) for r in rows)
        if kind == "map":
            features, meta = obj.get("features"), obj.get("meta")
            return obj.get("type") == "FeatureCollection" and isinstance(features, list) and len(features) <= op["params"]["limit"] and isinstance(meta, dict) and meta.get("compact") is True
    except (ValueError, UnicodeError, csv.Error, zipfile.BadZipFile, TypeError):
        return False
    return False


def measure(session, origin: str, op: dict, auth: dict) -> dict:
    start = time.perf_counter()
    status, size, reason = None, 0, "OK"
    headers_ms = None
    cap = (20 if op["kind"] == "zip" else 4) * MIB
    try:
        # Do not let public requests inherit login state; never follow redirects.
        session.cookies.clear()
        with session.get(origin + op["path"], params=op["params"],
                         headers=auth if op["auth"] else {}, timeout=(5, 10),
                         stream=True, allow_redirects=False) as response:
            headers_ms = round((time.perf_counter() - start) * 1000, 2)
            status = response.status_code
            if status != 200:
                reason = "HTTP_STATUS"
            else:
                body = bytearray()
                for chunk in response.iter_content(chunk_size=65536):
                    size += len(chunk)
                    if size > cap:
                        reason = "BODY_CAP"
                        break
                    if time.perf_counter() - start > 20:
                        reason = "TIME_BUDGET"
                        break
                    body.extend(chunk)
                if reason == "OK":
                    media = response.headers.get("Content-Type", "").split(";")[0].strip().lower()
                    if not valid_payload(op, media, body):
                        reason = "UNEXPECTED_PAYLOAD"
    except requests.Timeout:
        reason = "TIMEOUT"
    except requests.RequestException:
        reason = "TRANSPORT_ERROR"
    finally:
        session.cookies.clear()
    elapsed = round((time.perf_counter() - start) * 1000, 2)
    return {"http_status": status, "headers_ms": headers_ms, "elapsed_ms": elapsed,
            "decoded_bytes_read": size, "result": reason}


def summarize(op: dict, planned: int, attempts: list[dict]) -> dict:
    good = [a for a in attempts if a["result"] == "OK"]
    values = sorted(a["elapsed_ms"] for a in good)
    p95 = values[math.ceil(0.95 * len(values)) - 1] if values else None
    median = round(statistics.median(values), 2) if values else None
    target = op["target_ms"]
    complete = len(good) == planned
    target_result = "MEASUREMENT_ONLY" if target is None else "NOT_EVALUABLE" if not complete else "MEETS_PROPOSED_TARGET" if p95 <= target else "ABOVE_PROPOSED_TARGET"
    rates = [
        a["decoded_bytes_read"] / MIB / (a["elapsed_ms"] / 1000)
        for a in good if a["elapsed_ms"] > 0
    ]
    return {**op, "planned": planned, "attempted": len(attempts), "successful": len(good),
            "errors": len(attempts) - len(good), "not_run": planned - len(attempts),
            "error_rate_pct": round(100 * (len(attempts) - len(good)) / len(attempts), 2) if attempts else None,
            "median_ms": median, "p95_ms": p95, "max_ms": max(values) if values else None,
            "median_decoded_bytes": statistics.median(a["decoded_bytes_read"] for a in good) if good else None,
            "median_mib_per_s": round(statistics.median(rates), 3) if rates else None,
            "target_result": target_result, "attempts": attempts}


def run(origin, environment, country, repetitions, delay, auth, public_only, include_zip):
    started = utc()
    results = []
    stop = False
    with requests.Session() as session:
        # Never inherit proxy credentials, .netrc authentication or environment TLS settings.
        session.trust_env = False
        session.verify = True
        session.headers["User-Agent"] = "ECHOREPO-R73-validation/1.0"
        for op in operations(country, public_only, include_zip):
            n = op.get("repetitions", repetitions)
            attempts = []
            for _ in range(n):
                if stop:
                    break
                a = measure(session, origin, op, auth)
                attempts.append(a)
                # Stop immediately on explicit overload and repeated transport failures.
                if a["http_status"] in (429, 503) or (
                    len(attempts) >= 2 and all(x["result"] in {"TIMEOUT", "TRANSPORT_ERROR", "TIME_BUDGET"} for x in attempts[-2:])
                ):
                    stop = True
                time.sleep(delay)
            results.append(summarize(op, n, attempts))
    return {"kpi": "R7-3", "phase": 2, "criterion_ids": ["R73-P01", "R73-P02"],
            "started_at_utc": started, "finished_at_utc": utc(), "origin": origin,
            "environment": environment, "country_filter": country, "delay_seconds": delay,
            "repetitions": repetitions, "public_only": public_only, "include_zip": include_zip,
            "stopped_early": stop, "operations": results,
            "complete_success": all(r["successful"] == r["planned"] for r in results),
            "limitations": [
                "Sequential GETs only; they can create usage analytics. Not a load/stress test or uptime measurement.",
                "Full client-observed elapsed time includes request, transfer, and bounded payload sanity check, not browser rendering or isolated server time.",
                "Successful attempts only enter latency and throughput statistics; failures and unexecuted attempts are retained separately.",
                "p95 uses nearest rank; with small n it is near the maximum, not a stable tail estimate. Connection reused; no controlled cache or warm-up.",
                "Throughput uses decoded bytes received divided by full elapsed time, not compressed wire throughput.",
                "Targets are proposed WP2 evaluation targets, not approved requirements. ZIP export is optional, staging only, and one request.",
                "HTTP read timeout is per socket read; body budget is checked between chunks, not a hard end-to-end server cancellation.",
            ]}


def markdown(r: dict) -> str:
    lines = ["# R7-3 — HTTP performance observations", "",
             f"- UTC: {r['started_at_utc']} to {r['finished_at_utc']}",
             f"- Origin/environment: {r['origin']} ({r['environment']})",
             f"- Reporting commit: {r.get('report_commit', 'not recorded')}",
             f"- Country filter: {r['country_filter']}; spacing: {r['delay_seconds']} s",
             f"- All configured attempts successful: {r['complete_success']}",
             "", "| Operation | Success/planned | Errors | Not run | Median ms | p95 ms | Max ms | Median decoded bytes | MiB/s | Proposed p95 ms | Comparison |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    def val(x):
        return "—" if x is None else str(x)
    for op in r["operations"]:
        fields = [op["id"], f"{op['successful']}/{op['planned']}", op["errors"], op["not_run"],
                  op["median_ms"], op["p95_ms"], op["max_ms"], op["median_decoded_bytes"],
                  op["median_mib_per_s"], op["target_ms"], op["target_result"]]
        lines.append("| " + " | ".join(val(x) for x in fields) + " |")
    lines += ["", "## Limitations", "", *["- " + x for x in r["limitations"]], ""]
    return "\n".join(lines)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--base-url", required=True)
    p.add_argument("--environment", choices=["production-readonly", "staging"], required=True)
    p.add_argument("--ack", required=True)
    p.add_argument("--allowed-host", default="")
    p.add_argument("--country", default="ES")
    p.add_argument("--repetitions", type=int, default=10)
    p.add_argument("--delay", type=float, default=1.0)
    p.add_argument("--public-only", action="store_true")
    p.add_argument("--include-zip", action="store_true")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--commit", default="not recorded")
    args = p.parse_args()
    try:
        origin = approve_origin(args.base_url, args.environment, args.ack, args.allowed_host)
        if not (3 <= args.repetitions <= 20 and math.isfinite(args.delay) and 1 <= args.delay <= 30):
            raise ValueError("Use 3–20 repetitions and 1–30 seconds between requests")
        if not re.fullmatch(r"[A-Z]{2}", args.country):
            raise ValueError("Country must be two uppercase letters")
        if args.include_zip and (args.environment != "staging" or args.public_only):
            raise ValueError("ZIP export requires authenticated staging mode")
        key = os.environ.get("R73_API_KEY", "").strip()
        bearer = os.environ.get("R73_BEARER_TOKEN", "").strip()
        if not args.public_only and (bool(key) == bool(bearer)):
            raise ValueError("Set exactly one of R73_API_KEY or R73_BEARER_TOKEN, or use --public-only")
        if not args.public_only and urlsplit(origin).scheme != "https" and urlsplit(origin).hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("Credentials require HTTPS or local loopback")
    except ValueError as exc:
        # urlsplit may include a user-supplied URL in its error; do not echo raw exceptions.
        p.error("Invalid target, acknowledgement, credentials or workload configuration; see runbook")
    auth = {"X-API-Key": key} if key else {"Authorization": f"Bearer {bearer}"} if bearer else {}
    if args.public_only:
        auth = {}
    result = run(origin, args.environment, args.country, args.repetitions, args.delay,
                 auth, args.public_only, args.include_zip)
    result["report_commit"] = args.commit
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "performance.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    (args.out_dir / "performance.md").write_text(markdown(result), encoding="utf-8")
    for op in result["operations"]:
        print(f"{op['id']}: {op['successful']}/{op['planned']} successful; median={op['median_ms']} ms; p95={op['p95_ms']} ms; {op['target_result']}")
    return int(not result["complete_success"])


if __name__ == "__main__":
    raise SystemExit(main())
