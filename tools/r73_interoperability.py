#!/usr/bin/env python3
"""Bounded public interoperability evidence; no credentials, writes to services or live exports."""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import re
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import requests

MIB = 1024 * 1024
CANONICAL = ("samples.csv", "sample_images.csv", "sample_parameters.csv",
             "sample_biodiversity.csv", "parameter_definitions.csv", "analysis_methods.csv")
BIO = ("biodiversity_16S.csv", "biodiversity_ITS.csv", "biodiversity_taxonomy.csv")
RECORDS = {"23784504": (*CANONICAL, "file.json"),
           "22014977": ("biodiversity_raw.zip", "file.json")}
HOSTS = {"zenodo.org", "doi.org", "api.datacite.org", "echorepo.quanta-labs.com"}
ACK = "yes-public-readonly"


def observation(id_, title, status, evidence):
    return {"id": id_, "title": title, "status": status, "evidence": evidence}


def result(id_, title, ok, evidence):
    return observation(id_, title, "PASS" if ok else "REVIEW", evidence)


def utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha(data):
    return hashlib.sha256(data).hexdigest()


def json_doc(data):
    doc = json.loads(data.decode("utf-8-sig"))
    if not isinstance(doc, dict):
        raise ValueError("Expected JSON object")
    return doc


class PublicReader:
    def __init__(self):
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers["User-Agent"] = "ECHOREPO-R73-interoperability/1.0"
        self.total = 0
        self.last = 0.0

    def get(self, url, cap):
        deadline = time.monotonic() + 120
        for _ in range(4):
            parsed = urlsplit(url)
            if (parsed.scheme != "https" or parsed.hostname not in HOSTS
                    or parsed.port not in (None, 443) or parsed.username or parsed.password):
                raise ValueError("Unapproved HTTPS destination")
            if time.monotonic() > deadline:
                raise ValueError("Request budget exceeded")
            time.sleep(max(0, 1 - (time.monotonic() - self.last)))
            self.last = time.monotonic()
            self.session.cookies.clear()
            with self.session.get(url, stream=True, timeout=(8, 20), allow_redirects=False) as resp:
                if resp.status_code in (301, 302, 303, 307, 308):
                    url = urljoin(url, resp.headers["Location"])
                    continue
                resp.raise_for_status()
                if resp.status_code != 200:
                    raise ValueError("Unexpected response status")
                data = bytearray()
                for chunk in resp.iter_content(64 * 1024):
                    self.total += len(chunk)
                    data.extend(chunk)
                    if len(data) > cap or self.total > 80 * MIB or time.monotonic() > deadline:
                        raise ValueError("Public download budget exceeded")
                return bytes(data), url
        raise ValueError("Redirect limit exceeded")


def file_inventory(doc, record_id, expected):
    md = doc.get("metadata") or {}
    files = doc.get("files")
    if not isinstance(files, list):
        raise ValueError("No public file list")
    by_name = {f["key"]: f for f in files if isinstance(f, dict) and isinstance(f.get("key"), str)}
    identity = str(doc.get("id")) == record_id and (doc.get("doi") or md.get("doi")) == f"10.5281/zenodo.{record_id}"
    is_open = md.get("access_right") == "open" or (doc.get("access") or {}).get("files") == "public"
    license_value = md.get("license")
    license_id = license_value.get("id") if isinstance(license_value, dict) else license_value
    exact = set(by_name) == set(expected) and len(files) == len(by_name)
    evidence = {"record_id": record_id, "doi": f"10.5281/zenodo.{record_id}",
                "identity_matches": identity, "public_files": is_open,
                "expected_inventory_matches": exact, "license_present": bool(license_id),
                "version": str(md.get("version") or "")[:40],
                "file_count": len(files)}
    return by_name, evidence


def file_integrity(data, entry):
    expected = str(entry.get("checksum", ""))
    size_match = entry.get("size") == len(data)
    algorithm, _, value = expected.partition(":")
    if algorithm == "md5":
        actual = hashlib.md5(data, usedforsecurity=False).hexdigest()
    elif algorithm == "sha256":
        actual = sha(data)
    else:
        actual = None
    return {"bytes": len(data), "sha256": sha(data), "size_matches": size_match,
            "published_checksum_matches": actual is not None and actual == value.lower()}


def bio_members(data):
    # No extraction to disk; reject duplicate, unexpected, or oversized members.
    expected = set(BIO) | {"biodiversity_metadata.json"}
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        infos = z.infolist()
        if (len(infos) != len(expected) or set(z.namelist()) != expected
                or sum(x.file_size for x in infos) > 160 * MIB
                or any(x.file_size > 100 * MIB or x.is_dir() or x.flag_bits & 1 for x in infos)):
            raise ValueError("Unexpected archive inventory or uncompressed size")
        return {name: z.read(name) for name in BIO}, json_doc(z.read("biodiversity_metadata.json"))


def table_evidence(metadata, resources):
    tables = metadata.get("tables")
    if not isinstance(tables, list) or not all(isinstance(t, dict) for t in tables):
        raise ValueError("Expected CSVW table group")
    by_name = {t.get("url"): t for t in tables if isinstance(t.get("url"), str)}
    context = metadata.get("@context", [])
    context = [context] if isinstance(context, str) else context
    inventory = len(tables) == len(by_name) and set(by_name) == set(resources)
    csvw_context = isinstance(context, list) and any(x in ("http://www.w3.org/ns/csvw", "https://www.w3.org/ns/csvw") for x in context if isinstance(x, str))
    per_table = {}
    for name, data in resources.items():
        t = by_name.get(name, {})
        cols = (t.get("tableSchema") or {}).get("columns", [])
        if not isinstance(cols, list) or not all(isinstance(c, dict) for c in cols):
            raise ValueError("Invalid column metadata")
        # Published ECHO resources use comma and UTF-8; no external context resolution.
        reader = csv.reader(io.StringIO(data.decode("utf-8-sig"), newline=""), strict=True)
        header = next(reader, [])
        n = bad = 0
        for row in reader:
            n += 1
            bad += len(row) != len(header)
        props = [c["propertyUrl"] for c in cols if "propertyUrl" in c]
        scalar = all(isinstance(x, str) and bool(x.strip()) for x in props)
        absolute = sum(isinstance(x, str) and x.startswith(("http://", "https://")) for x in props)
        eusoil = sum(isinstance(x, str) and "eusoilvoc" in x.lower() for x in props)
        checks = {"header_matches": bool(header) and header == [c.get("name") for c in cols],
                  "unique_header": len(header) == len(set(header)),
                  "row_widths_match": bad == 0,
                  "sha256_matches": t.get("schema:sha256") == sha(data),
                  "row_count_matches": t.get("schema:numberOfItems") == n,
                  "byte_size_matches": t.get("schema:contentSize") == len(data),
                  "scalar_property_urls": scalar}
        per_table[name] = {"rows": n, "columns": len(header), "bad_width_rows": bad,
                           "property_url_columns": len(props), "absolute_http_property_urls": absolute,
                           "eusoilvoc_property_urls": eusoil, "checks": checks}
    ok = inventory and csvw_context and all(all(t["checks"].values()) for t in per_table.values())
    return ok, {"table_inventory_matches": inventory, "csvw_context_present": csvw_context,
                "tables": per_table, "note": "Selected structural and declared-integrity checks; not full CSVW or vocabulary validation."}


def loess_evidence(data):
    doc = json_doc(data)
    features = doc.get("features")
    if not isinstance(features, list):
        raise ValueError("Missing features")
    ids = set()
    invalid = duplicate = photos = 0
    for f in features:
        if not isinstance(f, dict):
            invalid += 1
            continue
        ident = f.get("id")
        if not isinstance(ident, (str, int)) or not str(ident).strip():
            invalid += 1
        elif str(ident) in ids:
            duplicate += 1
        else:
            ids.add(str(ident))
        geometry = f.get("geometry") or {}
        c = geometry.get("coordinates")
        if (f.get("type") != "Feature" or geometry.get("type") != "Point"
                or not isinstance(c, list) or len(c) < 2
                or not all(isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) for v in c[:2])
                or not -180 <= c[0] <= 180 or not -90 <= c[1] <= 90):
            invalid += 1
        photos += bool((f.get("properties") or {}).get("image_url"))
    metadata = doc.get("metadata") or {}
    source = metadata.get("source")
    source_ok = isinstance(source, str) and urlsplit(source).hostname in {"www.loess-soil-map.eu", "loess-soil-map.eu"}
    stamp = metadata.get("generated_at")
    try:
        generated = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        timestamp = generated.isoformat() if generated.tzinfo else None
    except (ValueError, TypeError, AttributeError):
        timestamp = None
    ok = doc.get("type") == "FeatureCollection" and bool(features) and not invalid and not duplicate and source_ok
    return ok, {"features": len(features), "invalid_observations": invalid, "duplicate_ids": duplicate,
                "with_photo_url": photos, "source_matches": source_ok, "generated_at": timestamp,
                "sha256": sha(data), "note": "Checks served overlay structure, not upstream equality, freshness or browser rendering."}


def run(reader, downloads):
    checks = []
    for record_id, expected in RECORDS.items():
        prefix = "CAN" if record_id == "23784504" else "BIO"
        doi = f"10.5281/zenodo.{record_id}"
        try:
            _, final_url = reader.get(f"https://doi.org/{doi}", 4 * MIB)
            resolved = urlsplit(final_url)
            checks.append(result(prefix + "-DOI", "Version DOI resolution",
                                 resolved.hostname == "zenodo.org" and resolved.path.rstrip("/") in (f"/records/{record_id}", f"/record/{record_id}"),
                                 {"doi": doi, "expected_landing_record": resolved.hostname == "zenodo.org" and resolved.path.rstrip("/") in (f"/records/{record_id}", f"/record/{record_id}")}))
        except (requests.RequestException, ValueError, KeyError):
            checks.append(observation(prefix + "-DOI", "Version DOI resolution", "NOT_ASSESSED", {"reason": "Request blocked, failed or exceeded budget"}))
        try:
            raw, _ = reader.get(f"https://zenodo.org/api/records/{record_id}", 4 * MIB)
            entries, info = file_inventory(json_doc(raw), record_id, expected)
            ok = all(info[k] for k in ("identity_matches", "public_files", "expected_inventory_matches", "license_present"))
            checks.append(result(prefix + "-REC", "Public repository record and inventory", ok, info))
        except (requests.RequestException, ValueError, KeyError, TypeError, AttributeError):
            checks.append(observation(prefix + "-REC", "Public repository record and inventory", "NOT_ASSESSED", {"reason": "Record API unavailable or unexpected format"}))
            entries = None
        if not downloads:
            checks.append(observation(prefix + "-FILES", "Published file integrity and metadata consistency", "NOT_ASSESSED", {"reason": "Run with --download-public-files"}))
            continue
        try:
            files = {}
            integrity = {}
            for name in expected:
                data, _ = reader.get(f"https://zenodo.org/records/{record_id}/files/{name}?download=1", 32 * MIB)
                files[name] = data
                integrity[name] = file_integrity(data, (entries or {}).get(name, {}))
            verified = entries is not None and all(x["size_matches"] and x["published_checksum_matches"] for x in integrity.values())
            checks.append(observation(prefix + "-FILES", "Published resource checksums",
                                      "PASS" if verified else ("NOT_ASSESSED" if entries is None else "REVIEW"), integrity))
            metadata = json_doc(files["file.json"])
            if prefix == "BIO":
                resources, _ = bio_members(files["biodiversity_raw.zip"])
            else:
                resources = {name: files[name] for name in CANONICAL}
            ok, evidence = table_evidence(metadata, resources)
            evidence["metadata_sha256"] = sha(files["file.json"])
            checks.append(result(prefix + "-CSVW", "CSVW and exact published CSV consistency", ok, evidence))
        except (requests.RequestException, ValueError, KeyError, TypeError, AttributeError, csv.Error, zipfile.BadZipFile, RuntimeError, OverflowError):
            checks.append(observation(prefix + "-DATA", "Published resource / CSVW assessment", "NOT_ASSESSED", {"reason": "Download, archive or metadata assessment incomplete"}))
    try:
        data, _ = reader.get("https://echorepo.quanta-labs.com/static/geo/loess_surveys.geojson", 8 * MIB)
        ok, evidence = loess_evidence(data)
        checks.append(result("LOESS01", "Public ECHOREPO LOESS overlay resource", ok, evidence))
    except (requests.RequestException, ValueError, TypeError, AttributeError, KeyError, OverflowError):
        checks.append(observation("LOESS01", "Public ECHOREPO LOESS overlay resource", "NOT_ASSESSED", {"reason": "Resource unavailable or unexpected format"}))
    for id_, title, reason in [
        ("SW01", "SoilWise independent catalogue discovery", "Capture matching DOI and title in browser or catalogue export"),
        ("EDP01", "European Data Portal independent discovery", "Capture matching DOI and title in browser or DCAT export"),
        ("EUSO01", "JRC/EUSO record-specific integration", "Need independently attributable record or operator confirmation"),
        ("SEM01", "Downstream interpretation of CSVW semantics", "Need metadata provenance, mapping or independent consumer test"),
        ("LOESS02", "LOESS browser interaction and attribution", "Operator browser checks required")]:
        checks.append(observation(id_, title, "NOT_ASSESSED", {"reason": reason}))
    return checks


def markdown(report):
    lines = ["# R7-3 — Phase 4 interoperability", "",
             f"- UTC: {report['generated_at_utc']}",
             f"- Reporting commit: {report['report_commit']}",
             "- Scope: pinned public records and served overlay; no live exports or publication.",
             "", "| ID | Verification | Status | Evidence |", "|---|---|---|---|"]
    for c in report["checks"]:
        evidence = json.dumps(c["evidence"], sort_keys=True, ensure_ascii=True).replace("|", r"\|")
        lines.append(f"| {c['id']} | {c['title']} | {c['status']} | {evidence} |")
    lines += ["", "## Limitations", "", *["- " + x for x in report["limitations"]], ""]
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ack", choices=[ACK], required=True)
    p.add_argument("--commit", default="not recorded")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--download-public-files", action="store_true")
    args = p.parse_args()
    reader = PublicReader()
    try:
        checks = run(reader, args.download_public_files)
    finally:
        reader.session.close()
    report = {"kpi": "R7-3", "phase": 4, "generated_at_utc": utc(),
              "report_commit": args.commit, "checks": checks, "downloaded_bytes": reader.total,
              "limitations": ["A public DOI or catalogue result does not prove downstream CSVW processing.",
                              "Pinned published versions are not assumed equal to the live database or Phase 2 snapshot.",
                              "No raw data, row identifiers, credentials, coordinates or image URLs retained.",
                              "NOT_ASSESSED and REVIEW are not passes. No publication, refresh or database operations performed.",
                              "HTTP checks are bounded sequential observations; no formal interoperability certification."]}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    args.out.with_suffix(".md").write_text(markdown(report), encoding="utf-8")
    print("Interoperability observations saved; review all statuses.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
