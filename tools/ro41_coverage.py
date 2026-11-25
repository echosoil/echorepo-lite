#!/usr/bin/env python3
"""Aggregate coverage of three pinned public soil datasets. No raw data retained."""
import argparse
import csv
import io
import json
import math
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import requests

from tools.r73_interoperability import PublicReader, MIB, bio_members, file_integrity, json_doc

RECORDS = {
    "23784504": ("samples.csv",),
    "22014977": ("biodiversity_raw.zip",),
    "24003680": ("ECHO_Subset_validation_samples.csv", "README_ECHO_validation_subset.txt"),
}


def csv_rows(data):
    reader = csv.reader(io.StringIO(data.decode("utf-8-sig"), newline=""), strict=True)
    header = next(reader, [])
    if not header or len(set(header)) != len(header):
        raise ValueError("Invalid CSV header")
    rows = list(reader)
    if any(len(row) != len(header) for row in rows):
        raise ValueError("Inconsistent CSV widths")
    return header, rows


def canonical(data):
    header, rows = csv_rows(data)
    country = header.index("country_code")
    identifier = header.index("sample_id")
    counts = Counter()
    missing = malformed = 0
    for row in rows:
        value = row[country].strip()
        if not value:
            missing += 1
        elif not re.fullmatch("[A-Z]{2}", value):
            malformed += 1
        else:
            counts[value] += 1
    ids = [r[identifier].strip() for r in rows]
    return {"rows": len(rows), "unique_nonempty_sample_ids": len(set(ids) - {""}),
            "blank_sample_ids": ids.count(""), "country_code_counts": dict(sorted(counts.items())),
            "distinct_country_codes": len(counts), "missing_country_code": missing,
            "malformed_country_code": malformed,
            "note": "Counts declared two-letter codes, not independently verified countries or sampling representativeness."}


def biodiversity(data):
    resources, _ = bio_members(data)
    labels = {}
    details = {}
    for marker in ("16S", "ITS"):
        name = f"biodiversity_{marker}.csv"
        reader = csv.reader(io.StringIO(resources[name].decode("utf-8-sig"), newline=""), strict=True)
        header = next(reader)
        if header[:2] != ["OTU ID", "taxonomy_id"] or len(header) != len(set(header)):
            raise ValueError("Unexpected matrix header")
        suffix = "-" + marker
        cols = header[2:]
        valid = [c[:-len(suffix)] for c in cols if c.endswith(suffix) and len(c) > len(suffix)]
        labels[marker] = set(valid)
        details[marker] = {"sample_marker_columns": len(cols), "valid_suffix_columns": len(valid),
                           "unique_base_labels": len(labels[marker])}
    return {"markers": details, "shared_base_labels": len(labels["16S"] & labels["ITS"]),
            "union_base_labels": len(labels["16S"] | labels["ITS"]),
            "note": "Exact suffix-stripped header labels, not verified distinct physical samples or linkage to canonical IDs. Invalid suffix columns excluded from overlap."}


def numeric(value):
    try:
        return math.isfinite(float(value.strip()))
    except ValueError:
        return False


def validation(data):
    header, rows = csv_rows(data)
    # Match only explicit known columns; do not silently infer a mapping.
    expected = {"qr": "QR_code", "lab_ph": "mean_pH_lab", "field_ph": "pH_CS",
                "lab_orgc": "mean_orgC_lab (%)", "field_orgc": "orgC_CS (%)",
                "lab_texture": "Lab_texture_reclass", "field_texture": "CS_texture"}
    indexes = {k: header.index(v) for k, v in expected.items() if v in header}
    result = {"rows": len(rows), "column_count": len(header),
              "expected_columns_present": {k: k in indexes for k in expected}}
    if "qr" in indexes:
        ids = [r[indexes["qr"]].strip() for r in rows]
        result["unique_nonempty_qr_labels"] = len(set(ids) - {""})
        result["blank_qr_labels"] = ids.count("")
    for name, a, b in (("ph", "lab_ph", "field_ph"), ("organic_carbon", "lab_orgc", "field_orgc"),
                       ("texture", "lab_texture", "field_texture")):
        if a in indexes and b in indexes:
            check = bool if name == "texture" else numeric
            result[name + "_paired_nonempty_or_numeric_rows"] = sum(
                check(r[indexes[a]].strip()) and check(r[indexes[b]].strip()) for r in rows)
    return result


def assess(reader, ident, names):
    result = {"record_id": ident, "doi": "10.5281/zenodo." + ident, "status": "NOT_ASSESSED"}
    try:
        raw, final = reader.get("https://doi.org/" + result["doi"], 4 * MIB)
        result["doi_resolution_matches"] = urlsplit(final).hostname == "zenodo.org" and urlsplit(final).path.rstrip("/") in (f"/records/{ident}", f"/record/{ident}")
    except (requests.RequestException, ValueError, KeyError):
        result["doi_resolution_matches"] = None
    try:
        raw, _ = reader.get(f"https://zenodo.org/api/records/{ident}", 4 * MIB)
        record = json_doc(raw)
        if str(record.get("id")) != ident:
            raise ValueError("Wrong record")
        entries = {x["key"]: x for x in record["files"]}
        data = {}
        integrity = {}
        for name in names:
            raw, _ = reader.get(f"https://zenodo.org/records/{ident}/files/{name}?download=1", 32 * MIB)
            data[name] = raw
            integrity[name] = file_integrity(raw, entries[name])
        result["integrity"] = integrity
        if not all(x["size_matches"] and x["published_checksum_matches"] for x in integrity.values()):
            result["status"] = "REVIEW"
            result["reason"] = "File integrity mismatch; no coverage inference"
            return result
        if ident == "23784504":
            result["coverage"] = canonical(data[names[0]])
        elif ident == "22014977":
            result["coverage"] = biodiversity(data[names[0]])
        else:
            result["coverage"] = validation(data[names[0]])
            result["readme_retrieved"] = bool(data[names[1]].strip())
        result["status"] = "OBSERVED"
    except (requests.RequestException, ValueError, KeyError, TypeError, AttributeError, csv.Error, RuntimeError, OSError, StopIteration) as exc:
        result["reason"] = type(exc).__name__
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ack", required=True, choices=["yes-public-readonly"])
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--commit", default="not recorded")
    args = p.parse_args()
    reader = PublicReader()
    try:
        rows = [assess(reader, ident, names) for ident, names in RECORDS.items()]
    finally:
        reader.session.close()
    report = {"kpi": "RO4-1", "phase": 2, "report_commit": args.commit,
              "generated_at_utc": datetime.now(timezone.utc).isoformat(),
              "records": rows,
              "limitations": ["Bounded aggregate observations of exact pinned public releases.",
                              "Prior R7-3 checks cover additional canonical/biodiversity resources.",
                              "No scientific comparability, representativeness, or external harvesting claim.",
                              "No sample identifiers, matrix labels, raw files or QR values retained.",
                              "OBSERVED is not KPI acceptance; missing columns or unavailable files require review."]}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    lines = ["# RO4-1 — published dataset coverage", "", report["generated_at_utc"], ""]
    for row in rows:
        lines += ["## " + row["doi"], "", row["status"], "", "    " + json.dumps(row, ensure_ascii=True), ""]
    lines += ["## Limitations", "", *["- " + x for x in report["limitations"]]]
    args.out.with_suffix(".md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
