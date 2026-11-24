#!/usr/bin/env python3
"""Aggregate-only quality assessment of an immutable canonical ZIP (no network)."""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import re
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from tools.validate_echorepo_bundle import (
    EXPECTED_SCHEMAS,
    INTEGER_COLUMNS,
    NUMERIC_COLUMNS,
    STRUCTURE_VALUES,
    TEXTURE_VALUES,
    TIMESTAMP_COLUMNS,
    parse_utc_timestamp,
)

MIB = 1024 * 1024
PRIMARY = {
    "samples.csv": ("sample_id",),
    "sample_images.csv": ("sample_id", "image_id"),
    "parameter_definitions.csv": ("parameter_code",),
    "analysis_methods.csv": ("method_code",),
}
FOREIGN = {
    "sample_images.csv": {"sample_id": ("samples.csv", False)},
    "sample_parameters.csv": {
        "sample_id": ("samples.csv", False),
        "parameter_code": ("parameter_definitions.csv", False),
        "method_code": ("analysis_methods.csv", False),
    },
    "sample_biodiversity.csv": {"sample_id": ("samples.csv", False)},
    "parameter_definitions.csv": {"method_code": ("analysis_methods.csv", True)},
}
ORDER = [
    "samples.csv", "analysis_methods.csv", "parameter_definitions.csv",
    "sample_images.csv", "sample_parameters.csv", "sample_biodiversity.csv",
]


def sha256(stream) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: stream.read(MIB), b""):
        digest.update(chunk)
    return digest.hexdigest()


def utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def assess(bundle: Path, metadata: Path | None = None) -> dict:
    if not bundle.is_file() or bundle.stat().st_size > 100 * MIB:
        raise ValueError("A local canonical ZIP of at most 100 MiB is required")
    with bundle.open("rb") as stream:
        fingerprint = sha256(stream)
    rules = {}
    tables = {}
    keys = {table: set() for table in PRIMARY}
    countries = {}
    covered = {table: set() for table in FOREIGN if table.startswith("sample_")}

    def check(rule, passed, severity="error"):
        record = rules.setdefault(rule, {"checked": 0, "issues": 0, "severity": severity})
        record["checked"] += 1
        if not passed:
            record["issues"] += 1

    with zipfile.ZipFile(bundle) as archive:
        infos = archive.infolist()
        if len(infos) > 30 or sum(x.file_size for x in infos) > 250 * MIB:
            raise ValueError("ZIP exceeds the safe resource limits")
        names = Counter(x.filename for x in infos)
        check("archive.unique_member_names", all(n == 1 for n in names.values()))
        check("archive.only_expected_resources", set(names) == set(EXPECTED_SCHEMAS), "warning")
        if any(n > 1 for n in names.values()):
            raise ValueError("Duplicate ZIP member names; archive rejected")
        for name in ORDER:
            expected = EXPECTED_SCHEMAS[name]
            present = name in names
            check("archive.required_resources", present)
            table = {"present": present, "header_matches": False, "rows": 0,
                     "blank_counts": dict.fromkeys(expected, 0), "sha256": None,
                     "bytes": None, "qa_wrong_coordinates": 0, "category_no_data": {}}
            tables[name] = table
            if not present:
                continue
            info = archive.getinfo(name)
            table["bytes"] = info.file_size
            try:
                with archive.open(info) as raw:
                    table["sha256"] = sha256(raw)
                with archive.open(info) as raw, io.TextIOWrapper(
                    raw, encoding="utf-8-sig", newline=""
                ) as stream:
                    reader = csv.reader(stream, strict=True)
                    header = next(reader, [])
                    table["header_matches"] = header == expected
                    check(f"{name}.header", header == expected)
                    if header != expected:
                        continue
                    for values in reader:
                        table["rows"] += 1
                        check(f"{name}.row_width", len(values) == len(expected))
                        if len(values) != len(expected):
                            continue
                        row = dict(zip(expected, (v.strip() for v in values)))
                        numeric = {}
                        for col, value in row.items():
                            if not value:
                                table["blank_counts"][col] += 1
                                continue
                            if col in NUMERIC_COLUMNS:
                                try:
                                    number = float(value)
                                    valid = math.isfinite(number)
                                except (ValueError, OverflowError):
                                    valid = False
                                check(f"{name}.{col}.finite_numeric", valid)
                                if valid:
                                    numeric[col] = number
                                    if col in INTEGER_COLUMNS:
                                        check(f"{name}.{col}.integer", number.is_integer())
                            if col in TIMESTAMP_COLUMNS:
                                try:
                                    parse_utc_timestamp(value)
                                    valid = True
                                except (ValueError, OverflowError):
                                    valid = False
                                check(f"{name}.{col}.utc_timestamp", valid)
                        if name in PRIMARY:
                            key = tuple(row[c] for c in PRIMARY[name])
                            valid = all(key)
                            check(f"{name}.primary_key_nonblank", valid)
                            if valid:
                                check(f"{name}.primary_key_unique", key not in keys[name])
                                keys[name].add(key)
                        for col, (parent, optional) in FOREIGN.get(name, {}).items():
                            value = row[col]
                            if optional and not value:
                                continue
                            check(f"{name}.{col}.foreign_key", bool(value) and (value,) in keys[parent])
                        cc = row.get("country_code", "")
                        if cc:
                            check(f"{name}.country_code_format", bool(re.fullmatch(r"[A-Z]{2}", cc)), "warning")
                        if "licence" in row:
                            check(f"{name}.licence_present", bool(row["licence"]), "warning")
                        if name == "samples.csv":
                            sid = row["sample_id"]
                            if sid:
                                countries.setdefault(sid, cc)
                            lat, lon = numeric.get("lat"), numeric.get("lon")
                            if lat is not None:
                                check("samples.csv.latitude_range", -90 <= lat <= 90)
                            if lon is not None:
                                check("samples.csv.longitude_range", -180 <= lon <= 180)
                            check("samples.csv.coordinate_pair_present", lat is not None and lon is not None, "warning")
                            if lat is not None and lon is not None:
                                check("samples.csv.nonzero_coordinate_pair", (lat, lon) != (0, 0), "warning")
                            for col, low, high in (
                                ("soil_ph_field", 0, 14),
                                ("soil_organic_matter_estimate_pct", 0, 100),
                            ):
                                if col in numeric:
                                    check(f"samples.csv.{col}.domain_range", low <= numeric[col] <= high, "warning")
                            if "coordinate_obfuscation_radius_m" in numeric:
                                check("samples.csv.obfuscation_radius_nonnegative", numeric["coordinate_obfuscation_radius_m"] >= 0)
                            for col, allowed in (
                                ("soil_texture_en", TEXTURE_VALUES), ("soil_structure_en", STRUCTURE_VALUES),
                            ):
                                if row[col]:
                                    check(f"samples.csv.{col}.vocabulary", row[col] in allowed)
                                    if row[col] == "no data":
                                        table["category_no_data"][col] = table["category_no_data"].get(col, 0) + 1
                            if row["qa_status"].casefold().startswith("wrong_coordinates"):
                                table["qa_wrong_coordinates"] += 1
                        elif name in covered:
                            sid = row["sample_id"]
                            if (sid,) in keys["samples.csv"]:
                                covered[name].add(sid)
                                parent_country = countries.get(sid)
                                if cc and parent_country:
                                    check(f"{name}.country_matches_parent", cc == parent_country, "warning")
                        for col in ("earthworms_count", "pollutants_count", "read_count"):
                            if col in numeric:
                                check(f"{name}.{col}.nonnegative", numeric[col] >= 0, "warning")
                        if "relative_abundance_pct" in numeric:
                            check(f"{name}.relative_abundance_pct.range", 0 <= numeric["relative_abundance_pct"] <= 100, "warning")
            except (UnicodeError, csv.Error, zipfile.BadZipFile, RuntimeError, OSError, NotImplementedError):
                check(f"{name}.readable_csv", False)
        check("samples.csv.nonempty", tables["samples.csv"]["rows"] > 0)
    # This deliberately reports only counts, schema names and file hashes, never row values.
    result = {
        "kpi": "R7-3", "phase": 2, "criterion_ids": ["R73-F02", "R73-F03", "R73-R02"],
        "generated_at_utc": utc(), "bundle_sha256": fingerprint, "bundle_bytes": bundle.stat().st_size,
        "tables": tables, "rules": rules,
        "errors": sum(r["issues"] for r in rules.values() if r["severity"] == "error"),
        "warnings": sum(r["issues"] for r in rules.values() if r["severity"] == "warning"),
        "distinct_samples": len(keys["samples.csv"]),
        "samples_with_linked_resources": {name: len(ids) for name, ids in covered.items()},
        "csvw": assess_metadata(metadata, tables) if metadata else {"status": "NOT_ASSESSED"},
        "limitations": [
            "Blank values are counted, not assumed invalid except where a rule requires a value.",
            "Country format and parent-country consistency do not establish geographic correctness.",
            "Obfuscation radius and QA flags do not prove anonymity or legal compliance.",
            "This is an aggregate structural/domain-sanity assessment, not laboratory accuracy, full CSVW conformance, or backup/restore validation.",
            "No rows, identifiers, coordinates, free text or credentials are emitted.",
        ],
    }
    result["status"] = "PASS_STRUCTURAL_CHECKS" if result["errors"] == 0 else "ISSUES_FOUND"
    return result


def assess_metadata(path: Path, tables: dict) -> dict:
    """Compare a separately supplied TableGroup with the exact ZIP resources."""
    if not path.is_file() or path.stat().st_size > 5 * MIB:
        raise ValueError("CSVW metadata must be a local JSON file of at most 5 MiB")
    try:
        with path.open("rb") as stream:
            digest = sha256(stream)
        doc = json.loads(path.read_text(encoding="utf-8-sig"))
    except (UnicodeError, json.JSONDecodeError):
        return {"status": "INVALID_JSON"}
    group = doc.get("tables") if isinstance(doc, dict) else None
    if not isinstance(group, list) or not all(isinstance(t, dict) for t in group):
        return {"status": "UNRECOGNIZED_TABLEGROUP", "sha256": digest}
    by_name = {t.get("url"): t for t in group if isinstance(t.get("url"), str)}
    results = {}
    group_matches = len(group) == len(by_name) and set(by_name) == set(tables)
    for name, actual in tables.items():
        t = by_name.get(name, {})
        schema = t.get("tableSchema")
        cols = schema.get("columns") if isinstance(schema, dict) else None
        cols_valid = isinstance(cols, list) and all(isinstance(c, dict) for c in cols)
        names_match = cols_valid and [c.get("name") for c in cols] == EXPECTED_SCHEMAS[name]
        scalar_uris = cols_valid and all(
            "propertyUrl" not in c or isinstance(c["propertyUrl"], str) for c in cols
        )
        declared_hash = t.get("schema:sha256")
        match_hash = isinstance(declared_hash, str) and actual["sha256"] is not None and declared_hash.lower() == actual["sha256"]
        count_matches = actual["header_matches"] and t.get("schema:numberOfItems") == actual["rows"]
        size_matches = actual["bytes"] is not None and t.get("schema:contentSize") == actual["bytes"]
        results[name] = {
            "column_names_match": bool(names_match), "scalar_property_urls": bool(scalar_uris),
            "sha256_match": bool(match_hash), "row_count_match": bool(count_matches),
            "byte_size_match": bool(size_matches),
        }
    matched = group_matches and all(all(r.values()) for r in results.values())
    return {"status": "MATCHED_SELECTED_CHECKS" if matched else "MISMATCH_OR_MISSING_FIELDS",
            "sha256": digest, "table_inventory_matches": group_matches, "tables": results,
            "note": "Selected CSVW layout and fingerprint checks only; not full W3C validation. A metadata file from another snapshot is expected to differ."}


def markdown(r: dict) -> str:
    lines = [
        "# R7-3 — canonical data quality and compliance",
        "", f"- UTC: {r['generated_at_utc']}", f"- Reporting commit: {r.get('report_commit', 'not recorded')}",
        f"- Bundle SHA-256: {r['bundle_sha256']}", f"- Bundle bytes: {r['bundle_bytes']}",
        f"- Structural result: **{r['status']}**",
        f"- Error observations: {r['errors']}; warning observations: {r['warnings']}",
        f"- Distinct nonblank sample IDs: {r['distinct_samples']}",
        f"- Separate CSVW assessment: **{r['csvw']['status']}**",
        "", "Counts are rule observations, not counts of distinct defective records.",
        "", "| Resource | Present | Header matches | Rows | Linked distinct samples |",
        "|---|---|---|---:|---:|",
    ]
    for name, t in r["tables"].items():
        lines.append(f"| {name} | {t['present']} | {t['header_matches']} | {t['rows']} | {r['samples_with_linked_resources'].get(name, '—')} |")
    lines += ["", "## Missing values", "", "| Resource | Column | Blank | Rows |", "|---|---|---:|---:|"]
    for name, t in r["tables"].items():
        if t["header_matches"]:
            for col, count in t["blank_counts"].items():
                lines.append(f"| {name} | {col} | {count} | {t['rows']} |")
    samples = r["tables"]["samples.csv"]
    lines += ["", f"Samples carrying wrong_coordinates QA flags: {samples['qa_wrong_coordinates']}.",
              "Explicit category 'no data' counts: " + json.dumps(samples["category_no_data"], sort_keys=True),
              "", "## Rule observations", "", "| Rule | Severity | Checked | Issues |", "|---|---|---:|---:|"]
    for name, rule in sorted(r["rules"].items()):
        lines.append(f"| {name} | {rule['severity']} | {rule['checked']} | {rule['issues']} |")
    if "tables" in r["csvw"]:
        lines += ["", "## Selected CSVW comparisons", "", "| Resource | Columns | Scalar URI | Hash | Rows | Bytes |", "|---|---|---|---|---|---|"]
        for name, t in r["csvw"]["tables"].items():
            lines.append(f"| {name} | " + " | ".join(str(v) for v in t.values()) + " |")
    lines += ["", "## Limitations", "", *["- " + x for x in r["limitations"]], ""]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--metadata", type=Path)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--commit", default="not recorded")
    args = parser.parse_args()
    try:
        result = assess(args.bundle, args.metadata)
    except (ValueError, zipfile.BadZipFile, OSError):
        print("Data quality assessment refused or failed: check local ZIP/JSON format and size.")
        return 2
    result["report_commit"] = args.commit
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "data-quality.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    (args.out_dir / "data-quality.md").write_text(markdown(result), encoding="utf-8")
    print(f"R7-3 data quality: {result['status']}; errors={result['errors']}; warnings={result['warnings']}; CSVW={result['csvw']['status']}")
    return int(bool(result["errors"]) or (args.metadata is not None and result["csvw"]["status"] != "MATCHED_SELECTED_CHECKS"))


if __name__ == "__main__":
    raise SystemExit(main())
