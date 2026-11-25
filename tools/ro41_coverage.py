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


VALIDATION_COLUMNS = {
    "qr": "QR_code",
    "lab_ph": "mean_pH_lab",
    "field_ph": "pH_CS",
    "lab_orgc": "mean_orgC_lab (%)",
    "field_orgc": "orgC_CS (%)",
    "lab_texture": "Lab_texture_reclass",
    "field_texture": "CS_texture",
}


class ValidationFormatError(ValueError):
    """Only sanitized structural diagnostics are included in reports."""

    def __init__(self, diagnostics):
        super().__init__("Validation CSV format not uniquely recognized")
        self.diagnostics = diagnostics


def validation_rows(data):
    # Do not modify the published bytes. The checksum is checked before this
    # analysis. Support BOM-marked UTF-16, UTF-8 and a labelled Windows fallback.
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        encodings = ("utf-16",)
    else:
        encodings = ("utf-8-sig", "cp1252")
    text = None
    for encoding in encodings:
        try:
            text = data.decode(encoding)
            break
        except UnicodeError:
            continue
    if text is None:
        raise ValidationFormatError({"reason": "unsupported_encoding"})
    # Optional Excel delimiter declaration, not a header or a data record.
    declared = None
    lines = text.splitlines(keepends=True)
    if lines and lines[0].strip().lower() in {"sep=,", "sep=;", "sep=\t"}:
        declared = lines.pop(0).rstrip("\r\n")[-1]
        text = "".join(lines)
    diagnostics = {"encoding": encoding, "candidates": {}}
    candidates = []
    for delimiter in (",", ";", "\t"):
        if declared is not None and delimiter != declared:
            continue
        name = {",": "comma", ";": "semicolon", "\t": "tab"}[delimiter]
        info = {}
        diagnostics["candidates"][name] = info
        try:
            reader = csv.reader(io.StringIO(text, newline=""), delimiter=delimiter, strict=True)
            header = [col.strip() for col in next(reader, [])]
            rows = list(reader)
        except csv.Error:
            info["reason"] = "csv_syntax_error"
            continue
        matched = [k for k, v in VALIDATION_COLUMNS.items() if v in header]
        width_errors = sum(len(row) != len(header) for row in rows)
        unique = len(header) == len(set(header))
        info.update({"columns": len(header), "rows": len(rows),
                     "matched_expected_fields": matched, "width_errors": width_errors,
                     "unique_headers": unique, "blank_headers": header.count("")})
        # A delimiter is only accepted if the exact known identifier and at
        # least one complete measurement pair are found. Never guess by width.
        pairs = (("lab_ph", "field_ph"), ("lab_orgc", "field_orgc"),
                 ("lab_texture", "field_texture"))
        if ("qr" in matched and any(a in matched and b in matched for a, b in pairs)
                and unique and all(header) and not width_errors):
            candidates.append((delimiter, name, header, rows))
    if len(candidates) != 1:
        raise ValidationFormatError(diagnostics)
    delimiter, name, header, rows = candidates[0]
    return header, rows, delimiter, {"encoding": encoding, "delimiter": name,
                                     "excel_delimiter_declaration": declared is not None}


def numeric(value, *, decimal_comma=False):
    value = value.strip()
    # Only a simple decimal-comma literal; no thousands separator inference.
    if decimal_comma and re.fullmatch(r"[+-]?\d+,\d+(?:[eE][+-]?\d+)?", value):
        value = value.replace(",", ".")
    try:
        return math.isfinite(float(value))
    except ValueError:
        return False


def validation(data):
    header, rows, delimiter, format_info = validation_rows(data)
    expected = VALIDATION_COLUMNS
    indexes = {k: header.index(v) for k, v in expected.items() if v in header}
    result = {"rows": len(rows), "column_count": len(header), "csv_format": format_info,
              "expected_columns_present": {k: k in indexes for k in expected}}
    ids = [r[indexes["qr"]].strip() for r in rows]
    result["unique_nonempty_qr_labels"] = len(set(ids) - {""})
    result["blank_qr_labels"] = ids.count("")
    for name, a, b in (("ph", "lab_ph", "field_ph"), ("organic_carbon", "lab_orgc", "field_orgc"),
                       ("texture", "lab_texture", "field_texture")):
        if a in indexes and b in indexes:
            if name == "texture":
                count = sum(bool(r[indexes[a]].strip()) and bool(r[indexes[b]].strip()) for r in rows)
            else:
                count = sum(numeric(r[indexes[a]], decimal_comma=delimiter != ",")
                            and numeric(r[indexes[b]], decimal_comma=delimiter != ",") for r in rows)
            result[name + "_paired_nonempty_or_numeric_rows"] = count
    return result


CANONICAL_CONCEPT_ID = "19722513"
CANONICAL_CONCEPT_DOI = "10.5281/zenodo." + CANONICAL_CONCEPT_ID


def canonical_record_matches(record):
    # A new release must belong to the existing canonical collection.
    # Neither title similarity nor a standalone DOI is sufficient.
    concept_id = str(record.get("conceptrecid") or "")
    concept_doi = str(record.get("conceptdoi") or "").lower()
    return (bool(concept_id or concept_doi)
            and (not concept_id or concept_id == CANONICAL_CONCEPT_ID)
            and (not concept_doi or concept_doi == CANONICAL_CONCEPT_DOI))


def assess(reader, ident, names, *, canonical_release=False):
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
        if canonical_release or ident == "23784504":
            result["canonical_concept_matches"] = canonical_record_matches(record)
            if not result["canonical_concept_matches"]:
                result["status"] = "REVIEW"
                result["reason"] = "Record does not identify the expected canonical concept"
                return result
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
        if canonical_release or ident == "23784504":
            result["coverage"] = canonical(data[names[0]])
        elif ident == "22014977":
            result["coverage"] = biodiversity(data[names[0]])
        else:
            result["coverage"] = validation(data[names[0]])
            result["readme_retrieved"] = bool(data[names[1]].strip())
        result["status"] = "OBSERVED"
        if ident == "24003680" and not all(result["coverage"]["expected_columns_present"].values()):
            result["status"] = "REVIEW"
            result["reason"] = "Some expected columns are missing"
    except ValidationFormatError as exc:
        result["reason"] = "ValidationFormatError"
        result["format_diagnostics"] = exc.diagnostics
    except (requests.RequestException, ValueError, KeyError, TypeError, AttributeError, csv.Error, RuntimeError, OSError, StopIteration) as exc:
        result["reason"] = type(exc).__name__
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ack", required=True, choices=["yes-public-readonly"])
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--commit", default="not recorded")
    p.add_argument("--record-id", action="append", choices=list(RECORDS),
                   help="Assess only selected pinned records; repeat for multiple")
    p.add_argument("--canonical-record-id",
                   help="Assess an explicit published version of the existing canonical concept")
    args = p.parse_args()
    if args.canonical_record_id and not re.fullmatch(r"[0-9]{1,15}", args.canonical_record_id):
        p.error("--canonical-record-id requires a numeric Zenodo version record ID")
    if args.canonical_record_id and args.record_id:
        p.error("Use --canonical-record-id separately from --record-id")
    selected = list(dict.fromkeys(args.record_id or RECORDS))
    reader = PublicReader()
    try:
        if args.canonical_record_id:
            rows = [assess(reader, args.canonical_record_id, ("samples.csv",),
                           canonical_release=True)]
        else:
            rows = [assess(reader, ident, RECORDS[ident]) for ident in selected]
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
