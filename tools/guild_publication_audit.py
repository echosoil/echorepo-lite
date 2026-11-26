#!/usr/bin/env python3
"""Audit local guild analysis artifacts without DB, MinIO or Zenodo access."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

FAP_FILES = (
    "8_faprotax_samples_x_functions.csv",
    "9_faprotax_annotated_fraction.csv",
)
INPUT_FILES = (
    "faprotax_work/6_otu_clean_counts_no_blanks.csv",
    "faprotax_work/7_taxonomy_clean.csv",
    "7_taxonomy_NA.csv",
)
REFERENCE = "FUNGuild_db.json"
ACCEPTED = {"Probable", "Highly Probable", "Higly Probable"}


def fingerprint(path, *, hash_file=True):
    if not path.is_file():
        return {"present": False}
    before = path.stat()
    result = {
        "present": True,
        "bytes": before.st_size,
        "modified_at_utc": datetime.fromtimestamp(
            before.st_mtime, timezone.utc
        ).isoformat(),
    }
    if hash_file:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        result["sha256"] = digest.hexdigest()
    after = path.stat()
    result["stable_during_inspection"] = (
        before.st_size == after.st_size and before.st_mtime_ns == after.st_mtime_ns
    )
    return result


def normalized_label(value):
    return value.strip().upper()


def distribution(values):
    if not values:
        return None
    return {
        "min": min(values),
        "median": median(values),
        "max": max(values),
    }


def bounded_number(value, upper):
    try:
        number = float(value)
    except (ValueError, TypeError):
        return None
    return number if math.isfinite(number) and 0 <= number <= upper else None


def audit_profiles(path):
    """File 8: first column is R row names, remaining columns are fractions."""
    ids = set()
    result = {
        "rows": 0,
        "blank_labels": 0,
        "duplicate_normalized_labels": 0,
        "inconsistent_width_rows": 0,
        "invalid_fraction_cells": 0,
        "all_zero_profile_rows": 0,
        "profile_sum_above_one_rows": 0,
    }
    sums = []
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        header = next(reader, [])
        functions = [name.strip() for name in header[1:]]
        result["function_columns"] = len(functions)
        result["unique_nonblank_function_headers"] = (
            bool(functions)
            and all(functions)
            and len(functions) == len(set(functions))
        )
        for row in reader:
            result["rows"] += 1
            if len(row) != len(header) or not row:
                result["inconsistent_width_rows"] += 1
                continue
            ident = normalized_label(row[0])
            if not ident:
                result["blank_labels"] += 1
            elif ident in ids:
                result["duplicate_normalized_labels"] += 1
            else:
                ids.add(ident)
            values = [bounded_number(value, 1) for value in row[1:]]
            invalid = values.count(None)
            result["invalid_fraction_cells"] += invalid
            if not invalid and values:
                total = sum(values)
                sums.append(total)
                result["all_zero_profile_rows"] += total == 0
                # File 8 is rounded to six decimals per function.
                result["profile_sum_above_one_rows"] += (
                    total > 1 + len(values) * 0.0000005 + 1e-10
                )
    result["unique_normalized_labels"] = len(ids)
    result["profile_sum_distribution"] = distribution(sums)
    return result, ids


def audit_coverage(path):
    """File 9: read-weighted coverage, fraction and percent reported separately."""
    ids = set()
    percentages = []
    result = {
        "rows": 0,
        "blank_labels": 0,
        "duplicate_normalized_labels": 0,
        "inconsistent_width_rows": 0,
        "invalid_numeric_rows": 0,
        "fraction_percent_mismatch_rows": 0,
        "zero_coverage_rows": 0,
    }
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        header = next(reader, [])
        result["expected_unique_headers"] = (
            len(header) == len(set(header))
            and {"Sample", "Annotated_frac", "Annotated_pct"}.issubset(header)
        )
        if not result["expected_unique_headers"]:
            return result, ids
        i, f, p = (
            header.index(name)
            for name in ("Sample", "Annotated_frac", "Annotated_pct")
        )
        for row in reader:
            result["rows"] += 1
            if len(row) != len(header):
                result["inconsistent_width_rows"] += 1
                continue
            ident = normalized_label(row[i])
            if not ident:
                result["blank_labels"] += 1
            elif ident in ids:
                result["duplicate_normalized_labels"] += 1
            else:
                ids.add(ident)
            frac = bounded_number(row[f], 1)
            pct = bounded_number(row[p], 100)
            if frac is None or pct is None:
                result["invalid_numeric_rows"] += 1
                continue
            result["fraction_percent_mismatch_rows"] += abs(frac * 100 - pct) > 0.010001
            result["zero_coverage_rows"] += pct == 0
            percentages.append(pct)
    result["unique_normalized_labels"] = len(ids)
    result["read_coverage_pct_distribution"] = distribution(percentages)
    return result, ids


def audit_reference(path):
    """Summarize local reference records, not verified fungal assignments."""
    with path.open(encoding="utf-8-sig") as stream:
        raw = json.load(stream)
    if isinstance(raw, dict):
        records = list(raw.values())
    elif isinstance(raw, list):
        records = raw
    else:
        raise ValueError("Unsupported reference JSON")
    confidence = Counter()
    accepted_taxa = set()
    object_count = 0
    for record in records:
        if not isinstance(record, dict):
            continue
        object_count += 1
        ranking = str(record.get("confidenceRanking") or "").strip()
        # Report only known labels, not unexpected source strings.
        category = ranking if ranking in ACCEPTED | {"Possible"} else "other_or_blank"
        confidence[category] += 1
        taxon = str(record.get("taxon") or record.get("queried_taxon") or "").strip()
        guild = str(record.get("guild") or "").strip()
        trophic = str(record.get("trophicMode") or "").strip()
        if (
            ranking in ACCEPTED
            and taxon
            and guild
            and guild.upper() != "NULL"
            and trophic
            and trophic.upper() != "NULL"
        ):
            accepted_taxa.add(taxon)
    return {
        "json_entries": len(records),
        "object_entries": object_count,
        "confidence_counts": dict(sorted(confidence.items())),
        "distinct_taxon_labels_with_accepted_confidence_and_nonempty_guild_trophic_mode": len(
            accepted_taxa
        ),
        "note": (
            "Pre-mapping reference inventory; not a count of matched ITS genera. "
            "Reference release, citation and redistribution terms require confirmation."
        ),
    }


def inspect_inputs(root, profile_ids, *, hash_inputs=False):
    result = {}
    for name in INPUT_FILES:
        path = root / name
        result[name] = fingerprint(path, hash_file=hash_inputs)
    source = root / INPUT_FILES[0]
    if source.is_file():
        with source.open(encoding="utf-8-sig", newline="") as stream:
            header = next(csv.reader(stream, strict=True), [])
        labels = [normalized_label(value) for value in header[1:]]
        unique = set(labels) - {""}
        result["otu_matrix_header"] = {
            "sample_columns": len(labels),
            "unique_nonblank_labels": len(unique),
            "blank_labels": labels.count(""),
            "duplicate_normalized_labels": len(labels) - labels.count("") - len(unique),
            "profile_only_labels": len(profile_ids - unique),
            "matrix_only_labels": len(unique - profile_ids),
        }
    return result


def audit(root, *, hash_inputs=False):
    observations = {}
    files = {}
    labels = {}
    readers = {
        FAP_FILES[0]: audit_profiles,
        FAP_FILES[1]: audit_coverage,
        REFERENCE: audit_reference,
    }
    for name, reader in readers.items():
        path = root / name
        try:
            files[name] = fingerprint(path)
            if not files[name]["present"]:
                observations[name] = {"status": "MISSING"}
                continue
            outcome = reader(path)
            if isinstance(outcome, tuple):
                detail, labels[name] = outcome
            else:
                detail = outcome
            # Hash again to detect files replaced during parsing.
            after = fingerprint(path)
            stable = (
                files[name]["stable_during_inspection"]
                and after.get("stable_during_inspection", False)
                and files[name]["sha256"] == after.get("sha256")
            )
            observations[name] = {
                "status": "OBSERVED" if stable else "REVIEW",
                "stable_bytes_across_read": stable,
                **detail,
            }
        except (OSError, ValueError, csv.Error, TypeError) as exc:
            observations[name] = {"status": "NOT_ASSESSED", "reason": type(exc).__name__}
    alignment = {"status": "NOT_ASSESSED"}
    if all(name in labels for name in FAP_FILES):
        first, second = (labels[name] for name in FAP_FILES)
        alignment = {
            "status": "OBSERVED",
            "shared_labels": len(first & second),
            "profile_only_labels": len(first - second),
            "coverage_only_labels": len(second - first),
            "sets_match": first == second,
        }
    try:
        inputs = inspect_inputs(root, labels.get(FAP_FILES[0], set()), hash_inputs=hash_inputs)
    except (OSError, ValueError, csv.Error) as exc:
        inputs = {"status": "NOT_ASSESSED", "reason": type(exc).__name__}
    return {
        "files": files,
        "observations": observations,
        "faprotax_label_alignment": alignment,
        "input_inventory": inputs,
        "publication_ready": False,
        "limitations": [
            "Local file inspection only; no DB or MinIO queries and no Zenodo writes.",
            "OBSERVED means inspected, not a scientific or publication pass.",
            "Labels and individual measurements are not included in this report.",
            "File 8 fractions of present retained OTUs may sum above one due to overlapping functions.",
            "File 9 is read-weighted coverage of retained OTUs; not file 8's denominator.",
            "Matching labels, hashes and modification times do not prove a common processing run.",
            "Current or pinned public source correspondence and processing parameters remain to be verified.",
            "FUNGuild reference inventory is not an ITS numerical profile audit.",
            "Reference database version and redistribution terms need documented review.",
            "No claim of direct measurement of microbial function or guild activity.",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--commit", default="not recorded")
    parser.add_argument(
        "--hash-inputs", action="store_true",
        help="Also stream SHA-256 of source inputs; file 6 can be very large.",
    )
    args = parser.parse_args()
    if not args.data_dir.is_dir():
        parser.error("--data-dir must be the directory containing files 8/9 and FUNGuild JSON")
    if args.out.suffix != ".json":
        parser.error("--out must end in .json")
    if args.out.exists() or args.out.with_suffix(".md").exists():
        parser.error("Use a fresh evidence output path")
    report = {
        "phase": "guild-publication-A",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "report_commit": args.commit,
        **audit(args.data_dir, hash_inputs=args.hash_inputs),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    lines = [
        "# Guild publication — Phase A local artifact audit",
        "",
        report["generated_at_utc"],
        "",
        "Reporting commit: " + report["report_commit"],
    ]
    for key in ("files", "observations", "faprotax_label_alignment", "input_inventory"):
        lines.extend(["", "## " + key, "", "~~~json",
                      json.dumps(report[key], indent=2), "~~~"])
    lines.extend(["", "## Limitations", "", *["- " + x for x in report["limitations"]]])
    args.out.with_suffix(".md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Local audit written to {args.out}; no publication performed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
