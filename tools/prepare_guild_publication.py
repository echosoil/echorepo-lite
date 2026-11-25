"""Prepare two independent local guild publication candidates; no network access."""

from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import subprocess
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import median

from tools.guild_publication_audit import fingerprint, normalized_label

FAP8 = "8_faprotax_samples_x_functions.csv"
FAP9 = "9_faprotax_annotated_fraction.csv"
FUN_FILES = ("guild_profiles.csv", "annotation_coverage.csv", "guild_definitions.csv")
FUN_HEADERS = (
    ("sample_id", "guild", "read_count", "fraction"),
    ("sample_id", "status", "source_kind", "total_reads", "assigned_reads",
     "unassigned_reads", "assigned_fraction"),
    ("guild", "primary_guild"),
)


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, obj):
    path.write_text(json.dumps(obj, indent=2, sort_keys=True, allow_nan=False) + "\n",
                    encoding="utf-8")


def file_meta(path):
    result = fingerprint(path)
    if not result.get("present") or not result.get("stable_during_inspection"):
        raise ValueError(f"Missing or unstable file: {path.name}")
    return {k: result[k] for k in ("bytes", "sha256")}


def checked_copy(source, target, expected):
    if file_meta(source) != {k: expected[k] for k in ("bytes", "sha256")}:
        raise ValueError(f"Source does not match audited fingerprint: {source.name}")
    shutil.copyfile(source, target)
    if file_meta(target) != {k: expected[k] for k in ("bytes", "sha256")}:
        raise ValueError(f"Copy does not match audited fingerprint: {source.name}")


def csv_rows(path):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        header = next(reader, [])
        if not header:
            raise ValueError(f"Empty CSV: {path.name}")
        rows = list(reader)
    if any(len(row) != len(header) for row in rows):
        raise ValueError(f"Inconsistent CSV row width: {path.name}")
    return header, rows


def records(path, expected):
    header, rows = csv_rows(path)
    if tuple(header) != tuple(expected):
        raise ValueError(f"Unexpected schema: {path.name}")
    return [dict(zip(header, row)) for row in rows]


def write_csv(path, header, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def label(value):
    result = normalized_label(value)
    if not result or any(ord(c) < 32 for c in result):
        raise ValueError("Blank or invalid sample identifier")
    return result


def number(value, upper=None):
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("Invalid numerical value") from exc
    if not result.is_finite() or result < 0 or (upper is not None and result > upper):
        raise ValueError("Numerical value out of range")
    return result


def close(a, b):
    return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-8)


def fap_tables(src, out):
    header, rows = csv_rows(src / FAP8)
    functions = header[1:]
    if not functions or len(set(functions)) != len(functions) or any(
        not name.strip() or name != name.strip() for name in functions
    ):
        raise ValueError("Invalid function headers")
    profiles = {}
    for row in rows:
        ident = label(row[0])
        if ident in profiles:
            raise ValueError("Duplicate FAPROTAX profile label")
        profiles[ident] = [number(v, 1) for v in row[1:]]
    coverage = {}
    for row in records(src / FAP9, ("Sample", "Annotated_frac", "Annotated_pct")):
        ident = label(row["Sample"])
        if ident in coverage:
            raise ValueError("Duplicate FAPROTAX coverage label")
        frac = number(row["Annotated_frac"], 1)
        pct = number(row["Annotated_pct"], 100)
        if abs(frac * 100 - pct) > Decimal("0.010001"):
            raise ValueError("Coverage fraction and percent disagree")
        coverage[ident] = (frac, pct)
    if not profiles or set(profiles) != set(coverage):
        raise ValueError("FAPROTAX sample sets differ or are empty")
    write_csv(out / "functional_profiles.csv", ("sample_id", "function_code", "otu_fraction"),
              ((ident, function, fraction)
               for ident, vals in sorted(profiles.items())
               for function, fraction in zip(functions, vals)))
    write_csv(out / "annotation_coverage.csv", ("sample_id", "annotated_read_fraction",
                                                "annotated_read_percent"),
              ((ident, *values) for ident, values in sorted(coverage.items())))
    write_csv(out / "function_definitions.csv", ("function_code",),
              ((name,) for name in sorted(functions)))
    return {
        "samples": len(profiles), "functions": len(functions),
        "profile_rows": len(profiles) * len(functions),
        "zero_profiles": sum(not any(v) for v in profiles.values()),
        "overlapping_profile_rows": sum(
            sum(v) > 1 + Decimal(len(functions)) * Decimal("0.0000005") + Decimal("0.0000000001")
            for v in profiles.values()),
        "median_annotated_read_percent": float(median(v[1] for v in coverage.values())),
    }


def fungal_tables(out, manifest):
    definitions = records(out / FUN_FILES[2], FUN_HEADERS[2])
    guilds = {r["guild"] for r in definitions}
    primaries = [r["primary_guild"] for r in definitions]
    if not guilds or "" in guilds or "" in primaries or len(set(primaries)) != len(primaries):
        raise ValueError("Invalid guild definitions")
    coverage = {}
    status = Counter()
    for row in records(out / FUN_FILES[1], FUN_HEADERS[1]):
        ident = label(row["sample_id"])
        if ident != row["sample_id"] or ident in coverage:
            raise ValueError("Duplicate or noncanonical ITS label")
        state = row["status"]
        status[state] += 1
        if state == "observed":
            total, assigned, unassigned = (number(row[k]) for k in
                                          ("total_reads", "assigned_reads", "unassigned_reads"))
            frac = number(row["assigned_fraction"], 1)
            if not total or not close(total, assigned + unassigned) or not close(frac, assigned / total):
                raise ValueError("ITS coverage does not conserve reads")
            coverage[ident] = (total, assigned, frac)
        else:
            if state not in {"not_found", "not_reconstructed", "zero_reconstructed_reads"}:
                raise ValueError("Unknown ITS coverage status")
            fields = ("total_reads", "assigned_reads", "unassigned_reads")
            if row["assigned_fraction"] or (
                any(number(row[k]) != 0 for k in fields) if state == "zero_reconstructed_reads"
                else any(row[k] for k in fields)
            ):
                raise ValueError("Missing/zero ITS status has invalid measurements")
            coverage[ident] = None
    seen = set()
    observed_guilds = set()
    counts = defaultdict(Decimal)
    for row in records(out / FUN_FILES[0], FUN_HEADERS[0]):
        ident = row["sample_id"]
        name = row["guild"]
        if (ident, name) in seen or name not in guilds or coverage.get(ident) is None:
            raise ValueError("Invalid ITS profile key or foreign key")
        seen.add((ident, name))
        observed_guilds.add(name)
        total, _, _ = coverage[ident]
        count = number(row["read_count"])
        frac = number(row["fraction"], 1)
        if not count or not close(frac, count / total):
            raise ValueError("ITS guild fraction disagrees with count")
        counts[ident] += count
    for ident, values in coverage.items():
        if values is not None and not close(counts[ident], values[1]):
            raise ValueError("ITS profile sum disagrees with assigned reads")
    if not status["observed"]:
        raise ValueError("No observed ITS samples")
    if (len(coverage) != manifest["expected_samples"]
            or status["observed"] != manifest["observed_samples"]
            or dict(status) != manifest["status_counts"]
            or len(seen) != manifest["profile_rows"]
            or len(observed_guilds) != manifest["distinct_observed_guilds"]):
        raise ValueError("ITS manifest summary disagrees with CSVs")
    percentages = [float(v[2] * 100) for v in coverage.values() if v is not None]
    return {
        "samples": len(coverage), "observed_samples": status["observed"],
        "status_counts": dict(status), "profile_rows": len(seen),
        "observed_guilds": len(observed_guilds),
        "annotation_percent": {"min": min(percentages), "median": median(percentages),
                               "max": max(percentages)},
        "zero_annotation_samples": sum(v == 0 for v in percentages),
    }


def column(name, datatype, description, required=True):
    return {"name": name, "titles": name, "datatype": datatype,
            "dc:description": description, "required": required}


def table(url, columns, key, foreign=()):
    schema = {"columns": columns, "primaryKey": key}
    if foreign:
        schema["foreignKeys"] = [
            {"columnReference": col, "reference": {"resource": resource, "columnReference": target}}
            for col, resource, target in foreign]
    return {"url": url, "tableSchema": schema}


def metadata(kind):
    sid = column("sample_id", "string", "ECHOREPO sample label; publication approval pending.")
    fraction = lambda name, desc, required=True: column(name, {"base": "decimal", "minimum": 0, "maximum": 1}, desc, required)
    count = lambda name, desc, required=True: column(name, {"base": "decimal", "minimum": 0}, desc, required)
    if kind == "faprotax":
        func = column("function_code", "string", "Original FAPROTAX output function identifier.")
        tables = [
            table("functional_profiles.csv", [sid, func, fraction("otu_fraction",
                  "Fraction of present retained OTUs assigned to this function; functions overlap.")],
                  ["sample_id", "function_code"],
                  [("sample_id", "annotation_coverage.csv", "sample_id"),
                   ("function_code", "function_definitions.csv", "function_code")]),
            table("annotation_coverage.csv", [sid, fraction("annotated_read_fraction",
                  "Fraction of retained reads assigned at least one function."),
                  column("annotated_read_percent", {"base": "decimal", "minimum": 0, "maximum": 100},
                         "Same read coverage expressed as percent.")], "sample_id"),
            table("function_definitions.csv", [func], "function_code"),
        ]
    else:
        g = column("guild", "string", "ECHOREPO mapped macro-guild label.")
        tables = [
            table("guild_profiles.csv", [sid, g, count("read_count", "Positive assigned reads."),
                  fraction("fraction", "Fraction of all positive reconstructed ITS reads.")],
                  ["sample_id", "guild"], [("sample_id", "annotation_coverage.csv", "sample_id")]),
            table("annotation_coverage.csv", [
                sid, column("status", "string", "Reconstruction/observation status."),
                column("source_kind", "string", "Current archive, structured or legacy source."),
                count("total_reads", "All positive reconstructed ITS reads.", False),
                count("assigned_reads", "Reads in accepted mapped guilds.", False),
                count("unassigned_reads", "Remaining reconstructed reads.", False),
                fraction("assigned_fraction", "Assigned reads / total; null if unavailable or zero total.", False),
            ], "sample_id"),
            table("guild_definitions.csv", [g, column("primary_guild", "string", "Primary guild mapped by ECHOREPO.")],
                  "primary_guild"),
        ]
    return {"@context": "http://www.w3.org/ns/csvw", "dc:title": f"ECHOREPO {kind} derived profiles",
            "dialect": {"encoding": "utf-8", "delimiter": ",", "header": True},
            "tables": tables}


def finish(out, kind, stats, provenance):
    write_json(out / "file.json", metadata(kind))
    if kind == "faprotax":
        explanation = (
            "Each sample/function pair is retained, including zeros. The denominator is present "
            "retained OTUs, not sequencing reads. Functions overlap; sums can exceed 1. "
            "Annotation coverage uses retained sequencing reads. Values retain original rounding. "
            "Function identifiers are the original output labels, not a redistributed reference "
            "database or independently curated biological definitions. Zero coverage alone does "
            "not distinguish no retained reads from no accepted assignments."
        )
    else:
        explanation = (
            "Positive mapped guild rows are sparse, without a 1% cutoff. The denominator is all "
            "positive reconstructed ITS reads, including unassigned reads. Absent profile rows "
            "mean zero for an observed sample only. Missing reconstruction is not zero coverage. "
            "Genus matching selects one mapped macro guild per genus using Probable or Highly "
            "Probable confidence (including the source misspelling); first encountered wins ties. "
            "Macro categories are ECHOREPO groupings, not direct FUNGuild categories."
        )
    (out / "README.md").write_text(
        f"# ECHOREPO {kind} derived data — local candidate\n\n"
        + explanation + "\n\nThese are predictions, not direct measurements of activity. "
        "See file.json for column metadata and provenance.json for source and limitations. "
        "Public identifiers, reference version/terms, source lineage and release metadata "
        "require review. No publication or licence approval is implied.\n",
        encoding="utf-8")
    write_json(out / "provenance.json", {**provenance, "statistics": stats,
               "files": {p.name: file_meta(p) for p in sorted(out.iterdir())},
               "publication_ready": False})
    return stats


def prepare(fap_dir, audit_path, fungal_dir, out, code_commit):
    if out.exists():
        raise FileExistsError(out)
    audit = read_json(audit_path)
    manifest = read_json(fungal_dir / "provenance.local.json")
    if (audit["phase"] != "guild-publication-A" or manifest["schema_version"] != "0.1"
            or manifest["marker"] != "ITS" or manifest["sample_filter_applied"]
            or manifest["display_cutoff_applied"] or manifest["tracked_worktree_dirty"]):
        raise ValueError("Expected full clean Phase A and ITS export reports")
    if audit["files"]["FUNGuild_db.json"]["sha256"] != manifest["reference_sha256"]:
        raise ValueError("Reference fingerprint differs from audit")
    out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".guild-candidates-", dir=out.parent) as tmp:
        stage = Path(tmp) / "bundle"
        stage.mkdir(mode=0o700)
        fap, fun, sources = (stage / n for n in ("faprotax", "funguild", "_inputs"))
        for path in (fap, fun, sources):
            path.mkdir()
        for name in (FAP8, FAP9):
            checked_copy(fap_dir / name, sources / name, audit["files"][name])
        fap_stats = fap_tables(sources, fap)
        expected = audit["observations"]
        if (fap_stats["samples"] != expected[FAP8]["rows"]
                or fap_stats["samples"] != expected[FAP9]["rows"]
                or fap_stats["functions"] != expected[FAP8]["function_columns"]
                or not audit["faprotax_label_alignment"]["sets_match"]):
            raise ValueError("FAPROTAX outputs disagree with audit")
        for name in FUN_FILES:
            checked_copy(fungal_dir / name, fun / name, manifest["files"][name])
        fun_stats = fungal_tables(fun, manifest)
        # Keep private input associations out of both candidate directories.
        inventory = file_meta(fungal_dir / "source_inventory.local.json")
        if inventory != manifest["files"]["source_inventory.local.json"]:
            raise ValueError("ITS internal inventory fingerprint mismatch")
        shutil.rmtree(sources)
        common = {"preparation_commit": code_commit,
                  "prepared_at_utc": datetime.now(timezone.utc).isoformat(),
                  "limitations": ["Source lineage to a pinned public release is not yet established.",
                                  "Public sample identifiers and reference terms require approval."]}
        finish(fap, "faprotax", fap_stats, {
            **common, "audit_commit": audit["report_commit"],
            "source_files": {name: audit["files"][name] for name in (FAP8, FAP9)},
            "analysis_reference_version": None,
            "analysis_commit": None,
            "analysis_parameters": None,
            "input_inventory": audit["input_inventory"],
        })
        finish(fun, "funguild", fun_stats, {
            **common, "analysis_commit": manifest["code_commit"],
            "analysis_started_at_utc": manifest["started_at_utc"],
            "analysis_generated_at_utc": manifest["generated_at_utc"],
            "reference_sha256": manifest["reference_sha256"],
            "reference_version": manifest["reference_version"],
            "confidence": manifest["confidence"], "tie_rule": manifest["tie_rule"],
            "source_manifest_sha256": file_meta(fungal_dir / "provenance.local.json")["sha256"],
            "limitations": common["limitations"] + manifest["limitations"],
        })
        summary = {"faprotax": fap_stats, "funguild": fun_stats, "publication_ready": False,
                   "candidate_manifests": {name: file_meta(stage / name / "provenance.json")
                                           for name in ("faprotax", "funguild")}}
        write_json(stage / "preparation-report.json", summary)
        if out.exists():
            raise FileExistsError(out)
        stage.rename(out)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--faprotax-dir", type=Path, required=True)
    parser.add_argument("--audit", type=Path, required=True)
    parser.add_argument("--funguild-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    print(json.dumps(prepare(args.faprotax_dir, args.audit, args.funguild_dir,
                             args.out_dir, commit), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
