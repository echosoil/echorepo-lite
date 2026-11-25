"""Prepare local, unfiltered FUNGuild macro-guild tables (no publication)."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from tools import generate_biodiversity_piecharts as guild


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_csv(path, fields, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def build_tables(frame, annotations, inventory, requested=None):
    """Return sparse positive profiles and one coverage row per expected sample."""
    profiles = []
    coverage = []
    sources = {key: dict(value) for key, value in inventory.items()}
    if not frame.empty:
        frame = frame.copy()
        if frame["sample_id"].isna().any():
            raise ValueError("Missing sample label in ITS source")
        frame["sample_id"] = frame["sample_id"].astype(str).str.strip().str.upper()
        if (frame["sample_id"] == "").any():
            raise ValueError("Blank sample label in ITS source")
    groups = dict(tuple(frame.groupby("sample_id"))) if not frame.empty else {}
    unexpected = set(groups) - set(sources)
    if unexpected:
        raise ValueError("ITS source rows missing from source inventory")
    if requested is not None:
        if set(sources) - requested:
            raise ValueError("Unexpected source outside requested sample filter")
        for ident in requested - set(sources):
            sources[ident] = {"source_kind": "unknown", "source_status": "not_found"}
    for ident, source in sorted(sources.items()):
        row = dict(
            sample_id=ident, status=source["source_status"],
            source_kind=source["source_kind"], total_reads=None,
            assigned_reads=None, unassigned_reads=None, assigned_fraction=None,
        )
        if ident in groups:
            sample = groups[ident]
            counts = pd.to_numeric(sample["count"], errors="coerce")
            if counts.isna().any() or not counts.map(math.isfinite).all() or (counts < 0).any():
                raise ValueError("Invalid reconstructed ITS count")
            grouped, total, assigned = guild.calculate_fungal_guilds(sample, annotations)
            if not math.isclose(float(grouped["count"].sum()), assigned, rel_tol=1e-10, abs_tol=1e-8):
                raise ValueError("Guild counts do not conserve assigned reads")
            if assigned > total and not math.isclose(assigned, total, rel_tol=1e-10):
                raise ValueError("Assigned reads exceed total reads")
            if total > 0:
                row.update(status="observed", total_reads=total, assigned_reads=assigned,
                           unassigned_reads=max(0.0, total-assigned), assigned_fraction=assigned/total)
                for entry in grouped.itertuples(index=False):
                    profiles.append(dict(sample_id=ident, guild=entry.macro,
                                         read_count=float(entry.count),
                                         fraction=float(entry.count)/total))
            else:
                row.update(status="zero_reconstructed_reads", total_reads=0,
                           assigned_reads=0, unassigned_reads=0)
        elif row["status"] == "reconstructed":
            raise ValueError("Reconstructed sample missing source rows")
        coverage.append(row)
    return profiles, coverage, sources


def export(out: Path, frame, annotations, inventory, provenance, requested=None):
    """Write a new private local bundle; never overwrite an existing directory."""
    if out.exists():
        raise FileExistsError(out)
    profiles, coverage, sources = build_tables(frame, annotations, inventory, requested)
    out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".funguild-", dir=out.parent) as tmp:
        stage = Path(tmp) / "bundle"
        stage.mkdir(mode=0o700)
        write_csv(stage / "guild_profiles.csv",
                  ["sample_id", "guild", "read_count", "fraction"], profiles)
        write_csv(stage / "annotation_coverage.csv",
                  ["sample_id", "status", "source_kind", "total_reads", "assigned_reads",
                   "unassigned_reads", "assigned_fraction"], coverage)
        write_csv(stage / "guild_definitions.csv", ["guild", "primary_guild"],
                  [dict(guild=macro, primary_guild=primary)
                   for primary, macro in sorted(guild.FUNGAL_GUILD_MACRO_MAP.items())])
        # Internal-only source associations; do not upload this manifest as-is.
        (stage / "source_inventory.local.json").write_text(
            json.dumps(sources, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        summary = {
            **provenance,
            "expected_samples": len(coverage),
            "observed_samples": sum(r["status"] == "observed" for r in coverage),
            "status_counts": {status: sum(r["status"] == status for r in coverage)
                              for status in sorted({r["status"] for r in coverage})},
            "profile_rows": len(profiles),
            "distinct_observed_guilds": len({r["guild"] for r in profiles}),
            "files": {p.name: {"sha256": sha256(p), "bytes": p.stat().st_size}
                      for p in sorted(stage.iterdir())},
            "publication_ready": False,
        }
        (stage / "provenance.local.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        if out.exists():
            raise FileExistsError(out)
        stage.rename(out)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--reference", type=Path, default=Path(guild.FUNGUILD_DB_JSON))
    parser.add_argument("--sample-id", action="append")
    args = parser.parse_args()
    if args.out_dir.exists():
        parser.error("Choose a new output directory")
    requested = guild.normalize_sample_filter(args.sample_id)
    # Load the exact local reference and check that it stays unchanged during extraction.
    reference_hash = sha256(args.reference)
    annotations = guild.load_funguild_best_by_genus(args.reference)
    if not annotations:
        parser.error("No accepted mapped genera in reference")
    started = datetime.now(timezone.utc).isoformat()
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=guild.DEFAULT_ROOT, text=True).strip()
    dirty = bool(subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        cwd=guild.DEFAULT_ROOT, text=True).strip())
    inventory = {}
    frame = guild.fetch_fungal_guild_source_data(
        guild.init_minio(read_only=True), sample_ids=requested, source_inventory=inventory)
    if sha256(args.reference) != reference_hash:
        raise RuntimeError("FUNGuild reference changed during export")
    summary = export(args.out_dir, frame, annotations, inventory, {
        "schema_version": "0.1",
        "marker": "ITS",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "started_at_utc": started,
        "code_commit": commit,
        "tracked_worktree_dirty": dirty,
        "reference_sha256": reference_hash,
        "reference_version": None,
        "accepted_mapped_genera": len(annotations),
        "sample_filter_applied": requested is not None,
        "denominator": "all positive reconstructed ITS reads",
        "profile_type": "ECHOREPO mapped macro guilds, one assignment per genus",
        "confidence": sorted(guild.FUNGUILD_KEEP_CONFIDENCE),
        "tie_rule": "first encountered at equal confidence score",
        "display_cutoff_applied": False,
        "limitations": [
            "Live source selection; not an immutable snapshot or complete provenance.",
            "not_reconstructed may mean missing/inaccessible source or no positive reads.",
            "Reference release and redistribution terms not verified.",
            "Sample labels and source inventory require privacy review before publication.",
            "Numerical predictions, not direct measurements of ecological activity.",
        ],
    }, requested)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["observed_samples"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
