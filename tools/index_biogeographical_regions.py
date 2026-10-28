#!/usr/bin/env python3
"""Index EEA 2016 biogeographical regions for ECHOREPO /search.

Inputs: official EEA 2016 polygon SHP/GPKG and ECHOREPO enriched source CSV.
Uses ORIGINAL GPS_lat / GPS_long, never public jittered sample coordinates.
Writes only sample_id -> region_name into PostgreSQL, not exact coordinates.

Run with --dry-run first. Run after the canonical/Postgres sync completes.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

SCRIPT_ROOT = Path(__file__).resolve().parent.parent if Path(__file__).resolve().parent.name == "tools" else Path.cwd()
from dotenv import load_dotenv
# Normally run next to the pipeline, even if the pipeline's working data root
# is a different (e.g. production) checkout. Respect that same PROJECT_ROOT.
load_dotenv(Path.cwd() / ".env")
ROOT = Path(os.getenv("PROJECT_ROOT") or SCRIPT_ROOT).resolve()
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from shapely.ops import unary_union
from shapely.prepared import prep

LOG = logging.getLogger("biogeo")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
VERSION = "EEA-2016-v1"


def clean_region(value: object) -> str:
    text = str(value or "").strip()
    for suffix in (" Bio-geographical Region", " Biogeographical Region", " biogeographical region"):
        if text.casefold().endswith(suffix.casefold()):
            text = text[: -len(suffix)].strip()
            break
    return text


def read_regions(path: Path):
    frame = gpd.read_file(path)
    if frame.crs is None:
        raise ValueError("EEA polygon layer has no declared CRS; do not guess one")
    cols = {str(col).casefold(): col for col in frame.columns}
    name_col = cols.get("name") or cols.get("short_name")
    if name_col is None:
        raise ValueError(f"Polygon layer has no name/short_name field: {list(frame.columns)}")
    frame = frame.to_crs("EPSG:4326")
    groups = {}
    for _, row in frame.iterrows():
        name = clean_region(row[name_col])
        geom = row.geometry
        if name and geom is not None and not geom.is_empty:
            groups.setdefault(name, []).append(geom)
    if not groups:
        raise ValueError("No polygon geometries with region names")
    prepared = [(name, prep(unary_union(parts))) for name, parts in sorted(groups.items())]
    LOG.info("Loaded %d region names: %s", len(prepared), ", ".join(name for name, _ in prepared))
    return prepared


def valid_coord(value: object, axis: str):
    # Use exactly the same coordinate interpretation as the canonical ingest.
    from echorepo.utils.load_csv import _parse_coord
    try:
        n = _parse_coord(value, axis)
    except (TypeError, ValueError):
        return None
    if n is None:
        return None
    import math
    try:
        n = float(n)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(n) or (axis == "lat" and not -90 <= n <= 90) or (axis == "lon" and not -180 <= n <= 180):
        return None
    return n


def _true(value):
    return str(value or "").strip().casefold() in ("true", "1", "yes", "on", "y")


def region_for(lat, lon, prepared):
    point = Point(lon, lat)
    matches = [name for name, geom in prepared if geom.intersects(point)]
    # An exact point on a region boundary can touch 2 regions; do not assign arbitrarily.
    return matches[0] if len(matches) == 1 else None


def rows_from_csv(csv_path: Path, prepared):
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    required = {"sampleId", "QR_qrCode", "GPS_lat", "GPS_long"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing columns: {', '.join(sorted(missing))}")
    # Same ordering used when the canonical importer chooses the most recent QR.
    sort_cols = [c for c in ("fs_updatedAt", "collectedAt", "fs_createdAt") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, na_position="last")
    df["_id"] = df.apply(lambda r: (r["QR_qrCode"].strip().replace(" ", "").upper()
                                    or r["sampleId"].strip()), axis=1)
    df = df[df["_id"].ne("")].drop_duplicates("_id", keep="last")
    out = []
    invalid = outside = flagged = 0
    for _, row in df.iterrows():
        if _true(row.get("wrong_coordinates_effective", "")):
            flagged += 1
            continue
        lat = valid_coord(row["GPS_lat"], "lat")
        lon = valid_coord(row["GPS_long"], "lon")
        if lat is None or lon is None:
            invalid += 1
            continue
        region = region_for(lat, lon, prepared)
        if not region:
            outside += 1
            continue
        out.append((row["_id"], region, VERSION))
    LOG.info("Source IDs=%d classified=%d QA-flagged=%d invalid coordinates=%d no unique EEA region=%d",
             len(df), len(out), flagged, invalid, outside)
    return out, len(df)


def write_postgres(rows, source_count, dry_run=False):
    if dry_run:
        from collections import Counter
        for name, number in sorted(Counter(r[1] for r in rows).items()):
            LOG.info("DRY RUN: %-16s %d", name, number)
        return
    if source_count <= 0 or not rows:
        raise RuntimeError(
            f"Refusing to replace existing index: source_count={source_count}, "
            f"classified={len(rows)} (check CSV, GPS, CRS and polygon layer)"
        )

    import psycopg2
    from psycopg2.extras import execute_values
    with psycopg2.connect(
        host=os.getenv("DB_HOST_OUTSIDE") or os.getenv("DB_HOST") or "localhost",
        port=int(os.getenv("DB_PORT_OUTSIDE") or os.getenv("DB_PORT") or 5432),
        dbname=os.getenv("DB_NAME", "echorepo"),
        user=os.getenv("DB_USER", "echorepo"),
        password=os.environ["DB_PASSWORD"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM samples")
            total = cur.fetchone()[0]
            if total == 0 or source_count < total * 0.75:
                raise RuntimeError(f"Source CSV count ({source_count}) is too small for PostgreSQL samples ({total}); refusing index replacement")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sample_biogeo_regions (
                    sample_id TEXT PRIMARY KEY REFERENCES samples(sample_id) ON DELETE CASCADE,
                    region_name TEXT NOT NULL,
                    dataset_version TEXT NOT NULL
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS ix_sample_biogeo_region ON sample_biogeo_regions (region_name, sample_id)")
            cur.execute("CREATE TEMP TABLE biogeo_stage (sample_id TEXT PRIMARY KEY, region_name TEXT, dataset_version TEXT) ON COMMIT DROP")
            execute_values(cur, "INSERT INTO biogeo_stage (sample_id, region_name, dataset_version) VALUES %s", rows, page_size=1000)

            # Guard against Firestore-document-ID / canonical-QR-ID mismatches.
            # An empty JOIN must never clear an otherwise healthy region table.
            cur.execute("""
                SELECT COUNT(*) FROM biogeo_stage st
                JOIN samples s ON s.sample_id = st.sample_id
            """)
            matched = cur.fetchone()[0]
            LOG.info("PostgreSQL sample-ID matches: %d of %d classified", matched, len(rows))
            if matched == 0:
                raise RuntimeError("No classified sample IDs match PostgreSQL; refusing to replace existing index")

            # Single transaction: if anything fails, old index remains unchanged.
            cur.execute("DELETE FROM sample_biogeo_regions")
            cur.execute("""
                INSERT INTO sample_biogeo_regions (sample_id, region_name, dataset_version)
                SELECT st.sample_id, st.region_name, st.dataset_version
                FROM biogeo_stage st JOIN samples s USING (sample_id)
            """)
            LOG.info("Updated biogeographical index: %d matching sample records", cur.rowcount)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--regions", type=Path, required=True, help="Official EEA 2016 polygon .shp/.gpkg")
    parser.add_argument("--samples", type=Path, default=ROOT / "data/echorepo_samples_with_email.csv")
    parser.add_argument("--dry-run", action="store_true", help="Classify and summarize, do not connect to Postgres")
    args = parser.parse_args()
    for path in (args.regions, args.samples):
        if not path.is_file():
            parser.error(f"Missing file: {path}")
    regions = read_regions(args.regions)
    rows, count = rows_from_csv(args.samples, regions)
    write_postgres(rows, count, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
