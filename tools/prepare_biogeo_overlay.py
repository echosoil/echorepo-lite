#!/usr/bin/env python3
"""Build a small, static EEA 2016 biogeographical regions GeoJSON for Leaflet.

Run from the ECHOREPO repository root. The original polygons remain untouched.
Requires geopandas, shapely, pyproj, and a shapefile reader (pyogrio/fiona).
"""
from __future__ import annotations

import argparse
from pathlib import Path


def clean_region(value: object) -> str:
    text = str(value or "").strip()
    for suffix in (
        " Bio-geographical Region",
        " Biogeographical Region",
        " biogeographical region",
    ):
        if text.casefold().endswith(suffix.casefold()):
            text = text[: -len(suffix)].strip()
            break
    return text


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--regions",
        type=Path,
        default=Path("data/biogeo_eea2016/BiogeoRegions2016.shp"),
        help="Input EEA 2016 shapefile on the server host",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("static/geo/biogeo_regions_2016.geojson"),
        help="Static GeoJSON for Leaflet (top-level Flask static folder)",
    )
    parser.add_argument(
        "--tolerance-m",
        type=float,
        default=2000,
        help="Display-only geometry simplification tolerance in metres (default: 2000)",
    )
    args = parser.parse_args()
    if not args.regions.is_file():
        parser.error(f"Regions shapefile does not exist: {args.regions}")
    if not (0 <= args.tolerance_m <= 10000):
        parser.error("--tolerance-m must be between 0 and 10000 metres")

    import geopandas as gpd

    source = gpd.read_file(args.regions)
    if source.crs is None:
        parser.error("Source shapefile has no CRS. Refusing to guess its projection.")
    columns = {str(c).casefold(): c for c in source.columns}
    name_col = columns.get("name") or columns.get("short_name")
    if name_col is None:
        parser.error(f"Missing name/short_name field. Available columns: {list(source.columns)}")

    # Use the same region-name normalisation as the EEA/Postgres indexer.
    source = source[[name_col, source.geometry.name]].copy()
    source["region_name"] = source[name_col].map(clean_region)
    source = source[source.geometry.notna() & ~source.geometry.is_empty & source["region_name"].ne("")]
    if source.empty:
        parser.error("No named polygon geometries found")
    source = source.set_geometry(source.geometry.name).to_crs("EPSG:3035")
    # Combine fragments belonging to the same region, rather than sending
    # every source polygon as an independent map feature.
    dissolved = source[["region_name", source.geometry.name]].dissolve(by="region_name", as_index=False)
    if args.tolerance_m:
        dissolved.geometry = dissolved.geometry.simplify(args.tolerance_m, preserve_topology=True)
    simplified = dissolved.to_crs("EPSG:4326")
    simplified = simplified[simplified.geometry.notna() & ~simplified.geometry.is_empty]
    if simplified.empty:
        parser.error("No polygons survived simplification")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    simplified.to_file(args.output, driver="GeoJSON")
    print(f"Wrote {args.output.resolve()} ({args.output.stat().st_size / 1024 / 1024:.2f} MiB)")
    print("Regions:", ", ".join(sorted(simplified["region_name"].astype(str))))
    print("Public URL: /static/geo/biogeo_regions_2016.geojson")


if __name__ == "__main__":
    main()
