#!/usr/bin/env python3
"""Create ECHOREPO's LOESS layer from the full /api/v1/survey/all download.

IMPORTANT: LOESS location.x is LATITUDE; location.y is LONGITUDE.
GeoJSON Point.coordinates must be [longitude, latitude] => [location.y, location.x].

Public map defaults to ACCEPTED observations only. No submitter/user information,
filenames, or image references are copied. The full source JSON stays untouched.
Requires only Python 3.9+ standard library.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

SOURCE_URL = "https://www.loess-soil-map.eu/map"
API_URL = "https://loess.wmi.amu.edu.pl/api/v1/survey/all"

CATEGORY_MAP = {
    "DRY_SOILS": "dry",
    "WET_SOILS": "wet",
    "EROSION": "erosion",
    "SEALED_SOILS": "sealed",
    "DEGRADATION": "degradation",
    "LOSS_OF_ORGANIC_MATTER": "organic",
    "PH": "ph",
    "BIODIVERSITY": "biodiversity",
}


def number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        val = float(str(value).strip().replace(",", "."))
    except (ValueError, TypeError):
        return None
    return val if math.isfinite(val) else None


def get_records(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return payload["data"]
    raise ValueError("Expected an array or a JSON object with a data array")


def category_for(value):
    key = str(value or "").strip().upper().replace("-", "_").replace(" ", "_")
    return CATEGORY_MAP.get(key, "other")


def loess_coords(record):
    """Return strict GeoJSON [lon,lat] or None; NO x/y heuristics or swapping."""
    location = record.get("location") if isinstance(record, dict) else None
    if not isinstance(location, dict):
        return None
    lat = number(location.get("x"))  # LOESS x = LATITUDE
    lon = number(location.get("y"))  # LOESS y = LONGITUDE
    if lat is None or lon is None or not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return None
    # GeoJSON always uses longitude first.
    return [lon, lat]


def clean_text(val):
    return str(val) if isinstance(val, (str, int, float)) and not isinstance(val, bool) else None


def make_feature(record):
    coords = loess_coords(record)
    if coords is None:
        return None
    loc = record["location"]
    area = number(record.get("affectedArea"))
    radius = area if area is not None and 0 <= area <= 100000 else None
    category_raw = clean_text(record.get("category"))
    source_id = clean_text(record.get("id"))
    properties = {
        "source": "LOESS Soil Map",
        "source_id": source_id,
        "category": category_for(category_raw),
        "category_original": category_raw,
        "place_name": clean_text(loc.get("name")),
        "country_code": clean_text(loc.get("countryCode")),
        "observed_at": clean_text(record.get("createdAt")),
        "radius_m": radius,
        "description": clean_text(record.get("description")),
        "potential_cause": clean_text(record.get("solution")),
        "status": clean_text(record.get("status")),
    }
    return {
        "type": "Feature",
        "id": source_id,
        "geometry": {"type": "Point", "coordinates": coords},
        "properties": properties,
    }


def compare_existing(path, records):
    """Compare to an old GeoJSON using LOESS IDs, report normal vs swapped.

    This diagnoses an old, geographically inverted export without modifying it.
    """
    if not path.exists():
        return {"exists": False}
    try:
        old = json.loads(path.read_text(encoding="utf-8-sig"))
        existing = {}
        for feat in old["features"]:
            fid = feat.get("properties", {}).get("source_id", feat.get("id"))
            if fid is not None:
                existing[str(fid)] = feat.get("geometry", {}).get("coordinates")
        matches, reversed_matches, other = 0, 0, 0
        examples = []
        for rec in records:
            if not isinstance(rec, dict):
                continue
            sid = rec.get("id")
            coord = loess_coords(rec)
            if sid is None or coord is None or str(sid) not in existing:
                continue
            current = existing[str(sid)]
            if not isinstance(current, list) or len(current) < 2:
                other += 1
                continue
            old_lon, old_lat = number(current[0]), number(current[1])
            if old_lon is None or old_lat is None:
                other += 1
            elif math.isclose(old_lon, coord[0], abs_tol=1e-7) and math.isclose(
                old_lat, coord[1], abs_tol=1e-7
            ):
                matches += 1
            elif math.isclose(old_lon, coord[1], abs_tol=1e-7) and math.isclose(
                old_lat, coord[0], abs_tol=1e-7
            ):
                reversed_matches += 1
                if len(examples) < 5:
                    examples.append(sid)
            else:
                other += 1
        return {
            "exists": True,
            "matching_correct": matches,
            "matching_reversed": reversed_matches,
            "matching_other": other,
            "example_reversed_ids": examples,
        }
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return {"exists": True, "error": str(exc)}


def build(records, all_statuses=False):
    features = []
    statuses = Counter()
    raw_categories = Counter()
    mapped_categories = Counter()
    skipped_invalid = []
    skipped_status = Counter()
    invalid_area = []
    unknown_category_ids = []
    for index, rec in enumerate(records):
        if not isinstance(rec, dict):
            skipped_invalid.append({"index": index, "id": None, "reason": "non-object record"})
            continue
        status = str(rec.get("status") or "MISSING").upper()
        statuses[status] += 1
        raw = str(rec.get("category") or "MISSING")
        raw_categories[raw] += 1
        cat = category_for(raw)
        if cat == "other":
            unknown_category_ids.append({"id": rec.get("id"), "category": raw, "status": status})
        if not all_statuses and status != "ACCEPTED":
            skipped_status[status] += 1
            continue
        feature = make_feature(rec)
        if feature is None:
            skipped_invalid.append(
                {
                    "index": index,
                    "id": rec.get("id"),
                    "reason": "invalid/missing location.x or location.y",
                }
            )
            continue
        if feature["properties"]["radius_m"] is None and rec.get("affectedArea") is not None:
            invalid_area.append(rec.get("id"))
        features.append(feature)
        mapped_categories[cat] += 1
    report = {
        "source": API_URL,
        "record_count": len(records),
        "statuses_in_source": dict(statuses),
        "original_categories_in_source": dict(raw_categories),
        "map_filter": "all statuses (QA only)" if all_statuses else "ACCEPTED only",
        "excluded_by_status": dict(skipped_status),
        "geolocated": len(features),
        "map_categories": dict(mapped_categories),
        "invalid_coordinates": skipped_invalid,
        "unknown_categories": unknown_category_ids,
        "invalid_radius_ids": invalid_area,
        "coordinate_convention": "LOESS location.x=latitude, location.y=longitude; GeoJSON=[location.y, location.x]",
        "privacy": "Output copies whitelisted observation properties only; never user, user permissions, filePath or photos.",
    }
    return features, report


def validate_output(features, records):
    """Assert no accidental x/y swap can survive a build."""
    index = {
        str(r.get("id")): r for r in records if isinstance(r, dict) and r.get("id") is not None
    }
    for feature in features:
        raw = index.get(str(feature["id"]))
        if raw:
            expected = loess_coords(raw)
            if feature["geometry"]["coordinates"] != expected:
                raise AssertionError(f"Coordinate order failure for LOESS id={feature['id']}")
    if any("user" in f["properties"] or "filePath" in f["properties"] for f in features):
        raise AssertionError("Private fields found in output properties")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("input", type=Path, help="The FULL loess_export_v4/surveys_raw.json")
    ap.add_argument("--out", type=Path, default=Path("static/geo/loess_surveys.geojson"))
    ap.add_argument("--report", type=Path, default=Path("loess_conversion_report.json"))
    ap.add_argument(
        "--all-statuses", action="store_true", help="QA ONLY: also export REJECTED/PENDING records"
    )
    args = ap.parse_args(argv)
    records = get_records(json.loads(args.input.read_text(encoding="utf-8-sig")))
    features, report = build(records, all_statuses=args.all_statuses)
    report["previous_geojson"] = compare_existing(args.out, records)
    validate_output(features, records)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    if not features:
        print(
            "ERROR: No geolocated records; output not overwritten. Check " + str(args.report),
            file=sys.stderr,
        )
        return 2
    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "source": SOURCE_URL,
            "api_source": API_URL,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "filter": report["map_filter"],
            "notes": "Citizen-reported soil conditions; radius_m is the reported affected radius, not coordinate uncertainty.",
        },
        "features": features,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(geojson, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )
    # Check persisted output, too.
    reloaded = json.loads(args.out.read_text(encoding="utf-8"))
    validate_output(reloaded["features"], records)
    old = report["previous_geojson"]
    if old.get("matching_reversed"):
        print(
            f"Existing file had {old['matching_reversed']} REVERSED records; replaced with corrected coordinates."
        )
    print(f"Source records: {len(records)}; statuses: {dict(report['statuses_in_source'])}")
    print(f"Exported map features: {len(features)} ({report['map_filter']})")
    print(f"Categories: {dict(report['map_categories'])}")
    print(
        f"Unknown source categories: {len(report['unknown_categories'])}; invalid coordinates: {len(report['invalid_coordinates'])}"
    )
    print("GeoJSON:", args.out)
    print("Report:", args.report)
    if report["unknown_categories"]:
        print("WARNING: unknown categories found; see report for IDs and names")
    return 0


if __name__ == "__main__":
    sys.exit(main())
