#!/usr/bin/env python3
"""Add externally hosted photo URLs to already-generated LOESS GeoJSON.

Use ONLY a verified public HTTPS photo endpoint, with {filePath} and/or {id}
as the URL placeholder. Reads the full survey API JSON, but never copies user
objects, raw filePath fields or non-ACCEPTED observations into GeoJSON.

Example (URL TEMPLATE IS AN EXAMPLE, NOT A REAL LOESS ENDPOINT):
  python tools/enrich_loess_photo_urls.py surveys_raw.json \\
    --template 'https://VERIFIED-HOST/VERIFIED-PATH/{filePath}'
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import quote, urlsplit


def records_from(data):
    if isinstance(data, dict):
        data = data.get("data")
    if not isinstance(data, list):
        raise ValueError("Expected list of surveys or object with a data array")
    return [record for record in data if isinstance(record, dict)]


def validate_template(template):
    if "{filePath}" not in template and "{id}" not in template:
        raise ValueError("Template must include {filePath} or {id}")
    parsed = urlsplit(template)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("Image URL must be public HTTPS without embedded credentials")
    if any(part in template.lower() for part in ("access_token=", "signature=", "api_key=", "secret=")):
        raise ValueError("Do not embed tokens or signed URL parameters in public GeoJSON")


def photo_url(record, template):
    filepath = record.get("filePath")
    # This is how LOESS identifies surveys with an attached photo.
    if not isinstance(filepath, str) or not filepath.strip():
        return None
    identifier = record.get("id")
    if identifier is None:
        return None
    url = template.replace("{filePath}", quote(filepath.strip(), safe=""))
    url = url.replace("{id}", quote(str(identifier), safe=""))
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("Generated URL is invalid or unsafe")
    return url


def enrich(raw, geojson, template):
    validate_template(template)
    survey_index = {str(rec["id"]): rec for rec in records_from(raw) if rec.get("id") is not None}
    features = geojson.get("features")
    if geojson.get("type") != "FeatureCollection" or not isinstance(features, list):
        raise ValueError("Expected FeatureCollection GeoJSON")

    stats = Counter()
    for feature in features:
        props = feature.get("properties") or {}
        # Remove old URL first, avoiding stale image links after a refresh.
        props.pop("image_url", None)
        feature["properties"] = props
        source_id = props.get("source_id", feature.get("id"))
        record = survey_index.get(str(source_id))
        if record is None:
            stats["no_source_record"] += 1
            continue
        if str(record.get("status") or "").upper() != "ACCEPTED" or str(props.get("status") or "").upper() != "ACCEPTED":
            stats["not_accepted"] += 1
            continue
        url = photo_url(record, template)
        if url:
            props["image_url"] = url
            stats["with_photo_url"] += 1
        else:
            stats["without_photo"] += 1
    return dict(stats)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("raw", type=Path, help="Full surveys_raw.json, kept privately")
    ap.add_argument("--geojson", type=Path, default=Path("static/geo/loess_surveys.geojson"))
    ap.add_argument("--template", required=True, help="Verified public HTTPS URL containing {filePath} and/or {id}")
    args = ap.parse_args(argv)
    raw = json.loads(args.raw.read_text(encoding="utf-8-sig"))
    data = json.loads(args.geojson.read_text(encoding="utf-8-sig"))
    counts = enrich(raw, data, args.template)
    # Existing file must not be touched if validation failed.
    args.geojson.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print("LOESS GeoJSON updated:", args.geojson)
    for name, count in sorted(counts.items()):
        print(f"  {name}: {count}")
    if not counts.get("with_photo_url", 0):
        print("WARNING: No photo URLs were added. Check the source filePath fields.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
