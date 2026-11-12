#!/usr/bin/env python3
"""Refresh ECHOREPO's LOESS GeoJSON and remote-photo links safely.

The full upstream JSON (including its user data) stays in memory, and only the
existing allowlisted GeoJSON fields plus public image URLs are published.

Designed to run on the Docker host: docker-compose.yml mounts ./static as
read-only *inside* the container. Use cron to schedule this command.

Example:
    python3 tools/refresh_loess_geojson.py
    python3 tools/refresh_loess_geojson.py --dry-run
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import os
import sys
import tempfile
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

# Works both when executed as a standalone script and when imported by tests.
TOOLS_DIR = Path(__file__).resolve().parent
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

import build_loess_geojson as converter  # noqa: E402
import enrich_loess_photo_urls as photos  # noqa: E402

PROJECT_ROOT = TOOLS_DIR.parent
OUTPUT_PATH = PROJECT_ROOT / "static/geo/loess_surveys.geojson"
REPORT_PATH = Path.home() / ".local/state/echorepo/loess_refresh_last.json"
API_URL = converter.API_URL
MAX_RESPONSE_BYTES = 20 * 1024 * 1024
# LOESS Next.js optimizer: the existing popup JavaScript changes w=256 to
# w=1080 only for its click-to-enlarge link.
PHOTO_TEMPLATE = (
    "https://www.loess-soil-map.eu/_next/image?"
    "url=https%3A%2F%2Floess.wmi.amu.edu.pl%2Fsurveyimages%2F{filePath}&w=256&q=75"
)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def fetch_api(timeout=30, opener=None):
    """Download a bounded, HTTPS-only JSON response; never write raw PII."""
    opener = opener or urllib.request.urlopen
    request = urllib.request.Request(
        API_URL,
        headers={
            "Accept": "application/json",
            "User-Agent": "ECHOREPO-LOESS-refresh/1.0 (+https://github.com/echosoil/echorepo-lite)",
        },
    )
    with opener(request, timeout=timeout) as response:
        actual_url = response.geturl()
        target = urlsplit(actual_url)
        if target.scheme != "https" or target.hostname != urlsplit(API_URL).hostname:
            raise RuntimeError(f"Refusing unexpected API redirect to {actual_url!r}")
        raw = response.read(MAX_RESPONSE_BYTES + 1)
    if len(raw) > MAX_RESPONSE_BYTES:
        raise RuntimeError(f"LOESS API response exceeds {MAX_RESPONSE_BYTES} bytes")
    return json.loads(raw.decode("utf-8-sig"))


def load_existing(path):
    if not path.exists():
        return None
    old = json.loads(path.read_text(encoding="utf-8"))
    if old.get("type") != "FeatureCollection" or not isinstance(old.get("features"), list):
        raise ValueError(f"Existing LOESS GeoJSON is invalid: {path}")
    return old


def feature_key(feature):
    """Stable comparison ignoring irrelevant order of records / object keys."""
    return json.dumps(feature, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def equivalent_features(old, new):
    if old is None:
        return False
    return sorted(map(feature_key, old["features"])) == sorted(map(feature_key, new["features"]))


def make_geojson(payload, min_features=50, old=None, min_retained=0.80, min_photos=0.50):
    """Build and validate in memory; raise before changing the live data."""
    records = converter.get_records(payload)
    if not records:
        raise ValueError("LOESS API returned no surveys")
    features, report = converter.build(records)
    converter.validate_output(features, records)
    if len(features) < min_features:
        raise ValueError(
            f"Only {len(features)} accepted geolocated surveys; expected at least {min_features}"
        )
    ids = [str(f.get("id")) for f in features]
    if len(ids) != len(set(ids)):
        raise ValueError("LOESS API contains duplicate accepted observation IDs")

    now = utc_now()
    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "source": converter.SOURCE_URL,
            "api_source": API_URL,
            "generated_at": now,
            "filter": "ACCEPTED only",
            "notes": "Citizen-reported soil conditions; radius_m is an affected radius, not coordinate uncertainty.",
        },
        "features": features,
    }
    images = photos.enrich(payload, geojson, PHOTO_TEMPLATE)
    old_count = len(old["features"]) if old else 0
    new_count = len(features)
    if old_count and new_count < old_count * min_retained:
        raise ValueError(
            f"Suspicious LOESS shrinkage: {old_count} -> {new_count} features "
            f"(minimum retained fraction: {min_retained:.0%}); keeping previous dataset"
        )
    old_images = sum(bool(f.get("properties", {}).get("image_url")) for f in old["features"]) if old else 0
    new_images = images.get("with_photo_url", 0)
    if old_images and new_images < old_images * min_photos:
        raise ValueError(
            f"Suspicious LOESS photo loss: {old_images} -> {new_images} image URLs "
            f"(minimum retained fraction: {min_photos:.0%}); keeping previous dataset"
        )
    # A change in upstream category/status/coordinates must be reflected in the map.
    converter.validate_output(geojson["features"], records)
    report.update(
        {
            "refreshed_at": now,
            "previous_feature_count": old_count,
            "photo_urls": images,
            "previous_photo_count": old_images,
            "changed": not equivalent_features(old, geojson),
        }
    )
    return geojson, report


def atomic_write_json(path, data, mode=0o644):
    """Write in the target filesystem and replace atomically, never partially."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.chmod(name, mode)
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


@contextmanager
def single_run_lock(out):
    """Prevent overlap between manual / cron runs without root privileges."""
    digest = hashlib.sha256(str(out.resolve()).encode("utf-8")).hexdigest()[:16]
    lock_path = Path(tempfile.gettempdir()) / f"echorepo-loess-{os.getuid()}-{digest}.lock"
    with lock_path.open("a+") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EAGAIN, errno.EACCES):
                raise RuntimeError("Another LOESS refresh is running; skipping") from exc
            raise
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def refresh(*, out=OUTPUT_PATH, report_path=REPORT_PATH, source_json=None,
            dry_run=False, min_features=50, min_retained=0.80,
            min_photos=0.50, timeout=30, fetcher=None):
    out = Path(out)
    report_path = Path(report_path)
    with single_run_lock(out):
        if source_json is None:
            payload = (fetcher or fetch_api)(timeout=timeout)
        else:
            payload = json.loads(Path(source_json).read_text(encoding="utf-8-sig"))
        old = load_existing(out)
        geojson, report = make_geojson(
            payload, min_features=min_features, old=old,
            min_retained=min_retained, min_photos=min_photos,
        )
        state = "CHANGED" if report["changed"] else "UNCHANGED"
        print(
            f"{utc_now()} LOESS {state}: source={report['record_count']}, "
            f"accepted_geojson={report['geolocated']}, "
            f"photos={report['photo_urls'].get('with_photo_url', 0)}, "
            f"excluded={report['excluded_by_status']}, "
            f"unknown_categories={len(report['unknown_categories'])}"
        )
        if dry_run:
            print("Dry run: published GeoJSON and diagnostics left unchanged")
            return report
        if report["changed"]:
            atomic_write_json(out, geojson, mode=0o644)
            print(f"Published atomically: {out}")
        else:
            print(f"Data unchanged; keeping current file and mtime: {out}")
        atomic_write_json(report_path, report, mode=0o600)
        return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--report", type=Path, default=REPORT_PATH)
    parser.add_argument("--source-json", type=Path, default=None,
                        help="Offline test from private raw JSON; never published")
    parser.add_argument("--dry-run", action="store_true", help="Fetch/validate, do not write files")
    parser.add_argument("--min-features", type=int, default=50,
                        help="Reject incomplete API responses (default 50)")
    parser.add_argument("--min-retained", type=float, default=0.80,
                        help="Reject losses >20%% of previously published features")
    parser.add_argument("--min-photos", type=float, default=0.50,
                        help="Reject losses >50%% of existing photo URLs")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args(argv)
    if args.min_features < 1 or args.timeout < 1 or not 0 <= args.min_retained <= 1 or not 0 <= args.min_photos <= 1:
        parser.error("min-features/timeout must be positive; retained ratios must be in [0,1]")
    if args.out.resolve() == args.report.resolve():
        parser.error("--out and --report must be different paths")
    try:
        refresh(
            out=args.out, report_path=args.report, source_json=args.source_json,
            dry_run=args.dry_run, min_features=args.min_features,
            min_retained=args.min_retained, min_photos=args.min_photos, timeout=args.timeout,
        )
    except Exception as exc:
        print(f"{utc_now()} ERROR: LOESS refresh failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
