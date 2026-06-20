#!/usr/bin/env python3
"""
Unified pipeline

Steps:
  1) Read Firestore "samples" (collection group), flatten, mirror images to MinIO.
  2) Enrich with Firebase Auth emails.
  3) Write CSVs:
       - INPUT_CSV            (flattened raw)
       - OUTPUT_CSV       (flattened + email)
       - USERS_CSV          (distinct emails)
  4) Refresh local SQLite from OUTPUT_CSV (like refresh_sqlite.py) but KEEP lab_enrichment.
  5) Build canonical 3-file export (samples.csv, sample_images.csv, sample_parameters.csv),
     where sample_parameters are read from THAT SQLite.
  6) Upload canonical files to MinIO under canonical/.
  7) (optional) upsert canonical data into Postgres.
"""

import hashlib
import json
import io
import math
import os
import re
import sqlite3
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
import logging

import firebase_admin
import pandas as pd
import requests
from dotenv import load_dotenv
from firebase_admin import auth, credentials, firestore

# ---------------------------------------------------------------------------
# 0. load env and basic paths
# ---------------------------------------------------------------------------
env_path = Path.cwd() / ".env"
load_dotenv(dotenv_path=env_path)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(levelname)s: %(message)s",
)
log = logging.getLogger("pull_and_enrich")

log.info("Loaded environment from %s", env_path)

# ---------------------------------------------------------------------------
# Make sure 'echorepo' can be imported (project root on sys.path)
# ---------------------------------------------------------------------------
THIS_DIR = Path(__file__).resolve().parent  # .../echorepo-lite-dev/tools
DEFAULT_ROOT = THIS_DIR.parent  # .../echorepo-lite-dev

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", str(DEFAULT_ROOT)))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
log.info("Using PROJECT_ROOT=%s", PROJECT_ROOT)

# helper: QR to country code
try:
    from echorepo.services.planned import load_qr_to_planned

    log.debug("Imported load_qr_to_planned from echorepo.services.planned")
except Exception:
    load_qr_to_planned = None  # we'll guard later

try:
    # optional: reuse your parsing / jitter helpers if you like
    from echorepo.utils.geo import _parse_coord as geo_parse_coord
except Exception:
    geo_parse_coord = None

from echorepo.utils.load_csv import (
    MAX_JITTER_METERS as LC_MAX_JITTER_METERS,
)
from echorepo.utils.load_csv import (
    _parse_coord as lc_parse_coord,
)
from echorepo.utils.load_csv import (
    deterministic_jitter as lc_det_jitter,
)

# parse helper for .env interpretation of booleans (1/0, true/false, yes/no, on/off)
def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)

    if value is None:
        return default

    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

# helper to convert local or absolute paths to project-root-relative paths
def _local_path_to_abs(maybe_path: str) -> str:
    p = Path(maybe_path)
    if p.is_absolute():
        if p.exists():
            return str(p)
        alt = PROJECT_ROOT / p.relative_to("/")
        return str(alt)
    return str(PROJECT_ROOT / p)

def _container_data_path_to_host_path(maybe_path: str) -> str:
    """
    Convert container-style /data/... path to host-side PROJECT_ROOT/data/...

    Example:
      /data/coordinate_check_approved.csv
      -> PROJECT_ROOT/data/coordinate_check_approved.csv

    This is needed because this script is usually run on the host, while web.py
    runs inside Docker where /data is the mounted data directory.
    """
    p = Path(maybe_path)

    if str(p).startswith("/data/"):
        return str(PROJECT_ROOT / "data" / p.relative_to("/data"))

    if p.is_absolute():
        return str(p)

    return str(PROJECT_ROOT / p)


COUNTRY_RESOLVER_ENABLED = env_bool("COUNTRY_RESOLVER_ENABLED", True)

COUNTRY_RESOLVER_URL = os.getenv(
    "COUNTRY_RESOLVER_URL",
    "http://127.0.0.1:8010/resolve",
)

COUNTRY_RESOLVER_TIMEOUT_SECONDS = float(
    os.getenv("COUNTRY_RESOLVER_TIMEOUT_SECONDS", "5")
)


def resolve_country_via_service(lat, lon) -> dict:
    """
    Resolve coordinates using the coord-country-resolver service.

    Returns a normalized dictionary compatible with the old enrichment fields.
    """
    if not COUNTRY_RESOLVER_ENABLED:
        return {
            "country_code": "",
            "country_source": "",
            "country_lookup_note": "country_resolver_service_disabled",
            "matched_country_name": "",
            "distance_deg": None,
        }

    try:
        response = requests.get(
            COUNTRY_RESOLVER_URL,
            params={"lat": lat, "lon": lon},
            timeout=COUNTRY_RESOLVER_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()

        return {
            "country_code": str(data.get("country_code") or "").strip().upper(),
            "country_source": str(data.get("country_source") or ""),
            "country_lookup_note": str(data.get("country_lookup_note") or ""),
            "matched_country_name": str(data.get("matched_country_name") or ""),
            "distance_deg": data.get("distance_deg"),
        }

    except Exception as e:
        return {
            "country_code": "",
            "country_source": "",
            "country_lookup_note": f"country_resolver_service_failed: {e}",
            "matched_country_name": "",
            "distance_deg": None,
        }

USERS_CSV = _local_path_to_abs(os.getenv("USERS_CSV", "/data/users.csv"))
PLANNED_XLSX = _local_path_to_abs(os.getenv("PLANNED_XLSX", "/data/utils/planned.xlsx"))
INPUT_CSV = _local_path_to_abs(os.getenv("INPUT_CSV", "/data/echorepo_samples.csv"))
OUTPUT_CSV = _local_path_to_abs(os.getenv("OUTPUT_CSV", "/data/echorepo_samples_with_email.csv"))

# location jitter in meters
MAX_JITTER_METERS = int(os.getenv("MAX_JITTER_METERS", "1000"))

# Firebase config
PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID", None)
FBS_PREFIX = "https://firebasestorage.googleapis.com/"

# SQLite path
SQLITE_PATH = _local_path_to_abs(os.getenv("SQLITE_PATH", "/data/db/echo.db"))
LAB_ENRICHMENT_DB = os.getenv("LAB_ENRICHMENT_DB", SQLITE_PATH)

# MinIO config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_OUTSIDE", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER") or ""
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD") or ""
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "echorepo-uploads")
PUBLIC_STORAGE_BASE = os.getenv("PUBLIC_STORAGE_BASE", "/storage")
MIRROR_VERBOSE = os.getenv("MIRROR_VERBOSE", "0") == "1"

# Max image size (after compression) and persistent progress log
MAX_IMAGE_BYTES = int(os.getenv("MAX_IMAGE_BYTES", str(3 * 1024 * 1024)))  # 3 MB
MIRROR_OVERWRITE_EXISTING = os.getenv("MIRROR_OVERWRITE_EXISTING", "0") == "1"

# File where we log successfully processed MinIO object names (one per line)
MIRROR_PROGRESS_FILE = os.getenv(
    "MIRROR_PROGRESS_FILE",
    str(PROJECT_ROOT / "data" / "mirror_progress_images.txt"),
)

DEFAULT_LICENCE = os.getenv("DEFAULT_LICENCE", "CC-BY-4.0")
DEFAULT_LAB_ID = os.getenv("DEFAULT_LAB_ID", "ECHO-LAB-1")

FILTER_WRONG_COORDINATES = env_bool("FILTER_WRONG_COORDINATES", False)

# Reverse decoder for country code (offline fallback)
ALLOW_SINGLE_PLANNED_COUNTRY_FALLBACK = env_bool("ALLOW_SINGLE_PLANNED_COUNTRY_FALLBACK", True)

COORDINATE_APPROVED_CSV = Path(
    _container_data_path_to_host_path(
        os.getenv("COORDINATE_APPROVED_CSV", "data/coordinate_check_approved.csv")
    )
)

WRITE_COUNTRY_RESOLUTION_DIAG = env_bool("WRITE_COUNTRY_RESOLUTION_DIAG", False)
WRITE_ORPHAN_PARAMETERS_DIAG = env_bool("WRITE_ORPHAN_PARAMETERS_DIAG", False)
VERBOSE_CSV_WRITES = env_bool("VERBOSE_CSV_WRITES", False)

def load_approved_coordinate_samples() -> dict[str, dict[str, str]]:
    path = COORDINATE_APPROVED_CSV

    if not path.exists():
        log.debug("Coordinate approval file not found: %s", path)
        return {}

    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception:
        return {}

    if "sample_id" not in df.columns:
        log.warning("Coordinate approval file has no sample_id column: %s", path)
        return {}

    for col in ["approved_by", "approved_at", "comment", "country_code_override"]:
        if col not in df.columns:
            df[col] = ""

    approvals = {}

    for _, row in df.iterrows():
        value = str(row.get("sample_id") or "").strip()
        if not value:
            continue

        try:
            sid = norm_qr_for_id(value).upper()
        except Exception:
            sid = value.upper()

        country_override = str(row.get("country_code_override") or "").strip().upper()

        approvals[sid] = {
            "sample_id": sid,
            "approved_by": str(row.get("approved_by") or "").strip(),
            "approved_at": str(row.get("approved_at") or "").strip(),
            "comment": str(row.get("comment") or "").strip(),
            "country_code_override": country_override,
        }

    return approvals


_COUNTRY_RESOLUTION_CACHE: dict[tuple[str, str], dict] = {}


def resolve_country_cached(lat, lon) -> dict:
    """
    Cached wrapper around the coord-country-resolver HTTP service.
    Avoids repeated HTTP calls for identical coordinates during one run.
    """
    key = (str(lat).strip(), str(lon).strip())

    if key not in _COUNTRY_RESOLUTION_CACHE:
        _COUNTRY_RESOLUTION_CACHE[key] = resolve_country_via_service(lat, lon)

    return _COUNTRY_RESOLUTION_CACHE[key]


def resolve_country_for_sample(
    lat,
    lon,
    planned_set=None,
    sample_id="",
    allow_planned_fallback=True,
):
    """
    Resolve the country for a sample using the external coord-country-resolver service.

    ECHOREPO-specific fallback to planned country is kept here, because that is
    project/business logic, not generic coordinate resolution logic.
    """
    planned_set = planned_set or set()

    lat_f = _coord_to_float(lat)
    lon_f = _coord_to_float(lon)

    if lat_f is None or lon_f is None:
        return {
            "country_code": "",
            "country_source": "",
            "country_lookup_note": "invalid_or_missing_coordinates",
            "matched_country_name": "",
            "distance_deg": None,
        }

    if not (-90 <= lat_f <= 90 and -180 <= lon_f <= 180):
        return {
            "country_code": "",
            "country_source": "",
            "country_lookup_note": "coordinates_out_of_range",
            "matched_country_name": "",
            "distance_deg": None,
        }

    # Main method: coord-country-resolver service.
    info = resolve_country_cached(lat_f, lon_f)

    cc = str(info.get("country_code") or "").strip().upper()
    if cc:
        return {
            "country_code": cc,
            "country_source": str(info.get("country_source") or "country_resolver_service"),
            "country_lookup_note": str(info.get("country_lookup_note") or ""),
            "matched_country_name": str(info.get("matched_country_name") or ""),
            "distance_deg": info.get("distance_deg"),
        }

    # Last-resort fallback: planned country only if explicitly allowed and unambiguous.
    if (
        allow_planned_fallback
        and ALLOW_SINGLE_PLANNED_COUNTRY_FALLBACK
        and len(planned_set) == 1
    ):
        cc_planned = next(iter(planned_set))
        return {
            "country_code": cc_planned,
            "country_source": "planned_single_fallback",
            "country_lookup_note": "coordinate_country_lookup_failed_used_single_planned_country",
            "matched_country_name": "",
            "distance_deg": None,
        }

    return {
        "country_code": "",
        "country_source": str(info.get("country_source") or ""),
        "country_lookup_note": str(info.get("country_lookup_note") or "country_lookup_failed"),
        "matched_country_name": str(info.get("matched_country_name") or ""),
        "distance_deg": info.get("distance_deg"),
    }

def _load_mirror_done() -> set[str]:
    """
    Load set of MinIO object names that were already processed
    (successfully mirrored + oriented + compressed).
    """
    path = Path(MIRROR_PROGRESS_FILE)
    if not path.exists():
        return set()
    done = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = str(line).strip()
            if line:
                done.add(line)
    return done


_MIRROR_DONE_OBJECTS: set[str] = _load_mirror_done()


def _mark_mirror_done(object_name: str) -> None:
    """
    Append an object_name to the progress file and in-memory set.
    Safe to call multiple times; duplicates in file are harmless.
    """
    if object_name in _MIRROR_DONE_OBJECTS and not MIRROR_OVERWRITE_EXISTING:
        return
    _MIRROR_DONE_OBJECTS.add(object_name)
    path = Path(MIRROR_PROGRESS_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(object_name + "\n")

def country_resolver_health_ok() -> bool:
    if not COUNTRY_RESOLVER_ENABLED:
        return True

    try:
        base_url = COUNTRY_RESOLVER_URL.rsplit("/", 1)[0]
        response = requests.get(f"{base_url}/health", timeout=3)
        return response.ok
    except Exception as e:
        log.warning("Country resolver health check failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# optional: Postgres (we only ensure tables here)
# ---------------------------------------------------------------------------
try:
    import psycopg2
except ImportError:
    psycopg2 = None

# ---------------------------------------------------------------------------
# MinIO
# ---------------------------------------------------------------------------
try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    Minio = None

    class S3Error(Exception):
        pass


# ---------------------------------------------------------------------------
# Pillow (for orientation + compression) / tolerate truncated images
# ---------------------------------------------------------------------------
try:
    from PIL import ExifTags, Image, ImageFile, ImageOps

    ImageFile.LOAD_TRUNCATED_IMAGES = True
except Exception:
    Image = None
    ImageOps = None
    ExifTags = None
    ImageFile = None

# EXIF orientation tag id (if Pillow available)
if ExifTags is not None:
    ORIENTATION_TAG = next(
        (tid for tid, name in ExifTags.TAGS.items() if name == "Orientation"),
        None,
    )
else:
    ORIENTATION_TAG = None

# EXIF handling
STRIP_IMAGE_EXIF = os.getenv("STRIP_IMAGE_EXIF", "1") == "1"
SAVE_EXIF_SIDECAR = os.getenv("SAVE_EXIF_SIDECAR", "1") == "1"

# Used only to make sidecar filenames hard to guess.
# Prefer setting EXIF_SIDECAR_SALT explicitly in .env.
EXIF_SIDECAR_SALT = (
    os.getenv("EXIF_SIDECAR_SALT")
    or os.getenv("MINIO_SECRET_KEY")
    or os.getenv("MINIO_ROOT_PASSWORD")
    or "echorepo-dev-exif-salt"
)

# ---------------------------------------------------------------------------
# Firebase init
# ---------------------------------------------------------------------------
def init_firebase():
    if firebase_admin._apps:
        return
    creds_path = _local_path_to_abs(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/echorepo/keys/firebase-sa.json")
    )
    if not creds_path or not os.path.exists(creds_path):
        log.error("Service account JSON not found: %s", creds_path)
        sys.exit(1)
    log.info("Initializing Firebase")
    log.debug("Firebase credentials path: %s", creds_path)
    cred = credentials.Certificate(creds_path)
    firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID} if PROJECT_ID else None)


# ---------------------------------------------------------------------------
# MinIO init
# ---------------------------------------------------------------------------
def init_minio():
    if Minio is None:
        log.info("python-minio not installed; will keep Firebase URLs and local canonical")
        return None

    secure = False
    endpoint = MINIO_ENDPOINT
    if endpoint.startswith("https://"):
        secure = True
        endpoint = endpoint[len("https://") :]
    elif endpoint.startswith("http://"):
        secure = False
        endpoint = endpoint[len("http://") :]

    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        log.warning("MinIO credentials not set; skipping mirroring and canonical upload")
        return None

    client = Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
    )

    try:
        found = client.bucket_exists(MINIO_BUCKET)
        if not found:
            client.make_bucket(MINIO_BUCKET)
            log.info("Created MinIO bucket %s", MINIO_BUCKET)
    except Exception as e:
        log.warning("Could not ensure MinIO bucket: %s", e)
        return None
    log.info("MinIO ready, bucket=%s", MINIO_BUCKET)
    return client


# ---------------------------------------------------------------------------
# helper: input sanitization
# ---------------------------------------------------------------------------
BAD_NUM = {"", " ", "-", "NA", "N/A", "null", "None"}


def _clean_int_val(v):
    """
    Convert numeric-ish input to int, safely.
    """
    if v is None:
        return None

    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    s = str(v).strip()
    if not s:
        return None

    s_norm = s.lower()
    if s_norm in {"na", "n/a", "nan", "null", "none", "-"}:
        return None

    s = s.replace(",", ".")

    try:
        n = float(s)
    except (TypeError, ValueError):
        return None

    if math.isnan(n) or math.isinf(n):
        return None

    return int(n)


def _clean_float_val(v):
    if v is None:
        return None
    s = str(v).strip().replace(",", ".")
    if s in BAD_NUM:
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# coordinate parsing helpers
# ---------------------------------------------------------------------------

def _coord_to_float(v):
    if v is None:
        return None

    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    s = str(v).strip().replace(",", ".")
    if not s:
        return None

    try:
        x = float(s)
    except Exception:
        return None

    if not math.isfinite(x):
        return None

    return x
    
# ---------------------------------------------------------------------------
# misc helpers
# ---------------------------------------------------------------------------
def _add_jitter_columns_to_sqlite(db_path: str, jitter_fn):
    """
    Adds/updates jittered columns 'lat'/'lon' in the main samples table,
    computed deterministically from original GPS_lat/GPS_long and a stable key.
    We do NOT overwrite the original GPS_lat/GPS_long.
    """
    if jitter_fn is None:
        log.info("[sqlite] jitter function missing; skipping jitter columns")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Find a table that has sampleId, GPS_lat, GPS_long
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]

    target = None
    for t in tables:
        cur.execute(f"PRAGMA table_info({t})")
        cols = {r[1] for r in cur.fetchall()}
        if {"sampleId", "GPS_lat", "GPS_long"}.issubset(cols):
            target = t
            break
    if not target:
        log.warning(
            "Could not find a SQLite table with sampleId/GPS_lat/GPS_long; skipping jitter columns"
        )
        conn.close()
        return

    # Ensure columns exist
    def _ensure_col(name: str):
        cur.execute(f"PRAGMA table_info({target})")
        cols = {r[1] for r in cur.fetchall()}
        if name not in cols:
            cur.execute(f"ALTER TABLE {target} ADD COLUMN {name} REAL")

    _ensure_col("lat")
    _ensure_col("lon")

    # Compute jittered coords row-by-row
    df = pd.read_sql_query(f"SELECT sampleId, GPS_lat, GPS_long FROM {target}", conn)
    updates = []
    for _, r in df.iterrows():
        sid = str(r["sampleId"])
        try:
            lt = (
                float(str(r["GPS_lat"]).replace(",", "."))
                if r["GPS_lat"] not in (None, "", "nan")
                else None
            )
            ln = (
                float(str(r["GPS_long"]).replace(",", "."))
                if r["GPS_long"] not in (None, "", "nan")
                else None
            )
        except Exception:
            lt = ln = None

        if lt is not None and ln is not None:
            jlt, jln = jitter_fn(lt, ln, sid, LC_MAX_JITTER_METERS)
        else:
            jlt = jln = None
        updates.append((jlt, jln, sid))

    cur.executemany(f"UPDATE {target} SET lat = ?, lon = ? WHERE sampleId = ?", updates)
    conn.commit()
    conn.close()
    log.info(f"[sqlite] added/updated jittered columns 'lat','lon' in {target}")


def norm_qr_for_id(q):
    """Make QR suitable as canonical sample_id."""
    if not q:
        return ""
    s = str(q).strip()
    # common cleanup: remove spaces, uppercase
    s = s.replace(" ", "").upper()
    return s


def parse_ph(value):
    if value is None:
        log.debug("[parse_ph] Value is None")
        return None
    s = str(value).strip().lower().replace(",", ".")
    m = re.search(r"(-?\d+(\.\d+)?)", s)
    if not m:
        log.debug("[parse_ph] No match found for value: %s", value)
        return value
    try:
        return float(m.group(1))
    except ValueError:
        log.debug("[parse_ph] Failed to convert value to float: %s", value)
        return value


def _ts_to_iso(ts):
    if ts is None:
        return ""
    if isinstance(ts, str):
        return ts
    if hasattr(ts, "isoformat"):
        dt = ts
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    return str(ts)


def _ts_to_iso_loose(v):
    """
    Convert various Firestore-ish timestamp shapes to ISO 8601.
    Handles:
      - datetime(...)
      - objects with .seconds / .nanos
      - strings like "seconds: 1747091048\nnanos: 20637000"
      - already-ISO strings
    Otherwise returns original value.
    """
    if v is None:
        return ""

    # already a datetime
    if hasattr(v, "isoformat"):
        dt = v
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()

    # Firestore timestamp object
    if hasattr(v, "seconds") and hasattr(v, "nanos"):
        sec = int(v.seconds)
        nanos = int(v.nanos or 0)
        dt = datetime.fromtimestamp(sec + nanos / 1_000_000_000, tz=timezone.utc)
        return dt.isoformat()

    # ugly string: "seconds: ... nanos: ..."
    if isinstance(v, str) and "seconds:" in v:
        # collapse newlines just in case
        s = v.replace("\r", " ").replace("\n", " ")
        m_sec = re.search(r"seconds:\s*(\d+)", s)
        m_nanos = re.search(r"nanos:\s*(\d+)", s)
        if m_sec:
            sec = int(m_sec.group(1))
            nanos = int(m_nanos.group(1)) if m_nanos else 0
            dt = datetime.fromtimestamp(sec + nanos / 1_000_000_000, tz=timezone.utc)
            return dt.isoformat()
        # if we can't parse, just return original
        return v

    # looks like ISO already
    if isinstance(v, str) and re.match(r"\d{4}-\d{2}-\d{2}T", v):
        return v

    return v


def _safe_part(s: str) -> str:
    s = str(s or "").strip()
    return re.sub(r"[^A-Za-z0-9_.-]", "_", s) or "x"


def _guess_ext_from_firebase_url(url: str) -> str:
    parsed = urlparse(url)
    last = parsed.path.rsplit("/", 1)[-1]
    if "." in last:
        return "." + last.rsplit(".", 1)[-1]
    return ".bin"

def _json_safe_exif_value(v):
    """
    Convert EXIF values to JSON-safe values.
    Keeps useful metadata but avoids bytes/object serialization errors.
    """
    if v is None or isinstance(v, str | int | float | bool):
        return v

    if isinstance(v, bytes):
        return {
            "__type": "bytes",
            "hex": v.hex(),
        }

    if isinstance(v, tuple | list):
        return [_json_safe_exif_value(x) for x in v]

    if isinstance(v, dict):
        return {str(k): _json_safe_exif_value(val) for k, val in v.items()}

    # Pillow IFDRational and other EXIF objects
    try:
        return float(v)
    except Exception:
        return str(v)


def _extract_exif_sidecar(img, object_name: str, raw_bytes: bytes) -> dict | None:
    """
    Extract original EXIF metadata to a JSON-serializable dict.
    This is for private/internal sidecar storage, not for public display.
    """
    if img is None or not hasattr(img, "getexif"):
        return None

    try:
        exif = img.getexif()
    except Exception:
        return None

    if not exif or len(exif) == 0:
        return None

    out = {
        "schema": "echorepo-image-exif-sidecar-v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "image_object_name": object_name,
        "original_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "exif": {},
    }

    # Main EXIF tags
    for tag_id, value in exif.items():
        tag_name = ExifTags.TAGS.get(tag_id, str(tag_id)) if ExifTags is not None else str(tag_id)

        # GPSInfo is usually nested; we decode it below separately where possible.
        if tag_name == "GPSInfo":
            continue

        out["exif"][tag_name] = _json_safe_exif_value(value)

    # Decode GPSInfo if present
    try:
        gps_ifd = exif.get_ifd(ExifTags.IFD.GPSInfo)
        if gps_ifd:
            gps = {}
            for tag_id, value in gps_ifd.items():
                tag_name = ExifTags.GPSTAGS.get(tag_id, str(tag_id))
                gps[tag_name] = _json_safe_exif_value(value)
            out["exif"]["GPSInfo"] = gps
    except Exception:
        # If Pillow version does not support get_ifd, keep going.
        pass

    return out


def _exif_sidecar_object_name(image_object_name: str) -> str:
    """
    Store sidecar next to the image but with a hashed component.

    Example:
      user/sample/photo.jpg
      user/sample/photo.jpg.exif.a1b2c3....json
    """
    digest = hashlib.sha256(
        f"{EXIF_SIDECAR_SALT}:{image_object_name}".encode("utf-8")
    ).hexdigest()[:32]
    return f"{image_object_name}.exif.{digest}.json"

def _compress_and_fix_orientation(
    raw_bytes: bytes,
    object_name: str = "",
    max_bytes: int = MAX_IMAGE_BYTES,
) -> tuple[bytes, dict | None]:
    """
    Use Pillow to:
      - read original EXIF
      - save original EXIF into a sidecar dict
      - apply EXIF orientation to pixels
      - strip EXIF from the public image
      - compress/downscale to max_bytes if needed

    Returns:
      (public_image_bytes_without_exif, exif_sidecar_dict_or_none)
    """
    if Image is None:
        return raw_bytes, None

    try:
        img = Image.open(io.BytesIO(raw_bytes))
    except Exception:
        return raw_bytes, None

    # Extract metadata BEFORE modifying the image
    sidecar = None
    if SAVE_EXIF_SIDECAR:
        try:
            sidecar = _extract_exif_sidecar(img, object_name=object_name, raw_bytes=raw_bytes)
        except Exception:
            sidecar = None

    exif = None
    orientation = None
    if hasattr(img, "getexif"):
        try:
            exif = img.getexif()
        except Exception:
            exif = None

    if exif is not None and len(exif) > 0:
        if ORIENTATION_TAG is not None:
            orientation = exif.get(ORIENTATION_TAG, None)
        else:
            orientation = exif.get(274, None)

    has_exif = exif is not None and len(exif) > 0
    needs_rotation = orientation not in (None, 1)

    # If no EXIF exists, no rotation is needed, and the image is already small,
    # keep original bytes.
    #
    # But if EXIF exists and STRIP_IMAGE_EXIF=1, we must re-save it to strip EXIF.
    if not has_exif and not needs_rotation and len(raw_bytes) <= max_bytes:
        return raw_bytes, sidecar

    # Apply EXIF orientation to pixels
    if needs_rotation and ImageOps is not None:
        try:
            img = ImageOps.exif_transpose(img)
        except Exception:
            pass

    def _save_with_quality(im, quality: int) -> bytes:
        buf = io.BytesIO()

        # JPEG cannot store alpha
        im_to_save = im.convert("RGB") if im.mode not in ("RGB", "L") else im

        kwargs = {
            "format": "JPEG",
            "quality": quality,
            "optimize": True,
        }

        # IMPORTANT:
        # Do NOT pass exif=... here.
        # This is what strips camera/GPS/private metadata from the public image.

        im_to_save.save(buf, **kwargs)
        return buf.getvalue()

    # Try several qualities at original size
    for quality in (90, 80, 70, 60, 50, 40):
        out = _save_with_quality(img, quality)
        if len(out) <= max_bytes:
            return out, sidecar

    # Downscale if still too large
    width, height = img.size
    current = img
    last_out = _save_with_quality(current, 50)

    while width > 300 and height > 300:
        width = int(width * 0.85)
        height = int(height * 0.85)
        current = current.resize((width, height), Image.LANCZOS)

        for quality in (70, 60, 50):
            out = _save_with_quality(current, quality)
            if len(out) <= max_bytes:
                return out, sidecar
            last_out = out

    return last_out, sidecar

# ----- Minio helpers ---------------------------------------------------------------------------
def _mirror_firebase_to_minio(url: str, user_id: str, sample_id: str, field: str, mclient) -> str:
    if not url or not url.startswith(FBS_PREFIX) or mclient is None:
        return url

    user_id = _safe_part(user_id)
    sample_id = _safe_part(sample_id)
    field = _safe_part(field)
    ext = _guess_ext_from_firebase_url(url)
    object_name = f"{user_id}/{sample_id}/{field}{ext}"

    # Skip if we've already processed this object in a previous run
    if object_name in _MIRROR_DONE_OBJECTS:
        if MIRROR_VERBOSE:
            log.info("[SKIP] already processed %s, skipping download", object_name)
        return f"{PUBLIC_STORAGE_BASE}/{object_name}"

    # If we are NOT overwriting, skip if object already exists in MinIO
    if not MIRROR_OVERWRITE_EXISTING:
        try:
            mclient.stat_object(MINIO_BUCKET, object_name)
            # Mark as done so it won't be checked again next time
            _mark_mirror_done(object_name)
            return f"{PUBLIC_STORAGE_BASE}/{object_name}"
        except Exception:
            # not found → proceed to download & upload
            pass

    # Download original from Firebase
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.content
    except Exception as e:
        if MIRROR_VERBOSE:
            log.warning("Could not download %s: %s", url, e)
        return url

    # Fix orientation + compress + strip public EXIF
    try:
        data_fixed, exif_sidecar = _compress_and_fix_orientation(
            data,
            object_name=object_name,
            max_bytes=MAX_IMAGE_BYTES,
        )
    except Exception as e:
        if MIRROR_VERBOSE:
            log.warning("Compression/orientation failed for %s: %s", url, e)
        data_fixed = data
        exif_sidecar = None

    # Upload to MinIO (overwrite if necessary)
    try:
        bio = io.BytesIO(data_fixed)
        mclient.put_object(
            MINIO_BUCKET,
            object_name,
            data=bio,
            length=len(data_fixed),
            content_type="image/jpeg",
        )

        # Store original EXIF separately, not inside the public image.
        _upload_exif_sidecar_to_minio(mclient, object_name, exif_sidecar)

        _mark_mirror_done(object_name)
        
        if MIRROR_VERBOSE:
            action = "overwrote" if MIRROR_OVERWRITE_EXISTING else "uploaded"
            log.info("%s %s (%.2f MB)", action, object_name, len(data_fixed) / 1024 / 1024)
        return f"{PUBLIC_STORAGE_BASE}/{object_name}"
    except Exception as e:
        if MIRROR_VERBOSE:
            log.warning("Could not upload to MinIO %s: %s", object_name, e)
        return url

def _upload_exif_sidecar_to_minio(mclient, image_object_name: str, sidecar: dict | None) -> None:
    """
    Upload original EXIF as a JSON sidecar with a hashed filename.

    The public image is stripped of EXIF.
    The sidecar is only for internal recovery/debug/research use.
    """
    if mclient is None or not sidecar or not SAVE_EXIF_SIDECAR:
        return

    sidecar_object_name = _exif_sidecar_object_name(image_object_name)

    try:
        data = json.dumps(sidecar, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")

        mclient.put_object(
            MINIO_BUCKET,
            sidecar_object_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/json",
        )

        if MIRROR_VERBOSE:
            log.debug("Uploaded EXIF sidecar to MinIO: %s", sidecar_object_name)

    except Exception as e:
        if MIRROR_VERBOSE:
            log.warning("Could not upload EXIF sidecar for %s: %s", image_object_name, e)

# ---------------------------------------------------------------------------
# 1. Firestore -> flattened rows
# ---------------------------------------------------------------------------
def fetch_samples_flat(mclient, max_stream_retries: int = 5) -> pd.DataFrame:
    """
    Fetch all 'samples' from Firestore, flatten, and mirror images to MinIO.

    Robust against intermittent Firestore stream errors:
      - Normal errors: retry up to max_stream_retries times.
      - Specific Firestore bug: '_UnaryStreamMultiCallable' object has no attribute '_retry'
        → retried indefinitely (does not count against max_stream_retries).
    """
    db = firestore.client()
    rows = []
    seen_doc_ids: set[str] = set()
    attempt = 0

    def _process_doc(doc):
        data = doc.to_dict() or {}
        row = {
            "sampleId": doc.id,
            "userId": (
                doc.reference.parent.parent.id
                if doc.reference.parent and doc.reference.parent.parent
                else None
            ),
            "collectedAt": data.get("collectedAt"),
            "fs_createdAt": _ts_to_iso(getattr(doc, "create_time", None)),
            "fs_updatedAt": _ts_to_iso(getattr(doc, "update_time", None)),
        }

        steps = data.get("data", [])
        if not isinstance(steps, list):
            rows.append(row)
            return

        for step in steps:
            if not isinstance(step, dict):
                continue
            step_type = step.get("type") or "unknown"
            state = step.get("state")
            info = step.get("info", {})
            row[f"{step_type}_state"] = state

            if isinstance(info, dict):
                for key, val in info.items():
                    base_col = f"{step_type}_{key}"
                    if isinstance(val, list):
                        for i, item in enumerate(val, start=1):
                            if isinstance(item, dict):
                                for subk, subv in item.items():
                                    col_name = f"{step_type}_{key}_{i}_{subk}"
                                    if isinstance(subv, str):
                                        subv = _mirror_firebase_to_minio(
                                            subv,
                                            row.get("userId") or "",
                                            row.get("sampleId") or "",
                                            col_name,
                                            mclient,
                                        )
                                    row[col_name] = subv
                    else:
                        if isinstance(val, str):
                            val = _mirror_firebase_to_minio(
                                val,
                                row.get("userId") or "",
                                row.get("sampleId") or "",
                                base_col,
                                mclient,
                            )
                        row[base_col] = val
            else:
                row[f"{step_type}_info"] = str(info)

        if "PH_ph" in row:
            row["PH_ph"] = parse_ph(row["PH_ph"])

        rows.append(row)

    while True:
        samples_ref = db.collection_group("samples")
        try:
            for doc in samples_ref.stream():
                # Skip docs already processed in this run
                if doc.id in seen_doc_ids:
                    continue
                seen_doc_ids.add(doc.id)
                _process_doc(doc)

            # Stream finished successfully → done
            break

        except Exception as e:
            msg = str(e)

            # --- Special handling for the weird Firestore bug you're seeing ---
            if "_UnaryStreamMultiCallable" in msg and "has no attribute '_retry'" in msg:
                # Don't count this towards max_stream_retries; just keep trying
                sleep_s = 10
                log.warning(
                    "Firestore stream hit known bug "
                    "('_UnaryStreamMultiCallable/_retry'); sleeping %ss and retrying...",
                    sleep_s,
                )
                time.sleep(sleep_s)
                db = firestore.client()
                continue

            # --- Normal retry path for network/transient errors ---
            attempt += 1
            if attempt > max_stream_retries:
                log.error("Firestore stream failed after %s attempts: %s", attempt, e)
                raise

            sleep_s = min(60, 5 * attempt)
            log.warning(
                "Firestore stream failed (attempt %s/%s): %s – retrying in %ss",
                attempt,
                max_stream_retries,
                e,
                sleep_s,
            )
            time.sleep(sleep_s)
            db = firestore.client()
            continue

    df = pd.DataFrame(rows, dtype=object)
    if not df.empty:
        if "QR_qrCode" in df.columns:
            # normalize missing QRs so they do not all collapse into one fake duplicate group
            qr_mask = df["QR_qrCode"].notna() & (df["QR_qrCode"].astype(str).str.strip() != "")

            # choose best "latest" signal
            sort_cols = []
            if "fs_updatedAt" in df.columns:
                sort_cols.append("fs_updatedAt")
            if "collectedAt" in df.columns:
                sort_cols.append("collectedAt")
            if "fs_createdAt" in df.columns:
                sort_cols.append("fs_createdAt")

            if sort_cols:
                # sort oldest -> newest, then keep last
                df = df.sort_values(by=sort_cols, na_position="last")

            df_with_qr = df.loc[qr_mask].drop_duplicates(subset=["QR_qrCode"], keep="last")
            df_without_qr = df.loc[~qr_mask]

            # keep rows without QR untouched
            df = pd.concat([df_with_qr, df_without_qr], ignore_index=True)
    return df


# ---------------------------------------------------------------------------
# 2. Firebase Auth
# ---------------------------------------------------------------------------
def fetch_uid_to_email(max_retries: int = 5) -> dict:
    """
    Fetch uid → email mapping from Firebase Auth with retries.

    If listing users fails mid-way, the whole listing is retried from scratch
    (mapping is rebuilt, but that's fine because it's small).
    """
    for attempt in range(1, max_retries + 1):
        mapping = {}
        try:
            page = auth.list_users()
            while page:
                for user in page.users:
                    mapping[user.uid] = str(user.email or "").strip()
                page = page.get_next_page()

            log.info("Retrieved %s Firebase Auth users", len(mapping))
            return mapping

        except Exception as e:
            if attempt >= max_retries:
                log.error("Firebase Auth list_users failed after %s attempts: %s", attempt, e)
                raise

            sleep_s = min(60, 5 * attempt)
            log.warning(
                "Firebase Auth list_users failed "
                f"(attempt {attempt}/{max_retries}): {e} – "
                f"retrying in {sleep_s}s"
            )
            time.sleep(sleep_s)


# ---------------------------------------------------------------------------
# 3. refresh sqlite (merged)
# ---------------------------------------------------------------------------
def _resolve_path(maybe_path: str) -> str:
    p = Path(maybe_path)
    if p.is_absolute():
        if p.exists():
            return str(p)
        alt = PROJECT_ROOT / p.relative_to("/")
        return str(alt)
    return str(PROJECT_ROOT / p)


def _backup_lab_enrichment(db_path: str):
    if not os.path.exists(db_path):
        return None
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("PRAGMA table_info(lab_enrichment)")
        cols = [r[1] for r in cur.fetchall()]
        if not cols:
            conn.close()
            return None
        cur = conn.execute("SELECT * FROM lab_enrichment")
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()
        return {"cols": cols, "rows": rows}
    except sqlite3.OperationalError:
        conn.close()
        return None


def _restore_lab_enrichment(db_path: str, backup):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lab_enrichment (
          qr_code    TEXT NOT NULL,
          param      TEXT NOT NULL,
          value      TEXT,
          unit       TEXT,
          user_id    TEXT,
          raw_row    TEXT,
          updated_at TEXT DEFAULT (datetime('now')),
          PRIMARY KEY (qr_code, param)
        )
    """)
    if backup:
        for r in backup["rows"]:
            qr_code = r.get("qr_code") or r.get("QR_code") or ""
            param = r.get("param") or ""
            value = r.get("value")
            unit = r.get("unit")
            user_id = r.get("user_id")
            raw_row = r.get("raw_row")
            updated = r.get("updated_at")
            if not qr_code or not param:
                continue
            cur.execute(
                """
                INSERT INTO lab_enrichment (qr_code, param, value, unit, user_id, raw_row, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, datetime('now')))
                ON CONFLICT(qr_code, param) DO UPDATE SET
                  value = excluded.value,
                  unit = excluded.unit,
                  user_id = excluded.user_id,
                  raw_row = excluded.raw_row,
                  updated_at = excluded.updated_at
            """,
                (qr_code, param, value, unit, user_id, raw_row, updated),
            )
    conn.commit()
    conn.close()


def _restore_original_coords_from_csv(
    db_path: str, csv_path: str, orig_lat_col: str = "GPS_lat", orig_lon_col: str = "GPS_long"
):
    """
    Force the ORIGINAL coordinates back into SQLite after ensure_sqlite(),
    in case that step overwrote them. We DO NOT touch jitter columns.
    """
    if not (os.path.exists(db_path) and os.path.exists(csv_path)):
        log.info("[sqlite] skip restore originals: db or csv missing")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # find a table that has sampleId + the two original columns
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]

    target = None
    for t in tables:
        cur.execute(f"PRAGMA table_info({t})")
        cols = {r[1] for r in cur.fetchall()}
        if {"sampleId", orig_lat_col, orig_lon_col}.issubset(cols):
            target = t
            break

    if not target:
        log.info("[sqlite] could not find table with sampleId/GPS_lat/GPS_long; nothing to restore")
        conn.close()
        return

    df_csv = pd.read_csv(csv_path, dtype=object)
    if (
        orig_lat_col not in df_csv.columns
        or orig_lon_col not in df_csv.columns
        or "sampleId" not in df_csv.columns
    ):
        log.info("[sqlite] CSV missing required columns; nothing to restore")
        conn.close()
        return

    def _to_float(v):
        if v is None:
            return None
        s = str(v).strip().replace(",", ".")
        try:
            return float(s)
        except Exception:
            return None

    updates = []
    for _, r in df_csv.iterrows():
        sid = str(r.get("sampleId") or "").strip()
        if not sid:
            continue
        lt = _to_float(r.get(orig_lat_col))
        ln = _to_float(r.get(orig_lon_col))
        # write back *originals* exactly (can be NULL)
        updates.append((lt, ln, sid))

    cur.executemany(
        f"UPDATE {target} SET {orig_lat_col} = ?, {orig_lon_col} = ? WHERE sampleId = ?", updates
    )
    conn.commit()
    conn.close()
    log.info("[sqlite] restored originals into %s.%s/%s from CSV", target, orig_lat_col, orig_lon_col)


def _truthy(v):
    return str(v or "").strip().lower() in {"true", "1", "yes", "y"}


def build_qa_status_from_row(r):
    """
    Convert coordinate validation flags into canonical qa_status.

    Use wrong_coordinates_effective when available so manually approved
    coordinate issues do not remain hidden on the map or shown as alerts.
    """
    wrong_flag = r.get("wrong_coordinates_effective", r.get("wrong_coordinates"))

    if _truthy(wrong_flag):
        reason = str(r.get("coordinate_check_reason") or "").strip()

        if reason:
            return f"wrong_coordinates:{reason}"

        return "wrong_coordinates"

    return ""

def refresh_sqlite_from_csv(OUTPUT_CSV: str, sqlite_path: str):
    sys.path.insert(0, str(PROJECT_ROOT))
    from echorepo.utils.load_csv import deterministic_jitter as lc_det_jitter
    from echorepo.utils.load_csv import ensure_sqlite

    csv_path = _resolve_path(OUTPUT_CSV)
    db_path = _resolve_path(sqlite_path)

    os.environ["CSV_PATH"] = csv_path
    os.environ["SQLITE_PATH"] = db_path

    log.info("[sqlite] CSV_PATH=%s", csv_path)
    log.info("[sqlite] SQLITE_PATH=%s", db_path)

    backup = _backup_lab_enrichment(db_path)
    if backup:
        log.info("[sqlite] backed up %s lab_enrichment rows", len(backup['rows']))
    else:
        log.info("[sqlite] no lab_enrichment to back up")

    # Rebuild DB (may jitter internally—we’ll fix originals next)
    ensure_sqlite()
    log.info("[sqlite] base SQLite refreshed from CSV")

    # Force ORIGINALS back (so GPS_lat/GPS_long are raw, not jittered)
    _restore_original_coords_from_csv(
        db_path, csv_path, orig_lat_col="GPS_lat", orig_lon_col="GPS_long"
    )

    _restore_lab_enrichment(db_path, backup)
    log.info("[sqlite] lab_enrichment restored")

    # Add/refresh separate jitter columns (lat/lon) without touching originals
    _add_jitter_columns_to_sqlite(db_path, lc_det_jitter)

    return db_path


# ---------------------------------------------------------------------------
# canonical builders
# ---------------------------------------------------------------------------
def _textify(v) -> str:
    if v is None:
        return ""
    # flatten lists/dicts safely
    try:
        if isinstance(v, list | tuple):
            return ", ".join(str(x) for x in v if x not in (None, ""))
        if isinstance(v, dict):
            # compact “k: v” pairs in stable order
            return ", ".join(f"{k}: {v[k]}" for k in sorted(v.keys()))
        return str(v)
    except Exception:
        return str(v)


def build_samples_df(
    df_flat: pd.DataFrame, planned_map: dict[str, set[str]] | None = None
) -> pd.DataFrame:
    """
    Build canonical samples dataframe.

    IMPORTANT: we NO LONGER call LibreTranslate here.
    All *_en fields are left empty (NULL in Postgres) and will be
    filled by translate_pg_en.py later, directly in Postgres.
    """
    planned_map = planned_map or {}
    pre_rows = []
    debug_missing = []

    for idx, r in df_flat.iterrows():
        qr_raw = r.get("QR_qrCode")
        qr_norm = norm_qr_for_id(qr_raw)
        sample_id = qr_norm or r.get("sampleId")

        # original coords -> float
        orig_lat = r.get("GPS_lat")
        orig_lon = r.get("GPS_long")
        lat_f = None
        lon_f = None
        try:
            if lc_parse_coord is not None:
                lat_f = lc_parse_coord(orig_lat, "lat")
                lon_f = lc_parse_coord(orig_lon, "lon")
            elif geo_parse_coord is not None:
                lat_f = geo_parse_coord(orig_lat, "lat")
                lon_f = geo_parse_coord(orig_lon, "lon")
            else:
                lat_f = float(orig_lat) if orig_lat not in (None, "") else None
                lon_f = float(orig_lon) if orig_lon not in (None, "") else None
        except Exception:
            lat_f = lon_f = None

        # planned countries are used for validation, not for assigning actual country
        planned_countries = set()
        if qr_norm and qr_norm in planned_map:
            planned_countries = planned_map[qr_norm] or set()

        country_override = str(r.get("country_code_override") or "").strip().upper()

        # Resolve actual country from ORIGINAL coordinates
        if country_override:
            orig_country = country_override
            country_source = "manual_override"
            country_lookup_note = "country_code_manually_overridden"
        else:
            country_info = resolve_country_for_sample(
                lat_f,
                lon_f,
                planned_set=planned_countries,
                sample_id=sample_id,
                allow_planned_fallback=False,
            )

            orig_country = country_info["country_code"]
            country_source = country_info["country_source"]
            country_lookup_note = country_info["country_lookup_note"]
        
        # jitter like before, but only for public lat/lon
        lat_j, lon_j = lat_f, lon_f
        if lat_f is not None and lon_f is not None and lc_det_jitter is not None:
            key = sample_id or r.get("userId") or str(idx)
            lat_j, lon_j = lc_det_jitter(lat_f, lon_f, key, LC_MAX_JITTER_METERS)

        # Optional diagnostic only. Do NOT use this to assign country_code.
        jitter_country = ""
        if lat_j is not None and lon_j is not None:
            jitter_info = resolve_country_for_sample(
                lat_j,
                lon_j,
                planned_set=set(),
                sample_id=sample_id,
                allow_planned_fallback=False,
            )
            jitter_country = jitter_info.get("country_code", "")

        country = orig_country

        # contamination fields
        cont_debris = r.get("SOIL_CONTAMINATION_debris") or 0
        cont_plastic = r.get("SOIL_CONTAMINATION_plastic") or 0
        cont_other_orig = str(r.get("SOIL_CONTAMINATION_comments") or "").strip()

        # translatable fields (orig only)
        soil_structure_orig = str(r.get("SOIL_STRUCTURE_structure") or "").strip()
        soil_texture_orig = str(r.get("SOIL_TEXTURE_texture") or "").strip()
        observations_orig = str(r.get("SOIL_DIVER_observations") or "").strip()
        metals_info_orig = str(r.get("METALS_info") or "").strip()

        pre_rows.append(
            {
                "sample_id": sample_id,
                "timestamp_utc": r.get("collectedAt") or r.get("fs_createdAt"),
                "lat": lat_j,
                "lon": lon_j,
                "country": country,
                "country_source": country_source,
                "country_lookup_note": country_lookup_note,
                "ph": r.get("PH_ph"),
                "earthworms_count": r.get("SOIL_DIVER_earthworms"),
                "contamination_debris": cont_debris,
                "contamination_plastic": cont_plastic,
                "contamination_other_orig": cont_other_orig,
                "soil_structure_orig": soil_structure_orig,
                "soil_texture_orig": soil_texture_orig,
                "observations_orig": observations_orig,
                "metals_info_orig": metals_info_orig,
                "collected_by": r.get("userId"),
                 "qa_status": build_qa_status_from_row(r),
                "organic_carbon_pct": _pct_to_float(r.get("SOIL_COLOR_color")),
            }
        )

        if (
            country_lookup_note
            or not country
            or (planned_countries and country and country not in planned_countries)
        ):
            debug_missing.append(
                {
                    "row_index": idx,
                    "sample_id": sample_id,
                    "qr": qr_norm,
                    "orig_lat": orig_lat,
                    "orig_lon": orig_lon,
                    "lat_f": lat_f,
                    "lon_f": lon_f,
                    "jitter_lat": lat_j,
                    "jitter_lon": lon_j,
                    "country": country,
                    "country_source": country_source,
                    "country_lookup_note": country_lookup_note,
                    "jitter_country": jitter_country,
                    "planned_countries": ",".join(sorted(planned_countries)) if planned_countries else "",
                }
            )
    # Build final rows; *_en fields are intentionally empty
    rows = []
    for pr in pre_rows:
        rows.append(
            {
                "sample_id": pr["sample_id"],
                "timestamp_utc": pr["timestamp_utc"],
                "lat": pr["lat"],
                "lon": pr["lon"],
                "country_code": pr["country"],
                "location_accuracy_m": MAX_JITTER_METERS,
                "ph": pr["ph"],
                "organic_carbon_pct": pr["organic_carbon_pct"],
                "earthworms_count": pr["earthworms_count"],
                "contamination_debris": pr["contamination_debris"],
                "contamination_plastic": pr["contamination_plastic"],
                "contamination_other_orig": pr["contamination_other_orig"],
                "contamination_other_en": "",  # will be filled later in Postgres
                "pollutants_count": (
                    (_clean_int_val(pr["contamination_debris"]) or 0)
                    + (_clean_int_val(pr["contamination_plastic"]) or 0)
                ),
                "soil_structure_orig": pr["soil_structure_orig"],
                "soil_structure_en": "",  # will be filled later
                "soil_texture_orig": pr["soil_texture_orig"],
                "soil_texture_en": "",  # will be filled later
                "observations_orig": pr["observations_orig"],
                "observations_en": "",  # will be filled later
                "metals_info_orig": pr["metals_info_orig"],
                "metals_info_en": "",  # will be filled later
                "collected_by": pr["collected_by"],
                "data_source": "mobile",
                "qa_status": pr["qa_status"],
                "licence": DEFAULT_LICENCE,
            }
        )

    if debug_missing and WRITE_COUNTRY_RESOLUTION_DIAG:
        diag_path = PROJECT_ROOT / "data" / "canonical" / "country_resolution_diag.csv"
        os.makedirs(diag_path.parent, exist_ok=True)
        pd.DataFrame(debug_missing).to_csv(diag_path, index=False)
        log.info(
            "Wrote %s country-resolution diagnostic rows to %s",
            len(debug_missing),
            diag_path,
        )
    elif debug_missing:
        log.debug("Country-resolution diagnostic rows not written: %s", len(debug_missing))    

    return pd.DataFrame(rows)


def annotate_and_filter_wrong_coordinates(
    df: pd.DataFrame,
    planned_map: dict[str, set[str]],
    qr_col: str = "QR_qrCode",
    lat_col: str = "GPS_lat",
    lon_col: str = "GPS_long",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Add coordinate validation columns and return:

      - filtered_df: rows where wrong_coordinates is False
      - annotated_df: all rows with validation columns

    wrong_coordinates=True when:
      - coordinates are invalid/out of range, OR
      - coordinates are the default/sentinel coordinates, OR
      - planned country exists for QR and actual country is not in planned set

    If there is no planned country for a QR, we do NOT mark it as wrong.
    That avoids dropping samples only because the planned.xlsx file is incomplete.
    """
    if df is None or df.empty:
        return df, df

    out = df.copy()

    if qr_col not in out.columns:
        log.info("[coords] QR column %s not found; skipping coordinate filtering", qr_col)
        out["wrong_coordinates"] = False
        out["actual_cc"] = ""
        out["planned_iso2"] = ""
        out["coordinate_check_reason"] = "missing_qr_column"
        return out, out

    if lat_col not in out.columns or lon_col not in out.columns:
        log.info("[coords] lat/lon columns %s/%s not found; skipping coordinate filtering", lat_col, lon_col)
        out["wrong_coordinates"] = False
        out["actual_cc"] = ""
        out["planned_iso2"] = ""
        out["coordinate_check_reason"] = "missing_lat_lon_columns"
        return out, out

    lat_s = out[lat_col].astype(str).str.replace(",", ".", regex=False).str.strip()
    lon_s = out[lon_col].astype(str).str.replace(",", ".", regex=False).str.strip()

    lat_f = pd.to_numeric(lat_s, errors="coerce")
    lon_f = pd.to_numeric(lon_s, errors="coerce")

    valid_mask = (
        lat_f.notna()
        & lon_f.notna()
        & lat_f.between(-90.0, 90.0)
        & lon_f.between(-180.0, 180.0)
    )

    # Your app sentinel/default coordinates
    default_mask = (lat_f == 46.5) & (lon_f == 11.35)

    actual_cc = []
    actual_cc_source = []
    actual_cc_note = []
    actual_cc_matched_name = []
    actual_cc_distance_deg = []

    for lt, ln, valid, is_default in zip(lat_f, lon_f, valid_mask, default_mask, strict=False):
        if not valid or is_default:
            actual_cc.append("")
            actual_cc_source.append("")
            actual_cc_note.append("invalid_or_default_coordinates")
            actual_cc_matched_name.append("")
            actual_cc_distance_deg.append(None)
            continue

        info = resolve_country_for_sample(
            lt,
            ln,
            planned_set=set(),
            allow_planned_fallback=False,  # validation must be coordinate-based only
        )

        actual_cc.append(info["country_code"])
        actual_cc_source.append(info["country_source"])
        actual_cc_note.append(info["country_lookup_note"])
        actual_cc_matched_name.append(info.get("matched_country_name"))
        actual_cc_distance_deg.append(info.get("distance_deg"))

    out["actual_cc"] = actual_cc
    out["actual_cc_source"] = actual_cc_source
    out["actual_cc_note"] = actual_cc_note
    out["actual_cc_matched_name"] = actual_cc_matched_name
    out["actual_cc_distance_deg"] = actual_cc_distance_deg

    def _planned_set(q):
        q_norm = norm_qr_for_id(q)
        if not q_norm:
            return set()
        return planned_map.get(q_norm, set())

    planned_sets = out[qr_col].map(_planned_set)
    planned_sets = planned_sets.apply(lambda v: v if isinstance(v, set) else set())

    out["planned_iso2"] = planned_sets.apply(lambda s: ",".join(sorted(s)) if s else "")

    reasons = []
    wrong = []

    for i, row in out.iterrows():
        planned = planned_sets.loc[i]
        cc = str(row.get("actual_cc") or "").strip()

        if bool(default_mask.loc[i]):
            wrong.append(True)
            reasons.append("default_coordinates")
            continue

        if not bool(valid_mask.loc[i]):
            wrong.append(True)
            reasons.append("invalid_coordinates")
            continue

        if not cc:
            wrong.append(True)
            reasons.append("country_not_found")
            continue

        if planned and cc not in planned:
            wrong.append(True)
            reasons.append("country_mismatch")
            continue

        if not planned:
            wrong.append(False)
            reasons.append("no_planned_country_for_qr")
            continue

        wrong.append(False)
        reasons.append("ok")

    out["wrong_coordinates"] = wrong
    out["coordinate_check_reason"] = reasons

    filtered = out[out["wrong_coordinates"] != True].copy()

    log.info(
        "coordinate check: "
        f"total={len(out)}, wrong={int(out['wrong_coordinates'].sum())}, kept={len(filtered)}"
    )

    return filtered, out

def _pct_to_float(v):
    if v in (None, "", "nan"):
        return None
    try:
        s = str(v).strip().replace("%", "").replace(",", ".")
        return float(s) if s else None
    except Exception:
        return None


def _to_float_num(v):
    if v is None:
        return None
    s = str(v).strip().replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def build_sample_images_df(df_flat: pd.DataFrame) -> pd.DataFrame:
    """
    Build canonical sample_images dataframe.

    IMPORTANT: no translation here; image_description_en is empty
    and will be filled in Postgres by translate_pg_en.py.
    """
    img_rows = []
    photo_slots = list(range(1, 14))

    for _, r in df_flat.iterrows():
        lt = _to_float_num(r.get("GPS_lat"))
        ln = _to_float_num(r.get("GPS_long"))
        country = (
            str(r.get("country_code_override") or "").strip().upper()
            or str(r.get("country_code") or "").strip().upper()
            or str(r.get("actual_cc") or "").strip().upper()
        )

        if not country:
            country_info = resolve_country_for_sample(
                lt,
                ln,
                planned_set=set(),
                allow_planned_fallback=False,
            )
            country = country_info.get("country_code", "")

        qr = norm_qr_for_id(r.get("QR_qrCode"))
        sample_id = qr or r.get("sampleId")

        for i in photo_slots:
            path_col = f"PHOTO_photos_{i}_path"
            comment_col = f"PHOTO_photos_{i}_comment"
            path = r.get(path_col)
            if (
                not path
                or (isinstance(path, float) and math.isnan(path))
                or str(path).strip().lower() == "nan"
            ):
                continue

            comment_orig = str(r.get(comment_col) or "").strip()

            img_rows.append(
                {
                    "sample_id": sample_id,
                    "country_code": country,
                    "image_id": i,
                    "image_url": path,
                    "image_description_orig": comment_orig,
                    "image_description_en": "",  # will be filled later in Postgres
                    "collected_by": r.get("userId"),
                    "timestamp_utc": r.get("collectedAt") or r.get("fs_createdAt"),
                    "licence": DEFAULT_LICENCE,
                }
            )

    return pd.DataFrame(img_rows)


def build_sample_parameters_df_from_sqlite(df_flat: pd.DataFrame, db_path: str) -> pd.DataFrame:
    if not db_path or not os.path.exists(db_path):
        log.info(f"SQLite for lab_enrichment not found at {db_path}, skipping parameters.")
        return pd.DataFrame([])

    def norm_qr(q):
        return norm_qr_for_id(q)

    # we need QR -> country
    qr_to_country = {}
    for _, r in df_flat.iterrows():
        qr = r.get("QR_qrCode")
        if not qr:
            continue
        qr_n = norm_qr(qr)
        lat = r.get("GPS_lat")
        lon = r.get("GPS_long")
        country = (
            str(r.get("country_code_override") or "").strip().upper()
            or str(r.get("country_code") or "").strip().upper()
            or str(r.get("actual_cc") or "").strip().upper()
        )

        if not country:
            country_info = resolve_country_for_sample(
                lat,
                lon,
                planned_set=set(),
                allow_planned_fallback=False,
            )
            country = country_info.get("country_code", "")        
        
        qr_to_country[qr_n] = country

    conn = sqlite3.connect(db_path)
    try:
        lab_df = pd.read_sql_query("SELECT * FROM lab_enrichment", conn)
    except Exception as e:
        conn.close()
        log.info(f"no lab_enrichment table in {db_path}: {e}")
        return pd.DataFrame([])
    conn.close()

    if lab_df.empty:
        log.info("lab_enrichment table is empty, nothing to export.")
        return pd.DataFrame([])

    rows = []
    for _, r in lab_df.iterrows():
        qr_raw = r["qr_code"]
        qr_n = norm_qr(qr_raw)
        if not qr_n:
            continue

        country = qr_to_country.get(qr_n, "")

        rows.append(
            {
                "sample_id": qr_n,  # <- HERE: use QR as sample_id
                "country_code": country,
                "parameter_code": r["param"],
                "parameter_name": r["param"],
                "value": r["value"],
                "uom": r["unit"],
                "analysis_method": "",
                "analysis_date": r.get("updated_at"),
                "lab_id": DEFAULT_LAB_ID,
                "created_by": r.get("user_id"),
                "licence": DEFAULT_LICENCE,
                "parameter_uri": "",
            }
        )

    log.info("built sample_parameters from sqlite: %s rows", len(rows))
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------
def write_csv_atomic(df: pd.DataFrame, path: str, *, verbose: bool | None = None):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with tempfile.NamedTemporaryFile(
        "w", delete=False, dir=os.path.dirname(path), suffix=".csv"
    ) as tmp:
        df.to_csv(tmp.name, index=False)
        tmp_path = tmp.name

    os.replace(tmp_path, path)

    if verbose is None:
        verbose = VERBOSE_CSV_WRITES

    if verbose:
        log.info("Wrote %s rows to %s", len(df), path)

# ---------------------------------------------------------------------------
# MinIO: upload canonical files
# ---------------------------------------------------------------------------
def upload_canonical_to_minio(mclient, local_path: Path, object_name: str):
    """
    Upload a local canonical file to MinIO under canonical/<object_name>.
    """
    if mclient is None:
        return
    key = f"canonical/{object_name}"
    try:
        size = local_path.stat().st_size
        with local_path.open("rb") as f:
            mclient.put_object(
                MINIO_BUCKET,
                key,
                data=f,
                length=size,
                content_type="text/csv" if object_name.endswith(".csv") else "application/zip",
            )
        log.info("[OK] uploaded to MinIO: %s", key)
    except Exception as e:
        log.warning("Could not upload %s to MinIO as %s: %s", local_path, key, e)


# ---------------------------------------------------------------------------
# Postgres tables
# ---------------------------------------------------------------------------
try:
    from psycopg2 import sql
except ImportError:
    sql = None

def ensure_pg_tables():
    if psycopg2 is None or sql is None:
        log.info("[PG] psycopg2 not installed, skipping PG table setup")
        return

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST_OUTSIDE", "echorepo-postgres"),
        port=int(os.getenv("DB_PORT_OUTSIDE", "5432")),
        dbname=os.getenv("DB_NAME", "echorepo"),
        user=os.getenv("DB_USER", "echorepo"),
        password=os.getenv("DB_PASSWORD", "echorepo-pass"),
    )
    cur = conn.cursor()

    # Create minimal shells if they don't exist
    cur.execute("CREATE TABLE IF NOT EXISTS samples (sample_id TEXT PRIMARY KEY)")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sample_images (
          sample_id TEXT,
          country_code CHAR(2),
          image_id INTEGER,
          image_url TEXT,
          image_description_orig TEXT,
          image_description_en   TEXT,
          collected_by TEXT,
          timestamp_utc TIMESTAMPTZ,
          licence TEXT,
          PRIMARY KEY (sample_id, image_id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sample_parameters (
          sample_id TEXT,
          country_code CHAR(2),
          parameter_code TEXT,
          parameter_name TEXT,
          value DOUBLE PRECISION,
          uom TEXT,
          analysis_method TEXT,
          analysis_date TIMESTAMPTZ,
          lab_id TEXT,
          created_by TEXT,
          licence TEXT,
          parameter_uri TEXT,
          PRIMARY KEY (sample_id, parameter_code)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sample_otu_counts (
            sample_id TEXT NOT NULL,
            marker    TEXT NOT NULL,
            otu_id    TEXT NOT NULL,
            count     DOUBLE PRECISION,
            taxa      JSONB,
            uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            uploaded_by TEXT,
            source_file TEXT,
            PRIMARY KEY (sample_id, marker, otu_id)
        );
    """)

    def ensure_col(table: str, col: str, typ: str):
        cur.execute(
            """
            SELECT 1 FROM information_schema.columns
             WHERE table_name=%s AND column_name=%s
        """,
            (table, col),
        )
        if cur.fetchone() is None:
            cur.execute(
                sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                    sql.Identifier(table), sql.Identifier(col), sql.SQL(typ)
                )
            )

    # Desired columns for the NEW samples schema
    cols = {
        "timestamp_utc": "TIMESTAMPTZ",
        "lat": "DOUBLE PRECISION",
        "lon": "DOUBLE PRECISION",
        "country_code": "CHAR(2)",
        "location_accuracy_m": "INTEGER",
        "ph": "DOUBLE PRECISION",
        "organic_carbon_pct": "DOUBLE PRECISION",
        "earthworms_count": "INTEGER",
        "contamination_debris": "INTEGER",
        "contamination_plastic": "INTEGER",
        "contamination_other_orig": "TEXT",
        "contamination_other_en": "TEXT",
        "pollutants_count": "INTEGER",
        "soil_structure_orig": "TEXT",
        "soil_structure_en": "TEXT",
        "soil_texture_orig": "TEXT",
        "soil_texture_en": "TEXT",
        "observations_orig": "TEXT",
        "observations_en": "TEXT",
        "metals_info_orig": "TEXT",
        "metals_info_en": "TEXT",
        "collected_by": "TEXT",
        "data_source": "TEXT",
        "qa_status": "TEXT",
        "licence": "TEXT",
    }

    # Make sure the base column exists (rare edge DBs might be missing it)
    ensure_col("samples", "sample_id", "TEXT")
    # Add any missing columns
    for c, t in cols.items():
        ensure_col("samples", c, t)

    ensure_col("sample_otu_counts", "taxa", "JSONB")

    conn.commit()
    cur.close()
    conn.close()


def load_canonical_into_pg_staging(samples_path, images_path, params_path):
    """
    Load canonical CSVs into Postgres using TEXT staging tables,
    then normalize+cast into real tables.
    """
    if psycopg2 is None:
        log.info("[PG] psycopg2 not installed, skipping PG load.")
        return

    # Ensure schema matches the new CSV columns
    try:
        ensure_pg_tables()
    except Exception as e:
        log.info("[PG] ensure_pg_tables() failed (will try to continue): %s", e)

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST_OUTSIDE", "echorepo-postgres"),
        port=int(os.getenv("DB_PORT_OUTSIDE", "5432")),
        dbname=os.getenv("DB_NAME", "echorepo"),
        user=os.getenv("DB_USER", "echorepo"),
        password=os.getenv("DB_PASSWORD", "echorepo-pass"),
    )
    cur = conn.cursor()
    try:
        # 1) text-only staging tables
        cur.execute("DROP TABLE IF EXISTS samples_stage_raw;")
        cur.execute("""
            CREATE TABLE samples_stage_raw (
            sample_id TEXT,
            timestamp_utc TEXT,
            lat TEXT,
            lon TEXT,
            country_code TEXT,
            location_accuracy_m TEXT,
            ph TEXT,
            organic_carbon_pct TEXT,
            earthworms_count TEXT,

            contamination_debris TEXT,
            contamination_plastic TEXT,
            contamination_other_orig TEXT,
            contamination_other_en   TEXT,
            pollutants_count TEXT,

            soil_structure_orig TEXT,
            soil_structure_en   TEXT,
            soil_texture_orig   TEXT,
            soil_texture_en     TEXT,
            observations_orig   TEXT,
            observations_en     TEXT,
            metals_info_orig    TEXT,
            metals_info_en      TEXT,

            collected_by TEXT,
            data_source  TEXT,
            qa_status    TEXT,
            licence      TEXT
            );
        """)

        cur.execute("DROP TABLE IF EXISTS sample_images_stage_raw;")
        cur.execute("""
            CREATE TABLE sample_images_stage_raw (
              sample_id              TEXT,
              country_code           TEXT,
              image_id               TEXT,
              image_url              TEXT,
              image_description_orig TEXT,
              image_description_en   TEXT,
              collected_by           TEXT,
              timestamp_utc          TEXT,
              licence                TEXT
            );
        """)

        cur.execute("DROP TABLE IF EXISTS sample_parameters_stage_raw;")
        cur.execute("""
            CREATE TABLE sample_parameters_stage_raw (
              sample_id      TEXT,
              country_code   TEXT,
              parameter_code TEXT,
              parameter_name TEXT,
              value          TEXT,
              uom            TEXT,
              analysis_method TEXT,
              analysis_date  TEXT,
              lab_id         TEXT,
              created_by     TEXT,
              licence        TEXT,
              parameter_uri  TEXT
            );
        """)

        # 2) COPY raw CSVs → TEXT staging
        with open(samples_path, encoding="utf-8") as f:
            cur.copy_expert(
                """
                COPY samples_stage_raw FROM STDIN
                WITH CSV HEADER NULL ''
                """,
                f,
            )

        with open(images_path, encoding="utf-8") as f:
            cur.copy_expert(
                """
                COPY sample_images_stage_raw FROM STDIN
                WITH CSV HEADER NULL ''
                """,
                f,
            )

        with open(params_path, encoding="utf-8") as f:
            cur.copy_expert(
                """
                COPY sample_parameters_stage_raw FROM STDIN
                WITH CSV HEADER NULL ''
                """,
                f,
            )

        # commit COPY (it’s big)
        conn.commit()
        conn.autocommit = False
        cur = conn.cursor()

        # 3) swap into real tables
        # children first
        cur.execute("TRUNCATE sample_parameters;")

        # delete stale sample_images first, because they reference samples
        cur.execute("""
            DELETE FROM sample_images i
            WHERE NOT EXISTS (
                SELECT 1
                FROM sample_images_stage_raw st
                WHERE TRIM(st.sample_id) = i.sample_id
                AND NULLIF(TRIM(st.image_id), '')::integer = i.image_id
            );
        """)

        # now safe to delete stale samples
        cur.execute("""
            DELETE FROM samples s
            WHERE NOT EXISTS (
                SELECT 1
                FROM samples_stage_raw st
                WHERE TRIM(st.sample_id) = s.sample_id
            );
        """)

        # --- samples: upsert on sample_id, do NOT touch *_en on conflict ---
        cur.execute("""
        INSERT INTO samples (
            sample_id, timestamp_utc, lat, lon, country_code, location_accuracy_m,
            ph, organic_carbon_pct, earthworms_count,
            contamination_debris, contamination_plastic, contamination_other_orig, contamination_other_en, pollutants_count,
            soil_structure_orig, soil_structure_en, soil_texture_orig, soil_texture_en,
            observations_orig, observations_en, metals_info_orig, metals_info_en,
            collected_by, data_source, qa_status, licence
        )
        SELECT
            TRIM(sample_id),
            NULLIF(TRIM(timestamp_utc), '')::timestamptz,
            NULLIF(TRIM(lat), '')::double precision,
            NULLIF(TRIM(lon), '')::double precision,
            NULLIF(TRIM(country_code), ''),
            NULLIF(TRIM(location_accuracy_m), '')::integer,
            NULLIF(TRIM(ph), '')::double precision,
            NULLIF(TRIM(organic_carbon_pct), '')::double precision,
            NULLIF(TRIM(earthworms_count), '')::integer,
            NULLIF(TRIM(contamination_debris), '')::integer,
            NULLIF(TRIM(contamination_plastic), '')::integer,
            NULLIF(TRIM(contamination_other_orig), ''),
            NULLIF(TRIM(contamination_other_en), ''),
            NULLIF(TRIM(pollutants_count), '')::integer,
            NULLIF(TRIM(soil_structure_orig), ''),
            NULLIF(TRIM(soil_structure_en), ''),
            NULLIF(TRIM(soil_texture_orig), ''),
            NULLIF(TRIM(soil_texture_en), ''),
            NULLIF(TRIM(observations_orig), ''),
            NULLIF(TRIM(observations_en), ''),
            NULLIF(TRIM(metals_info_orig), ''),
            NULLIF(TRIM(metals_info_en), ''),
            NULLIF(TRIM(collected_by), ''),
            NULLIF(TRIM(data_source), ''),
            NULLIF(TRIM(qa_status), ''),
            NULLIF(TRIM(licence), '')
        FROM samples_stage_raw
        ON CONFLICT (sample_id) DO UPDATE SET
            timestamp_utc            = EXCLUDED.timestamp_utc,
            lat                      = EXCLUDED.lat,
            lon                      = EXCLUDED.lon,
            country_code             = EXCLUDED.country_code,
            location_accuracy_m      = EXCLUDED.location_accuracy_m,
            ph                       = EXCLUDED.ph,
            organic_carbon_pct       = EXCLUDED.organic_carbon_pct,
            earthworms_count         = EXCLUDED.earthworms_count,
            contamination_debris     = EXCLUDED.contamination_debris,
            contamination_plastic    = EXCLUDED.contamination_plastic,
            contamination_other_orig = EXCLUDED.contamination_other_orig,
            pollutants_count         = EXCLUDED.pollutants_count,
            soil_structure_orig      = EXCLUDED.soil_structure_orig,
            soil_texture_orig        = EXCLUDED.soil_texture_orig,
            observations_orig        = EXCLUDED.observations_orig,
            metals_info_orig         = EXCLUDED.metals_info_orig,
            collected_by             = EXCLUDED.collected_by,
            data_source              = EXCLUDED.data_source,
            qa_status                = EXCLUDED.qa_status,
            licence                  = EXCLUDED.licence;
        """)

        # --- sample_images: upsert on (sample_id, image_id), keep *_en ---
        cur.execute("""
            INSERT INTO sample_images (
            sample_id,
            country_code,
            image_id,
            image_url,
            image_description_orig,
            image_description_en,
            collected_by,
            timestamp_utc,
            licence
            )
            SELECT
            TRIM(sample_id),
            NULLIF(TRIM(country_code), ''),
            NULLIF(TRIM(image_id), '')::integer,
            NULLIF(TRIM(image_url), ''),
            NULLIF(TRIM(image_description_orig), ''),
            NULLIF(TRIM(image_description_en), ''),
            NULLIF(TRIM(collected_by), ''),
            NULLIF(TRIM(timestamp_utc), '')::timestamptz,
            NULLIF(TRIM(licence), '')
            FROM sample_images_stage_raw
            ON CONFLICT (sample_id, image_id) DO UPDATE SET
            country_code           = EXCLUDED.country_code,
            image_url              = EXCLUDED.image_url,
            image_description_orig = EXCLUDED.image_description_orig,
            collected_by           = EXCLUDED.collected_by,
            timestamp_utc          = EXCLUDED.timestamp_utc,
            licence                = EXCLUDED.licence;
        """)
        # --- sample_parameters: still refresh from scratch (no translations here) ---
        cur.execute("""
            INSERT INTO sample_parameters (
              sample_id,
              country_code,
              parameter_code,
              parameter_name,
              value,
              uom,
              analysis_method,
              analysis_date,
              lab_id,
              created_by,
              licence,
              parameter_uri
            )
            SELECT
              TRIM(sample_id),
              NULLIF(TRIM(country_code), ''),
              NULLIF(TRIM(parameter_code), ''),
              NULLIF(TRIM(parameter_name), ''),
              NULLIF(TRIM(value), '')::double precision,
              NULLIF(TRIM(uom), ''),
              NULLIF(TRIM(analysis_method), ''),
              NULLIF(TRIM(analysis_date), '')::timestamptz,
              NULLIF(TRIM(lab_id), ''),
              NULLIF(TRIM(created_by), ''),
              NULLIF(TRIM(licence), ''),
              NULLIF(TRIM(parameter_uri), '')
            FROM sample_parameters_stage_raw;
        """)

        conn.commit()
        log.info("[PG] staging swap completed.")
    except Exception as e:
        conn.rollback()
        log.error("[PG] staging load failed: %s", e)
    finally:
        try:
            cur.execute("DROP TABLE IF EXISTS samples_stage_raw;")
            cur.execute("DROP TABLE IF EXISTS sample_images_stage_raw;")
            cur.execute("DROP TABLE IF EXISTS sample_parameters_stage_raw;")
            conn.commit()
        except Exception:
            conn.rollback()
        cur.close()
        conn.close()

def _issue_sample_id_for_row(row) -> str:
    """
    Return the stable sample identifier used in coordinate_check_approved.csv.
    Prefer QR_qrCode because this is what the coordinate issue UI displays.
    """
    for col in ("QR_qrCode", "sample_id", "sampleId"):
        if col in row and str(row.get(col) or "").strip():
            value = str(row.get(col) or "").strip()
            try:
                return norm_qr_for_id(value).upper()
            except Exception:
                return value.upper()
    return ""


def apply_coordinate_approvals(df_annotated: pd.DataFrame) -> pd.DataFrame:
    """
    Apply persistent human approvals and country overrides.

    Adds:
      - _issue_sample_id
      - coordinate_issue_approved
      - country_code_override
      - wrong_coordinates_effective
    """
    out = df_annotated.copy()

    if out.empty:
        out["coordinate_issue_approved"] = []
        out["country_code_override"] = []
        out["wrong_coordinates_effective"] = []
        return out

    approved_map = load_approved_coordinate_samples()
    approved_ids = set(approved_map.keys())

    out["_issue_sample_id"] = out.apply(_issue_sample_id_for_row, axis=1)

    out["coordinate_issue_approved"] = out["_issue_sample_id"].isin(approved_ids)

    def _country_override_for_row(row):
        sid = str(row.get("_issue_sample_id") or "").strip().upper()
        if not sid:
            return ""

        override = str(
            approved_map.get(sid, {}).get("country_code_override") or ""
        ).strip().upper()

        if override:
            return override

        planned = str(row.get("planned_iso2") or "").strip().upper()
        planned_set = {x.strip() for x in planned.split(",") if x.strip()}

        if sid in approved_ids and len(planned_set) == 1:
            return next(iter(planned_set))

        return ""

    out["country_code_override"] = out.apply(_country_override_for_row, axis=1)

    out["wrong_coordinates_effective"] = out.apply(
        lambda row: _truthy(row.get("wrong_coordinates"))
        and not _truthy(row.get("coordinate_issue_approved")),
        axis=1,
    )

    log.info(
        "Coordinate approvals: approved=%s, overridden_country=%s, actionable_wrong=%s",
        int(out["coordinate_issue_approved"].sum()),
        int((out["country_code_override"].astype(str).str.strip() != "").sum()),
        int(out["wrong_coordinates_effective"].sum()),
    )

    return out

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    if COUNTRY_RESOLVER_ENABLED and not country_resolver_health_ok():
        log.warning(
            "Country resolver service is not available. "
            "Coordinate resolution will probably fail and rows may be flagged."
        )

    init_firebase()
    minio_client = init_minio()

    # 1) fetch from Firestore
    df_raw = fetch_samples_flat(minio_client)

    # normalize timestamps in raw data
    for col in ("collectedAt", "fs_createdAt", "fs_updatedAt"):
        if col in df_raw.columns:
            df_raw[col] = df_raw[col].apply(_ts_to_iso_loose)

    write_csv_atomic(df_raw, INPUT_CSV)

    # 2) enrich with emails
    uid_to_email = fetch_uid_to_email()

    df_enriched = df_raw.copy()
    if not df_enriched.empty:
        if "userId" not in df_enriched.columns:
            df_enriched["userId"] = ""
        df_enriched["email"] = df_enriched["userId"].map(uid_to_email).fillna("")

    # normalize timestamps again just in case
    for col in ("collectedAt", "fs_createdAt", "fs_updatedAt"):
        if col in df_enriched.columns:
            df_enriched[col] = df_enriched[col].apply(_ts_to_iso_loose)

    # Load planned QR -> country map for coordinate validation and country overrides.
    planned_map = {}
    if load_qr_to_planned is not None:
        try:
            planned_map = load_qr_to_planned(PLANNED_XLSX)
            log.info("Loaded planned QR -> country map: %d entries", len(planned_map))
        except Exception as e:
            log.warning("Could not load planned QR countries: %s", e)

    # 3) users.csv
    df_users = pd.DataFrame(columns=["email"], data=sorted(list(uid_to_email.values())))
    write_csv_atomic(df_users, USERS_CSV)

    # 3b) coordinate validation/filtering
    df_filtered, df_annotated = annotate_and_filter_wrong_coordinates(
        df_enriched,
        planned_map=planned_map,
        qr_col="QR_qrCode",
        lat_col="GPS_lat",
        lon_col="GPS_long",
    )

    df_annotated = apply_coordinate_approvals(df_annotated)

    coord_diag_path = str(PROJECT_ROOT / "data" / "coordinate_check_annotated.csv")
    coord_bad_path = str(PROJECT_ROOT / "data" / "coordinate_check_wrong.csv")

    write_csv_atomic(df_annotated, coord_diag_path)

    write_csv_atomic(
        df_annotated[df_annotated["wrong_coordinates_effective"] == True].copy(),
        coord_bad_path,
    )

    if FILTER_WRONG_COORDINATES:
        log.warning(
            "FILTER_WRONG_COORDINATES=true: dropping effective wrong-coordinate rows"
        )
        df_enriched = df_annotated[
            df_annotated["wrong_coordinates_effective"] != True
        ].copy()
    else:
        log.info("Keeping all rows in enriched output; coordinate issues are only flagged")
        df_enriched = df_annotated

    # This is the ONLY write of OUTPUT_CSV.
    write_csv_atomic(df_enriched, OUTPUT_CSV)

    # 4) refresh sqlite from OUTPUT_CSV, preserve lab_enrichment
    refreshed_db_path = refresh_sqlite_from_csv(OUTPUT_CSV, SQLITE_PATH)

    # 5) build canonical export
    if not df_enriched.empty:
        canon_dir = PROJECT_ROOT / "data" / "canonical"
        canon_dir.mkdir(parents=True, exist_ok=True)

        samples_df = build_samples_df(df_enriched, planned_map=planned_map)
        images_df = build_sample_images_df(df_enriched)
        params_df = build_sample_parameters_df_from_sqlite(df_enriched, refreshed_db_path)

        # 1) build a set of valid sample_ids (whatever you decided sample_id is now — QR)
        valid_sample_ids = set(samples_df["sample_id"].dropna().astype(str).str.strip())

        # 1a) log orphaned parameters
        orphan_params = params_df[~params_df["sample_id"].isin(valid_sample_ids)]

        if WRITE_ORPHAN_PARAMETERS_DIAG and not orphan_params.empty:
            orphan_path = PROJECT_ROOT / "data" / "canonical" / "orphan_sample_parameters.csv"
            os.makedirs(orphan_path.parent, exist_ok=True)
            orphan_params.to_csv(orphan_path, index=False)
            log.info("Wrote %s orphan parameter rows to %s", len(orphan_params), orphan_path)

        # 2) drop parameter rows that point to a non-existing sample
        if not params_df.empty:
            before = len(params_df)
            params_df = params_df[
                params_df["sample_id"].astype(str).str.strip().isin(valid_sample_ids)
            ].copy()
            after = len(params_df)
            if before != after:
                log.info("Dropped %s parameter rows without matching sample", before - after)

        # sanitize numeric-ish columns so Postgres COPY is happy
        if not samples_df.empty:
            int_cols = [
                "location_accuracy_m",
                "earthworms_count",
                "contamination_debris",
                "contamination_plastic",
                "pollutants_count",
            ]
            float_cols = [
                "ph",
                "organic_carbon_pct",
            ]

            for col in int_cols:
                if col in samples_df.columns:
                    samples_df[col] = samples_df[col].apply(_clean_int_val)
                    # IMPORTANT: make pandas actually store as integer so CSV becomes "256" not "256.0"
                    samples_df[col] = samples_df[col].astype("Int64")  # nullable int

            for col in float_cols:
                if col in samples_df.columns:
                    samples_df[col] = samples_df[col].apply(_clean_float_val)

        # 2) images: image_id should also be int
        if not images_df.empty and "image_id" in images_df.columns:
            images_df["image_id"] = images_df["image_id"].apply(_clean_int_val).astype("Int64")

        # 3) params: value is usually numeric but may be text — keep as float/str if you want
        if not params_df.empty and "value" in params_df.columns:
            params_df["value"] = params_df["value"].apply(_clean_float_val)

        # 4a) write canonical CSVs
        samples_path = canon_dir / "samples.csv"
        images_path = canon_dir / "sample_images.csv"
        params_path = canon_dir / "sample_parameters.csv"

        write_csv_atomic(samples_df, str(samples_path))
        write_csv_atomic(images_df, str(images_path))
        write_csv_atomic(params_df, str(params_path))
        log.info("[OK] Wrote canonical 3-file export.")

        # 4b) produce all.zip locally
        all_zip_path = canon_dir / "all.zip"
        with zipfile.ZipFile(all_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(samples_path, arcname="samples.csv")
            zf.write(images_path, arcname="sample_images.csv")
            zf.write(params_path, arcname="sample_parameters.csv")
        log.info("[OK] Wrote %s", all_zip_path)

        # 5) optional: Postgres (just ensuring tables for now)
        try:
            ensure_pg_tables()
        except Exception as e:
            log.warning("ensure_pg_tables() failed (will try to continue): %s", e)

        # 6) optional: load into Postgres staging + swap
        try:
            load_canonical_into_pg_staging(
                str(samples_path),
                str(images_path),
                str(params_path),
            )
        except Exception as e:
            log.warning("PG staging load skipped/failed: %s", e)
    log.info(
        "Pipeline complete: raw=%s, enriched=%s, users=%s, canonical_samples=%s, images=%s, parameters=%s",
        len(df_raw),
        len(df_enriched),
        len(df_users),
        len(samples_df) if "samples_df" in locals() else 0,
        len(images_df) if "images_df" in locals() else 0,
        len(params_df) if "params_df" in locals() else 0,
    )

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error("[ERROR] %s", e)
        sys.exit(1)
