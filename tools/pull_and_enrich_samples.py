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

import os
import sys
import math
import tempfile
from pathlib import Path
from datetime import datetime, timezone

import re
import requests
from urllib.parse import urlparse
import sqlite3
import io
import zipfile

import pandas as pd
from dotenv import load_dotenv

import firebase_admin
from firebase_admin import credentials, firestore, auth

import shapefile  # pyshp
from shapely.geometry import shape as shp_shape, Point

import numpy as np


# ---------------------------------------------------------------------------
# 0. load env and basic paths
# ---------------------------------------------------------------------------
env_path = Path.cwd() / ".env"
load_dotenv(dotenv_path=env_path)
print(f"[INFO] Loaded environment from {env_path}")

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/home/echo/ECHO-STORE/echorepo-lite"))
sys.path.insert(0, str(PROJECT_ROOT))

# helper: QR to country code
try:
    from echorepo.services.planned import load_qr_to_planned
    print("[INFO] imported load_qr_to_planned from echorepo.services.planned")
except Exception:
    load_qr_to_planned = None  # we'll guard later

try:
    # optional: reuse your parsing / jitter helpers if you like
    from echorepo.utils.geo import _parse_coord as geo_parse_coord
except Exception:
    geo_parse_coord = None

from echorepo.utils.load_csv import (
    _parse_coord as lc_parse_coord,
    deterministic_jitter as lc_det_jitter,
    _choose_stable_key as lc_choose_key,
    MAX_JITTER_METERS as LC_MAX_JITTER_METERS,
)

# helper to convert local or absolute paths to project-root-relative paths
def _local_path_to_abs(maybe_path: str) -> str:
    p = Path(maybe_path)
    if p.is_absolute():
        if p.exists():
            return str(p)
        alt = PROJECT_ROOT / p.relative_to("/")
        return str(alt)
    return str(PROJECT_ROOT / p)

USERS_CSV = _local_path_to_abs(os.getenv("USERS_CSV", "/data/users.csv"))
PLANNED_XLSX = _local_path_to_abs(os.getenv("PLANNED_XLSX", "/echorepo/utils/data/planned.xlsx"))
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

DEFAULT_LICENCE = os.getenv("DEFAULT_LICENCE", "CC-BY-4.0")
DEFAULT_LAB_ID = os.getenv("DEFAULT_LAB_ID", "ECHO-LAB-1")

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
# tolerate truncated images
# ---------------------------------------------------------------------------
try:
    from PIL import ImageFile
    ImageFile.LOAD_TRUNCATED_IMAGES = True
except Exception:
    pass

# ---------------------------------------------------------------------------
# Firebase init
# ---------------------------------------------------------------------------
def init_firebase():
    if firebase_admin._apps:
        return
    creds_path = _local_path_to_abs(os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/echorepo/keys/firebase-sa.json"))
    if not creds_path or not os.path.exists(creds_path):
        print(f"[ERROR] Service account JSON not found: {creds_path}")
        sys.exit(1)
    print(f"[INFO] Initializing Firebase with creds: {creds_path}")
    cred = credentials.Certificate(creds_path)
    firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID} if PROJECT_ID else None)

# ---------------------------------------------------------------------------
# MinIO init
# ---------------------------------------------------------------------------
def init_minio():
    if Minio is None:
        print("[INFO] python-minio not installed; will keep Firebase URLs and local canonical.")
        return None

    secure = False
    endpoint = MINIO_ENDPOINT
    if endpoint.startswith("https://"):
        secure = True
        endpoint = endpoint[len("https://"):]
    elif endpoint.startswith("http://"):
        secure = False
        endpoint = endpoint[len("http://"):]

    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        print("[WARN] MinIO credentials not set; skipping mirroring & canonical upload.")
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
            print(f"[INFO] Created MinIO bucket {MINIO_BUCKET}")
    except Exception as e:
        print(f"[WARN] Could not ensure MinIO bucket: {e}")
        return None
    print(f"[INFO] MinIO ready at {MINIO_ENDPOINT}, bucket={MINIO_BUCKET}")
    return client

# ---------------------------------------------------------------------------
# helper: input sanitization
# ---------------------------------------------------------------------------
BAD_NUM = {"", " ", "-", "NA", "N/A", "null", "None"}

def _clean_int_val(v):
    if v is None:
        return None
    s = str(v).strip().replace(",", ".")
    if s in BAD_NUM:
        return None
    try:
        # this handles "256.0", "12.00", "5"
        return int(float(s))
    except ValueError:
        return None

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
# helper: shapefile -> country polygons
# ---------------------------------------------------------------------------
_COUNTRY_SHAPES = []

def load_country_shapes_from_shp(shp_path="data/ne_50m_admin_0_countries.shp"):
    global _COUNTRY_SHAPES
    r = shapefile.Reader(shp_path)
    field_names = [f[0] for f in r.fields[1:]]
    try:
        iso_idx = field_names.index("ISO_A2")
    except ValueError:
        iso_idx = None
    shapes = []
    for sr in r.shapeRecords():
        geom = shp_shape(sr.shape.__geo_interface__)
        rec = sr.record
        if iso_idx is not None:
            iso2 = rec[iso_idx]
        else:
            iso2 = rec.get("ISO_A2", "") if hasattr(rec, "get") else ""
        if iso2 == "-99":
            iso2 = ""
        shapes.append((geom, iso2))
    _COUNTRY_SHAPES = shapes
    return shapes

def latlon_to_country_code(lat, lon):
    if lat is None or lon is None:
        return ""
    if not _COUNTRY_SHAPES:
        load_country_shapes_from_shp()
    pt = Point(lon, lat)
    for geom, iso2 in _COUNTRY_SHAPES:
        if geom.contains(pt):
            return iso2 or ""
    return ""

# ---------------------------------------------------------------------------
# misc helpers
# ---------------------------------------------------------------------------
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
        return None
    s = str(value).strip().lower().replace(",", ".")
    m = re.search(r"(-?\d+(\.\d+)?)", s)
    if not m:
        return value
    try:
        return float(m.group(1))
    except ValueError:
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
    s = (s or "").strip()
    return re.sub(r"[^A-Za-z0-9_.-]", "_", s) or "x"

def _guess_ext_from_firebase_url(url: str) -> str:
    parsed = urlparse(url)
    last = parsed.path.rsplit("/", 1)[-1]
    if "." in last:
        return "." + last.rsplit(".", 1)[-1]
    return ".bin"

def translate_to_en(text: str) -> str:
    LT_ENDPOINT = os.getenv("LT_ENDPOINT")
    if not text or not LT_ENDPOINT:
        return ""
    try:
        det = requests.post(f"{LT_ENDPOINT}/detect", data={"q": text}, timeout=5).json()
        src = det[0]["language"]
        if src == "en":
            return text
        trans = requests.post(
            f"{LT_ENDPOINT}/translate",
            data={"q": text, "source": src, "target": "en"},
            timeout=5,
        ).json()
        return trans["translatedText"]
    except Exception:
        return ""

def _mirror_firebase_to_minio(url: str, user_id: str, sample_id: str, field: str, mclient) -> str:
    if not url or not url.startswith(FBS_PREFIX) or mclient is None:
        return url
    user_id = _safe_part(user_id)
    sample_id = _safe_part(sample_id)
    field = _safe_part(field)
    ext = _guess_ext_from_firebase_url(url)
    object_name = f"{user_id}/{sample_id}/{field}{ext}"
    try:
        mclient.stat_object(MINIO_BUCKET, object_name)
        return f"{PUBLIC_STORAGE_BASE}/{object_name}"
    except Exception:
        pass
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.content
    except Exception as e:
        if MIRROR_VERBOSE:
            print(f"[WARN] could not download {url}: {e}")
        return url
    try:
        mclient.put_object(
            MINIO_BUCKET,
            object_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type="image/jpeg",
        )
        return f"{PUBLIC_STORAGE_BASE}/{object_name}"
    except Exception as e:
        if MIRROR_VERBOSE:
            print(f"[WARN] could not upload to MinIO {object_name}: {e}")
        return url

# ---------------------------------------------------------------------------
# 1. Firestore -> flattened rows
# ---------------------------------------------------------------------------
def fetch_samples_flat(mclient) -> pd.DataFrame:
    db = firestore.client()
    samples_ref = db.collection_group("samples")
    rows = []
    for doc in samples_ref.stream():
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
            continue
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
    df = pd.DataFrame(rows, dtype=object)
    if not df.empty:
        if "collectedAt" in df.columns:
            df = df.sort_values(by=["collectedAt"], na_position="last")
        if "QR_qrCode" in df.columns:
            df = df.drop_duplicates(subset=["QR_qrCode"], keep="first")
    return df

# ---------------------------------------------------------------------------
# 2. Firebase Auth
# ---------------------------------------------------------------------------
def fetch_uid_to_email() -> dict:
    mapping = {}
    page = auth.list_users()
    while page:
        for user in page.users:
            mapping[user.uid] = (user.email or "").strip()
        page = page.get_next_page()
    print(f"[INFO] Retrieved {len(mapping)} users from Firebase Auth.")
    return mapping

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
            param   = r.get("param") or ""
            value   = r.get("value")
            unit    = r.get("unit")
            user_id = r.get("user_id")
            raw_row = r.get("raw_row")
            updated = r.get("updated_at")
            if not qr_code or not param:
                continue
            cur.execute("""
                INSERT INTO lab_enrichment (qr_code, param, value, unit, user_id, raw_row, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, datetime('now')))
                ON CONFLICT(qr_code, param) DO UPDATE SET
                  value = excluded.value,
                  unit = excluded.unit,
                  user_id = excluded.user_id,
                  raw_row = excluded.raw_row,
                  updated_at = excluded.updated_at
            """, (qr_code, param, value, unit, user_id, raw_row, updated))
    conn.commit()
    conn.close()

def refresh_sqlite_from_csv(OUTPUT_CSV: str, sqlite_path: str):
    sys.path.insert(0, str(PROJECT_ROOT))
    from echorepo.utils.load_csv import ensure_sqlite

    csv_path = _resolve_path(OUTPUT_CSV)
    db_path = _resolve_path(sqlite_path)

    os.environ["CSV_PATH"] = csv_path
    os.environ["SQLITE_PATH"] = db_path

    print(f"[sqlite] CSV_PATH={csv_path}")
    print(f"[sqlite] SQLITE_PATH={db_path}")

    backup = _backup_lab_enrichment(db_path)
    if backup:
        print(f"[sqlite] backed up {len(backup['rows'])} lab_enrichment rows")
    else:
        print("[sqlite] no lab_enrichment to back up")

    ensure_sqlite()
    print("[sqlite] base SQLite refreshed from CSV")

    _restore_lab_enrichment(db_path, backup)
    print("[sqlite] lab_enrichment restored")

    return db_path

# ---------------------------------------------------------------------------
# canonical builders
# ---------------------------------------------------------------------------
def build_samples_df(df_flat: pd.DataFrame, planned_map: dict[str, set[str]] | None = None) -> pd.DataFrame:
    planned_map = planned_map or {}
    rows = []
    debug_missing = []   # collect rows that still have no country

    for idx, r in df_flat.iterrows():
        # --- 0) IDs / QR
        qr_raw = r.get("QR_qrCode")
        qr_norm = norm_qr_for_id(qr_raw)
        sample_id = qr_norm or r.get("sampleId")

        # --- 1) get original coords from Firestore
        orig_lat = r.get("GPS_lat")
        orig_lon = r.get("GPS_long")

        # normalize to float using existing helpers
        lat_f = None
        lon_f = None

        # prefer load_csv parser (you already imported it)
        if lc_parse_coord is not None:
            lat_f = lc_parse_coord(orig_lat, "lat")
            lon_f = lc_parse_coord(orig_lon, "lon")
        elif geo_parse_coord is not None:
            lat_f = geo_parse_coord(orig_lat, "lat")
            lon_f = geo_parse_coord(orig_lon, "lon")
        else:
            # ultra simple fallback
            try:
                lat_f = float(orig_lat) if orig_lat not in (None, "") else None
                lon_f = float(orig_lon) if orig_lon not in (None, "") else None
            except Exception:
                lat_f = lon_f = None

        # --- 2) country from ORIGINAL coords
        orig_country = ""
        if lat_f is not None and lon_f is not None:
            try:
                orig_country = latlon_to_country_code(lat_f, lon_f) or ""
            except Exception as e:
                orig_country = ""
                # we could print here, but let's collect below

        # --- 3) country from PLANNED file (by QR)
        planned_country = ""
        if qr_norm and qr_norm in planned_map:
            s = planned_map[qr_norm]
            if len(s) == 1:
                planned_country = next(iter(s))
            elif s:
                planned_country = sorted(s)[0]

        # --- 4) jittered coords (we actually want to use the same jitter as SQLite)
        # if you want to actually jitter here, do it deterministically:
        lat_j = lat_f
        lon_j = lon_f
        if lat_f is not None and lon_f is not None and lc_det_jitter is not None:
            key = sample_id or r.get("userId") or str(idx)
            lat_j, lon_j = lc_det_jitter(lat_f, lon_f, key, LC_MAX_JITTER_METERS)

        # --- 5) country from jittered coords
        jitter_country = ""
        if lat_j is not None and lon_j is not None:
            try:
                jitter_country = latlon_to_country_code(lat_j, lon_j) or ""
            except Exception:
                jitter_country = ""

        # --- 6) final pick
        country = jitter_country or planned_country or orig_country

        if not country:
            debug_missing.append({
                "row_index": idx,
                "sample_id": sample_id,
                "qr": qr_norm,
                "orig_lat": orig_lat,
                "orig_lon": orig_lon,
                "lat_f": lat_f,
                "lon_f": lon_f,
                "jitter_lat": lat_j,
                "jitter_lon": lon_j,
                "orig_country": orig_country,
                "jitter_country": jitter_country,
                "planned_country": planned_country,
            })

        # contamination
        cont_debris = r.get("SOIL_CONTAMINATION_debris") or 0
        cont_plastic = r.get("SOIL_CONTAMINATION_plastic") or 0
        cont_other_orig = r.get("SOIL_CONTAMINATION_comments") or ""
        cont_other_en = translate_to_en(cont_other_orig) if cont_other_orig else ""
        pollutants_count = 0
        for v in (cont_debris, cont_plastic, cont_other_orig):
            if v not in (0, "", None, False):
                pollutants_count += 1

        rows.append({
            "sample_id": sample_id,
            "timestamp_utc": r.get("collectedAt") or r.get("fs_createdAt"),
            "lat": lat_j,
            "lon": lon_j,
            "country_code": country,
            "location_accuracy_m": MAX_JITTER_METERS,
            "ph": r.get("PH_ph"),
            "organic_carbon_pct": None,
            "soil_structure": r.get("SOIL_STRUCTURE_structure"),
            "earthworms_count": r.get("SOIL_DIVER_earthworms"),
            "contamination_debris": cont_debris,
            "contamination_plastic": cont_plastic,
            "contamination_other_orig": cont_other_orig,
            "contamination_other_en": cont_other_en,
            "pollutants_count": pollutants_count,
            "vegetation_cover_pct": None,
            "forest_cover_pct": None,
            "collected_by": r.get("userId"),
            "data_source": "mobile",
            "qa_status": r.get("QA_state") or "",
            "licence": DEFAULT_LICENCE,
        })

    # --- write a tiny diag file so you can open it
    if debug_missing:
        diag_path = PROJECT_ROOT / "data" / "canonical" / "country_missing_diag.csv"
        os.makedirs(diag_path.parent, exist_ok=True)
        pd.DataFrame(debug_missing).to_csv(diag_path, index=False)
        print(f"[DIAG] wrote {len(debug_missing)} rows with missing country to {diag_path}")

    return pd.DataFrame(rows)

def build_sample_images_df(df_flat: pd.DataFrame) -> pd.DataFrame:
    img_rows = []
    photo_slots = list(range(1, 14))
    for _, r in df_flat.iterrows():
        lat = r.get("GPS_lat")
        lon = r.get("GPS_long")
        country = r.get("country_code") or latlon_to_country_code(lat, lon)

        qr = norm_qr_for_id(r.get("QR_qrCode"))
        sample_id = qr or r.get("sampleId")

        for i in photo_slots:
            path_col = f"PHOTO_photos_{i}_path"
            comment_col = f"PHOTO_photos_{i}_comment"
            path = r.get(path_col)
            if not path or (isinstance(path, float) and math.isnan(path)) or str(path).strip().lower() == "nan":
                continue
            comment_orig = r.get(comment_col) or ""
            comment_en = translate_to_en(comment_orig) if comment_orig else ""
            img_rows.append({
                "sample_id": sample_id,
                "country_code": country,
                "image_id": i,
                "image_url": path,
                "image_description_orig": comment_orig,
                "image_description_en": comment_en,
                "collected_by": r.get("userId"),
                "timestamp_utc": r.get("collectedAt") or r.get("fs_createdAt"),
                "licence": DEFAULT_LICENCE,
            })
    return pd.DataFrame(img_rows)

def build_sample_parameters_df_from_sqlite(df_flat: pd.DataFrame, db_path: str) -> pd.DataFrame:
    if not db_path or not os.path.exists(db_path):
        print(f"[INFO] SQLite for lab_enrichment not found at {db_path}, skipping parameters.")
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
        country = r.get("country_code") or latlon_to_country_code(lat, lon)
        qr_to_country[qr_n] = country

    conn = sqlite3.connect(db_path)
    try:
        lab_df = pd.read_sql_query("SELECT * FROM lab_enrichment", conn)
    except Exception as e:
        conn.close()
        print(f"[INFO] no lab_enrichment table in {db_path}: {e}")
        return pd.DataFrame([])
    conn.close()

    if lab_df.empty:
        print("[INFO] lab_enrichment table is empty, nothing to export.")
        return pd.DataFrame([])

    rows = []
    for _, r in lab_df.iterrows():
        qr_raw = r["qr_code"]
        qr_n = norm_qr(qr_raw)
        if not qr_n:
            continue

        country = qr_to_country.get(qr_n, "")

        rows.append({
            "sample_id": qr_n,                  # <- HERE: use QR as sample_id
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
        })

    print(f"[INFO] built sample_parameters from sqlite: {len(rows)} rows")
    return pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------
def write_csv_atomic(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=os.path.dirname(path), suffix=".csv") as tmp:
        df.to_csv(tmp.name, index=False)
        tmp_path = tmp.name
    os.replace(tmp_path, path)
    print(f"[OK] Wrote {path} (rows: {len(df)})")

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
        print(f"[OK] uploaded to MinIO: {key}")
    except Exception as e:
        print(f"[WARN] could not upload {local_path} to MinIO as {key}: {e}")

# ---------------------------------------------------------------------------
# Postgres tables
# ---------------------------------------------------------------------------
def ensure_pg_tables():
    if psycopg2 is None:
        return
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST_OUTSIDE", "echorepo-postgres"),
        port=int(os.getenv("DB_PORT_OUTSIDE", "5432")),
        dbname=os.getenv("DB_NAME", "echorepo"),
        user=os.getenv("DB_USER", "echorepo"),
        password=os.getenv("DB_PASSWORD", "echorepo-pass"),
    )
    cur = conn.cursor()
    cur.execute("""
CREATE TABLE IF NOT EXISTS samples (
  sample_id TEXT PRIMARY KEY,
  timestamp_utc TIMESTAMPTZ,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  country_code CHAR(2),
  location_accuracy_m INTEGER,
  ph DOUBLE PRECISION,
  organic_carbon_pct DOUBLE PRECISION,
  soil_structure TEXT,
  earthworms_count INTEGER,
  contamination_debris INTEGER,
  contamination_plastic INTEGER,
  contamination_other_orig TEXT,
  contamination_other_en TEXT,
  pollutants_count INTEGER,
  vegetation_cover_pct DOUBLE PRECISION,
  forest_cover_pct DOUBLE PRECISION,
  collected_by TEXT,
  data_source TEXT,
  qa_status TEXT,
  licence TEXT
);

CREATE TABLE IF NOT EXISTS sample_parameters (
  sample_id TEXT REFERENCES samples(sample_id),
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
);

CREATE TABLE IF NOT EXISTS sample_images (
  sample_id TEXT REFERENCES samples(sample_id),
  country_code CHAR(2),
  image_id INTEGER,
  image_url TEXT,
  image_description_orig TEXT,
  image_description_en TEXT,
  collected_by TEXT,
  timestamp_utc TIMESTAMPTZ,
  licence TEXT,
  PRIMARY KEY (sample_id, image_id)
);""")
    conn.commit()
    cur.close()
    conn.close()

def load_canonical_into_pg_staging(samples_path, images_path, params_path):
    """
    Load canonical CSVs into Postgres using TEXT staging tables,
    then normalize+cast into real tables.
    """
    if psycopg2 is None:
        print("[PG] psycopg2 not installed, skipping PG load.")
        return

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
              sample_id              TEXT,
              timestamp_utc          TEXT,
              lat                    TEXT,
              lon                    TEXT,
              country_code           TEXT,
              location_accuracy_m    TEXT,
              ph                     TEXT,
              organic_carbon_pct     TEXT,
              soil_structure         TEXT,
              earthworms_count       TEXT,
              contamination_debris   TEXT,
              contamination_plastic  TEXT,
              contamination_other_orig TEXT,
              contamination_other_en   TEXT,
              pollutants_count       TEXT,
              vegetation_cover_pct   TEXT,
              forest_cover_pct       TEXT,
              collected_by           TEXT,
              data_source            TEXT,
              qa_status              TEXT,
              licence                TEXT
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
        with open(samples_path, "r", encoding="utf-8") as f:
            cur.copy_expert(
                """
                COPY samples_stage_raw FROM STDIN
                WITH CSV HEADER NULL '' 
                """,
                f,
            )

        with open(images_path, "r", encoding="utf-8") as f:
            cur.copy_expert(
                """
                COPY sample_images_stage_raw FROM STDIN
                WITH CSV HEADER NULL ''
                """,
                f,
            )

        with open(params_path, "r", encoding="utf-8") as f:
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
        cur.execute("TRUNCATE sample_parameters, sample_images, samples;")

        # samples
        cur.execute("""
            INSERT INTO samples (
              sample_id,
              timestamp_utc,
              lat,
              lon,
              country_code,
              location_accuracy_m,
              ph,
              organic_carbon_pct,
              soil_structure,
              earthworms_count,
              contamination_debris,
              contamination_plastic,
              contamination_other_orig,
              contamination_other_en,
              pollutants_count,
              vegetation_cover_pct,
              forest_cover_pct,
              collected_by,
              data_source,
              qa_status,
              licence
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
              NULLIF(TRIM(soil_structure), ''),
              NULLIF(TRIM(earthworms_count), '')::integer,
              NULLIF(TRIM(contamination_debris), '')::integer,
              NULLIF(TRIM(contamination_plastic), '')::integer,
              NULLIF(TRIM(contamination_other_orig), ''),
              NULLIF(TRIM(contamination_other_en), ''),
              NULLIF(TRIM(pollutants_count), '')::integer,
              NULLIF(TRIM(vegetation_cover_pct), '')::double precision,
              NULLIF(TRIM(forest_cover_pct), '')::double precision,
              NULLIF(TRIM(collected_by), ''),
              NULLIF(TRIM(data_source), ''),
              NULLIF(TRIM(qa_status), ''),
              NULLIF(TRIM(licence), '')
            FROM samples_stage_raw;
        """)

        # sample_images
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
            FROM sample_images_stage_raw;
        """)

        # sample_parameters
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
        print("[PG] staging swap completed.")
    except Exception as e:
        conn.rollback()
        print(f"[PG] staging load failed: {e}")
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

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    init_firebase()
    minio_client = init_minio()

    # load country polygons once
    load_country_shapes_from_shp("data/ne_50m_admin_0_countries/ne_50m_admin_0_countries.shp")

    # 1) fetch from Firestore
    df_raw = fetch_samples_flat(minio_client)

    # normalize timestamps    
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

    write_csv_atomic(df_enriched, OUTPUT_CSV)

    # 3) users.csv
    df_users = pd.DataFrame(columns=["email"], data=sorted(list(uid_to_email.values())))
    write_csv_atomic(df_users, USERS_CSV)

    # 4) refresh sqlite from OUTPUT_CSV, preserve lab_enrichment
    refreshed_db_path = refresh_sqlite_from_csv(OUTPUT_CSV, SQLITE_PATH)
    
    # 5) load planned QR -> country map
    planned_map = {}
    if load_qr_to_planned is not None:
        try:
            planned_map = load_qr_to_planned(PLANNED_XLSX)
            print(f"[INFO] loaded planned QR countries: {len(planned_map)} entries")
        except Exception as e:
            print(f"[WARN] could not load planned QR countries: {e}")

    # 6) build canonical export
    if not df_enriched.empty:
        canon_dir = PROJECT_ROOT / "data" / "canonical"
        canon_dir.mkdir(parents=True, exist_ok=True)

        samples_df = build_samples_df(df_enriched, planned_map=planned_map)
        images_df = build_sample_images_df(df_enriched)
        params_df = build_sample_parameters_df_from_sqlite(df_enriched, refreshed_db_path)

        # 1) build a set of valid sample_ids (whatever you decided sample_id is now — QR)
        valid_sample_ids = set(
            samples_df["sample_id"]
            .dropna()
            .astype(str)
            .str.strip()
        )

        # 1a) log orphaned parameters
        orphan_params = params_df[~params_df["sample_id"].isin(valid_sample_ids)]
        orphan_params.to_csv("data/canonical/orphan_sample_parameters.csv", index=False)

        # 2) drop parameter rows that point to a non-existing sample
        if not params_df.empty:
            before = len(params_df)
            params_df = params_df[
                params_df["sample_id"]
                .astype(str)
                .str.strip()
                .isin(valid_sample_ids)
            ].copy()
            after = len(params_df)
            if before != after:
                print(f"[INFO] dropped {before - after} parameter rows without matching sample")

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
                "vegetation_cover_pct",
                "forest_cover_pct",
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
        print("[OK] Wrote canonical 3-file export.")

        # 4b) produce all.zip locally
        all_zip_path = canon_dir / "all.zip"
        with zipfile.ZipFile(all_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(samples_path, arcname="samples.csv")
            zf.write(images_path, arcname="sample_images.csv")
            zf.write(params_path, arcname="sample_parameters.csv")
        print(f"[OK] Wrote {all_zip_path}")

        # 5) upload canonical files to MinIO (so Flask can just redirect)
        upload_canonical_to_minio(minio_client, samples_path, "samples.csv")
        upload_canonical_to_minio(minio_client, images_path, "sample_images.csv")
        upload_canonical_to_minio(minio_client, params_path, "sample_parameters.csv")
        upload_canonical_to_minio(minio_client, all_zip_path, "all.zip")

        # 6) optional: Postgres (just ensuring tables for now)
        try:
            ensure_pg_tables()
        except Exception as e:
            print(f"[WARN] Could not ensure Postgres tables: {e}")

        # 7) optional: load into Postgres staging + swap
        try:
            load_canonical_into_pg_staging(
                str(samples_path),
                str(images_path),
                str(params_path),
            )
        except Exception as e:
            print(f"[WARN] PG staging load skipped/failed: {e}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
