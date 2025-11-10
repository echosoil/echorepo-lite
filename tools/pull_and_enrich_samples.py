#!/usr/bin/env python3
"""
Pipeline:
  1) Read Firestore collection group 'samples' and flatten to CSV.
  2) Enrich with Firebase Auth emails (by userId).
  3) Mirror Firebase Storage file URLs into MinIO (if configured) and rewrite the URLs.
  4) Build canonical 3-file export (samples, sample_images, sample_parameters).
  5) Write:
       - RAW_CSV            (raw flattened)
       - ENRICHED_CSV       (flattened + email)
       - USERS_CSV          (distinct valid emails)
       - data/canonical/*.csv
  6) (optional) Upsert canonical data into Postgres.
"""

import os
import sys
import tempfile
from pathlib import Path
from datetime import datetime, timezone
import re
import requests
from urllib.parse import urlparse
import sqlite3

import pandas as pd
from dotenv import load_dotenv

import firebase_admin
from firebase_admin import credentials, firestore, auth

# --- Postgres (optional) ------------------------------------------------------
try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    psycopg2 = None

# --- MinIO (optional) ---------------------------------------------------------
try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    Minio = None

    class S3Error(Exception):
        pass

# --- tolerate truncated images ------------------------------------------------
try:
    from PIL import ImageFile
    ImageFile.LOAD_TRUNCATED_IMAGES = True
except Exception:
    pass

# ------------------------------------------------------------------------------
# env + basic paths
# ------------------------------------------------------------------------------
env_path = Path.cwd() / ".env"
load_dotenv(dotenv_path=env_path)
print(f"[INFO] Loaded environment from {env_path}")

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/home/echo/ECHO-STORE/echorepo-lite"))
CREDS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/echorepo/keys/firebase-sa.json")
CREDS_PATH = CREDS_PATH if not CREDS_PATH.startswith("/keys") else str(PROJECT_ROOT / CREDS_PATH[1:])

INPUT_CSV = os.getenv("INPUT_CSV", "/data/echorepo_samples.csv")
INPUT_CSV = INPUT_CSV if not INPUT_CSV.startswith("/") else INPUT_CSV[1:]
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "/data/echorepo_samples_with_email.csv")
OUTPUT_CSV = OUTPUT_CSV if not OUTPUT_CSV.startswith("/") else OUTPUT_CSV[1:]
USERS_CSV = os.getenv("USERS_CSV", "/data/users.csv")
USERS_CSV = USERS_CSV if not USERS_CSV.startswith("/") else USERS_CSV[1:]

PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID", None)

RAW_CSV = str(PROJECT_ROOT / INPUT_CSV)
ENRICHED_CSV = str(PROJECT_ROOT / OUTPUT_CSV)
USERS_CSV = str(PROJECT_ROOT / USERS_CSV)

# ------------------------------------------------------------------------------
# MinIO config
# ------------------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_OUTSIDE", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER") or ""
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD") or ""
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "echorepo-uploads")
PUBLIC_STORAGE_BASE = os.getenv("PUBLIC_STORAGE_BASE", "/storage")

FBS_PREFIX = "https://firebasestorage.googleapis.com/"

DEFAULT_LICENCE = os.getenv("DEFAULT_LICENCE", "CC-BY-4.0")
DEFAULT_LAB_ID = os.getenv("DEFAULT_LAB_ID", "ECHO-LAB-1")
LAB_ENRICHMENT_DB = os.getenv("LAB_ENRICHMENT_DB", "")

# ------------------------------------------------------------------------------
# Firebase init
# ------------------------------------------------------------------------------
def init_firebase():
    if firebase_admin._apps:
        return
    if not CREDS_PATH or not os.path.exists(CREDS_PATH):
        print(f"[ERROR] Service account JSON not found: {CREDS_PATH}")
        sys.exit(1)
    print(f"[INFO] Initializing Firebase with creds: {CREDS_PATH}")
    cred = credentials.Certificate(CREDS_PATH)
    firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID} if PROJECT_ID else None)


# ------------------------------------------------------------------------------
# MinIO init
# ------------------------------------------------------------------------------
def init_minio():
    if Minio is None:
        print("[INFO] python-minio not installed; will keep Firebase URLs.")
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
        print("[WARN] MinIO credentials not set; skipping mirroring.")
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
    except S3Error as e:
        print(f"[WARN] Could not ensure MinIO bucket (S3Error): {e}")
        return None
    except Exception as e:
        print(f"[WARN] Could not ensure MinIO bucket: {e}")
        return None

    print(f"[INFO] MinIO ready at {MINIO_ENDPOINT}, bucket={MINIO_BUCKET}")
    return client


# ------------------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------------------
def parse_ph(value):
    """
    Turn things like 'ph 8.5', 'pH:7', '8,2' into float.
    Return original value if we can't parse (so we don't crash).
    """
    if value is None:
        return None
    s = str(value).strip().lower().replace(",", ".")
    m = re.search(r"(-?\d+(\.\d+)?)", s)
    if not m:
        return value  # keep original if it's something unexpected
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
    if hasattr(ts, "seconds") and hasattr(ts, "nanos"):
        total = ts.seconds + ts.nanos / 1_000_000_000
        dt = datetime.fromtimestamp(total, tz=timezone.utc)
        return dt.isoformat()
    return str(ts)


def _safe_part(s: str) -> str:
    s = (s or "").strip()
    return re.sub(r"[^A-Za-z0-9_.-]", "_", s) or "x"


def _guess_ext_from_firebase_url(url: str) -> str:
    parsed = urlparse(url)
    last = parsed.path.rsplit("/", 1)[-1]
    if "." in last:
        return "." + last.rsplit(".", 1)[-1]
    return ".bin"


def infer_country_from_latlon(lat, lon):
    """placeholder country detector; replace with proper PIP later"""
    try:
        if pd.isna(lat) or pd.isna(lon):
            return ""
    except Exception:
        if lat is None or lon is None:
            return ""
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return ""
    # Spain
    if 27.0 <= lat <= 44.5 and -19.0 <= lon <= 5.0:
        return "ES"
    # Portugal
    if 36.8 <= lat <= 42.3 and -9.6 <= lon <= -6.0:
        return "PT"
    # France
    if 41.0 <= lat <= 51.5 and -5.5 <= lon <= 9.8:
        return "FR"
    return ""


def translate_to_en(text: str) -> str:
    LT_ENDPOINT = os.getenv("LT_ENDPOINT")  # e.g. http://libretranslate:5000
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
    except S3Error as e:
        if getattr(e, "code", "") not in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
            print(f"[WARN] stat_object S3Error for {object_name}: {e}")
            return url
    except Exception as e:
        print(f"[WARN] generic stat error for {object_name}: {e}")
        return url

    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.content
    except Exception as e:
        print(f"[WARN] could not download {url}: {e}")
        return url

    try:
        import io

        mclient.put_object(
            MINIO_BUCKET,
            object_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type="image/jpeg",
        )
        return f"{PUBLIC_STORAGE_BASE}/{object_name}"
    except S3Error as e:
        print(f"[WARN] S3Error uploading to MinIO {object_name}: {e}")
        return url
    except Exception as e:
        print(f"[WARN] could not upload to MinIO {object_name}: {e}")
        return url


# ------------------------------------------------------------------------------
# Firestore -> flattened rows
# ------------------------------------------------------------------------------
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
            print(
                f"[WARN] sample {doc.id}: 'data' is {type(steps).__name__}, skipping steps flatten"
            )
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


# ------------------------------------------------------------------------------
# Firebase Auth -> {uid: email}
# ------------------------------------------------------------------------------
def fetch_uid_to_email() -> dict:
    mapping = {}
    page = auth.list_users()
    while page:
        for user in page.users:
            mapping[user.uid] = (user.email or "").strip()
        page = page.get_next_page()
    print(f"[INFO] Retrieved {len(mapping)} users from Firebase Auth.")
    return mapping


# ------------------------------------------------------------------------------
# canonical builders
# ------------------------------------------------------------------------------
def build_samples_df(df_flat: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in df_flat.iterrows():
        lat = r.get("GPS_lat")
        lon = r.get("GPS_long")
        country = r.get("country_code") or infer_country_from_latlon(lat, lon)

        cont_debris = r.get("SOIL_CONTAMINATION_debris") or 0
        cont_plastic = r.get("SOIL_CONTAMINATION_plastic") or 0
        cont_other_orig = r.get("SOIL_CONTAMINATION_comments") or ""
        cont_other_en = translate_to_en(cont_other_orig) if cont_other_orig else ""

        pollutants_count = 0
        for v in (cont_debris, cont_plastic, cont_other_orig):
            if v not in (0, "", None, False):
                pollutants_count += 1

        rows.append(
            {
                "sample_id": r.get("sampleId"),
                "timestamp_utc": r.get("collectedAt") or r.get("fs_createdAt"),
                "lat": lat,
                "lon": lon,
                "country_code": country,
                "location_accuracy_m": None,
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
            }
        )
    return pd.DataFrame(rows)


def build_sample_images_df(df_flat: pd.DataFrame) -> pd.DataFrame:
    img_rows = []
    photo_slots = list(range(1, 14))
    for _, r in df_flat.iterrows():
        lat = r.get("GPS_lat")
        lon = r.get("GPS_long")
        country = r.get("country_code") or infer_country_from_latlon(lat, lon)

        for i in photo_slots:
            path_col = f"PHOTO_photos_{i}_path"
            comment_col = f"PHOTO_photos_{i}_comment"
            path = r.get(path_col)
            if not path:
                continue
            comment_orig = r.get(comment_col) or ""
            comment_en = translate_to_en(comment_orig) if comment_orig else ""
            img_rows.append(
                {
                    "sample_id": r.get("sampleId"),
                    "country_code": country,
                    "image_id": i,
                    "image_url": path,
                    "image_description_orig": comment_orig,
                    "image_description_en": comment_en,
                    "collected_by": r.get("userId"),
                    "timestamp_utc": r.get("collectedAt") or r.get("fs_createdAt"),
                    "licence": DEFAULT_LICENCE,
                }
            )
    return pd.DataFrame(img_rows)


def build_sample_parameters_df(df_flat: pd.DataFrame) -> pd.DataFrame:
    if not LAB_ENRICHMENT_DB or not os.path.exists(LAB_ENRICHMENT_DB):
        return pd.DataFrame([])

    conn = sqlite3.connect(LAB_ENRICHMENT_DB)
    lab_df = pd.read_sql_query("SELECT * FROM lab_enrichment", conn)
    conn.close()

    qr_to_sample = {}
    qr_to_country = {}
    for _, r in df_flat.iterrows():
        qr = r.get("QR_qrCode")
        if not qr:
            continue
        lat = r.get("GPS_lat")
        lon = r.get("GPS_long")
        country = r.get("country_code") or infer_country_from_latlon(lat, lon)
        qr_to_sample[qr] = r.get("sampleId")
        qr_to_country[qr] = country

    rows = []
    for _, r in lab_df.iterrows():
        qr = r["qr_code"]
        sample_id = qr_to_sample.get(qr)
        if not sample_id:
            continue
        country = qr_to_country.get(qr, "")
        rows.append(
            {
                "sample_id": sample_id,
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
    return pd.DataFrame(rows)


# ------------------------------------------------------------------------------
# Atomic CSV writer
# ------------------------------------------------------------------------------
def write_csv_atomic(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", delete=False, dir=os.path.dirname(path), suffix=".csv"
    ) as tmp:
        df.to_csv(tmp.name, index=False)
        tmp_path = tmp.name
    os.replace(tmp_path, path)
    print(f"[OK] Wrote {path} (rows: {len(df)})")


# ------------------------------------------------------------------------------
# Postgres helpers (optional)
# ------------------------------------------------------------------------------
def get_pg_conn():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not installed")
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "echorepo-postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "echorepo"),
        user=os.getenv("DB_USER", "echorepo"),
        password=os.getenv("DB_PASSWORD", "echorepo-pass"),
    )


def upsert_to_postgres(samples_df, images_df, params_df):
    if psycopg2 is None:
        print("[INFO] psycopg2 not available; skipping Postgres upsert.")
        return

    conn = get_pg_conn()
    cur = conn.cursor()

    # samples
    if not samples_df.empty:
        cols = [
            "sample_id",
            "timestamp_utc",
            "lat",
            "lon",
            "country_code",
            "location_accuracy_m",
            "ph",
            "organic_carbon_pct",
            "soil_structure",
            "earthworms_count",
            "contamination_debris",
            "contamination_plastic",
            "contamination_other_orig",
            "contamination_other_en",
            "pollutants_count",
            "vegetation_cover_pct",
            "forest_cover_pct",
            "collected_by",
            "data_source",
            "qa_status",
            "licence",
        ]
        records = [tuple(samples_df.get(c).iloc[i] for c in cols) for i in range(len(samples_df))]
        sql = f"""
        INSERT INTO samples ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (sample_id) DO UPDATE SET
          timestamp_utc = EXCLUDED.timestamp_utc,
          lat = EXCLUDED.lat,
          lon = EXCLUDED.lon,
          country_code = EXCLUDED.country_code,
          ph = EXCLUDED.ph,
          soil_structure = EXCLUDED.soil_structure,
          earthworms_count = EXCLUDED.earthworms_count,
          contamination_debris = EXCLUDED.contamination_debris,
          contamination_plastic = EXCLUDED.contamination_plastic,
          contamination_other_orig = EXCLUDED.contamination_other_orig,
          contamination_other_en = EXCLUDED.contamination_other_en,
          pollutants_count = EXCLUDED.pollutants_count,
          collected_by = EXCLUDED.collected_by,
          qa_status = EXCLUDED.qa_status,
          licence = EXCLUDED.licence;
        """
        execute_values(cur, sql, records)

    # images
    if not images_df.empty:
        cols = [
            "sample_id",
            "country_code",
            "image_id",
            "image_url",
            "image_description_orig",
            "image_description_en",
            "collected_by",
            "timestamp_utc",
            "licence",
        ]
        records = [tuple(images_df.get(c).iloc[i] for c in cols) for i in range(len(images_df))]
        sql = f"""
        INSERT INTO sample_images ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (sample_id, image_id) DO UPDATE SET
          country_code = EXCLUDED.country_code,
          image_url = EXCLUDED.image_url,
          image_description_orig = EXCLUDED.image_description_orig,
          image_description_en = EXCLUDED.image_description_en,
          collected_by = EXCLUDED.collected_by,
          timestamp_utc = EXCLUDED.timestamp_utc,
          licence = EXCLUDED.licence;
        """
        execute_values(cur, sql, records)

    # parameters
    if not params_df.empty:
        cols = [
            "sample_id",
            "country_code",
            "parameter_code",
            "parameter_name",
            "value",
            "uom",
            "analysis_method",
            "analysis_date",
            "lab_id",
            "created_by",
            "licence",
            "parameter_uri",
        ]
        records = [tuple(params_df.get(c).iloc[i] for c in cols) for i in range(len(params_df))]
        sql = f"""
        INSERT INTO sample_parameters ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (sample_id, parameter_code) DO UPDATE SET
          country_code    = EXCLUDED.country_code,
          value           = EXCLUDED.value,
          uom             = EXCLUDED.uom,
          analysis_method = EXCLUDED.analysis_method,
          analysis_date   = EXCLUDED.analysis_date,
          lab_id          = EXCLUDED.lab_id,
          created_by      = EXCLUDED.created_by,
          licence         = EXCLUDED.licence,
          parameter_uri   = EXCLUDED.parameter_uri;
        """
        execute_values(cur, sql, records)

    conn.commit()
    cur.close()
    conn.close()
    print("[OK] Upserted canonical data into Postgres.")

def ensure_pg_tables():
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS samples (
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

# ------------------------------------------------------------------------------
# main
# ------------------------------------------------------------------------------
def main():
    init_firebase()
    minio_client = init_minio()

    # 1) pull raw
    df_raw = fetch_samples_flat(minio_client)
    write_csv_atomic(df_raw, RAW_CSV)

    # 2) enrich with emails
    uid_to_email = fetch_uid_to_email()
    df_enriched = df_raw.copy()
    if not df_enriched.empty:
        if "userId" not in df_enriched.columns:
            df_enriched["userId"] = ""
        df_enriched["email"] = df_enriched["userId"].map(uid_to_email).fillna("")
    write_csv_atomic(df_enriched, ENRICHED_CSV)

    # 3) users CSV
    df_users = pd.DataFrame(columns=["email"], data=sorted(list(uid_to_email.values())))
    write_csv_atomic(df_users, USERS_CSV)

    # 4) canonical exports
    if not df_enriched.empty:
        canon_dir = PROJECT_ROOT / "data" / "canonical"
        canon_dir.mkdir(parents=True, exist_ok=True)

        samples_df = build_samples_df(df_enriched)
        sample_images_df = build_sample_images_df(df_enriched)
        sample_parameters_df = build_sample_parameters_df(df_enriched)

        write_csv_atomic(samples_df, str(canon_dir / "samples.csv"))
        write_csv_atomic(sample_images_df, str(canon_dir / "sample_images.csv"))
        write_csv_atomic(sample_parameters_df, str(canon_dir / "sample_parameters.csv"))
        print("[OK] Wrote canonical 3-file export.")

        # 5) optional Postgres sink
        try:
            ensure_pg_tables()
        except Exception as e:
            print(f"[WARN] Could not ensure Postgres tables: {e}")

        try:
            upsert_to_postgres(samples_df, sample_images_df, sample_parameters_df)
        except Exception as e:
            print(f"[WARN] Could not upsert into Postgres: {e}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
