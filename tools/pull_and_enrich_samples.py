#!/usr/bin/env python3
"""
Pipeline:
  1) Read Firestore collection group 'samples' and flatten to CSV.
  2) Enrich with Firebase Auth emails (by userId).
  3) Mirror Firebase Storage file URLs into MinIO (if configured) and rewrite the URLs.
  4) Write:
       - INPUT_CSV  (raw flattened)
       - OUTPUT_CSV (flattened + email)
       - OUTPUT_USERS_CSV (distinct valid emails)
"""

import os
import sys
import tempfile
from pathlib import Path
from datetime import datetime, timezone
import re
import requests
from urllib.parse import urlparse

import pandas as pd
from dotenv import load_dotenv

import firebase_admin
from firebase_admin import credentials, firestore, auth

# --- MinIO (optional) ---------------------------------------------------------
try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    Minio = None
    S3Error = Exception  # just to have the name

# ------------------------------------------------------------------------------ 
# env + basic paths
# ------------------------------------------------------------------------------
load_dotenv(dotenv_path=Path.cwd() / ".env")

print(f"[INFO] Loaded environment from {os.getenv('PROJECT_ROOT')}/.env")

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/home/echo/ECHO-STORE/echorepo-lite"))
CREDS_PATH   = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/echorepo/keys/firebase-sa.json")
CREDS_PATH   = CREDS_PATH if not CREDS_PATH.startswith("/keys") else str(PROJECT_ROOT / CREDS_PATH[1:])

INPUT_CSV    = os.getenv("INPUT_CSV", "/data/echorepo_samples.csv")
INPUT_CSV    = INPUT_CSV if not INPUT_CSV.startswith("/") else INPUT_CSV[1:]
OUTPUT_CSV   = os.getenv("OUTPUT_CSV", "/data/echorepo_samples_with_email.csv")
OUTPUT_CSV   = OUTPUT_CSV if not OUTPUT_CSV.startswith("/") else OUTPUT_CSV[1:]
USERS_CSV    = os.getenv("USERS_CSV", "/data/users.csv")
USERS_CSV    = USERS_CSV if not USERS_CSV.startswith("/") else USERS_CSV[1:]

PROJECT_ID   = os.getenv("FIREBASE_PROJECT_ID", None)

RAW_CSV      = str(PROJECT_ROOT / INPUT_CSV)
ENRICHED_CSV = str(PROJECT_ROOT / OUTPUT_CSV)
USERS_CSV    = str(PROJECT_ROOT / USERS_CSV)

# ------------------------------------------------------------------------------ 
# MinIO config (we'll try to use it if present)
# ------------------------------------------------------------------------------
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER") or ""
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD") or ""
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "echorepo-uploads")
# what your Flask app will later expose as public URL (reverse-proxy)
PUBLIC_STORAGE_BASE = os.getenv("PUBLIC_STORAGE_BASE", "/storage")

# firebase storage URL prefix
FBS_PREFIX = "https://firebasestorage.googleapis.com/"

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
    firebase_admin.initialize_app(cred, {'projectId': PROJECT_ID} if PROJECT_ID else None)

# ------------------------------------------------------------------------------ 
# MinIO init
# ------------------------------------------------------------------------------
def init_minio():
    """
    Return a ready Minio client or None if not available.
    """
    if Minio is None:
        print("[INFO] python-minio not installed; will keep Firebase URLs.")
        return None

    # allow endpoints like https://minio.example.com:9000
    secure = False
    endpoint = MINIO_ENDPOINT
    if endpoint.startswith("https://"):
        secure = True
        endpoint = endpoint[len("https://"):]
    elif endpoint.startswith("http://"):
        secure = False
        endpoint = endpoint[len("http://"):]

    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        print("[WARN] MinIO credentials not set; skipping mirroring.")
        return None

    client = Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
    )

    # make sure bucket exists
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

# ------------------------------------------------------------------------------ 
# helpers
# ------------------------------------------------------------------------------
def _ts_to_iso(ts):
    """
    Convert Firestore / protobuf timestamp → ISO8601 string in UTC.
    """
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
    # .../o/some%2Fpath%2Fphoto.jpg?alt=media&token=...
    parsed = urlparse(url)
    last = parsed.path.rsplit("/", 1)[-1]  # "photo.jpg" (still URL-encoded, but .jpg is visible)
    if "." in last:
        return "." + last.rsplit(".", 1)[-1]
    return ".bin"

def _mirror_firebase_to_minio(url: str, user_id: str, sample_id: str, field: str, mclient) -> str:
    """
    Option A:
    - if not Firebase URL → return as is
    - else build object_name = user_id/sample_id/field.ext
    - try stat; if exists → return PUBLIC_STORAGE_BASE/object_name
    - if not exists → download from Firebase → put_object → return PUBLIC_STORAGE_BASE/object_name
    - on any error → return original url
    """
    if not url or not url.startswith(FBS_PREFIX) or mclient is None:
        return url

    user_id = _safe_part(user_id)
    sample_id = _safe_part(sample_id)
    field = _safe_part(field)
    ext = _guess_ext_from_firebase_url(url)
    object_name = f"{user_id}/{sample_id}/{field}{ext}"

    # 1) check if already there
    try:
        mclient.stat_object(MINIO_BUCKET, object_name)
        # exists
        return f"{PUBLIC_STORAGE_BASE}/{object_name}"
    except S3Error as e:
        # if it's something other than NotFound, also bail out
        if e.code not in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
            print(f"[WARN] stat_object error for {object_name}: {e}")
            return url
        # else: we will try to download+upload
    except Exception as e:
        print(f"[WARN] generic stat error for {object_name}: {e}")
        return url

    # 2) download from Firebase
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.content
    except Exception as e:
        print(f"[WARN] could not download {url}: {e}")
        return url

    # 3) upload to MinIO
    try:
        import io
        mclient.put_object(
            MINIO_BUCKET,
            object_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type="image/jpeg",  # good enough; you can detect later
        )
        return f"{PUBLIC_STORAGE_BASE}/{object_name}"
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
            print(f"[WARN] sample {doc.id}: 'data' is {type(steps).__name__}, skipping steps flatten")
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
# Atomic CSV writer
# ------------------------------------------------------------------------------
def write_csv_atomic(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=os.path.dirname(path), suffix=".csv") as tmp:
        df.to_csv(tmp.name, index=False)
        tmp_path = tmp.name
    os.replace(tmp_path, path)
    print(f"[OK] Wrote {path} (rows: {len(df)})")

# ------------------------------------------------------------------------------ 
# main
# ------------------------------------------------------------------------------
def main():
    init_firebase()
    minio_client = init_minio()

    # 1) fetch & flatten (this is where URLs are rewritten)
    df_raw = fetch_samples_flat(minio_client)
    write_csv_atomic(df_raw, RAW_CSV)

    # 2) enrich with email
    uid_to_email = fetch_uid_to_email()
    df_enriched = df_raw.copy()
    if not df_enriched.empty:
        if "userId" not in df_enriched.columns:
            df_enriched["userId"] = ""
        df_enriched["email"] = df_enriched["userId"].map(uid_to_email).fillna("")

    write_csv_atomic(df_enriched, ENRICHED_CSV)

    # 3) distinct users
    df_users = pd.DataFrame(columns=["email"], data=sorted(list(uid_to_email.values())))
    write_csv_atomic(df_users, USERS_CSV)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
