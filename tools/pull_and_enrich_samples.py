#!/usr/bin/env python3
"""
Pipeline:
  1) Read Firestore collection group 'samples' and flatten to CSV.
  2) Enrich with Firebase Auth emails (by userId).
  3) Write:
       - INPUT_CSV  (raw flattened)
       - OUTPUT_CSV (flattened + email)
       - OUTPUT_USERS_CSV (distinct valid emails)

Environment:
  - .env is loaded explicitly from /home/echo/ECHO-STORE/echorepo-lite/.env (override via ENV_PATH)
  - GOOGLE_APPLICATION_CREDENTIALS must point to the service account JSON
  - FIREBASE_PROJECT_ID optional
  - INPUT_CSV, OUTPUT_CSV, OUTPUT_USERS_CSV define outputs
"""

import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

import firebase_admin
from firebase_admin import credentials, firestore, auth
from pathlib import Path

load_dotenv()
# ---------- Load .env explicitly ----------
print(f"[INFO] Loaded environment from {os.getenv('PROJECT_ROOT')}/.env")

# ---------- Config from env (with sane defaults) ----------
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

# ---------- Firebase init ----------
def init_firebase():
    if firebase_admin._apps:
        return
    if not CREDS_PATH or not os.path.exists(CREDS_PATH):
        print(f"[ERROR] Service account JSON not found: {CREDS_PATH}")
        sys.exit(1)
    print(f"[INFO] Initializing Firebase with creds: {CREDS_PATH}")
    cred = credentials.Certificate(CREDS_PATH)
    firebase_admin.initialize_app(cred, {'projectId': PROJECT_ID} if PROJECT_ID else None)

# ---------- Firestore -> flattened rows ----------
def fetch_samples_flat() -> pd.DataFrame:
    db = firestore.client()
    samples_ref = db.collection_group('samples')
    rows = []
    for doc in samples_ref.stream():
        data = doc.to_dict() or {}
        row = {
            'sampleId': doc.id,
            'userId': doc.reference.parent.parent.id if doc.reference.parent and doc.reference.parent.parent else None,
            'collectedAt': data.get('collectedAt'),
        }

        steps = data.get('data', [])
        if not isinstance(steps, list):
            print(f"[WARN] sample {doc.id}: 'data' is {type(steps).__name__}, skipping steps flatten")
            rows.append(row)
            continue

        for step in steps:
            if not isinstance(step, dict):
                # keep a minimal trace instead of crashing
                continue
            step_type = step.get('type') or 'unknown'
            state = step.get('state')
            info = step.get('info', {})

            row[f"{step_type}_state"] = state
            if isinstance(info, dict):
                for key, val in info.items():
                    if isinstance(val, list):
                        for i, item in enumerate(val, start=1):
                            if isinstance(item, dict):
                                for subk, subv in item.items():
                                    row[f"{step_type}_{key}_{i}_{subk}"] = subv
                    else:
                        row[f"{step_type}_{key}"] = val
            else:
                row[f"{step_type}_info"] = str(info)

        rows.append(row)

    df = pd.DataFrame(rows, dtype=object)
    if not df.empty:
        if 'collectedAt' in df.columns:
            df = df.sort_values(by=['collectedAt'], na_position='last')
        if 'QR_qrCode' in df.columns:
            df = df.drop_duplicates(subset=['QR_qrCode'], keep='first')
    return df

# ---------- Firebase Auth -> {uid: email} ----------
def fetch_uid_to_email() -> dict:
    mapping = {}
    page = auth.list_users()
    while page:
        for user in page.users:
            mapping[user.uid] = (user.email or "").strip()
        page = page.get_next_page()
    print(f"[INFO] Retrieved {len(mapping)} users from Firebase Auth.")
    return mapping

# ---------- Atomic CSV writer ----------
def write_csv_atomic(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=os.path.dirname(path), suffix=".csv") as tmp:
        df.to_csv(tmp.name, index=False)
        tmp_path = tmp.name
    os.replace(tmp_path, path)
    print(f"[OK] Wrote {path} (rows: {len(df)})")

# ---------- Main pipeline ----------
def main():
    init_firebase()

    # 1) Firestore -> RAW_CSV
    df_raw = fetch_samples_flat()
    write_csv_atomic(df_raw, RAW_CSV)

    # 2) Enrich with email using Firebase Auth
    uid_to_email = fetch_uid_to_email()
    df_enriched = df_raw.copy()
    if not df_enriched.empty:
        if 'userId' not in df_enriched.columns:
            # create missing userId to avoid KeyError; fill with empty
            df_enriched['userId'] = ""
        df_enriched['email'] = df_enriched['userId'].map(uid_to_email).fillna("")

    write_csv_atomic(df_enriched, ENRICHED_CSV)

    # 3) Distinct valid emails -> USERS_CSV
    # if df_enriched.empty or 'email' not in df_enriched.columns:
    #     df_users = pd.DataFrame(columns=['email'])
    # else:
    #     df_users = (df_enriched['email']
    #                 .dropna()
    #                 .astype(str)
    #                 .str.strip()
    #                 .str.lower())
    #     df_users = df_users[(df_users.str.len() > 3) & (df_users.str.contains("@"))]
    #     df_users = pd.DataFrame({'email': df_users.drop_duplicates().sort_values()})
    df_users = pd.DataFrame(columns=['email'], data=sorted(list(uid_to_email.values())))

    write_csv_atomic(df_users, USERS_CSV)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
