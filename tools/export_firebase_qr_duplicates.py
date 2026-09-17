#!/usr/bin/env python3
"""
Export every Firestore sample document matching selected QR codes.

This script is intentionally different from pull_and_enrich_samples.py:
- it NEVER deduplicates by QR code;
- it preserves the same doc.id under different users;
- it only suppresses the same full Firestore path when a stream retry replays it;
- it enriches matches with Firebase Auth email addresses;
- it is read-only and does not mirror images to MinIO.

Examples:
  python3 tools/export_firebase_qr_duplicates.py \
      --qr XRXB-4454 ABCD-1234 \
      --output data/firebase_qr_matches.csv

  python3 tools/export_firebase_qr_duplicates.py \
      --qr-file data/qrs.txt \
      --output data/firebase_qr_matches.csv
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import firebase_admin
import pandas as pd
from dotenv import load_dotenv
from firebase_admin import auth, credentials, firestore


THIS_DIR = Path(__file__).resolve().parent
DEFAULT_ROOT = THIS_DIR.parent
ENV_PATH = Path(os.getenv("ENV_FILE", str(DEFAULT_ROOT / ".env")))
load_dotenv(dotenv_path=ENV_PATH)

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", str(DEFAULT_ROOT))).resolve()
PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID") or None

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(levelname)s: %(message)s",
)
log = logging.getLogger("export_firebase_qr_duplicates")

log.info("Loaded environment from %s", ENV_PATH)
log.info("Using PROJECT_ROOT=%s", PROJECT_ROOT)


def _local_path_to_abs(maybe_path: str) -> str:
    p = Path(maybe_path)
    if p.is_absolute():
        if p.exists():
            return str(p)
        return str(PROJECT_ROOT / p.relative_to("/"))
    return str(PROJECT_ROOT / p)


def init_firebase() -> None:
    if firebase_admin._apps:
        return

    creds_path = _local_path_to_abs(
        os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS",
            "/opt/echorepo/keys/firebase-sa.json",
        )
    )

    if not creds_path or not os.path.exists(creds_path):
        raise FileNotFoundError(
            f"Firebase service account JSON not found: {creds_path}"
        )

    log.info("Initializing Firebase")
    cred = credentials.Certificate(creds_path)
    firebase_admin.initialize_app(
        cred,
        {"projectId": PROJECT_ID} if PROJECT_ID else None,
    )


def norm_qr(q) -> str:
    if q is None:
        return ""
    try:
        if pd.isna(q):
            return ""
    except Exception:
        pass
    return str(q).strip().replace(" ", "").upper()


def ts_to_iso(ts):
    if ts is None:
        return ""
    if isinstance(ts, str):
        return ts
    if hasattr(ts, "isoformat"):
        dt = ts
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    return str(ts)


def ts_to_iso_loose(v):
    if v is None:
        return ""

    if hasattr(v, "isoformat"):
        dt = v
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()

    if hasattr(v, "seconds") and hasattr(v, "nanos"):
        sec = int(v.seconds)
        nanos = int(v.nanos or 0)
        dt = datetime.fromtimestamp(sec + nanos / 1_000_000_000, tz=timezone.utc)
        return dt.isoformat()

    if isinstance(v, str) and "seconds:" in v:
        s = v.replace("\r", " ").replace("\n", " ")
        m_sec = re.search(r"seconds:\s*(\d+)", s)
        m_nanos = re.search(r"nanos:\s*(\d+)", s)
        if m_sec:
            sec = int(m_sec.group(1))
            nanos = int(m_nanos.group(1)) if m_nanos else 0
            dt = datetime.fromtimestamp(sec + nanos / 1_000_000_000, tz=timezone.utc)
            return dt.isoformat()

    return v


def parse_ph(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    s = str(value).strip().lower().replace(",", ".")
    if s in {"", "-", "na", "n/a", "nan", "null", "none"}:
        return None

    m = re.search(r"([+-]?\d+(?:\.\d+)?)", s)
    if not m:
        return None

    try:
        ph = float(m.group(1))
    except ValueError:
        return None

    if not math.isfinite(ph) or ph < 0 or ph > 14:
        return None

    return ph


def parse_qr_text(text: str) -> list[str]:
    out = []
    for token in re.split(r"[\s,;]+", text or ""):
        q = norm_qr(token)
        if q:
            out.append(q)
    return out


def load_requested_qrs(cli_qrs: list[str], qr_file: str | None) -> list[str]:
    qrs = []

    for value in cli_qrs or []:
        qrs.extend(parse_qr_text(value))

    if qr_file:
        path = Path(qr_file)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        if not path.exists():
            raise FileNotFoundError(f"QR file not found: {path}")
        qrs.extend(parse_qr_text(path.read_text(encoding="utf-8-sig")))

    return list(dict.fromkeys(qrs))


def qr_values_from_sample_data(data: dict) -> set[str]:
    """Find every info.qrCode inside data[]; tolerate historical step naming."""
    found = set()
    steps = data.get("data", [])
    if not isinstance(steps, list):
        return found

    for step in steps:
        if not isinstance(step, dict):
            continue
        info = step.get("info", {})
        if not isinstance(info, dict):
            continue
        q = norm_qr(info.get("qrCode"))
        if q:
            found.add(q)

    return found


def flatten_sample_doc(doc, data: dict) -> dict:
    """Flatten one Firestore document with the same column naming style as pull_and_enrich."""
    parent_user = (
        doc.reference.parent.parent.id
        if doc.reference.parent and doc.reference.parent.parent
        else ""
    )

    row = {
        "sampleId": doc.id,
        "userId": parent_user,
        "collectedAt": data.get("collectedAt"),
        "fs_createdAt": ts_to_iso(getattr(doc, "create_time", None)),
        "fs_updatedAt": ts_to_iso(getattr(doc, "update_time", None)),
        "firestore_path": doc.reference.path,
    }

    steps = data.get("data", [])
    if not isinstance(steps, list):
        return row

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
                                row[f"{step_type}_{key}_{i}_{subk}"] = subv
                        else:
                            row[f"{step_type}_{key}_{i}"] = item
                else:
                    row[base_col] = val
        else:
            row[f"{step_type}_info"] = str(info)

    if "PH_ph" in row:
        row["PH_ph"] = parse_ph(row["PH_ph"])

    return row


def fetch_matching_samples(
    requested_qrs: list[str],
    max_stream_retries: int = 5,
) -> pd.DataFrame:
    wanted = set(requested_qrs)
    db = firestore.client()

    rows = []
    seen_doc_paths: set[str] = set()
    attempt = 0

    while True:
        try:
            samples_ref = db.collection_group("samples")

            for doc in samples_ref.stream():
                path = doc.reference.path

                # Important: full path, not doc.id. This preserves real duplicates
                # under different users while avoiding replay from a retry.
                if path in seen_doc_paths:
                    continue

                data = doc.to_dict() or {}
                doc_qrs = qr_values_from_sample_data(data)
                matched = sorted(doc_qrs & wanted)

                if matched:
                    row = flatten_sample_doc(doc, data)
                    row["matched_requested_qr"] = ",".join(matched)
                    rows.append(row)

                seen_doc_paths.add(path)

            break

        except Exception as e:
            msg = str(e)

            if "_UnaryStreamMultiCallable" in msg and "has no attribute '_retry'" in msg:
                sleep_s = 10
                log.warning(
                    "Firestore stream hit known _UnaryStreamMultiCallable/_retry bug; "
                    "sleeping %ss and retrying...",
                    sleep_s,
                )
                time.sleep(sleep_s)
                db = firestore.client()
                continue

            attempt += 1
            if attempt > max_stream_retries:
                log.error("Firestore stream failed after %s attempts: %s", attempt, e)
                raise

            sleep_s = min(60, 5 * attempt)
            log.warning(
                "Firestore stream failed (attempt %s/%s): %s; retrying in %ss",
                attempt,
                max_stream_retries,
                e,
                sleep_s,
            )
            time.sleep(sleep_s)
            db = firestore.client()

    df = pd.DataFrame(rows, dtype=object)

    for col in ("collectedAt", "fs_createdAt", "fs_updatedAt"):
        if col in df.columns:
            df[col] = df[col].apply(ts_to_iso_loose)

    return df


def fetch_uid_to_email(max_retries: int = 5) -> dict[str, str]:
    for attempt in range(1, max_retries + 1):
        mapping: dict[str, str] = {}
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
                "Firebase Auth list_users failed (attempt %s/%s): %s; retrying in %ss",
                attempt,
                max_retries,
                e,
                sleep_s,
            )
            time.sleep(sleep_s)

    return {}


def enrich_with_email(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if out.empty:
        if "email" not in out.columns:
            out["email"] = pd.Series(dtype=object)
        return out

    uid_to_email = fetch_uid_to_email()
    if "userId" not in out.columns:
        out["userId"] = ""
    out["email"] = out["userId"].map(uid_to_email).fillna("")
    return out


def add_duplicate_diagnostics(
    df: pd.DataFrame,
    requested_qrs: list[str],
) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    if "QR_qrCode" in out.columns:
        qr_norm = out["QR_qrCode"].map(norm_qr)
    else:
        qr_norm = (
            out["matched_requested_qr"]
            .astype(str)
            .str.split(",")
            .str[0]
            .map(norm_qr)
        )

    out["qr_normalized"] = qr_norm
    owner_counts = (
        out.groupby("qr_normalized")["userId"]
        .transform(lambda s: s.astype(str).str.strip().replace("", pd.NA).nunique())
    )

    out["owner_count_for_qr"] = owner_counts
    out["multiple_owners_for_qr"] = owner_counts > 1
    counts = Counter(qr_norm)
    out["duplicate_count_for_qr"] = qr_norm.map(counts)
    out["duplicate_index_for_qr"] = (
        out.groupby("qr_normalized", dropna=False).cumcount() + 1
    )

    order = {q: i for i, q in enumerate(requested_qrs)}
    out["_requested_order"] = qr_norm.map(lambda q: order.get(q, len(order)))

    sort_cols = ["_requested_order"]
    if "fs_updatedAt" in out.columns:
        sort_cols.append("fs_updatedAt")
    if "userId" in out.columns:
        sort_cols.append("userId")
    if "sampleId" in out.columns:
        sort_cols.append("sampleId")

    out = out.sort_values(sort_cols, na_position="last").reset_index(drop=True)
    return out.drop(columns=["_requested_order"])


def write_csv(df: pd.DataFrame, output_path: str) -> Path:
    path = Path(output_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path

    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path


def print_summary(df: pd.DataFrame, requested_qrs: list[str]) -> None:
    counts = Counter()
    if not df.empty and "qr_normalized" in df.columns:
        counts = Counter(df["qr_normalized"].astype(str))

    print("\nMatches by requested QR:")
    for qr in requested_qrs:
        n = counts.get(qr, 0)
        suffix = "  <-- DUPLICATES" if n > 1 else ""
        print(f"  {qr}: {n}{suffix}")

    missing = [qr for qr in requested_qrs if counts.get(qr, 0) == 0]
    if missing:
        print("\nNot found:")
        for qr in missing:
            print(f"  {qr}")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Export every Firestore sample document matching selected QR codes, "
            "including duplicate QR records."
        )
    )
    p.add_argument(
        "--qr",
        nargs="*",
        default=[],
        help="QR code(s), e.g. --qr XRXB-4454 ABCD-1234",
    )
    p.add_argument(
        "--qr-file",
        help="File containing QR codes separated by newlines, commas, semicolons, or spaces",
    )
    p.add_argument(
        "--output",
        default="data/firebase_qr_matches.csv",
        help="Output CSV (default: data/firebase_qr_matches.csv)",
    )
    p.add_argument(
        "--max-stream-retries",
        type=int,
        default=5,
        help="Maximum ordinary Firestore stream retries (default: 5)",
    )
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    requested_qrs = load_requested_qrs(args.qr, args.qr_file)

    if not requested_qrs:
        print("ERROR: supply QR codes with --qr or --qr-file", file=sys.stderr)
        return 2

    log.info("Requested %s unique QR code(s)", len(requested_qrs))

    init_firebase()
    df = fetch_matching_samples(
        requested_qrs,
        max_stream_retries=args.max_stream_retries,
    )
    df = enrich_with_email(df)
    df = add_duplicate_diagnostics(df, requested_qrs)

    output_path = write_csv(df, args.output)
    print_summary(df, requested_qrs)

    print(f"\nWrote {len(df)} matching Firestore document(s) to:")
    print(f"  {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
