#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv


def _normalize_qr(raw: str) -> str:
    if not raw:
        return ""
    raw = str(raw).strip()
    if raw.upper().startswith("ECHO-"):
        raw = raw[5:]
    if "-" not in raw and len(raw) >= 5:
        raw = raw[:4] + "-" + raw[4:]
    return raw.upper()


def _project_root() -> Path:
    this_dir = Path(__file__).resolve().parent
    default_root = this_dir.parent
    return Path(os.getenv("PROJECT_ROOT", str(default_root)))


def _local_path_to_abs(maybe_path: str, project_root: Path) -> str:
    p = Path(maybe_path)
    if p.is_absolute():
        if p.exists():
            return str(p)
        try:
            alt = project_root / p.relative_to("/")
            return str(alt)
        except Exception:
            return str(p)
    return str(project_root / p)


def read_input_file(path: str) -> pd.DataFrame:
    lower = path.lower()
    if lower.endswith(".xlsx"):
        return pd.read_excel(path, dtype=str)
    try:
        return pd.read_csv(path, sep="\t", dtype=str)
    except Exception:
        return pd.read_csv(path, dtype=str)


def extract_file_qrs(df: pd.DataFrame) -> set[str]:
    qr_col = None
    for cand in ("ID", "id", "QR", "qr", "QR Code", "QR_code", "qrCode", "QR_qrCode"):
        if cand in df.columns:
            qr_col = cand
            break

    if qr_col is None:
        raise ValueError(
            "Could not find a QR column. Expected one of: "
            "ID, id, QR, qr, QR Code, QR_code, qrCode, QR_qrCode"
        )

    qrs = set()
    for v in df[qr_col].fillna("").astype(str):
        qr = _normalize_qr(v)
        if qr:
            qrs.add(qr)
    return qrs


def fetch_db_qrs(sqlite_path: str) -> set[str]:
    import sqlite3

    conn = sqlite3.connect(sqlite_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT qr_code FROM lab_enrichment WHERE qr_code IS NOT NULL")
        rows = cur.fetchall()
        return {_normalize_qr(r[0]) for r in rows if r and r[0]}
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare QR codes in a lab file against SQLite lab_enrichment"
    )
    parser.add_argument("input_file", help="Path to metals lab file (.xlsx/.csv/.tsv)")
    parser.add_argument(
        "--sqlite-path",
        default=None,
        help="Override SQLite path. Default: read SQLITE_PATH from .env",
    )
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Print only QR codes missing from lab_enrichment",
    )
    args = parser.parse_args()

    project_root = _project_root()

    env_path = Path.cwd() / ".env"
    load_dotenv(dotenv_path=env_path)
    print(f"[INFO] Loaded environment from {env_path}")
    print(f"[INFO] Using PROJECT_ROOT={project_root}")

    sqlite_path = args.sqlite_path or os.getenv("SQLITE_PATH", "/data/db/echo.db")
    sqlite_path = _local_path_to_abs(sqlite_path, project_root)
    input_path = str(Path(args.input_file).resolve())

    print(f"[INFO] Input file: {input_path}")
    print(f"[INFO] SQLite DB : {sqlite_path}")

    if not os.path.exists(input_path):
        print(f"[ERROR] Input file not found: {input_path}")
        return 1

    if not os.path.exists(sqlite_path):
        print(f"[ERROR] SQLite DB not found: {sqlite_path}")
        return 1

    try:
        df = read_input_file(input_path)
    except Exception as e:
        print(f"[ERROR] Cannot read input file: {e}")
        return 1

    if df.empty:
        print("[ERROR] Input file has no rows")
        return 1

    try:
        file_qrs = extract_file_qrs(df)
    except Exception as e:
        print(f"[ERROR] {e}")
        return 1

    try:
        db_qrs = fetch_db_qrs(sqlite_path)
    except Exception as e:
        print(f"[ERROR] Cannot query lab_enrichment: {e}")
        return 1

    missing_in_db = sorted(file_qrs - db_qrs)
    extra_in_db = sorted(db_qrs - file_qrs)
    common = len(file_qrs & db_qrs)

    print()
    print(f"[INFO] QR codes in file         : {len(file_qrs)}")
    print(f"[INFO] QR codes in lab_enrichment: {len(db_qrs)}")
    print(f"[INFO] Matching QR codes       : {common}")
    print(f"[INFO] Missing in DB           : {len(missing_in_db)}")
    print(f"[INFO] Extra in DB             : {len(extra_in_db)}")

    if args.only_missing:
        print()
        for qr in missing_in_db:
            print(qr)
        return 0

    if missing_in_db:
        print("\n=== Missing in lab_enrichment ===")
        for qr in missing_in_db:
            print(qr)

    if extra_in_db:
        print("\n=== Present in lab_enrichment but not in file ===")
        for qr in extra_in_db:
            print(qr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
