#!/usr/bin/env python3
"""
Refresh the local SQLite database from the latest enriched CSV.

- Loads env from /home/echo/ECHO-STORE/echorepo-lite/.env (override via ENV_PATH).
- Uses load_csv.ensure_sqlite() which handles jitter + signature skip + write.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# --- Paths / .env ---
load_dotenv()  # load any existing .env first to get PROJECT_ROOT
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/home/echo/ECHO-STORE/echorepo-lite-dev"))
ENV_PATH = Path(PROJECT_ROOT / ".env")

# --- Defaults if misload_csvsing in .env ---
os.environ.setdefault("CSV_PATH",     str(PROJECT_ROOT / "data" / "echorepo_samples_with_email.csv"))
os.environ.setdefault("SQLITE_PATH",  str(PROJECT_ROOT / "data" / "db" / "echo.db"))
os.environ.setdefault("TABLE_NAME",   "samples")
os.environ.setdefault("MAX_JITTER_METERS", "1000")
os.environ.setdefault("JITTER_SALT",  "change-this-salt")
os.environ.setdefault("KEEP_ORIGINALS", "true")
# set to "true" to force rebuild ignoring signature:
os.environ.setdefault("ECHO_FORCE_REBUILD", "false")

# --- Import and run builder ---
sys.path.insert(0, str(PROJECT_ROOT))
from echorepo.utils.load_csv import ensure_sqlite

def main():
    print(f"[refresh_sqlite] CSV_PATH={os.environ['CSV_PATH']}")
    print(f"[refresh_sqlite] SQLITE_PATH={os.environ['SQLITE_PATH']}")
    ensure_sqlite()
    print("[refresh_sqlite] Done.")

if __name__ == "__main__":
    main()
