#!/usr/bin/env python3
"""
Refresh the local SQLite database from the latest enriched CSV,
but preserve the lab_enrichment table (if it existed).

This version normalizes container-style paths (/data/...) to the current
PROJECT_ROOT when you run it on the host.
"""

import os
import sqlite3
import sys
from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------
# load env / defaults
# ---------------------------------------------------------------------
load_dotenv()  # so we can get PROJECT_ROOT

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/home/echo/ECHO-STORE/echorepo-lite"))
ENV_PATH = PROJECT_ROOT / ".env"

# set defaults (may be container-style)
os.environ.setdefault("CSV_PATH", str(PROJECT_ROOT / "data" / "echorepo_samples_with_email.csv"))
os.environ.setdefault("SQLITE_PATH", str(PROJECT_ROOT / "data" / "db" / "echo.db"))
os.environ.setdefault("TABLE_NAME", "samples")
os.environ.setdefault("MAX_JITTER_METERS", "1000")
os.environ.setdefault("JITTER_SALT", "change-this-salt")
os.environ.setdefault("KEEP_ORIGINALS", "true")
os.environ.setdefault("ECHO_FORCE_REBUILD", "false")

# make project importable
sys.path.insert(0, str(PROJECT_ROOT))
from echorepo.utils.load_csv import ensure_sqlite  # noqa


def _resolve_path(maybe_path: str) -> str:
    """
    Take a path that might be a container path (/data/...) and turn it
    into a real host path under PROJECT_ROOT if the original one
    doesn't exist.
    """
    p = Path(maybe_path)
    if p.is_absolute():
        if p.exists():
            return str(p)
        # try under project root: /data/foo -> <proj>/data/foo
        alt = PROJECT_ROOT / p.relative_to("/")
        return str(alt)
    # relative -> join with project root
    return str(PROJECT_ROOT / p)


# ---------------------------------------------------------------------
# helper: read current lab_enrichment contents (if any)
# ---------------------------------------------------------------------
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


# ---------------------------------------------------------------------
# helper: create canonical lab_enrichment and reinsert rows
# ---------------------------------------------------------------------
def _restore_lab_enrichment(db_path: str, backup):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # canonical schema (matches your web/data_api newer version)
    cur.execute(
        """
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
        """
    )

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


def main():
    # normalize the paths first
    raw_csv = _resolve_path(os.environ["CSV_PATH"])
    db_path = _resolve_path(os.environ["SQLITE_PATH"])

    # update env so ensure_sqlite() uses the normalized ones too
    os.environ["CSV_PATH"] = raw_csv
    os.environ["SQLITE_PATH"] = db_path

    print(f"[refresh_sqlite] CSV_PATH={raw_csv}")
    print(f"[refresh_sqlite] SQLITE_PATH={db_path}")

    # 1) backup lab_enrichment from the *current* db (if present)
    backup = _backup_lab_enrichment(db_path)
    if backup:
        print(f"[refresh_sqlite] Backed up {len(backup['rows'])} lab_enrichment rows")
    else:
        print("[refresh_sqlite] No lab_enrichment to back up (table missing or empty)")

    # 2) rebuild sqlite from CSV
    ensure_sqlite()
    print("[refresh_sqlite] Base SQLite refreshed from CSV")

    # 3) restore lab_enrichment into the freshly built db
    print(f"[refresh_sqlite] Restoring lab_enrichment to {db_path}")
    _restore_lab_enrichment(db_path, backup)
    print("[refresh_sqlite] lab_enrichment restored")
    print("[refresh_sqlite] Done.")


if __name__ == "__main__":
    main()
