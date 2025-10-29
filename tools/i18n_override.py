#!/usr/bin/env python3
import argparse, sqlite3
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/data/db/echo.db")
    ap.add_argument("--locale", required=True)
    ap.add_argument("--key", required=True)
    ap.add_argument("--value", default="", help="Empty to delete override")
    args = ap.parse_args()

    db = Path(args.db)
    db.parent.mkdir(parents=True, exist_ok=True)
    cx = sqlite3.connect(str(db))
    with cx:
        cx.execute("""
            CREATE TABLE IF NOT EXISTS i18n_overrides (
                locale TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(locale,key)
            )
        """)
        if args.value == "":
            cx.execute("DELETE FROM i18n_overrides WHERE locale=? AND key=?", (args.locale, args.key))
            print(f"Deleted override: [{args.locale}] {args.key}")
        else:
            cx.execute("""
                INSERT INTO i18n_overrides(locale,key,value) VALUES (?,?,?)
                ON CONFLICT(locale,key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP
            """, (args.locale, args.key, args.value))
            print(f"Set override: [{args.locale}] {args.key} = {args.value!r}")

if __name__ == "__main__":
    main()
