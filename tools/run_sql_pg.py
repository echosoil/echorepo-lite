#!/usr/bin/env python3
"""
Run a .sql file against the Postgres DB configured in .env.

Usage:
    python tools/run_sql_pg.py tools/sql/make_usage_events_pg.sql
"""

import os
import sys
from pathlib import Path

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv


def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/run_sql_pg.py <sql-file.sql>")
        sys.exit(1)

    sql_file = Path(sys.argv[1])
    if not sql_file.exists():
        print(f"SQL file not found: {sql_file}")
        sys.exit(1)

    # Load .env from project root (same pattern as other tools)
    env_path = Path.cwd() / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)

    db_host = os.getenv("DB_HOST_OUTSIDE", "echorepo-postgres")
    db_port = int(os.getenv("DB_PORT_OUTSIDE", "5432"))
    db_name = os.getenv("DB_NAME", "echorepo")
    db_user = os.getenv("DB_USER", "echorepo")
    db_password = os.getenv("DB_PASSWORD", "echorepo-pass")

    print(f"[INFO] Using Postgres DB: {db_user}@{db_host}:{db_port}/{db_name}")
    sql_text = sql_file.read_text(encoding="utf-8")

    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password,
        )
    except Exception as e:
        print(f"[ERROR] Could not connect to Postgres: {e}")
        sys.exit(1)

    try:
        # autocommit off by default, we'll commit at the end
        cur = conn.cursor()
        cur.execute(sql_text)
        conn.commit()
        cur.close()
        print("[OK] SQL executed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] SQL execution failed: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
