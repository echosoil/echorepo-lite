import os
import time

import psycopg2


def get_pg_dsn():
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN")
    if dsn:
        return dsn

    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "echorepo")
    user = os.getenv("POSTGRES_USER", "echorepo")
    password = os.getenv("POSTGRES_PASSWORD", "")

    return f"host={host} port={port} dbname={db} user={user} password={password}"


dsn = get_pg_dsn()

for i in range(60):
    try:
        conn = psycopg2.connect(dsn)
        conn.close()
        print("Postgres is ready.")
        raise SystemExit(0)
    except Exception as e:
        print(f"Waiting for Postgres... {i + 1}/60: {e}")
        time.sleep(2)

raise SystemExit("Postgres did not become ready.")
