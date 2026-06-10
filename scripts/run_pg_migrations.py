import os
from pathlib import Path

import psycopg2

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations" / "postgres"


def get_pg_dsn():
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN")
    if dsn:
        return dsn

    host = (
        os.getenv("POSTGRES_HOST")
        or os.getenv("DB_HOST")
        or os.getenv("DB_HOST_INSIDE")
        or "postgres"
    )
    port = (
        os.getenv("POSTGRES_PORT")
        or os.getenv("DB_PORT")
        or os.getenv("DB_PORT_INSIDE")
        or "5432"
    )
    db = (
        os.getenv("POSTGRES_DB")
        or os.getenv("DB_NAME")
        or "echorepo"
    )
    user = (
        os.getenv("POSTGRES_USER")
        or os.getenv("DB_USER")
        or "echorepo"
    )
    password = (
        os.getenv("POSTGRES_PASSWORD")
        or os.getenv("DB_PASSWORD")
        or ""
    )

    return f"host={host} port={port} dbname={db} user={user} password={password}"
    
def ensure_migration_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            filename TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)


def migration_already_applied(cur, filename):
    cur.execute(
        "SELECT 1 FROM schema_migrations WHERE filename = %s",
        (filename,),
    )
    return cur.fetchone() is not None


def mark_migration_applied(cur, filename):
    cur.execute(
        "INSERT INTO schema_migrations (filename) VALUES (%s)",
        (filename,),
    )


def main():
    sql_files = sorted(MIGRATIONS_DIR.glob("*.sql"))

    if not sql_files:
        print(f"No migrations found in {MIGRATIONS_DIR}")
        return

    dsn = get_pg_dsn()

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            ensure_migration_table(cur)

            for path in sql_files:
                filename = path.name

                if migration_already_applied(cur, filename):
                    print(f"[skip] {filename}")
                    continue

                print(f"[apply] {filename}")
                sql = path.read_text(encoding="utf-8")
                cur.execute(sql)
                mark_migration_applied(cur, filename)

        conn.commit()

    print("Postgres migrations complete.")


if __name__ == "__main__":
    main()
