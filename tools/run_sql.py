# tools/run_sql.py
import sqlite3
import sys
from pathlib import Path

# adjust this if your DB is elsewhere
DB_PATH = Path("data/db/echo.db")


def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/run_sql.py <sql-file.sql>")
        sys.exit(1)

    sql_file = Path(sys.argv[1])
    if not sql_file.exists():
        print(f"SQL file not found: {sql_file}")
        sys.exit(1)

    if not DB_PATH.exists():
        print(f"DB not found at {DB_PATH} — adjust DB_PATH in the script.")
        sys.exit(1)

    sql_text = sql_file.read_text(encoding="utf-8")

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(sql_text)
        conn.commit()
        print("SQL executed successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
