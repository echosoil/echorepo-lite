#!/usr/bin/env python3
"""Read-only check of ECHOREPO planned.xlsx import and PostgreSQL table.

Run in application container:
  docker exec -i echorepo-lite python /app/tools/diagnose_planned_import.py
No uploads, file writes, schema changes, or data changes are performed.
"""

from __future__ import annotations

import os
from pathlib import Path

from echorepo.services.db import get_pg_conn
from echorepo.services.planned_pg import MAX_XLSX_BYTES, read_xlsx


def run():
    print("PLANNED_COUNTRY_SOURCE:", os.getenv("PLANNED_COUNTRY_SOURCE", "xlsx (unset)"))
    xlsx = Path(os.getenv("PLANNED_XLSX", "/data/utils/planned.xlsx"))
    print("PLANNED_XLSX:", xlsx)
    if not xlsx.is_file():
        print("XLSX: NOT FOUND at that path (check Docker data mount)")
    else:
        print("XLSX size:", xlsx.stat().st_size, "bytes; upload limit:", MAX_XLSX_BYTES)
        try:
            rows = read_xlsx(xlsx.read_bytes())
            print("XLSX validation: OK;", len(rows), "data rows")
            print("First QR codes:", [r[3] for r in rows[:5]])
        except Exception as exc:
            print("XLSX validation: FAILED:", type(exc).__name__, str(exc))
    try:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.planned_country_rows')")
                exists = cur.fetchone()[0]
                if not exists:
                    print("PostgreSQL: planned_country_rows does not exist")
                else:
                    cur.execute("SELECT count(*) FROM planned_country_rows")
                    print("PostgreSQL planned rows:", cur.fetchone()[0])
                    cur.execute("SELECT to_regclass('public.planned_country_imports')")
                    if cur.fetchone()[0]:
                        cur.execute(
                            "SELECT id, filename, row_count, imported_at FROM planned_country_imports ORDER BY id DESC LIMIT 3"
                        )
                        print("Last 3 imports:")
                        for row in cur.fetchall():
                            print("  ", row)
    except Exception as exc:
        print("PostgreSQL connection/query failed:", type(exc).__name__, str(exc))


if __name__ == "__main__":
    run()
