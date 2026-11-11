"""PostgreSQL source-of-truth for planned countries with five-column XLSX exchange."""

from __future__ import annotations

import os
import re
from contextlib import closing
from io import BytesIO

from openpyxl import Workbook, load_workbook
from psycopg2 import connect
from psycopg2.extras import execute_values

from .planned import _split_planned

HEADERS = ("Organization", "Batch", "QR Code", "File Path", "Country Planned")
MAX_XLSX_BYTES = 10 * 1024 * 1024
MAX_ROWS = 100_000


class PlannedCountryError(ValueError):
    pass


def norm_qr(v):
    return str(v or "").strip().replace(" ", "").upper()


def connect_planned(*, outside=False):
    hosts = (
        ("DB_HOST_OUTSIDE", "DB_HOST", "DB_HOST_INSIDE")
        if outside
        else ("DB_HOST_INSIDE", "DB_HOST", "DB_HOST_OUTSIDE")
    )
    ports = (
        ("DB_PORT_OUTSIDE", "DB_PORT", "DB_PORT_INSIDE")
        if outside
        else ("DB_PORT_INSIDE", "DB_PORT", "DB_PORT_OUTSIDE")
    )
    host = next(
        (os.environ[k] for k in hosts if os.getenv(k)), "127.0.0.1" if outside else "postgres"
    )
    port = int(next((os.environ[k] for k in ports if os.getenv(k)), "5432"))
    return connect(
        host=host,
        port=port,
        dbname=os.getenv("DB_NAME", "echorepo"),
        user=os.getenv("DB_USER", "echorepo"),
        password=os.getenv("DB_PASSWORD", "echorepo-pass"),
        connect_timeout=10,
    )


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS planned_country_rows (
          id BIGSERIAL PRIMARY KEY, row_number INTEGER NOT NULL UNIQUE,
          organization TEXT NOT NULL DEFAULT '', batch TEXT NOT NULL DEFAULT '',
          qr_code TEXT NOT NULL DEFAULT '', qr_key TEXT NOT NULL DEFAULT '',
          file_path TEXT NOT NULL DEFAULT '', country_planned TEXT NOT NULL DEFAULT '',
          modified_at TIMESTAMPTZ NOT NULL DEFAULT now(), modified_by TEXT NOT NULL DEFAULT '')""")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS planned_country_rows_qr_key_idx ON planned_country_rows(qr_key)"
        )
        cur.execute("""CREATE TABLE IF NOT EXISTS planned_country_changes (
          id BIGSERIAL PRIMARY KEY, changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          changed_by TEXT NOT NULL, qr_key TEXT NOT NULL,
          before_value TEXT NOT NULL, after_value TEXT NOT NULL)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS planned_country_imports (
          id BIGSERIAL PRIMARY KEY, imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          imported_by TEXT NOT NULL, filename TEXT NOT NULL,
          row_count INTEGER NOT NULL, previous_rows JSONB NOT NULL)""")


def read_xlsx(payload):
    if not payload or len(payload) > MAX_XLSX_BYTES:
        raise PlannedCountryError("XLSX is empty or exceeds 10 MiB")
    try:
        book = load_workbook(BytesIO(payload), read_only=True, data_only=False)
    except Exception as exc:
        raise PlannedCountryError("Invalid XLSX workbook") from exc
    try:
        ws = book.worksheets[0]
        iterator = ws.iter_rows(values_only=True)
        header = next(iterator, ())
        found_headers = tuple(str(v or "").strip() for v in header)
        if found_headers[:5] != HEADERS or any(v not in (None, "") for v in header[5:]):
            raise PlannedCountryError(
                f"Wrong columns in worksheet {ws.title!r}. "
                f"Expected {HEADERS!r}; found {found_headers!r}. "
                "Use the original five-column planned.xlsx layout."
            )
        rows = []
        for rownum, values in enumerate(iterator, 2):
            if rownum > MAX_ROWS + 1:
                raise PlannedCountryError("Too many rows")
            if any(v not in (None, "") for v in values[5:]):
                raise PlannedCountryError(f"Unexpected column at row {rownum}")
            cells = ["" if v is None else str(v).strip() for v in values[:5]]
            cells.extend([""] * (5 - len(cells)))
            if not any(cells):
                continue
            if any(x.startswith("=") for x in cells):
                raise PlannedCountryError(f"Excel formula at row {rownum}; use plain values")
            rows.append(
                (rownum, cells[0], cells[1], cells[2], norm_qr(cells[2]), cells[3], cells[4])
            )
        if not rows:
            raise PlannedCountryError("No data rows; refusing to replace existing table")
        return rows
    finally:
        book.close()


def replace_all(conn, rows, *, actor, filename):
    ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute("LOCK TABLE planned_country_rows IN ACCESS EXCLUSIVE MODE")
        cur.execute(
            """INSERT INTO planned_country_imports(imported_by,filename,row_count,previous_rows)
            SELECT %s,%s,%s,COALESCE(jsonb_agg(to_jsonb(p) ORDER BY p.row_number),'[]'::jsonb)
            FROM planned_country_rows p""",
            (actor, filename, len(rows)),
        )
        cur.execute("DELETE FROM planned_country_rows")
        execute_values(
            cur,
            """INSERT INTO planned_country_rows
            (row_number,organization,batch,qr_code,qr_key,file_path,country_planned,modified_by) VALUES %s""",
            [
                (n, org, batch, qr, key, fp, country, actor)
                for n, org, batch, qr, key, fp, country in rows
            ],
            page_size=1000,
        )
    freq = {}
    unrecognized = 0
    for _, _, _, _, key, _, country in rows:
        if key:
            freq[key] = freq.get(key, 0) + 1
        if country and not _split_planned(country):
            unrecognized += 1
    return {
        "rows": len(rows),
        "duplicate_qrs": sum(x > 1 for x in freq.values()),
        "unrecognized_countries": unrecognized,
    }


def count_rows(conn):
    ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM planned_country_rows")
        return cur.fetchone()[0]


def load_map(*, outside=False):
    with closing(connect_planned(outside=outside)) as conn:
        with conn:
            ensure_schema(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT qr_key,country_planned FROM planned_country_rows WHERE qr_key<>''"
                )
                pairs = cur.fetchall()
    if not pairs:
        raise PlannedCountryError(
            "Planned table is empty: upload planned.xlsx before selecting the PostgreSQL source"
        )
    out = {}
    for qr, raw in pairs:
        codes = _split_planned(raw)
        if codes:
            out.setdefault(qr, set()).update(codes)
    if not out:
        raise PlannedCountryError(
            "No usable planned-country values in PostgreSQL; refusing to proceed"
        )
    return out


def country_matches(conn, qrs):
    """Current ISO codes for QRs with exactly one row (duplicates fail closed)."""
    keys = list({norm_qr(q) for q in qrs if norm_qr(q)})
    if not keys:
        return {}
    ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT qr_key,country_planned FROM planned_country_rows WHERE qr_key=ANY(%s)", (keys,)
        )
        pairs = cur.fetchall()
    grouped = {}
    for qr, raw in pairs:
        grouped.setdefault(qr, []).append(_split_planned(raw))
    return {qr: vals[0] for qr, vals in grouped.items() if len(vals) == 1}


def change_country(conn, qr, actual_country, *, expected_planned, actor):
    """One indexed, row-locked update. Stale QA and ambiguous QR fail closed."""
    qr = norm_qr(qr)
    actual = str(actual_country or "").strip().upper()
    if not re.fullmatch(r"[A-Z]{4}-\d{4}", qr):
        raise PlannedCountryError("Invalid QR code")
    if not re.fullmatch(r"[A-Z]{2}", actual):
        raise PlannedCountryError("Invalid ISO2 actual country")
    ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id,country_planned FROM planned_country_rows WHERE qr_key=%s FOR UPDATE", (qr,)
        )
        matches = cur.fetchall()
        if len(matches) != 1:
            raise PlannedCountryError(f"QR {qr} has {len(matches)} rows; expected exactly one")
        rid, old = matches[0]
        expected = {x.strip().upper() for x in expected_planned.split(",") if x.strip()}
        if _split_planned(old) != expected:
            raise PlannedCountryError(
                "Planned country changed since QA snapshot; refresh coordinate issues"
            )
        if actual in _split_planned(old):
            raise PlannedCountryError("Already corrected; refresh coordinate issues")
        cur.execute(
            "UPDATE planned_country_rows SET country_planned=%s,modified_at=now(),modified_by=%s WHERE id=%s",
            (actual, actor, rid),
        )
        cur.execute(
            "INSERT INTO planned_country_changes(changed_by,qr_key,before_value,after_value) VALUES (%s,%s,%s,%s)",
            (actor, qr, old, actual),
        )
    return old


def export_xlsx(conn):
    """Stream planned rows as XLSX without quadratic worksheet-row lookups."""
    from openpyxl.cell import WriteOnlyCell

    ensure_schema(conn)
    book = Workbook(write_only=True)
    ws = book.create_sheet(title="Planned")
    # In write-only mode, worksheet configuration comes before row insertion.
    ws.freeze_panes = "A2"
    for col, width in {"A": 24, "B": 18, "C": 16, "D": 35, "E": 24}.items():
        ws.column_dimensions[col].width = width

    def text_cell(value):
        # Prevent formula interpretation, including values beginning with '='.
        cell = WriteOnlyCell(ws, value="" if value is None else str(value))
        cell.data_type = "s"
        return cell

    ws.append([text_cell(value) for value in HEADERS])
    with conn.cursor() as cur:
        cur.execute(
            "SELECT organization,batch,qr_code,file_path,country_planned "
            "FROM planned_country_rows ORDER BY row_number"
        )
        for values in cur:
            ws.append([text_cell(value) for value in values])

    out = BytesIO()
    book.save(out)
    book.close()
    return out.getvalue()
