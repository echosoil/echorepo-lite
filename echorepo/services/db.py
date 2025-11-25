import os
import re
import sqlite3
import pandas as pd
from ..config import settings

from echorepo.utils.load_csv import deterministic_jitter, MAX_JITTER_METERS as LC_MAX_JITTER_METERS

import psycopg2 
import math

OXIDE_NAMES = {"MN2O3","AL2O3","CAO","FE2O3","MGO","SIO2","P2O5","TIO2","K2O"}

def _round_sig_str(v: float, sig: int = 2) -> str:
    """Round to `sig` significant figures, never using scientific notation."""
    if v == 0 or not math.isfinite(v):
        return "0"
    exp = int(math.floor(math.log10(abs(v))))
    dec = sig - 1 - exp                  # decimals to keep
    rounded = round(v, dec)
    if dec > 0:
        s = f"{rounded:.{dec}f}"
    else:
        s = f"{int(rounded)}"
    return s.rstrip("0").rstrip(".")

def _clean_metals_info(s: str) -> str:
    """
    - Drop oxides (left side == oxide code)
    - Keep only 2 sig figs on numeric values
    - Preserve units and original parameter names
    Input format tokens: "PARAM=VALUE [UNIT]" separated by ';'
    """
    if not isinstance(s, str) or not s.strip():
        return ""
    out = []
    for tok in (t.strip() for t in s.split(";") if t.strip()):
        left, sep, right = tok.partition("=")
        norm_left = re.sub(r"\s+", "", left).upper()
        if norm_left in OXIDE_NAMES:
            continue
        if not sep:  # no '=' — keep as is
            out.append(tok)
            continue

        right = right.strip()
        if not right:
            continue

        # split value and unit (first whitespace separates them)
        val_str, *unit_parts = right.split()
        unit = " ".join(unit_parts)

        # try numeric → 2 sig figs
        try:
            val = float(val_str.replace(",", "."))
            val_fmt = _round_sig_str(val, 2)
        except Exception:
            val_fmt = val_str  # non-numeric, keep as is

        out.append(f"{left.strip()}={val_fmt}{(' ' + unit) if unit else ''}")
    return "; ".join(out)

def _strip_oxides_from_info_str(s: str) -> str:
    if not isinstance(s, str) or not s.strip():
        return ""
    parts = [p.strip() for p in s.split(";") if p.strip()]
    keep = []
    for token in parts:
        left = token.split("=", 1)[0]           # param part before '='
        norm = re.sub(r"\s+", "", left).upper() # drop spaces, upper
        if norm in OXIDE_NAMES:
            continue
        keep.append(token)
    return "; ".join(keep)

def get_pg_conn():
    """
    Central Postgres connection helper.

    Inside the container, you usually set:
      DB_HOST_INSIDE=echorepo-postgres
      DB_PORT_INSIDE=5432

    From outside (tools on the host), you may use:
      DB_HOST_OUTSIDE=localhost
      DB_PORT_OUTSIDE=5434
    """
    host = (
        os.getenv("DB_HOST_INSIDE")
        or os.getenv("DB_HOST_OUTSIDE")
        or os.getenv("DB_HOST")
        or "echorepo-postgres"
    )
    port = int(
        os.getenv("DB_PORT_INSIDE")
        or os.getenv("DB_PORT_OUTSIDE")
        or os.getenv("DB_PORT")
        or "5432"
    )

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=os.getenv("DB_NAME", "echorepo"),
        user=os.getenv("DB_USER", "echorepo"),
        password=os.getenv("DB_PASSWORD", "echorepo-pass"),
    )


def _find_main_table(conn: sqlite3.Connection) -> str | None:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    for (tname,) in cur.fetchall():
        c2 = conn.execute(f"PRAGMA table_info({tname})")
        cols = {r[1] for r in c2.fetchall()}
        if {"sampleId", "GPS_lat", "GPS_long"}.issubset(cols):
            return tname
    return None

def _ensure_col(conn: sqlite3.Connection, table: str, name: str, decl: str):
    cur = conn.execute(f"PRAGMA table_info({table})")
    cols = {r[1] for r in cur.fetchall()}
    if name not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")

def update_coords_sqlite(sample_id: str, lat: float, lon: float) -> tuple[bool, str]:
    """
    Update the local SQLite so the change is visible immediately:
      - write ORIGINAL columns (GPS_lat/GPS_long)
      - recompute deterministic jitter into 'lat'/'lon'
    """
    db_path = getattr(settings, "SQLITE_PATH", os.getenv("SQLITE_PATH", "/data/db/echo.db"))
    if not os.path.exists(db_path):
        return False, f"SQLite not found at {db_path}"

    conn = sqlite3.connect(db_path)
    try:
        tbl = _find_main_table(conn)
        if not tbl:
            return False, "Could not locate main samples table with sampleId/GPS_lat/GPS_long"

        # ensure jittered columns exist
        _ensure_col(conn, tbl, "lat", "REAL")
        _ensure_col(conn, tbl, "lon", "REAL")

        # write ORIGINAL coords
        conn.execute(f"UPDATE {tbl} SET GPS_lat = ?, GPS_long = ? WHERE sampleId = ?",
                     (lat, lon, sample_id))

        # recompute jitter
        jlat, jlon = deterministic_jitter(lat, lon, sample_id, LC_MAX_JITTER_METERS)
        conn.execute(f"UPDATE {tbl} SET lat = ?, lon = ? WHERE sampleId = ?",
                     (jlat, jlon, sample_id))

        conn.commit()
        return True, f"SQLite updated: GPS=({lat},{lon}) / jitter=({jlat},{jlon})"
    except Exception as e:
        conn.rollback()
        return False, f"SQLite update failed: {e}"
    finally:
        conn.close()

def _ensure_lab_enrichment(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lab_enrichment (
            qr_code    TEXT NOT NULL,
            param      TEXT NOT NULL,
            value      TEXT,
            unit       TEXT,
            user_id    TEXT,
            raw_row    TEXT,
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (qr_code, param)
        );
    """)
    conn.commit()


def init_db_sanity():
    if not os.path.exists(settings.SQLITE_PATH):
        print(f"[app] SQLite DB not found at {settings.SQLITE_PATH}.")
        return
    try:
        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            c = conn.execute(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?;",
                (settings.TABLE_NAME,)
            )
            if c.fetchone()[0] != 1:
                print(f"[app] Table '{settings.TABLE_NAME}' missing in {settings.SQLITE_PATH}.")
            _ensure_lab_enrichment(conn)
    except Exception as e:
        print(f"[app] SQLite check failed: {e}")


def _merge_metals_cols(df: pd.DataFrame, html: bool = False) -> pd.DataFrame:
    """
    End up with exactly ONE column named 'METALS_info'.

    Inputs we might see:
      - METALS_info (from main table)
      - lab_METALS_info (from enrichment CTE)
      - METALS (old name)

    Priority: existing METALS_info → lab_METALS_info → METALS
    """
    cols = df.columns
    has_base_info = "METALS_info" in cols
    has_lab_info = "lab_METALS_info" in cols
    metals_col = next((c for c in cols if c.lower() == "metals"), None)

    # helper to “prefer A, but if A is null or empty-string, take B”
    def prefer(a: pd.Series, b: pd.Series) -> pd.Series:
        # a might be object with "", so treat "" as missing too
        mask = a.isna() | (a.astype(str).str.strip() == "")
        return a.where(~mask, b)

    # 1) merge lab into base
    if has_base_info and has_lab_info:
        df["METALS_info"] = prefer(df["METALS_info"], df["lab_METALS_info"])
        df = df.drop(columns=["lab_METALS_info"])
    elif not has_base_info and has_lab_info:
        df = df.rename(columns={"lab_METALS_info": "METALS_info"})
        has_base_info = True  # now we have it

    # 2) merge plain 'METALS'
    if metals_col:
        if "METALS_info" in df.columns:
            df["METALS_info"] = prefer(df["METALS_info"], df[metals_col])
            df = df.drop(columns=[metals_col])
        else:
            df = df.rename(columns={metals_col: "METALS_info"})

    # 3) final cleanup + optional HTML formatting
    if "METALS_info" in df.columns:
        # 3a) drop oxides
        df["METALS_info"] = (
            df["METALS_info"]
            .fillna("")
            .astype(str)
            .map(_clean_metals_info)
        )
        # 3b) pretty HTML (only after filtering)
        if html:
            df["METALS_info"] = df["METALS_info"].str.replace(r";\s*", "<br>", regex=True)

    return df

def query_user_df(user_key: str) -> pd.DataFrame:
    user_col = settings.USER_KEY_COLUMN
    with sqlite3.connect(settings.SQLITE_PATH) as conn:
        # make sure the table exists even if refresh_sqlite just recreated the DB
        _ensure_lab_enrichment(conn)

        q = f"""
        WITH lab AS (
            SELECT
                qr_code,
                GROUP_CONCAT(
                    CASE
                        WHEN (unit IS NOT NULL AND unit <> '')
                            THEN param || '=' || value || ' ' || unit
                        ELSE param || '=' || value
                    END,
                    '; '
                ) AS METALS_info
            FROM lab_enrichment
            WHERE UPPER(REPLACE(param, ' ', '')) NOT IN
                ('MN2O3','AL2O3','CAO','FE2O3','MGO','SIO2','P2O5','TIO2','K2O', 'SO3')
            GROUP BY qr_code
        )
        SELECT
            s.*,
            lab.METALS_info AS lab_METALS_info
        FROM {settings.TABLE_NAME} AS s
        LEFT JOIN lab
          ON lab.qr_code = s.QR_qrCode
             OR lab.qr_code = s.sampleId
        WHERE s.{user_col} = ?
           OR s.userId = ?
        """
        df = pd.read_sql_query(q, conn, params=(user_key, user_key))

    df = _merge_metals_cols(df, html=True)
    return df

# helper: this is the normalized join condition we’ll reuse
# strip ECHO-, uppercase, trim
def _normalized_join_clause() -> str:
    # in SQLite:
    #   TRIM(UPPER(col)) removes spaces+uppercases
    #   REPLACE(..., 'ECHO-', '') drops the prefix
    # we do this on both sides
    return """
      REPLACE(TRIM(UPPER(lab.qr_code)), 'ECHO-', '') = REPLACE(TRIM(UPPER(s.QR_qrCode)), 'ECHO-', '')
      OR REPLACE(TRIM(UPPER(lab.qr_code)), 'ECHO-', '') = REPLACE(TRIM(UPPER(s.sampleId)), 'ECHO-', '')
    """


def query_others_df(user_key: str) -> pd.DataFrame:
    user_col = settings.USER_KEY_COLUMN
    join_clause = _normalized_join_clause()
    with sqlite3.connect(settings.SQLITE_PATH) as conn:
        _ensure_lab_enrichment(conn)
        q = f"""
        WITH lab AS (
            SELECT
                qr_code,
                GROUP_CONCAT(
                    CASE
                        WHEN (unit IS NOT NULL AND unit <> '')
                            THEN param || '=' || value || ' ' || unit
                        ELSE param || '=' || value
                    END,
                    '; '
                ) AS METALS_info
            FROM lab_enrichment
            WHERE UPPER(REPLACE(param, ' ', '')) NOT IN
                ('MN2O3','AL2O3','CAO','FE2O3','MGO','SIO2','P2O5','TIO2','K2O', 'SO3')
            GROUP BY qr_code
        )
        SELECT s.*,
               lab.METALS_info AS lab_METALS_info
        FROM {settings.TABLE_NAME} AS s
        LEFT JOIN lab
          ON {join_clause}
        WHERE (s.{user_col} IS NULL OR s.{user_col} <> ?)
          AND (s.userId IS NULL OR s.userId <> ?)
        """
        df = pd.read_sql_query(q, conn, params=(user_key, user_key))
    
    df = _merge_metals_cols(df, html=True)
    return df


def query_sample(sample_id: str) -> pd.DataFrame:
    join_clause = _normalized_join_clause()
    with sqlite3.connect(settings.SQLITE_PATH) as conn:
        _ensure_lab_enrichment(conn)
        q = f"""
        WITH lab AS (
            SELECT
                qr_code,
                GROUP_CONCAT(
                    CASE
                        WHEN (unit IS NOT NULL AND unit <> '')
                            THEN param || '=' || value || ' ' || unit
                        ELSE param || '=' || value
                    END,
                    '; '
                ) AS METALS_info
            FROM lab_enrichment
            WHERE UPPER(REPLACE(param, ' ', '')) NOT IN
                ('MN2O3','AL2O3','CAO','FE2O3','MGO','SIO2','P2O5','TIO2','K2O', 'SO3')
            GROUP BY qr_code
        )
        SELECT s.*,
               lab.METALS_info AS lab_METALS_info
        FROM {settings.TABLE_NAME} AS s
        LEFT JOIN lab
          ON {join_clause}
        WHERE s.sampleId = ?
        """
        df = pd.read_sql_query(q, conn, params=(sample_id,))

    df = _merge_metals_cols(df, html=False)
    return df


def query_sample_df(sample_id: str) -> pd.DataFrame:
    with sqlite3.connect(settings.SQLITE_PATH) as conn:
        return pd.read_sql_query(
            f"SELECT * FROM {settings.TABLE_NAME} WHERE sampleId = ?",
            conn, params=(sample_id,)
        )