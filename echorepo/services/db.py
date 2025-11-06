import os
import re
import sqlite3
import pandas as pd
from ..config import settings


def update_coords_sqlite(sample_id: str, lat: float, lon: float):
    with sqlite3.connect(settings.SQLITE_PATH) as conn:
        conn.execute(
            f"UPDATE {settings.TABLE_NAME} SET {settings.LAT_COL}=?, {settings.LON_COL}=? WHERE sampleId=?",
            (float(lat), float(lon), sample_id)
        )
        conn.commit()


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
        df["METALS_info"] = df["METALS_info"].fillna("")
        if html:
            df["METALS_info"] = df["METALS_info"].apply(
                lambda s: re.sub(r";\s*", "<br>", s) if isinstance(s, str) else ""
            )

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
