import os
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


def query_sample_df(sample_id: str) -> pd.DataFrame:
    with sqlite3.connect(settings.SQLITE_PATH) as conn:
        return pd.read_sql_query(
            f"SELECT * FROM {settings.TABLE_NAME} WHERE sampleId = ?",
            conn, params=(sample_id,)
        )

def _ensure_lab_enrichment(conn: sqlite3.Connection):
    # this is safe to call on every startup
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lab_enrichment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            qr_code   TEXT NOT NULL,
            param     TEXT NOT NULL,
            value     TEXT,
            unit      TEXT,
            user_id   TEXT,
            created_at TEXT DEFAULT (datetime('now'))
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
            # NEW: make sure enrichment table exists too
            _ensure_lab_enrichment(conn)
    except Exception as e:
        print(f"[app] SQLite check failed: {e}")

def query_user_df(user_key: str) -> pd.DataFrame:
    """
    Return all samples for this user, plus METALS_info aggregated from lab_enrichment.
    We join by QR_qrCode and sampleId because that's what your schema actually has.
    """
    user_col = settings.USER_KEY_COLUMN  # e.g. "email"
    with sqlite3.connect(settings.SQLITE_PATH) as conn:
        # we wrap the lab_enrichment in a CTE so we only group once
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
        SELECT s.*, lab.METALS_info
        FROM {settings.TABLE_NAME} AS s
        LEFT JOIN lab
          ON lab.qr_code = s.QR_qrCode
             OR lab.qr_code = s.sampleId
        WHERE s.{user_col} = ?
           OR s.userId = ?
        """
        return pd.read_sql_query(q, conn, params=(user_key, user_key))

def query_others_df(user_key: str) -> pd.DataFrame:
    user_col = settings.USER_KEY_COLUMN
    with sqlite3.connect(settings.SQLITE_PATH) as conn:
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
        SELECT s.*, lab.METALS_info
        FROM {settings.TABLE_NAME} AS s
        LEFT JOIN lab
          ON lab.qr_code = s.QR_qrCode
             OR lab.qr_code = s.sampleId
        WHERE (s.{user_col} IS NULL OR s.{user_col} <> ?)
          AND (s.userId IS NULL OR s.userId <> ?)
        """
        return pd.read_sql_query(q, conn, params=(user_key, user_key))

def query_sample(sample_id: str) -> pd.DataFrame:
    with sqlite3.connect(settings.SQLITE_PATH) as conn:
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
        SELECT s.*, lab.METALS_info
        FROM {settings.TABLE_NAME} AS s
        LEFT JOIN lab
          ON lab.qr_code = s.QR_qrCode
             OR lab.qr_code = s.sampleId
        WHERE s.sampleId = ?
        """
        return pd.read_sql_query(q, conn, params=(sample_id,))