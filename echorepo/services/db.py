import os, sqlite3, pandas as pd
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
    
def init_db_sanity():
    if not os.path.exists(settings.SQLITE_PATH):
        print(f"[app] SQLite DB not found at {settings.SQLITE_PATH}.")
        return
    try:
        with sqlite3.connect(settings.SQLITE_PATH) as conn:
            c = conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?;", (settings.TABLE_NAME,))
            if c.fetchone()[0] != 1:
                print(f"[app] Table '{settings.TABLE_NAME}' missing in {settings.SQLITE_PATH}.")
    except Exception as e:
        print(f"[app] SQLite check failed: {e}")

def query_user_df(user_key: str) -> pd.DataFrame:
    with sqlite3.connect(settings.SQLITE_PATH) as conn:
        q = f"SELECT * FROM {settings.TABLE_NAME} WHERE {settings.USER_KEY_COLUMN} = ? OR userId = ?"
        return pd.read_sql_query(q, conn, params=(user_key, user_key))

def query_others_df(user_key: str) -> pd.DataFrame:
    with sqlite3.connect(settings.SQLITE_PATH) as conn:
        q = f"""
        SELECT * FROM {settings.TABLE_NAME}
        WHERE ({settings.USER_KEY_COLUMN} IS NULL OR {settings.USER_KEY_COLUMN} <> ?)
          AND (userId IS NULL OR userId <> ?)
        """
        return pd.read_sql_query(q, conn, params=(user_key, user_key))

def query_sample(sample_id: str) -> pd.DataFrame:
    with sqlite3.connect(settings.SQLITE_PATH) as conn:
        q = f"SELECT * FROM {settings.TABLE_NAME} WHERE sampleId = ?"
        return pd.read_sql_query(q, conn, params=(sample_id,))
