"""Private exact coordinates saved by the coordinate-QA editor.

Do NOT merge these into public samples.lat/lon: the public columns are jittered.
Only the login-protected /coordinate-issues page reads this table.
"""
from __future__ import annotations
from contextlib import closing
from .db import get_pg_conn


def _schema(conn):
    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS coordinate_qa_exact_overrides (
            sample_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            lat DOUBLE PRECISION NOT NULL CHECK (lat BETWEEN -90 AND 90),
            lon DOUBLE PRECISION NOT NULL CHECK (lon BETWEEN -180 AND 180),
            updated_by TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )""")


def read_exact_overrides(sample_ids):
    ids = [str(x).strip() for x in sample_ids if str(x).strip()]
    if not ids:
        return {}
    with closing(get_pg_conn()) as conn:
        with conn:
            _schema(conn)
            with conn.cursor() as cur:
                cur.execute("SELECT sample_id, user_id, lat, lon, updated_at "
                            "FROM coordinate_qa_exact_overrides WHERE sample_id = ANY(%s)", (ids,))
                return {sid: (uid, lat, lon, updated_at) for sid, uid, lat, lon, updated_at in cur.fetchall()}


def save_exact_override(sample_id, user_id, lat, lon, actor):
    with closing(get_pg_conn()) as conn:
        with conn:
            _schema(conn)
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO coordinate_qa_exact_overrides
                   (sample_id,user_id,lat,lon,updated_by) VALUES (%s,%s,%s,%s,%s)
                   ON CONFLICT (sample_id) DO UPDATE SET
                     user_id=EXCLUDED.user_id, lat=EXCLUDED.lat, lon=EXCLUDED.lon,
                     updated_by=EXCLUDED.updated_by, updated_at=now()""",
                    (sample_id, user_id, float(lat), float(lon), actor))
