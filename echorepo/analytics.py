# echorepo/analytics.py
import hashlib
import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import Json

DB_HOST = os.getenv("DB_HOST_INSIDE", "echorepo-postgres")
DB_PORT = int(os.getenv("DB_PORT_INSIDE", "5432"))
DB_NAME = os.getenv("DB_NAME", "echorepo")
DB_USER = os.getenv("DB_USER", "echorepo")
DB_PASSWORD = os.getenv("DB_PASSWORD", "echorepo-pass")

ANALYTICS_SALT = os.getenv("ANALYTICS_SALT", "change-me-analytics-salt-3210@echo-repo")


@contextmanager
def _get_conn():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    try:
        yield conn
    finally:
        conn.close()


def hash_ip(ip: str | None) -> str | None:
    if not ip:
        return None
    h = hashlib.sha256()
    h.update((ANALYTICS_SALT + ip).encode("utf-8"))
    return h.hexdigest()


def log_usage_event(
    *,
    ts=None,
    user_id=None,
    event_type="page_view",
    path="/",
    method="GET",
    status_code=200,
    bytes_sent=None,
    duration_ms=None,
    ip_hash=None,
    user_agent=None,
    extra=None,
):
    extra = extra or {}

    with _get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO usage_events
              (ts, user_id, event_type, path, method,
               status_code, bytes_sent, duration_ms,
               ip_hash, user_agent, extra)
            VALUES
              (COALESCE(%s, now()), %s, %s, %s, %s,
               %s, %s, %s,
               %s, %s, %s)
            """,
            (
                ts,
                user_id,
                event_type,
                path,
                method,
                status_code,
                bytes_sent,
                duration_ms,
                ip_hash,
                user_agent,
                Json(extra),
            ),
        )
        conn.commit()
