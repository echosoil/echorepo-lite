# -*- coding: utf-8 -*-
from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Dict, Optional
from flask import current_app

# Uses your existing SQLite under /data/db/echo.db
def _db_path() -> Path:
    # If you already centralize DB path, reuse that; otherwise:
    p = Path(current_app.config.get("SQLITE_PATH", "/data/db/echo.db"))
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def _conn():
    c = sqlite3.connect(str(_db_path()))
    c.row_factory = sqlite3.Row
    return c

def ensure_schema():
    with _conn() as cx:
        cx.execute("""
            CREATE TABLE IF NOT EXISTS i18n_overrides (
                locale TEXT NOT NULL,
                key     TEXT NOT NULL,       -- your UI key, e.g. 'privacyRadius'
                value   TEXT NOT NULL,       -- override translation
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (locale, key)
            )
        """)

def get_overrides(locale: str) -> Dict[str, str]:
    ensure_schema()
    with _conn() as cx:
        cur = cx.execute("SELECT key, value FROM i18n_overrides WHERE locale = ?", (locale,))
        return {row["key"]: row["value"] for row in cur.fetchall()}

def set_override(locale: str, key: str, value: str) -> None:
    ensure_schema()
    with _conn() as cx:
        cx.execute("""
            INSERT INTO i18n_overrides(locale, key, value)
            VALUES (?, ?, ?)
            ON CONFLICT(locale, key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP
        """, (locale, key, value))

def delete_override(locale: str, key: str) -> None:
    ensure_schema()
    with _conn() as cx:
        cx.execute("DELETE FROM i18n_overrides WHERE locale = ? AND key = ?", (locale, key))
