"""PostgreSQL-backed Flask sessions with an opaque browser ID.

Feature gated by ECHOREPO_SESSION_STORAGE=postgres. The legacy Flask session
cookie is read only during the explicitly enabled cutover window and is deleted
from responses. Tokens and the signed legacy payload are never sent back to the
browser. Uses the application's existing psycopg2 dependency.

Flask serves public static assets without consulting session storage. All
application routes continue to read the server-side session normally.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import psycopg2
from flask import request as flask_request
from flask.json.tag import TaggedJSONSerializer
from flask.sessions import SecureCookieSessionInterface, SessionInterface, SessionMixin
from werkzeug.datastructures import CallbackDict
from werkzeug.exceptions import ServiceUnavailable

# A 32-byte random ID encoded without padding is 43 URL-safe characters.
_SID_RE = re.compile(r"^[A-Za-z0-9_-]{43}$")
_JSON = TaggedJSONSerializer()
_MAX_SESSION_BYTES = 256 * 1024


def _new_sid() -> str:
    return secrets.token_urlsafe(32)


def _sid_hash(sid: str) -> str:
    return hashlib.sha256(sid.encode("ascii")).hexdigest()


def _connection_options() -> dict:
    """Match existing Compose DB_HOST / DB_PASSWORD, or a configured DSN."""
    dsn = os.environ.get("SESSION_DATABASE_URL") or os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_DSN")
    if dsn:
        return {"dsn": dsn, "connect_timeout": 5}
    return {
        "host": os.getenv("DB_HOST") or os.getenv("DB_HOST_INSIDE") or "postgres",
        "port": int(os.getenv("DB_PORT") or os.getenv("DB_PORT_INSIDE") or "5432"),
        "dbname": os.getenv("DB_NAME") or "echorepo",
        "user": os.getenv("DB_USER") or "echorepo",
        "password": os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD") or "",
        "connect_timeout": 5,
    }


@contextmanager
def _db_connection():
    """A fresh, bounded connection per operation; no shared Gunicorn state."""
    conn = psycopg2.connect(**_connection_options())
    try:
        with conn:
            yield conn
    finally:
        conn.close()


class PostgresSession(CallbackDict, SessionMixin):
    def __init__(self, initial=None, *, sid=None, new=True, version=0, from_legacy=False):
        def on_update(_):
            self.modified = True
            self.accessed = True

        super().__init__(initial or {}, on_update)
        self.sid = sid or _new_sid()
        self.new = new
        self.version = version
        self.from_legacy = from_legacy
        self.previous_sid = None
        self.modified = bool(from_legacy)
        self.accessed = False

    def __getitem__(self, key):
        self.accessed = True
        return super().__getitem__(key)

    def get(self, key, default=None):
        self.accessed = True
        return super().get(key, default)

    def setdefault(self, key, default=None):
        self.accessed = True
        return super().setdefault(key, default)


class PostgresSessionInterface(SessionInterface):
    session_class = PostgresSession
    pickle_based = False

    def __init__(self, *, legacy_cookie_name="session", migrate_legacy=False, connection=None):
        self.legacy_cookie_name = legacy_cookie_name
        self.migrate_legacy = migrate_legacy
        self.connection = connection or _db_connection
        self.legacy_interface = SecureCookieSessionInterface()

    def _legacy(self, app, cookie):
        if not self.migrate_legacy or not cookie:
            return None
        serializer = self.legacy_interface.get_signing_serializer(app)
        if serializer is None:
            return None
        try:
            raw = serializer.loads(
                cookie,
                max_age=int(app.permanent_session_lifetime.total_seconds()),
            )
        except Exception:  # Invalid signature, expired session, malformed payload.
            return None
        return raw if isinstance(raw, dict) else None

    def open_session(self, app, request):
        # Static files are always public. Avoid one PostgreSQL lookup per CSS,
        # image, or JS request (the caller cannot access auth state here).
        if request.path.startswith(app.static_url_path.rstrip("/") + "/"):
            session = self.session_class()
            session.skip_persistence = True
            return session
        name = self.get_cookie_name(app)
        raw_sid = request.cookies.get(name)
        sid = raw_sid if isinstance(raw_sid, str) and _SID_RE.fullmatch(raw_sid) else None
        saw_legacy = self.legacy_cookie_name != name and self.legacy_cookie_name in request.cookies

        if sid:
            try:
                with self.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT data, version FROM echorepo_web_sessions "
                            "WHERE sid_hash = %s AND expires_at > now()",
                            (_sid_hash(sid),),
                        )
                        row = cur.fetchone()
            except psycopg2.Error as exc:
                app.logger.error("Postgres session read unavailable: %s", type(exc).__name__)
                raise ServiceUnavailable("Session storage is temporarily unavailable") from exc
            if row is not None:
                try:
                    payload = _JSON.loads(json.dumps(row[0], ensure_ascii=False))
                    if not isinstance(payload, dict):
                        raise ValueError("not a dict")
                except (TypeError, ValueError) as exc:
                    app.logger.error("Stored session payload could not be decoded")
                    raise ServiceUnavailable("Session storage is temporarily unavailable") from exc
                session = self.session_class(payload, sid=sid, new=False, version=row[1])
                session.from_legacy = saw_legacy  # Delete old cookie even if SID is valid.
                return session

        # During staged cutover, import only a *verified* signed Flask cookie.
        legacy_data = self._legacy(app, request.cookies.get(self.legacy_cookie_name))
        if legacy_data is not None:
            return self.session_class(legacy_data, from_legacy=True)
        session = self.session_class()
        session.from_legacy = saw_legacy
        return session

    def regenerate(self, session):
        """Rotate SID on successful authentication; never on token refresh."""
        # flask.session is a LocalProxy; unwrap explicitly for all Werkzeug versions.
        if hasattr(session, "_get_current_object"):
            session = session._get_current_object()
        if not isinstance(session, PostgresSession):
            return
        if not session.new and session.previous_sid is None:
            session.previous_sid = session.sid
        session.sid = _new_sid()
        session.new = True
        session.version = 0
        session.modified = True

    def _discard_legacy_cookie(self, response):
        response.delete_cookie(self.legacy_cookie_name, path="/")

    def save_session(self, app, session, response):
        if not isinstance(session, PostgresSession) or getattr(session, "skip_persistence", False):
            return
        if session.accessed or session.modified or session.from_legacy or flask_request.cookies.get(self.get_cookie_name(app)):
            response.vary.add("Cookie")

        name = self.get_cookie_name(app)
        domain = self.get_cookie_domain(app)
        path = self.get_cookie_path(app)
        if session.from_legacy:
            self._discard_legacy_cookie(response)

        if not session:
            if session.modified or session.previous_sid:
                try:
                    if not session.new or session.previous_sid:
                        with self.connection() as conn:
                            with conn.cursor() as cur:
                                for old in {session.sid, session.previous_sid} - {None}:
                                    cur.execute(
                                        "DELETE FROM echorepo_web_sessions WHERE sid_hash = %s",
                                        (_sid_hash(old),),
                                    )
                except psycopg2.Error as exc:
                    app.logger.error("Postgres session logout failed: %s", type(exc).__name__)
                    raise ServiceUnavailable("Session storage is temporarily unavailable") from exc
            if session.modified or session.previous_sid or flask_request.cookies.get(name):
                response.delete_cookie(name, domain=domain, path=path)
            return

        set_cookie = self.should_set_cookie(app, session) or session.new and session.modified
        if not set_cookie:
            return
        expires = self.get_expiration_time(app, session)
        server_expiry = datetime.now(timezone.utc) + app.permanent_session_lifetime
        if server_expiry <= datetime.now(timezone.utc):
            raise ServiceUnavailable("Invalid session expiration configuration")
        try:
            if session.modified or session.new:
                encoded = _JSON.dumps(dict(session))
                if len(encoded.encode("utf-8")) > _MAX_SESSION_BYTES:
                    raise ValueError("Session data exceeds the configured size limit")
            with self.connection() as conn:
                with conn.cursor() as cur:
                    if session.previous_sid:
                        cur.execute(
                            "DELETE FROM echorepo_web_sessions WHERE sid_hash = %s",
                            (_sid_hash(session.previous_sid),),
                        )
                    if session.new:
                        cur.execute(
                            "INSERT INTO echorepo_web_sessions (sid_hash, data, expires_at) "
                            "VALUES (%s, %s::jsonb, %s)",
                            (_sid_hash(session.sid), encoded, server_expiry),
                        )
                    elif session.modified:
                        # Optimistic locking: concurrent token refresh must not be
                        # overwritten by a stale map/API request.
                        cur.execute(
                            "UPDATE echorepo_web_sessions SET data = %s::jsonb, "
                            "version = version + 1, expires_at = %s, updated_at = now() "
                            "WHERE sid_hash = %s AND version = %s AND expires_at > now()",
                            (encoded, server_expiry, _sid_hash(session.sid), session.version),
                        )
                        if cur.rowcount != 1:
                            raise RuntimeError("Concurrent session update; retry request")
                    else:
                        # Only extend expiry; never overwrite a newer token set.
                        cur.execute(
                            "UPDATE echorepo_web_sessions SET expires_at = %s, updated_at = now() "
                            "WHERE sid_hash = %s AND expires_at > now()",
                            (server_expiry, _sid_hash(session.sid)),
                        )
                        if cur.rowcount != 1:
                            return  # Stale SID after logout/rotation; do not reissue cookie.
        except (psycopg2.Error, ValueError, RuntimeError) as exc:
            app.logger.error("Postgres session write refused: %s", type(exc).__name__)
            raise ServiceUnavailable("Session could not be saved; please retry") from exc

        response.set_cookie(
            name,
            session.sid,
            expires=expires,
            httponly=self.get_cookie_httponly(app),
            secure=self.get_cookie_secure(app),
            samesite=self.get_cookie_samesite(app),
            domain=domain,
            path=path,
        )


def enable_postgres_sessions(app):
    """Activate after SECRET_KEY/config, before any requests or OAuth setup."""
    storage = os.getenv("ECHOREPO_SESSION_STORAGE", "cookie").strip().lower()
    if storage not in {"cookie", "postgres"}:
        raise RuntimeError("ECHOREPO_SESSION_STORAGE must be cookie or postgres")
    if storage == "cookie":
        return False

    legacy_cookie_name = os.getenv("ECHOREPO_LEGACY_SESSION_COOKIE", "session").strip()
    new_cookie_name = os.getenv("ECHOREPO_SESSION_COOKIE", "echorepo_sid").strip()
    if not legacy_cookie_name or not new_cookie_name or legacy_cookie_name == new_cookie_name:
        raise RuntimeError("Server and legacy session cookie names must be nonempty and different")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", new_cookie_name):
        raise RuntimeError("Invalid ECHOREPO_SESSION_COOKIE")

    app.config.update(
        SESSION_COOKIE_NAME=new_cookie_name,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_REFRESH_EACH_REQUEST=True,
        PERMANENT_SESSION_LIFETIME=timedelta(
            days=int(os.getenv("ECHOREPO_SESSION_TTL_DAYS", "31"))
        ),
    )
    if app.permanent_session_lifetime.total_seconds() <= 0:
        raise RuntimeError("ECHOREPO_SESSION_TTL_DAYS must be positive")

    # Deployment migration must run before the app switches to PostgreSQL.
    # Refuse to start with a missing table instead of failing only at login.
    try:
        with _db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT sid_hash FROM echorepo_web_sessions LIMIT 0")
    except psycopg2.Error as exc:
        raise RuntimeError("PostgreSQL session storage is not ready; run DB migrations first") from exc

    migrate_legacy = os.getenv("ECHOREPO_MIGRATE_LEGACY_SESSIONS", "false").lower() in {
        "true", "1", "yes"
    }
    # An attacker who can sign a forged old cookie could otherwise import it as
    # a trusted server session. Never enable that path with example secrets.
    if migrate_legacy and (
        len(app.secret_key or "") < 24
        or app.secret_key in {"please-change-me", "replace-with-long-random-string"}
    ):
        raise RuntimeError("A strong SECRET_KEY is required for legacy cookie migration")

    interface = PostgresSessionInterface(
        legacy_cookie_name=legacy_cookie_name,
        migrate_legacy=migrate_legacy,
    )
    app.session_interface = interface
    return True


def prune_expired_sessions():
    """Optional scheduled maintenance, run only after SQL migration is installed."""
    with _db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM echorepo_web_sessions WHERE expires_at <= now()")
            return cur.rowcount
