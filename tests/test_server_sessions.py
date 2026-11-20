"""Functional server-session smoke tests with a fake PostgreSQL transaction API.

Run: python -m unittest discover -s tests -p 'test_server_sessions.py' -v
No live database or Keycloak calls are made.
"""

import json
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from flask import Flask, current_app, request, session
from flask.testing import FlaskClient
from flask.sessions import SecureCookieSessionInterface
from werkzeug.exceptions import ServiceUnavailable

from echorepo.server_sessions import PostgresSessionInterface, _sid_hash


class HTTPSClient(FlaskClient):
    def open(self, *args, **kwargs):
        kwargs.setdefault("base_url", "https://localhost")
        return super().open(*args, **kwargs)


class Cursor:
    def __init__(self, storage):
        self.storage = storage
        self.rowcount = -1
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, statement, params):
        self.result = None
        self.rowcount = 0
        key = params[0] if statement.startswith("SELECT") else None
        if statement.startswith("SELECT data, version"):
            row = self.storage.get(key)
            if row and row["expires_at"] > datetime.now(timezone.utc):
                self.result = (json.loads(row["data"]), row["version"])
                self.rowcount = 1
        elif statement.startswith("INSERT INTO echorepo_web_sessions"):
            key, data, expires = params
            if key in self.storage:
                raise RuntimeError("duplicate session")
            self.storage[key] = {"data": data, "version": 0, "expires_at": expires}
            self.rowcount = 1
        elif statement.startswith("UPDATE echorepo_web_sessions"):
            modified = "data = %s::jsonb" in statement
            key = params[2] if modified else params[1]
            row = self.storage.get(key)
            if row and row["expires_at"] > datetime.now(timezone.utc):
                if modified and row["version"] != params[3]:
                    return
                if modified:
                    row["data"] = params[0]
                    row["version"] += 1
                    row["expires_at"] = params[1]
                else:
                    row["expires_at"] = params[0]
                self.rowcount = 1
        elif statement.startswith("DELETE FROM echorepo_web_sessions"):
            key = params[0]
            self.rowcount = int(key in self.storage)
            self.storage.pop(key, None)
        else:
            raise AssertionError("Unexpected SQL statement: " + statement)

    def fetchone(self):
        return self.result


class Connection:
    def __init__(self, storage):
        self.storage = storage

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def cursor(self):
        return Cursor(self.storage)


class SessionTests(unittest.TestCase):
    def app(self, *, migrate=False):
        storage = {}

        @contextmanager
        def connection():
            yield Connection(storage)

        app = Flask(__name__)
        app.secret_key = "only-for-tests-not-for-deployment"
        app.test_client_class = HTTPSClient
        app.config.update(
            TESTING=True,
            SESSION_COOKIE_NAME="echorepo_sid",
            SESSION_COOKIE_SECURE=True,
            SESSION_COOKIE_HTTPONLY=True,
            SESSION_COOKIE_SAMESITE="Lax",
            PERMANENT_SESSION_LIFETIME=timedelta(days=7),
        )
        app.session_interface = PostgresSessionInterface(
            migrate_legacy=migrate,
            legacy_cookie_name="session",
            connection=connection,
        )

        @app.get("/me")
        def me():
            return {"user": session.get("user"), "has_kc": bool(session.get("kc"))}

        @app.post("/login")
        def login():
            session["user"] = "example@example.org"
            session["kc"] = {"access_token": "access-secret", "refresh_token": "refresh-secret"}
            session.permanent = True
            current_app.session_interface.regenerate(session)
            return {"ok": True}

        @app.post("/refresh")
        def refresh():
            # Refresh must NOT rotate SID.
            session["kc"] = {"access_token": "updated-secret", "refresh_token": "refresh-new"}
            return {"ok": True}

        @app.post("/logout")
        def logout():
            session.clear()
            return {"ok": True}

        return app, storage

    @staticmethod
    def browser(app):
        client = app.test_client()
        return client

    def test_tokens_not_in_cookie_and_persist_between_requests(self):
        app, storage = self.app()
        client = self.browser(app)
        response = client.post("/login")
        self.assertEqual(response.status_code, 200)
        headers = " ".join(response.headers.getlist("Set-Cookie"))
        self.assertIn("echorepo_sid=", headers)
        self.assertIn("HttpOnly", headers)
        self.assertIn("Secure", headers)
        self.assertIn("SameSite=Lax", headers)
        self.assertNotIn("access-secret", headers)
        self.assertNotIn("refresh-secret", headers)
        self.assertEqual(client.get("/me").json["user"], "example@example.org")
        self.assertEqual(len(storage), 1)

    def test_sid_rotation_login_vs_refresh_and_logout_revokes(self):
        app, storage = self.app()
        client = self.browser(app)
        client.post("/login")
        original_hash = next(iter(storage))
        client.post("/refresh")
        self.assertEqual(next(iter(storage)), original_hash)
        self.assertIn("updated-secret", next(iter(storage.values()))["data"])
        client.post("/login")
        self.assertEqual(len(storage), 1)
        self.assertNotIn(original_hash, storage)
        client.post("/logout")
        self.assertEqual(len(storage), 0)
        self.assertIsNone(client.get("/me").json["user"])

    def test_legacy_signed_cookie_imports_and_is_deleted(self):
        app, storage = self.app(migrate=True)
        cookie = SecureCookieSessionInterface().get_signing_serializer(app).dumps(
            {"kc": {"access_token": "secret", "refresh_token": "oldsecret"},
             "user": "legacy@example.org", "_permanent": True}
        )
        client = self.browser(app)
        client.set_cookie("session", cookie)
        response = client.get("/me")
        self.assertEqual(response.json["user"], "legacy@example.org")
        cookies = response.headers.getlist("Set-Cookie")
        self.assertTrue(any("session=;" in x and "Max-Age=0" in x for x in cookies))
        self.assertTrue(any("echorepo_sid=" in x for x in cookies))
        self.assertFalse(any("oldsecret" in x for x in cookies))
        self.assertEqual(len(storage), 1)
        self.assertEqual(client.get("/me").json["user"], "legacy@example.org")

    def test_invalid_legacy_signature_does_not_authenticate(self):
        app, _ = self.app(migrate=True)
        client = self.browser(app)
        client.set_cookie("session", "fake.legacy.signature")
        self.assertIsNone(client.get("/me").json["user"])

    def test_migration_opt_out_ignores_old_cookie(self):
        app, _ = self.app(migrate=False)
        cookie = SecureCookieSessionInterface().get_signing_serializer(app).dumps(
            {"user": "legacy@example.org", "_permanent": True}
        )
        client = self.browser(app)
        client.set_cookie("session", cookie)
        response = client.get("/me")
        self.assertIsNone(response.json["user"])
        self.assertTrue(any("session=;" in x for x in response.headers.getlist("Set-Cookie")))

    def test_replay_old_sid_after_logout_does_not_authenticate(self):
        app, storage = self.app()
        client = self.browser(app)
        response = client.post("/login")
        sid = response.headers["Set-Cookie"].split("echorepo_sid=", 1)[1].split(";", 1)[0]
        client.post("/logout")
        stale_client = self.browser(app)
        stale_client.set_cookie("echorepo_sid", sid)
        self.assertIsNone(stale_client.get("/me").json["user"])
        self.assertNotIn(_sid_hash(sid), storage)

    def test_expired_sid_cannot_authenticate(self):
        app, storage = self.app()
        client = self.browser(app)
        client.post("/login")
        record = next(iter(storage.values()))
        record["expires_at"] = datetime.now(timezone.utc) - timedelta(seconds=10)
        self.assertIsNone(client.get("/me").json["user"])

    def test_session_debug_route_does_not_print_token_values(self):
        from pathlib import Path

        source = (Path(__file__).resolve().parents[1] / "echorepo/__init__.py").read_text()
        whoami = source.split('def debug_whoami():', 1)[1].split('@app.get("/i18n/debug")', 1)[0]
        self.assertNotIn("session_snapshot", whoami)
        self.assertNotIn("repr(vv)", whoami)

    def test_no_cookies_for_plain_anonymous_visitor(self):
        app, storage = self.app()
        response = self.browser(app).get("/me")
        self.assertEqual(response.json["has_kc"], False)
        self.assertEqual(len(storage), 0)
        self.assertFalse(any("echorepo_sid=" in x for x in response.headers.getlist("Set-Cookie")))

    def test_concurrent_stale_update_cannot_overwrite_refresh(self):
        app, storage = self.app()
        client = self.browser(app)
        response = client.post("/login")
        sid = response.headers["Set-Cookie"].split("echorepo_sid=", 1)[1].split(";", 1)[0]
        with app.test_request_context("/me"):
            req = request
            # Two requests have read the same SID and version.
            req.cookies  # Ensure request exists.
            client2 = app.session_interface.session_class(
                {"kc": {"access_token": "old"}, "user": "example@example.org", "_permanent": True},
                sid=sid, new=False, version=0,
            )
            client.post("/refresh")
            client2["kc"] = {"access_token": "stale"}
            from flask import make_response
            with self.assertRaises(ServiceUnavailable):
                app.session_interface.save_session(app, client2, make_response("ok"))
            self.assertNotIn("stale", next(iter(storage.values()))["data"])


if __name__ == "__main__":
    unittest.main()
