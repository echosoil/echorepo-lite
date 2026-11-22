"""R7-1 acceptance checks for the HTTP API and protected web routes.

No external databases, credentials, or network services are used.
These tests validate implemented behaviour, not the historical M40 baseline.
"""
from __future__ import annotations

import unittest
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from flask import Blueprint, Flask, g, session

from echorepo.auth.decorators import login_required
from echorepo.routes import data_api as api


class R71APIContractTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(
            TESTING=True,
            SECRET_KEY="r71-local-test-key",
            API_KEY="r71-read-test-key",
            CANONICAL_PUBLIC=False,
            CANONICAL_MAP_PUBLIC=False,
        )
        self.app.register_blueprint(api.data_api, url_prefix="/api/v1")
        self.client = self.app.test_client()

    def test_r71_api_01_public_health_endpoint(self):
        response = self.client.get("/api/v1/ping")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"ok": True})

    def test_r71_api_02_private_exports_require_auth(self):
        for path in ("/api/v1/canonical/all.zip", "/api/v1/canonical/samples"):
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 401)
                self.assertEqual(response.get_json()["ok"], False)
                self.assertIn("WWW-Authenticate", response.headers)

    def test_r71_api_03_valid_api_key_is_accepted(self):
        with self.app.test_request_context(
            "/api/v1/canonical/samples",
            headers={"X-API-Key": "r71-read-test-key"},
        ):
            self.assertIsNone(api.require_api_auth())
            self.assertEqual(g.api_auth_method, "api_key")

    def test_r71_api_04_private_identity_field_is_rejected(self):
        with patch.object(api, "get_pg_conn", side_effect=AssertionError("unexpected DB access")):
            response = self.client.get(
                "/api/v1/canonical/samples?fields=sample_id,collected_by",
                headers={"X-API-Key": "r71-read-test-key"},
            )
        self.assertEqual(response.status_code, 400)

    def test_r71_api_05_invalid_format_and_order_rejected(self):
        with patch.object(api, "get_pg_conn", side_effect=AssertionError("unexpected DB access")):
            for suffix in ("format=xml", "order=not_a_column", "dir=sideways"):
                with self.subTest(query=suffix):
                    response = self.client.get(
                        "/api/v1/canonical/samples?" + suffix,
                        headers={"X-API-Key": "r71-read-test-key"},
                    )
                    self.assertEqual(response.status_code, 400)

    def test_r71_api_06_pagination_and_filters_bounded(self):
        with self.app.test_request_context("/?limit=999999&offset=2"):
            self.assertEqual(api._canonical_pagination("json"), (api.MAX_PAGE_SIZE, 2))
        with self.app.test_request_context("/?limit=bad"):
            with self.assertRaises(Exception) as raised:
                api._canonical_pagination("json")
            self.assertEqual(raised.exception.code, 400)
        with self.app.test_request_context("/?"):
            self.assertEqual(api._canonical_pagination("csv"), (None, 0))
        self.assertEqual(api.parse_bbox("1,2,3,4"), (1.0, 2.0, 3.0, 4.0))
        for bad in ("1,2,3", "nan,1,2,3", "3,2,1,4", "1,100,2,110"):
            with self.subTest(bbox=bad), self.assertRaises(ValueError):
                api.parse_bbox(bad)
        self.assertEqual(api.parse_within("41.4,2.1,5"), (41.4, 2.1, 5.0))
        with self.assertRaises(ValueError):
            api.parse_within("41.4,2.1,0")

    def test_r71_api_07_date_range_validation(self):
        self.assertEqual(api._parse_date_or_datetime("2026-09-30", end_bound=True), ("2026-10-01", "<"))
        with self.app.test_request_context("/?from=2026-10-01&to=2026-09-01"):
            with self.assertRaises(Exception) as raised:
                api._time_window_from_request()
            self.assertEqual(raised.exception.code, 400)


class R71WebAccessTests(unittest.TestCase):
    def setUp(self):
        app = Flask(__name__)
        app.config.update(TESTING=True, SECRET_KEY="r71-web-local-key")
        auth = Blueprint("auth", __name__)

        @auth.get("/login")
        def login():
            return "Sign in"

        app.register_blueprint(auth)

        @app.get("/private")
        @login_required
        def private():
            return "Authorized"

        @app.post("/private")
        @login_required
        def submit():
            return "Authorized"

        self.client = app.test_client()

    def test_r71_web_01_anonymous_get_redirects_with_next(self):
        response = self.client.get("/private?country=ES")
        self.assertEqual(response.status_code, 302)
        location = urlsplit(response.headers["Location"])
        self.assertEqual(location.path, "/login")
        self.assertEqual(parse_qs(location.query)["next"], ["/private?country=ES"])

    def test_r71_web_02_post_not_replayed_after_login(self):
        response = self.client.post("/private", data={"value": "private"})
        self.assertEqual(response.status_code, 302)
        self.assertNotIn("next=", response.headers["Location"])

    def test_r71_web_03_authenticated_user_can_access(self):
        with self.client.session_transaction() as state:
            state["kc"] = {"profile": {"sub": "test-subject"}}
        response = self.client.get("/private")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_data(as_text=True), "Authorized")


if __name__ == "__main__":
    unittest.main()
