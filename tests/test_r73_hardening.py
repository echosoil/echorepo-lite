"""Offline checks for baseline response protections and identity state cleanup."""

from unittest.mock import Mock

import pytest
import requests
from flask import Flask, session

from echorepo.auth import tokens
from echorepo.http_security import configure_http_security, debug_routes_enabled


def test_production_policy(monkeypatch):
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("ENABLE_DEBUG_ROUTES", "1")
    monkeypatch.setenv("ECHOREPO_HSTS_MAX_AGE", "86400")
    app = Flask(__name__)
    app.debug = True
    configure_http_security(app)

    @app.get("/")
    def home():
        return "ok"

    response = app.test_client().get("/")
    assert not app.debug
    assert not debug_routes_enabled(app)
    assert response.headers["Strict-Transport-Security"] == "max-age=86400"
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["X-Frame-Options"] == "SAMEORIGIN"
    assert "object-src 'none'" in response.headers["Content-Security-Policy"]
    assert "default-src" not in response.headers["Content-Security-Policy"]
    assert response.headers["Referrer-Policy"]
    assert response.headers["Permissions-Policy"]
    assert app.test_client().get("/missing").headers["X-Content-Type-Options"] == "nosniff"


def test_development_policy(monkeypatch):
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("ENABLE_DEBUG_ROUTES", "1")
    monkeypatch.setenv("ECHOREPO_HSTS_MAX_AGE", "86400")
    app = Flask(__name__)
    configure_http_security(app)
    assert debug_routes_enabled(app)
    assert "Strict-Transport-Security" not in app.test_client().get("/").headers


@pytest.mark.parametrize("outcome", ["rejected", "timeout", "invalid_response"])
def test_refresh_clears_identity_on_failure(monkeypatch, outcome):
    app = Flask(__name__)
    app.secret_key = "test-only"
    reply = Mock(status_code=401 if outcome == "rejected" else 200)
    reply.json.side_effect = ValueError("invalid response")
    post = Mock(return_value=reply)
    if outcome == "timeout":
        post.side_effect = requests.Timeout()
    monkeypatch.setattr(tokens.requests, "post", post)
    with app.test_request_context("/"):
        session.update(kc={"exp": 0, "refresh_token": "test-only"}, user="test-user", lang="en")
        tokens.refresh_tokens_if_needed()
        assert "kc" not in session
        assert "user" not in session
        assert session["lang"] == "en"


def test_orphan_identity_is_cleared():
    app = Flask(__name__)
    app.secret_key = "test-only"
    with app.test_request_context("/"):
        session["user"] = "test-user"
        tokens.refresh_tokens_if_needed()
        assert "user" not in session


def test_current_session_not_refreshed(monkeypatch):
    app = Flask(__name__)
    app.secret_key = "test-only"
    post = Mock()
    monkeypatch.setattr(tokens.requests, "post", post)
    with app.test_request_context("/"):
        session.update(kc={"exp": 9999999999}, user="test-user")
        tokens.refresh_tokens_if_needed()
        assert session["user"] == "test-user"
        post.assert_not_called()
