"""Security regression tests for the JupyterHub edge-auth endpoint."""
import os
import unittest
from unittest.mock import patch

from flask import Flask

from echorepo.auth.routes import auth_bp


class JupyterHubAuthTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(TESTING=True, SECRET_KEY="test-only-not-a-real-secret")
        self.app.register_blueprint(auth_bp)
        self.client = self.app.test_client()

    def login_as(self, email="researcher@example.org", sub="4d133a89-abc0-4000-8222-bbea1caa349d"):
        with self.client.session_transaction() as sess:
            sess["kc"] = {"profile": {"email": email, "sub": sub}}

    def test_anonymous_cannot_inject_identity_header(self):
        with patch.dict(os.environ, {"JUPYTER_ALLOWED_EMAILS": "all"}):
            response = self.client.get(
                "/internal/jupyterhub-auth",
                headers={"X-ECHOREPO-User": "someone-else"},
            )
        self.assertEqual(response.status_code, 401)
        self.assertNotIn("X-ECHOREPO-User", response.headers)

    def test_all_authenticated_users_get_stable_subject(self):
        self.login_as()
        with patch.dict(os.environ, {"JUPYTER_ALLOWED_EMAILS": "all"}):
            response = self.client.get("/internal/jupyterhub-auth")
        self.assertEqual(response.status_code, 204)
        self.assertEqual(
            response.headers.get("X-ECHOREPO-User"),
            "4d133a89-abc0-4000-8222-bbea1caa349d",
        )
        self.assertEqual(response.headers.get("Cache-Control"), "no-store")

    def test_allowlisted_email(self):
        self.login_as()
        with patch.dict(os.environ, {"JUPYTER_ALLOWED_EMAILS": "RESEARCHER@example.org"}):
            response = self.client.get("/internal/jupyterhub-auth")
        self.assertEqual(response.status_code, 204)

    def test_non_allowlisted_user_is_denied(self):
        self.login_as()
        with patch.dict(os.environ, {"JUPYTER_ALLOWED_EMAILS": "other@example.org"}):
            response = self.client.get("/internal/jupyterhub-auth")
        self.assertEqual(response.status_code, 403)
        self.assertNotIn("X-ECHOREPO-User", response.headers)

    def test_empty_allowlist_is_denied(self):
        self.login_as()
        with patch.dict(os.environ, {"JUPYTER_ALLOWED_EMAILS": ""}):
            response = self.client.get("/internal/jupyterhub-auth")
        self.assertEqual(response.status_code, 403)

    def test_missing_subject_is_denied(self):
        self.login_as(sub="")
        with patch.dict(os.environ, {"JUPYTER_ALLOWED_EMAILS": "all"}):
            response = self.client.get("/internal/jupyterhub-auth")
        self.assertEqual(response.status_code, 403)

    def test_invalid_subject_is_denied(self):
        self.login_as(sub="someone\r\nX-Hacked: yes")
        with patch.dict(os.environ, {"JUPYTER_ALLOWED_EMAILS": "all"}):
            response = self.client.get("/internal/jupyterhub-auth")
        self.assertEqual(response.status_code, 403)
        self.assertNotIn("X-ECHOREPO-User", response.headers)


if __name__ == "__main__":
    unittest.main()
