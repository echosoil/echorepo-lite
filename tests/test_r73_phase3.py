import csv
import io
import json
import zipfile
from http.cookiejar import Cookie

import pytest

from tools import r73_security as sec


def test_config_redaction():
    env = {"SECRET_KEY": "private-secret-value-that-is-long", "API_KEY": "private-api-key",
           "DB_PASSWORD": "echorepo-pass", "ECHOREPO_SESSION_STORAGE": "postgres",
           "JUPYTER_ALLOWED_EMAILS": "private@example.org"}
    result = sec.config_observations(env)
    text = json.dumps(result)
    assert "private" not in text and "echorepo-pass" not in text
    checks = {c["id"]: c for c in result["checks"]}
    assert checks["CFG01"]["status"] == "PASS"
    assert checks["CFG05"]["status"] == "REVIEW"


def test_cookie_attributes_only():
    c = Cookie(0, "echorepo_sid", "A" * 43, None, False, "example.org", True, False,
               "/", True, True, None, False, None, None, {"HttpOnly": None, "SameSite": "Lax"})
    result = sec.cookie_observations([c], {"echorepo_sid"})
    assert result == [{"secure": True, "http_only": True, "same_site": "lax", "opaque_sid_shape": True}]
    assert "A" * 43 not in json.dumps(result)


def test_issuer_and_redirect():
    assert sec.issuer_url("https://example.org/realms/echo/") == "https://example.org/realms/echo"
    for url in ("http://example.org/realms/echo", "https://user:secret@example.org/realms/echo",
                "https://example.org/realms/echo?token=secret", "https://example.org/path"):
        with pytest.raises(ValueError):
            sec.issuer_url(url)
    assert sec.local_login("/login?next=%2Fmy")
    assert not sec.local_login("https://other.example/login?next=/my")
    assert not sec.local_login("/login?next=/other")


def test_privacy_aggregate(tmp_path):
    bundle = tmp_path / "snapshot.zip"
    with zipfile.ZipFile(bundle, "w") as z:
        for filename in ("samples.csv", "sample_images.csv"):
            f = io.StringIO()
            writer = csv.writer(f)
            writer.writerow(["sample_id", "collected_by"])
            writer.writerows([["private-id", "private@example.org"], ["other-id", "a" * 64],
                              ["another-id", "pseudonym"], ["last-id", ""]])
            z.writestr(filename, f.getvalue())
    result = sec.privacy_observations(bundle)
    for c in result["checks"]:
        e = c["evidence"]
        assert e["rows"] == 4 and e["email_like"] == 1 and e["uuid_or_hex_like"] == 1
        assert e["other_nonblank"] == 1 and e["blank"] == 1
    assert "private" not in json.dumps(result)
    assert "pseudonym" not in sec.markdown(result)


def test_audit_summary(tmp_path):
    p = tmp_path / "pip.json"
    b = tmp_path / "bandit.json"
    p.write_text(json.dumps({"dependencies": [{"name": "privatepkg", "vulns": [{"id": "CVE-TEST"}]},
                                             {"name": "another", "skip_reason": "private URL"}]}))
    b.write_text(json.dumps({"results": [{"issue_severity": "HIGH", "issue_confidence": "MEDIUM",
                                         "test_id": "B101", "code": "private source"}],
                             "errors": [], "metrics": {"_totals": {"loc": 10}}}))
    result = sec.audit_observations(p, b, 1, 1)
    assert all(c["status"] == "REVIEW" for c in result["checks"])
    assert result["checks"][0]["evidence"]["affected_packages"] == 1
    assert result["checks"][1]["evidence"]["severity"]["HIGH"] == 1
    assert "private" not in json.dumps(result)
    assert sec.audit_observations(p, b, 2, 2)["checks"][0]["status"] == "NOT_ASSESSED"


def test_live_never_follows_redirects_or_leaks_payload(monkeypatch):
    import requests

    class Response:
        def __init__(self, status, headers=None, body=b""):
            self.status_code = status
            self.headers = headers or {}
            self.cookies = []
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def iter_content(self, *args):
            yield self.body

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url, **kwargs):
            assert self.trust_env is False and self.verify is True
            assert kwargs["allow_redirects"] is False
            assert not kwargs["headers"] or url.startswith(sec.ORIGIN + "/api/")
            if url.startswith("http://"):
                return Response(301, {"Location": sec.ORIGIN + "/explore"})
            if url.endswith("/my"):
                return Response(302, {"Location": "/login?next=/my"})
            if url.endswith("/internal/jupyterhub-auth"):
                return Response(401)
            if url.endswith("/api/v1/canonical/samples"):
                return Response(200, body=b'{"meta":{"fields":["sample_id"]},"data":[{"sample_id":"PRIVATE"}]}')
            return Response(200)

    monkeypatch.setattr(requests, "Session", Session)
    result = sec.live_observations(api_key="SECRET")
    checks = {c["id"]: c for c in result["checks"]}
    assert checks["AUTH01"]["status"] == "PASS"
    assert checks["PRIV01"]["status"] == "PASS"
    assert checks["SES01"]["status"] == "NOT_ASSESSED"
    assert "SECRET" not in json.dumps(result) and "PRIVATE" not in json.dumps(result)
