"""Offline tests for Phase 5 report and request guard."""
import json

import pytest

from tools.r73_ux import AXE_VERSION, ORIGIN, allowed_request, blocked_bucket, summarize_axe


@pytest.mark.parametrize("method,path,kind", [
    ("POST", "/login", "document"),
    ("GET", "/logout", "document"),
    ("GET", "/search?format=zip", "document"),
    ("GET", "/api/v1/canonical/snapshot/all.zip", "fetch"),
    ("GET", "/my", "document"),
    ("GET", "/i18n/admin", "xhr"),
])
def test_block_non_public_workflows(method, path, kind):
    assert not allowed_request(method, ORIGIN + path, kind)


def test_allow_public_pages_and_assets():
    assert allowed_request("GET", ORIGIN + "/explore", "document")
    assert allowed_request("GET", ORIGIN + "/lang/es", "document")
    assert allowed_request("GET", ORIGIN + "/static/js/map.js?v=46", "script")
    assert allowed_request("GET", ORIGIN + "/api/v1/canonical/map.geojson?limit=200", "fetch")
    assert allowed_request("GET", "https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css", "stylesheet")
    assert not allowed_request("GET", "https://example.org/", "document")
    assert not allowed_request("GET", "https://cdn.jsdelivr.net/", "document")


def test_axe_sanitization():
    raw = {"testEngine": {"version": AXE_VERSION}, "violations": [
        {"id": "label", "impact": "serious",
         "nodes": [{"html": "private fixture", "target": ["private-selector"]}]}
    ], "incomplete": [], "passes": [{"id": "html-has-lang", "nodes": [{}]}], "inapplicable": []}
    result = summarize_axe(raw)
    assert result["categories"]["violations"]["nodes"] == 1
    assert result["categories"]["passes"]["rules"] == 1
    assert "private" not in json.dumps(result)
    raw["testEngine"]["version"] = "wrong"
    with pytest.raises(ValueError):
        summarize_axe(raw)


def test_known_stylesheets_only_for_axe_fetch():
    css = "https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css"
    assert allowed_request("GET", css, "fetch")
    assert not allowed_request("POST", css, "fetch")
    assert not allowed_request("GET", "https://cdn.jsdelivr.net/npm/unknown", "fetch")
    assert not allowed_request("GET", ORIGIN + "/search?format=zip", "fetch")
    bucket = blocked_bucket("POST", "https://unknown.invalid/path?secret=fixture", "fetch")
    assert bucket == "policy:other:fetch:non-read"
    assert "fixture" not in bucket


def test_accessibility_template_contracts():
    from pathlib import Path

    templates = Path(__file__).resolve().parents[1] / "echorepo/templates"
    explore = (templates / "explore.html").read_text(encoding="utf-8")
    assert 'aria-label="{{ _(\'pH min\') }}"' in explore
    assert 'aria-label="{{ _(\'pH max\') }}"' in explore
    assert 'for="countryFilter"' in explore
    assert 'aria-label="{{ _(\'Date from\') }}"' in explore
    assert 'aria-label="{{ _(\'Date to\') }}"' in explore
    assert 'explore-actions d-flex flex-wrap' in explore
    assert 'autofocus' not in (templates / "login.html").read_text(encoding="utf-8")
    assert '<main id="main" tabindex="-1"' in (templates / "base.html").read_text(encoding="utf-8")
    assert '<h1 class="h4 mb-3">' in (templates / "search.html").read_text(encoding="utf-8")
