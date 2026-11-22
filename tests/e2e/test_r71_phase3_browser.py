"""R7-1 Phase 3 — real-browser ECHOREPO acceptance checks.

Playwright Chromium visits *only* read-oriented UI flows. No login/password
submission, logout, data export/download or privacy-acceptance POST is automated.
A production run requires R71_PHASE3_RUN=production-readonly and an explicit
acknowledgment. Chromium is optional and is never installed by this test suite.

Install: python -m pip install playwright; python -m playwright install chromium
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from tools.r71_phase3_guard import BrowserTargetError, authentication_state, browser_target, validate_target


# No Playwright import or external navigation during normal CI.
pytestmark = pytest.mark.skipif(
    not os.getenv("R71_PHASE3_RUN"),
    reason="Browser acceptance is opt-in: set R71_PHASE3_RUN",
)

RUN_TIMEOUT_MS = 15000


@pytest.fixture(scope="module")
def target() -> tuple[str, str]:
    try:
        return browser_target()
    except BrowserTargetError as exc:
        pytest.fail(f"R7-1 Phase 3 browser target rejected: {exc}", pytrace=False)


@pytest.fixture(scope="module")
def chromium(target):
    playwright = pytest.importorskip(
        "playwright.sync_api",
        reason="Install Playwright for real browser checks: python -m pip install playwright",
    )
    with playwright.sync_playwright() as engine:
        options = {"headless": True}
        executable = os.getenv("R71_PHASE3_CHROMIUM_PATH", "").strip()
        if executable:
            options["executable_path"] = executable
        try:
            browser = engine.chromium.launch(**options)
        except Exception as exc:
            pytest.fail(
                "Could not launch Chromium. Install the browser with "
                "'python -m playwright install chromium' and verify OS dependencies. "
                f"Error type: {type(exc).__name__}",
                pytrace=False,
            )
        yield browser
        browser.close()


def new_page(browser, base_url: str, *, state: Path | None = None, mobile: bool = False):
    opts = {
        "base_url": base_url,
        "viewport": {"width": 390, "height": 844} if mobile else {"width": 1366, "height": 900},
        "accept_downloads": False,
    }
    if state is not None:
        opts["storage_state"] = str(state)
    context = browser.new_context(**opts)
    # Defensive guard even in staging: never emit form POSTs or other mutations.
    context.route(
        "**/*",
        lambda route: route.continue_()
        if route.request.method.upper() in {"GET", "HEAD", "OPTIONS"}
        else route.abort(),
    )
    page = context.new_page()
    page.set_default_timeout(RUN_TIMEOUT_MS)
    return context, page


@pytest.fixture
def page(chromium, target):
    _mode, base = target
    context, current = new_page(chromium, base)
    try:
        yield current
    finally:
        context.close()


@pytest.fixture
def auth_page(chromium, target):
    try:
        state = authentication_state()
    except BrowserTargetError as exc:
        pytest.fail(f"Invalid browser session state: {exc}", pytrace=False)
    if state is None:
        pytest.skip("Authenticated browser checks require R71_PHASE3_STORAGE_STATE")
    _mode, base = target
    context, current = new_page(chromium, base, state=state)
    try:
        yield current
    finally:
        context.close()


@pytest.fixture
def mobile_page(chromium, target):
    _mode, base = target
    context, current = new_page(chromium, base, mobile=True)
    try:
        yield current
    finally:
        context.close()


def goto(page, path: str):
    assert path.startswith("/") and not path.startswith("//")
    response = page.goto(path, wait_until="domcontentloaded", timeout=30000)
    assert response is not None and response.status < 500, (
        f"{path} failed at HTTP level: {response.status if response else 'no response'}"
    )
    return response


def map_ready(page):
    page.wait_for_function(
        "() => Boolean(window.__echomap && document.querySelector('#map.leaflet-container')"
        " && window.__echomap._loaded)",
        timeout=RUN_TIMEOUT_MS,
    )
    assert page.locator("#map").is_visible()
    assert page.locator("#map").bounding_box()["height"] >= 200


def selected_query(page, field: str) -> str:
    return parse_qs(urlsplit(page.url).query).get(field, [""])[0]


def test_r71_b01_anonymous_landing_opens_explore(page):
    goto(page, "/")
    assert urlsplit(page.url).path == "/explore"
    assert page.locator("main #map").count() == 1


def test_r71_b02_public_explore_has_accessible_navigation(page):
    goto(page, "/explore")
    assert page.locator("h1").count() >= 1
    assert page.locator("#map").is_visible()
    assert page.locator(".explore-actions a[href='/search']").count() == 1
    assert page.locator("#btnExportFiltered").count() == 0, (
        "Anonymous visitors must not see authenticated ZIP export controls"
    )


def test_r71_b03_public_leaflet_map_initializes(page):
    goto(page, "/explore")
    map_ready(page)
    assert page.evaluate("Boolean(window.ECHOREPO_CFG && window.ECHOREPO_CFG.public_mode)")


def test_r71_b04_map_ph_filters_update_url(page):
    goto(page, "/explore")
    map_ready(page)
    page.locator("#phMin").fill("5.5")
    page.locator("#phMax").fill("7.5")
    page.locator("#btnApplyFilter").click()
    page.wait_for_function(
        "() => new URL(location.href).searchParams.get('ph_min') === '5.5'",
        timeout=RUN_TIMEOUT_MS,
    )
    assert selected_query(page, "ph_max") == "7.5"
    assert page.locator("#map").is_visible()


def test_r71_b05_map_deep_link_centers_correctly(page):
    goto(page, "/explore?lat=41.3851&lng=2.1734&z=10")
    map_ready(page)
    page.wait_for_function(
        "() => { const m=window.__echomap; if(!m||!m._loaded) return false;"
        "const c=m.getCenter(); return Math.abs(c.lat-41.3851)<0.02"
        "&&Math.abs(c.lng-2.1734)<0.02&&m.getZoom()===10; }",
        timeout=RUN_TIMEOUT_MS,
    )


def test_r71_b06_search_form_and_results_render(page):
    goto(page, "/search")
    for locator in (
        "#sample_id", "#country_code", "#biogeo_region", "#ph_min",
        "#ph_max", "#date_from", "#date_to", ".sample-table",
    ):
        assert page.locator(locator).count() == 1, f"Search form missing {locator}"
    assert page.locator(".sample-table thead th").count() == 8


def test_r71_b07_search_country_filter_round_trip(page):
    goto(page, "/search")
    available = page.locator("#country_code option").evaluate_all(
        "(options) => options.map(o => o.value).filter(Boolean)"
    )
    assert available, "The deployed search must expose at least one dataset country"
    country = "ES" if "ES" in available else available[0]
    page.locator("#country_code").select_option(country)
    page.locator("form.search-form button[type='submit']").click()
    page.wait_for_load_state("domcontentloaded")
    assert selected_query(page, "country_code") == country
    assert page.locator("#country_code").input_value() == country
    rows = page.locator(".sample-table tbody tr")
    for idx in range(rows.count()):
        cells = rows.nth(idx).locator("td")
        if cells.count() == 8:
            assert cells.nth(3).inner_text().strip().upper() == country


def test_r71_b08_search_ph_filter_enforces_visible_range(page):
    goto(page, "/search")
    page.locator("#ph_min").fill("5")
    page.locator("#ph_max").fill("8")
    page.locator("form.search-form button[type='submit']").click()
    page.wait_for_load_state("domcontentloaded")
    assert selected_query(page, "ph_min") == "5"
    assert selected_query(page, "ph_max") == "8"
    rows = page.locator(".sample-table tbody tr")
    observed = 0
    for idx in range(rows.count()):
        cells = rows.nth(idx).locator("td")
        if cells.count() != 8:
            continue
        value = cells.nth(4).inner_text().strip()
        if value:
            ph = float(value)
            assert 5 <= ph <= 8, f"Visible search result outside selected pH bounds: {ph}"
            observed += 1
    assert observed > 0, "No numeric pH results available to validate range filtering"


def test_r71_b09_search_pagination_preserves_country(page):
    goto(page, "/search")
    countries = page.locator("#country_code option").evaluate_all(
        "(options) => options.map(o => o.value).filter(Boolean)"
    )
    assert countries
    country = "ES" if "ES" in countries else countries[0]
    goto(page, "/search?country_code=" + country)
    next_page = page.locator("a.page-link[href*='page=2']").first
    if next_page.count() == 0:
        pytest.skip("Selected country has fewer than two pages; no pagination to exercise")
    first_sample = page.locator(".sample-table tbody tr:first-child td:nth-child(2)").inner_text()
    next_page.click()
    assert selected_query(page, "page") == "2"
    assert selected_query(page, "country_code") == country
    second_sample = page.locator(".sample-table tbody tr:first-child td:nth-child(2)").inner_text()
    assert first_sample != second_sample


def test_r71_b10_anonymous_search_redacts_submitter_id(page):
    goto(page, "/search")
    rows = page.locator(".sample-table tbody tr")
    found = 0
    for idx in range(rows.count()):
        cells = rows.nth(idx).locator("td")
        if cells.count() != 8:
            continue
        found += 1
        assert cells.nth(7).inner_text().strip() == "", (
            "Public search must not expose another user's collected_by identifier"
        )
    assert found > 0, "No public rows present to check submitter redaction"


def test_r71_b11_anonymous_zip_export_is_forbidden(page):
    goto(page, "/search")
    assert page.locator("a[href*='format=zip']").count() == 0
    response = goto(page, "/search?format=zip")
    assert response.status == 403, "Anonymous filtered ZIP exports must be forbidden"


def test_r71_b12_personal_page_requires_login_and_preserves_next(page):
    goto(page, "/my?lat=41.3851&lng=2.1734&z=10")
    assert urlsplit(page.url).path == "/login"
    destination = parse_qs(urlsplit(page.url).query).get("next", [""])[0]
    assert destination.startswith("/my?")
    assert "lat=41.3851" in destination
    assert page.locator("form input#username").count() == 1
    assert page.locator("form input#password").count() == 1


def test_r71_b13_language_selection_sets_locale_cookie(page):
    goto(page, "/set-lang/es")
    assert urlsplit(page.url).path == "/explore"
    assert page.locator("html").get_attribute("lang").lower().startswith("es")
    cookies = page.context.cookies()
    assert any(item["name"] == "locale" and item["value"] == "es" for item in cookies)


def test_r71_b14_responsive_public_map_on_mobile(mobile_page):
    goto(mobile_page, "/explore")
    assert mobile_page.locator("#map").is_visible()
    assert mobile_page.locator("#echoLayoutSwitch").is_visible()
    assert "desktop=1" in mobile_page.locator("#echoLayoutSwitch").get_attribute("href")


def test_r71_b15_authenticated_personal_page_renders(auth_page):
    goto(auth_page, "/my")
    assert urlsplit(auth_page.url).path == "/my", (
        "Saved browser session is expired or not authorized for /my"
    )
    assert auth_page.locator("#map").count() == 1
    assert auth_page.locator("#tableWrap").count() == 1
    # Never submit privacy consent automatically.


def test_r71_b16_authenticated_search_shows_export_link(auth_page):
    goto(auth_page, "/search")
    assert urlsplit(auth_page.url).path == "/search"
    assert auth_page.locator("a[href*='format=zip']").count() >= 1
    # Do not click: downloading may create large data files and usage events.


def optional_service_url(name: str, *, mode: str) -> str:
    raw = os.getenv(name, "").strip()
    if not raw:
        pytest.skip(f"{name} is not configured (optional service)")
    parsed = urlsplit(raw)
    assert parsed.path in {"", "/", "/jupyter/"}, "Only a service root or /jupyter/ is allowed"
    try:
        validate_target(mode, f"{parsed.scheme}://{parsed.netloc}")
    except BrowserTargetError as exc:
        pytest.fail(f"Optional service URL rejected: {exc}", pytrace=False)
    return raw


def test_r71_b17_optional_jupyter_interface_reachable(page, target):
    mode, _base = target
    url = optional_service_url("R71_PHASE3_JUPYTER_URL", mode=mode)
    response = page.goto(url, wait_until="domcontentloaded", timeout=30000)
    assert response is not None and response.status < 500
    assert "jupyter" in (page.title() + " " + page.url).lower(), (
        "The configured Jupyter endpoint did not display the research interface"
    )


def test_r71_b18_optional_streamlit_interface_reachable(page, target):
    mode, _base = target
    url = optional_service_url("R71_PHASE3_STREAMLIT_URL", mode=mode)
    response = page.goto(url, wait_until="domcontentloaded", timeout=30000)
    assert response is not None and response.status < 500
    assert "streamlit" in (page.title() + " " + page.content()[:6000]).lower(), (
        "Configured Streamlit endpoint is not recognizable as a Streamlit interface"
    )
