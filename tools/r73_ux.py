#!/usr/bin/env python3
"""Bounded anonymous browser and accessibility observations; no submissions or downloads."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

ORIGIN = "https://echorepo.quanta-labs.com"
ACK = "yes-production-readonly"
AXE_VERSION = "4.10.3"
TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"]
DOCS = {"/explore", "/search", "/login", "/lang/es", "/set-lang/es"}
READ_API = {"/api/v1/canonical/map.geojson", "/api/v1/canonical/map.count",
            "/public/others_geojson", "/i18n/labels", "/i18n/labels.js", "/i18n/labels.json"}
ASSET_HOSTS = {"cdn.jsdelivr.net", "unpkg.com", "fonts.googleapis.com", "fonts.gstatic.com",
               "a.tile.openstreetmap.org", "b.tile.openstreetmap.org", "c.tile.openstreetmap.org",
               "tile.openstreetmap.org", "server.arcgisonline.com"}
CRITICAL = {"document", "script", "stylesheet", "xhr", "fetch"}
CASES = [(page, width, height, "en")
         for page in ("/explore", "/search", "/login")
         for width, height in ((1366, 900), (390, 844))]
CASES.append(("/search", 1366, 900, "es"))


def allowed_request(method, url, resource_type):
    """Permit only ordinary public UI GET/HEAD paths and known asset hosts."""
    try:
        p = urlsplit(url)
        if (method not in {"GET", "HEAD"} or p.scheme != "https"
                or p.username or p.password or p.port not in (None, 443)):
            return False
        if p.hostname == "echorepo.quanta-labs.com":
            if resource_type == "document":
                return p.path in DOCS and not p.query
            return (p.path.startswith("/static/") or p.path == "/favicon.ico"
                    or p.path in READ_API)
        return p.hostname in ASSET_HOSTS and resource_type in {
            "script", "stylesheet", "font", "image"
        }
    except ValueError:
        return False


def summarize_axe(raw):
    """Keep rule IDs/counts only, not DOM excerpts, URLs, selectors or text."""
    engine = raw.get("testEngine") or {}
    if engine.get("version") != AXE_VERSION:
        raise ValueError("Unexpected axe-core version")
    result = {"engine_version": AXE_VERSION, "tags": TAGS, "categories": {}}
    for category in ("violations", "incomplete", "passes", "inapplicable"):
        items = raw[category]
        if not isinstance(items, list):
            raise ValueError("Unexpected axe result")
        rules = []
        for rule in items:
            rule_id = rule.get("id")
            if not isinstance(rule_id, str) or not re.fullmatch(r"[a-z0-9-]+", rule_id):
                raise ValueError("Unexpected rule ID")
            impact = rule.get("impact")
            rules.append({"id": rule_id,
                          "impact": impact if impact in {"minor", "moderate", "serious", "critical"} else None,
                          "nodes": len(rule.get("nodes") or [])})
        result["categories"][category] = {
            "rules": len(rules), "nodes": sum(r["nodes"] for r in rules),
            "details": rules if category in {"violations", "incomplete"} else [],
        }
    return result


AXE_RUN = """(tags) => Promise.race([
  window.axe.run(document, {runOnly: {type: 'tag', values: tags},
    resultTypes: ['violations', 'incomplete', 'passes', 'inapplicable']}),
  new Promise((_, reject) => setTimeout(() => reject(new Error('axe budget exceeded')), 30000))
])"""

DOM_STATE = """() => ({
  language: (document.documentElement.lang || '').split('-')[0].toLowerCase(),
  title_present: Boolean(document.title.trim()),
  main_count: document.querySelectorAll('main').length,
  h1_count: document.querySelectorAll('h1').length,
  viewport_width: window.innerWidth,
  document_width: document.documentElement.scrollWidth,
  horizontal_overflow_px: Math.max(0, document.documentElement.scrollWidth - window.innerWidth)
})"""


def keyboard_observation(page):
    # Never submit forms or activate arbitrary controls. Activate only the known skip link.
    page.keyboard.press("Tab")
    skip = page.evaluate("""() => {
      const e = document.activeElement;
      return Boolean(e && e.matches('a.skip-link[href="#main"]'));
    }""")
    if not skip:
        return {"skip_link_first_tab": False, "skip_link_reaches_main": None}
    page.keyboard.press("Enter")
    reaches = page.evaluate("""() => {
      const m = document.querySelector('main#main');
      return Boolean(m && m.contains(document.activeElement));
    }""")
    if not reaches:
        page.keyboard.press("Tab")
        reaches = page.evaluate("""() => {
          const m = document.querySelector('main#main');
          return Boolean(m && m.contains(document.activeElement));
        }""")
    return {"skip_link_first_tab": True, "skip_link_reaches_main": reaches}


def inspect_case(browser, axe_path, case):
    path, width, height, locale = case
    result = {"page": path, "viewport": [width, height], "requested_locale": locale,
              "status": "NOT_ASSESSED"}
    metrics = {"requests": 0, "blocked": 0, "blocked_critical": 0,
               "critical_http_errors": 0, "critical_request_failures": 0, "page_errors": 0}
    context = browser.new_context(viewport={"width": width, "height": height},
                                  locale="en-GB", accept_downloads=False,
                                  service_workers="block")
    try:
        context.add_init_script(path=str(axe_path))

        def guard(route):
            request = route.request
            metrics["requests"] += 1
            if metrics["requests"] <= 220 and allowed_request(
                    request.method, request.url, request.resource_type):
                route.continue_()
            else:
                metrics["blocked"] += 1
                metrics["blocked_critical"] += request.resource_type in CRITICAL
                route.abort()

        context.route("**/*", guard)
        page = context.new_page()
        page.set_default_timeout(12000)
        page.on("pageerror", lambda _: metrics.__setitem__("page_errors", metrics["page_errors"] + 1))

        def response_seen(response):
            if response.status >= 400 and response.request.resource_type in CRITICAL:
                metrics["critical_http_errors"] += 1

        def failed_request(request):
            if request.resource_type in CRITICAL:
                metrics["critical_request_failures"] += 1

        page.on("response", response_seen)
        page.on("requestfailed", failed_request)
        response = page.goto(ORIGIN + path, wait_until="load", timeout=35000)
        if not response or response.status != 200 or page.url != ORIGIN + path:
            result["reason"] = "Expected public page not reached"
            return result
        if locale == "es":
            # Use the actual public language control, not a synthetic cookie.
            switch = page.locator("nav a[href='/lang/es'], nav a[href='/set-lang/es']").first
            href = switch.get_attribute("href")
            if href not in {"/lang/es", "/set-lang/es"}:
                raise ValueError("Expected Spanish language route missing")
            response = page.goto(ORIGIN + href, referer=ORIGIN + path,
                                 wait_until="load", timeout=35000)
            if not response or response.status != 200 or page.url != ORIGIN + path:
                result["reason"] = "Language route did not return to expected page"
                return result
        result["http_status"] = response.status
        if path == "/explore":
            try:
                page.wait_for_function(
                    "() => Boolean(window.__echomap && window.__echomap._loaded && "
                    "document.querySelector('#map.leaflet-container'))", timeout=12000)
                result["map_initialized"] = page.locator("#map").is_visible()
            except Exception:
                result["map_initialized"] = False
        page.evaluate("""() => Promise.race([document.fonts.ready,
                         new Promise(resolve => setTimeout(resolve, 3000))])""")
        result["dom"] = page.evaluate(DOM_STATE)
        result["axe"] = summarize_axe(page.evaluate(AXE_RUN, TAGS))
        result["keyboard"] = keyboard_observation(page)
        if width == 390:
            toggle = page.locator("button.navbar-toggler").first
            mobile = {"navigation_toggle_visible": toggle.is_visible(), "opens": None}
            if mobile["navigation_toggle_visible"]:
                toggle.click()
                try:
                    page.wait_for_function(
                        "() => document.querySelector('button.navbar-toggler')"
                        "?.getAttribute('aria-expanded') === 'true'", timeout=5000)
                    mobile["opens"] = True
                except Exception:
                    mobile["opens"] = False
            result["mobile_navigation"] = mobile
        dom = result["dom"]
        categories = result["axe"]["categories"]
        ok = (dom["language"] == locale and dom["title_present"] and dom["main_count"] == 1
              and dom["horizontal_overflow_px"] <= 2
              and all(result["keyboard"].values())
              and not categories["violations"]["rules"] and not categories["incomplete"]["rules"]
              and categories["passes"]["rules"] > 0
              and result.get("map_initialized", True)
              and all(result.get("mobile_navigation", {}).values())
              and not any(metrics[k] for k in ("blocked_critical", "critical_http_errors",
                                               "critical_request_failures", "page_errors")))
        result["status"] = "PASS" if ok else "REVIEW"
    except Exception as exc:
        # Never retain exception text: it can contain request URLs or DOM values.
        result["reason"] = "Browser or scan incomplete: " + type(exc).__name__
    finally:
        result["network"] = metrics
        context.close()
    return result


def markdown(report):
    lines = ["# R7-3 — Phase 5 technical usability", "",
             "- UTC: " + report["generated_at_utc"],
             "- Reporting commit: " + report["report_commit"],
             "- Browser: " + report["browser_version"],
             "- Axe-core: " + report["axe_version"],
             "", "| Page | Viewport | Locale | Status | Axe violations (rules/nodes) | Incomplete rules | Overflow px |",
             "|---|---|---|---|---:|---:|---:|"]
    for c in report["cases"]:
        cat = c.get("axe", {}).get("categories", {})
        v = cat.get("violations", {})
        i = cat.get("incomplete", {})
        lines.append(f"| {c['page']} | {c['viewport'][0]}x{c['viewport'][1]} | "
                     f"{c['requested_locale']} | {c['status']} | "
                     f"{v.get('rules', '-')}/{v.get('nodes', '-')} | {i.get('rules', '-')} | "
                     f"{c.get('dom', {}).get('horizontal_overflow_px', '-')} |")
    lines += ["", "## Per-case details (sanitized)", ""]
    for c in report["cases"]:
        lines += [f"### {c['page']} {c['viewport'][0]}x{c['viewport'][1]} {c['requested_locale']}",
                  "", "    " + json.dumps(c, sort_keys=True, ensure_ascii=True), ""]
    lines += ["## Limitations", "", *["- " + s for s in report["limitations"]], ""]
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ack", required=True, choices=[ACK])
    p.add_argument("--axe-js", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--commit", default="not recorded")
    args = p.parse_args()
    if not args.axe_js.is_file() or args.axe_js.stat().st_size > 4 * 1024 * 1024:
        p.error("Supply local trusted axe-core JavaScript, at most 4 MiB")
    # Import only in opt-in CLI. CI tests use synthetic pages, not production.
    from playwright.sync_api import sync_playwright
    with sync_playwright() as engine:
        browser = engine.chromium.launch(headless=True)
        try:
            version = browser.version
            cases = []
            for case in CASES:
                cases.append(inspect_case(browser, args.axe_js.resolve(), case))
                print(f"{case[0]} {case[1]}px {case[3]}: {cases[-1]['status']}", flush=True)
                time.sleep(1)
        finally:
            browser.close()
    report = {"kpi": "R7-3", "phase": 5, "generated_at_utc": datetime.now(timezone.utc).isoformat(),
              "report_commit": args.commit, "browser_version": version, "axe_version": AXE_VERSION,
              "axe_sha256": hashlib.sha256(args.axe_js.read_bytes()).hexdigest(), "cases": cases,
              "limitations": [
                  "Anonymous public pages only; no login, logout, survey submission, export or private data access.",
                  "Selected WCAG 2.1 A/AA automated rules, not a conformity assessment or usability survey.",
                  "Incomplete rules require human judgment; node counts may recur across views.",
                  "One headless Chromium browser, two viewport sizes, two locales; no assistive-technology assessment.",
                  "Keyboard checks cover the skip link, not all focus order, visual focus or keyboard traps.",
                  "Mobile navigation is opened after the initial axe scan; its expanded state is not scanned.",
                  "Requests are filtered and capped per case; ordinary page GETs may create anonymous sessions or analytics. Browser redirect handling is not a network sandbox.",
                  "No screenshots, cookie values, DOM fragments, row identifiers or request URLs retained.",
                  "REVIEW and NOT_ASSESSED are not passes; exit zero means report created."
              ]}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    args.out.with_suffix(".md").write_text(markdown(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
