#!/usr/bin/env python3
"""Inspect image URLs used by the public LOESS Soil Map (no image downloads).

Run on a machine where Playwright can open Chrome/Chromium. Click a few
accepted survey markers that visibly have photographs, then press Enter in
this terminal. Outputs sanitized URLs and request metadata without cookies,
headers, tokens, or image bytes.
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

LOESS_MAP = "https://www.loess-soil-map.eu/map"
SENSITIVE = ("token", "secret", "signature", "key", "credential", "auth", "session", "policy", "expires", "access", "jwt")
ALLOWED_VALUE_PARAMS = {"filepath", "file_path", "path", "filename", "id", "surveyid", "survey_id"}


def scrub(url):
    """Keep URL structure but never include signed URLs or credentials in the report."""
    try:
        parts = urlsplit(url)
        host = parts.hostname or ""
        entries = []
        for key, val in parse_qsl(parts.query, keep_blank_values=True):
            if any(word in key.lower() for word in SENSITIVE):
                entries.append((key, "[redacted]"))
            elif key.lower() in ALLOWED_VALUE_PARAMS:
                entries.append((key, val[:200]))
            else:
                entries.append((key, "[value omitted]"))
        # Preserve query KEY names, to identify an API's request shape.
        return urlunsplit((parts.scheme, host + ((":" + str(parts.port)) if parts.port else ""), parts.path,
                           urlencode(entries), ""))
    except (ValueError, TypeError):
        return "[invalid URL]"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--browser", choices=("chrome", "msedge", "chromium"), default="chrome")
    parser.add_argument("--out", type=Path, default=Path("loess_photo_probe.json"))
    args = parser.parse_args()
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise SystemExit("Install Playwright: python -m pip install playwright") from exc

    calls = []
    with sync_playwright() as playwright:
        kwargs = {"headless": False}
        if args.browser in {"chrome", "msedge"}:
            kwargs["channel"] = args.browser
        browser = playwright.chromium.launch(**kwargs)
        page = browser.new_page(viewport={"width": 1440, "height": 900})

        def track(response):
            url = response.url
            host = (urlsplit(url).hostname or "").lower()
            content_type = response.headers.get("content-type", "").split(";")[0].lower()
            is_image = content_type.startswith("image/") or response.request.resource_type == "image"
            is_backend = host.endswith("loess.wmi.amu.edu.pl")
            # Do not log tile assets, fonts, JS or analytics from other sites.
            if not (is_image or is_backend):
                return
            if "tile.openstreetmap.org" in host or "/_next/static/" in url:
                return
            calls.append({"url": scrub(url), "status": response.status,
                          "content_type": content_type,
                          "resource_type": response.request.resource_type})

        page.on("response", track)
        page.goto(LOESS_MAP, wait_until="domcontentloaded", timeout=60000)
        print("LOESS map opened. Click several ACCEPTED observations with photos.")
        print("Make sure any popup photo is visible, then press Enter here.")
        input("Press Enter to finish: ")
        dom_images = page.locator("img").evaluate_all("""nodes => nodes.map(n => ({
          src: n.currentSrc || n.src, alt: n.alt || '', visible: !!(n.offsetWidth || n.offsetHeight)
        }))""")
        dom_images = [dict(src=scrub(item.get("src", "")), alt=item.get("alt", "")[:100],
                           visible=item.get("visible", False)) for item in dom_images]
        browser.close()

    unique = list({(e["url"], e["status"], e["content_type"]): e for e in calls}.values())
    report = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "notice": "Request URLs sanitized; no headers, cookies, credentials or image bytes collected.",
        "calls": unique,
        "dom_images": dom_images,
    }
    args.out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Saved {args.out}; network entries: {len(unique)}")
    for event in unique:
        if event["content_type"].startswith("image/") or "survey" in event["url"].lower() or "file" in event["url"].lower():
            print(event["status"], event["content_type"], event["url"])
    print("Next: identify a public image GET URL that works in a private browser window.")
    print("If it contains a survey's filePath (or id), convert that URL to a template.")


if __name__ == "__main__":
    main()
