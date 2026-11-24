"""Baseline browser response protections and production debug policy."""

from __future__ import annotations

import os

from flask import Flask


def is_production() -> bool:
    return os.getenv("APP_ENV", "prod").strip().lower() not in {"dev", "development", "test", "testing", "local"}


def debug_routes_enabled(app: Flask) -> bool:
    """Never register development diagnostics in production-like environments."""
    return not is_production() and (app.debug or os.getenv("ENABLE_DEBUG_ROUTES", "0") == "1")


def configure_http_security(app: Flask) -> None:
    production = is_production()
    if production:
        app.debug = False

    max_age = int(os.getenv("ECHOREPO_HSTS_MAX_AGE", "0"))
    if max_age < 0:
        raise ValueError("ECHOREPO_HSTS_MAX_AGE must be nonnegative")

    @app.after_request
    def security_headers(response):
        headers = response.headers
        headers.setdefault("X-Content-Type-Options", "nosniff")
        headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        headers.setdefault("X-Frame-Options", "SAMEORIGIN")
        headers.setdefault("Permissions-Policy", "payment=(), usb=()")
        # Limited policy intentionally avoids blocking the existing scripts,
        # styles, tiles and same-origin research frames. Further CSP restrictions
        # require a reviewed source inventory and browser acceptance testing.
        headers.setdefault(
            "Content-Security-Policy",
            "object-src 'none'; base-uri 'self'; frame-ancestors 'self'",
        )
        # TLS terminates at the production edge. Browsers ignore HSTS received
        # over HTTP; no untrusted forwarded-proto or host header is consulted.
        if production and max_age:
            headers.setdefault("Strict-Transport-Security", f"max-age={max_age}")
        return response
