"""R7-1 Phase 3 browser target validation; no network access here.

Production runs require explicit, separately labeled READ-ONLY authorization.
This gate is deliberately independent of R71_PHASE2_RUN (staging only).
"""
from __future__ import annotations

import ipaddress
import os
from pathlib import Path
from urllib.parse import urlsplit


class BrowserTargetError(ValueError):
    """Browser acceptance target is not approved for this run."""


PRODUCTION_HOSTS = {"echorepo.quanta-labs.com"}
MODES = {"staging", "production-readonly"}
PROD_ACK = "yes-production-readonly"


def is_loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def validate_target(mode: str, base_url: str, *, environ: dict | None = None) -> str:
    env = os.environ if environ is None else environ
    if mode not in MODES:
        raise BrowserTargetError("Set R71_PHASE3_RUN=staging or production-readonly")

    parsed = urlsplit(base_url)
    host = (parsed.hostname or "").lower().rstrip(".")
    if (
        parsed.scheme not in {"http", "https"}
        or not host
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise BrowserTargetError("Use only an HTTP(S) origin, without credentials, query or path")

    allowed = {
        item.strip().lower().rstrip(".")
        for item in env.get("R71_PHASE3_ALLOWED_HOSTS", "").split(",")
        if item.strip()
    }
    if not is_loopback(host) and host not in allowed:
        raise BrowserTargetError("Remote browser target not in R71_PHASE3_ALLOWED_HOSTS")

    if mode == "staging":
        if host in PRODUCTION_HOSTS or (is_loopback(host) and parsed.port == 8001):
            raise BrowserTargetError(
                "Known production target cannot be labeled staging; use production-readonly"
            )
    else:
        if env.get("R71_PHASE3_PROD_ACK") != PROD_ACK:
            raise BrowserTargetError(
                "Production checks require R71_PHASE3_PROD_ACK=yes-production-readonly"
            )
        # No unrestricted remote HTTP in production mode. Loopback reverse proxies
        # can use HTTP without sending session credentials off the host.
        if not is_loopback(host) and parsed.scheme != "https":
            raise BrowserTargetError("Remote production browser checks require HTTPS")

    if parsed.scheme == "http" and not is_loopback(host):
        if env.get("R71_PHASE3_ALLOW_HTTP_STAGING") != "yes":
            raise BrowserTargetError("Remote HTTP is blocked unless explicitly approved for staging")

    return base_url.rstrip("/")


def browser_target() -> tuple[str, str]:
    env = os.environ
    mode = env.get("R71_PHASE3_RUN", "").strip()
    target = env.get("R71_PHASE3_BASE_URL", "").strip()
    return mode, validate_target(mode, target)


def authentication_state() -> Path | None:
    raw = os.getenv("R71_PHASE3_STORAGE_STATE", "").strip()
    if not raw:
        return None
    path = Path(raw).expanduser().resolve()
    if not path.is_file():
        raise BrowserTargetError("R71_PHASE3_STORAGE_STATE does not exist")
    if path.suffix.lower() != ".json":
        raise BrowserTargetError("R71_PHASE3_STORAGE_STATE must be an existing JSON file")
    return path
