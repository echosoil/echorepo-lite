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


RFC1918_NETWORKS = tuple(
    ipaddress.ip_network(value)
    for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)


def validate_service_url(
    mode: str, url: str, *, environ: dict | None = None
) -> str:
    """Validate optional unauthenticated research UI URLs.

    Production primary browser origins still require HTTPS unless loopback.
    Explicitly allowed RFC1918 HTTP services may be checked in an isolated
    anonymous browser context, *only* with an additional opt-in flag.
    This exception is never used for R71_PHASE3_BASE_URL or storage state.
    """
    env = os.environ if environ is None else environ
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower().rstrip(".")
    if (
        parsed.scheme not in {"http", "https"}
        or not host
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in {
            "", "/", "/jupyter/", "/research/workspace/",
            "/research/dashboard/",
        }
    ):
        raise BrowserTargetError(
            "Optional service URL must be an HTTP(S) root or /jupyter/ without credentials or query"
        )

    origin = f"{parsed.scheme}://{parsed.netloc}"
    try:
        validate_target(mode, origin, environ=env)
    except BrowserTargetError:
        allowed = {
            item.strip().lower().rstrip(".")
            for item in env.get("R71_PHASE3_ALLOWED_HOSTS", "").split(",")
            if item.strip()
        }
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            raise BrowserTargetError("Optional HTTP service must use approved HTTPS or RFC1918 IP") from None

        is_private_service = any(address in network for network in RFC1918_NETWORKS)
        if not (
            mode == "production-readonly"
            and env.get("R71_PHASE3_PROD_ACK") == PROD_ACK
            and env.get("R71_PHASE3_PRIVATE_SERVICE_ACK") == "yes-private-readonly-services"
            and parsed.scheme == "http"
            and host in allowed
            and is_private_service
        ):
            raise BrowserTargetError(
                "Private HTTP service requires explicit host allowlist and "
                "R71_PHASE3_PRIVATE_SERVICE_ACK=yes-private-readonly-services"
            ) from None

    # Preserve service base paths exactly: the trailing slash matters for
    # Jupyter ServerApp.base_url and Streamlit server.baseUrlPath.
    return url


def storage_state_for_target(path: Path, base_url: str) -> None:
    """Fail clearly if a browser state cannot authenticate to the target host.

    Validate the cookie domains without displaying or logging cookie contents.
    Origin-scoped HTTPS Secure cookies cannot authenticate localhost over HTTP.
    """
    import json

    try:
        contents = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise BrowserTargetError("Browser state JSON is unreadable or malformed") from exc

    if not isinstance(contents, dict):
        raise BrowserTargetError("Browser state JSON must be a Playwright storage state object")

    target = urlsplit(base_url)
    hostname = (target.hostname or "").lower()
    cookies = contents.get("cookies")
    if not isinstance(cookies, list):
        raise BrowserTargetError("Storage state must contain a cookies list")

    matched = False
    for cookie in cookies:
        if not isinstance(cookie, dict):
            continue
        domain = str(cookie.get("domain", "")).lower().lstrip(".")
        if domain != hostname and not (hostname.endswith("." + domain) and domain):
            continue
        if target.scheme == "http" and cookie.get("secure", False):
            continue
        matched = True
        break
    if not matched:
        raise BrowserTargetError(
            "No suitable cookies for this browser origin. "
            "Capture the session on the same HTTPS hostname you intend to test, "
            "then set R71_PHASE3_BASE_URL to that origin."
        )
