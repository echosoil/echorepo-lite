"""Fail-closed configuration gate for R7-1 Phase 2 staging tests.

No network operations occur in this module. Never log environment secrets.
"""
from __future__ import annotations

import ipaddress
import os
from urllib.parse import urlsplit

import psycopg2.extensions


class UnsafeTarget(ValueError):
    """The selected integration target is not explicitly approved as staging."""


BLOCKED_HOSTS = frozenset({
    "echorepo.quanta-labs.com",
    "zenodo.org",
    "www.zenodo.org",
    "sandbox.zenodo.org",
})


def require_opt_in() -> None:
    if os.getenv("R71_PHASE2_RUN", "").strip() != "staging":
        raise UnsafeTarget("Set R71_PHASE2_RUN=staging to enable Phase 2 integration checks")


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def approved_host(host: str, *, require_https: bool = False, scheme: str = "https") -> str:
    require_opt_in()
    host = host.lower().rstrip(".")
    if not host or host in BLOCKED_HOSTS:
        raise UnsafeTarget("Production or unknown integration host is forbidden")
    hosts = {
        item.strip().lower().rstrip(".")
        for item in os.getenv("R71_STAGING_HOSTS", "").split(",")
        if item.strip()
    }
    if not (_is_loopback(host) or host in hosts):
        raise UnsafeTarget(
            "Integration host is not in R71_STAGING_HOSTS; use an explicit staging allowlist"
        )
    if require_https and scheme != "https" and not _is_loopback(host):
        raise UnsafeTarget("HTTPS is required for remote identity endpoints")
    if scheme == "http" and not _is_loopback(host):
        if os.getenv("R71_ALLOW_HTTP_STAGING") != "yes":
            raise UnsafeTarget("Remote HTTP requires R71_ALLOW_HTTP_STAGING=yes")
    return host


def approved_url(value: str, *, require_https: bool = False) -> str:
    require_opt_in()
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"https", "http"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise UnsafeTarget("Integration URL must have an HTTP(S) scheme and no credentials/query")
    approved_host(parsed.hostname, scheme=parsed.scheme, require_https=require_https)
    return value.rstrip("/")


def approved_pg_dsn(value: str) -> str:
    require_opt_in()
    try:
        params = psycopg2.extensions.parse_dsn(value)
    except Exception as exc:
        raise UnsafeTarget("R71_PG_DSN is not a valid libpq connection string") from exc

    if params.get("service") or not params.get("host"):
        raise UnsafeTarget("PostgreSQL DSN must explicitly name the staging host (no service/Unix socket)")
    host = str(params["host"])
    if "," in host or host.startswith("/"):
        raise UnsafeTarget("PostgreSQL multi-host and Unix socket targets are not permitted")
    approved_host(host)

    hostaddr = params.get("hostaddr")
    if hostaddr:
        approved_host(str(hostaddr))
    if not params.get("dbname"):
        raise UnsafeTarget("PostgreSQL DSN must name a specific staging database")
    return str(params["dbname"])


def endpoint_url(base: str, suffix: str) -> str:
    """Build a fixed API route from a guarded root or /api/v1 URL."""
    base = approved_url(base)
    if base.endswith("/api/v1"):
        base = base[:-7]
    if not suffix.startswith("/api/v1/"):
        raise ValueError("Only fixed /api/v1/ endpoints are allowed")
    return base + suffix
