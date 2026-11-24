#!/usr/bin/env python3
"""R7-3 bounded security observations; no credentials or raw payloads in reports."""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlsplit

ORIGIN = "https://echorepo.quanta-labs.com"
MIB = 1024 * 1024
EMAIL = re.compile(r"[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+")
SID = re.compile(r"[A-Za-z0-9_-]{43}")
ACK = "yes-production-readonly"


def utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def entry(id_, title, status, evidence):
    return {"id": id_, "title": title, "status": status, "evidence": evidence}


def check(id_, title, ok, evidence, negative="REVIEW"):
    return entry(id_, title, "PASS" if ok else negative, evidence)


def report(section, checks, **extra):
    return {"kpi": "R7-3", "phase": 3, "section": section, "generated_at_utc": utc(),
            "checks": checks, **extra,
            "limitations": [
                "Individual observations are not a penetration test, ISO conformity statement or GDPR assessment.",
                "REVIEW indicates a gap or observation needing triage; NOT_ASSESSED is not a pass.",
                "No credential values, cookie contents, raw HTTP bodies or individual dataset values are retained.",
            ]}


def enabled(value):
    return str(value).strip().lower() in {"true", "1", "yes", "on"}


def config_observations(env):
    """Only classify container environment. Never instantiate the Flask application."""
    checks = []
    storage = env.get("ECHOREPO_SESSION_STORAGE", "cookie").strip().lower()
    storage = storage if storage in {"cookie", "postgres"} else "unrecognized"
    checks.append(check("CFG01", "Configured server-side session storage", storage == "postgres",
                        {"backend": storage, "legacy_migration_enabled": enabled(env.get("ECHOREPO_MIGRATE_LEGACY_SESSIONS", ""))}))
    secure = env.get("SESSION_COOKIE_SECURE", "true").lower() in {"1", "true", "yes"}
    same = env.get("SESSION_COOKIE_SAMESITE", "Lax").lower()
    same = same if same in {"lax", "strict", "none"} else "unrecognized"
    checks.append(check("CFG02", "Configured cookie security", secure and same in {"lax", "strict"},
                        {"secure": secure, "same_site": same, "http_only_runtime": "verify in live cookie check"}))
    for id_, title, key, defaults, minimum in [
        ("CFG03", "Application signing secret screening", "SECRET_KEY", {"please-change-me", "replace-with-long-random-string"}, 24),
        ("CFG04", "Coordinate jitter salt screening", "JITTER_SALT", {"change-this-salt"}, 16),
        ("CFG05", "Database password screening", "DB_PASSWORD", {"echorepo-pass", "postgres", "password", "changeme"}, 12),
        ("CFG06", "Configured API key screening", "API_KEY", {"changeme", "your-api-key"}, 24),
        ("CFG07", "Keycloak client secret screening", "KEYCLOAK_CLIENT_SECRET", {"changeme", "your-client-secret"}, 16),
    ]:
        value = env.get(key, "")
        if key == "DB_PASSWORD" and not value:
            value = env.get("POSTGRES_PASSWORD", "")
        present = bool(value.strip())
        default = value.strip().lower() in defaults
        long_enough = len(value) >= minimum
        checks.append(check(id_, title, present and not default and long_enough,
                            {"present": present, "known_example_value": default, "minimum_length_met": long_enough,
                             "note": "Basic screening only; entropy, rotation and secret storage not assessed."}))
    demo = env.get("DEMO_MODE", "false").lower() in {"1", "true", "yes"}
    debug = env.get("ENABLE_DEBUG_ROUTES", "0") == "1" or enabled(env.get("FLASK_DEBUG", "0"))
    checks.append(check("CFG08", "Demo and debug settings", not demo and not debug,
                        {"demo_enabled": demo, "debug_configured": debug}))
    checks.append(entry("CFG09", "Canonical access policy", "OBSERVED", {
        "canonical_public": enabled(env.get("CANONICAL_PUBLIC", "false")),
        "canonical_map_public": enabled(env.get("CANONICAL_MAP_PUBLIC", "false")),
        "note": "Public data policy and actual projection must be reviewed; public mode is not inherently a defect.",
    }))
    allowed = {x.strip().lower() for x in env.get("JUPYTER_ALLOWED_EMAILS", "").split(",") if x.strip()}
    checks.append(entry("CFG10", "Research authorization policy", "OBSERVED", {
        "policy": "all_authenticated" if allowed == {"all"} else "allowlist" if allowed else "deny_by_default",
    }))
    bearer_issuer = bool(env.get("OIDC_ISSUER_URL", "").strip())
    checks.append(check("CFG11", "Configured bearer-token audience/client restriction",
                        bearer_issuer and bool(env.get("OIDC_AUDIENCE", "").strip() or env.get("OIDC_CLIENT_ID", "").strip()),
                        {"issuer_env_present": bearer_issuer,
                         "audience_env_present": bool(env.get("OIDC_AUDIENCE", "").strip()),
                         "client_id_env_present": bool(env.get("OIDC_CLIENT_ID", "").strip()),
                         "note": "Environment only; inspect effective bearer configuration if enabled."}))
    return report("container configuration", checks, environment="operator-designated production container",
                  scope="Process environment of a new command in the running container; not effective Flask configuration or secret-strength audit.")


def safe_https(url):
    p = urlsplit(url)
    return (p.scheme == "https" and bool(p.hostname) and not p.username and not p.password
            and not p.query and not p.fragment and p.port in (None, 443))


def issuer_url(url):
    if not safe_https(url):
        raise ValueError("Issuer must be an explicitly approved HTTPS realm URL")
    if not re.fullmatch(r"/(?:auth/)?realms/[A-Za-z0-9_.-]+/?", urlsplit(url).path):
        raise ValueError("Expected a Keycloak realm URL")
    return url.rstrip("/")


def origin_of(url):
    p = urlsplit(url)
    return p.scheme, p.hostname, p.port or (443 if p.scheme == "https" else 80)


def local_login(location):
    target = urljoin(ORIGIN, location or "")
    p = urlsplit(target)
    return (origin_of(target) == origin_of(ORIGIN) and p.path == "/login"
            and parse_qs(p.query).get("next") == ["/my"])


def cookie_observations(cookies, names):
    """Use parsed Cookie objects; report only attributes and shape, never cookie text."""
    selected = [c for c in cookies if c.name in names]
    values = []
    for c in selected:
        rest = {str(k).lower(): v for k, v in c._rest.items()}
        same = str(rest.get("samesite", "")).lower()
        values.append({
            "secure": bool(c.secure),
            "http_only": "httponly" in rest,
            "same_site": same if same in {"lax", "strict", "none"} else "absent_or_other",
            "opaque_sid_shape": bool(SID.fullmatch(c.value or "")),
        })
    return values


def live_observations(issuer=None, api_key="", cookie_name=None):
    import requests

    checks = []
    if issuer:
        issuer = issuer_url(issuer)
    names = {"session", "echorepo_session", "echorepo_sid"}
    if cookie_name:
        names.add(cookie_name)

    def get(url, params=None, key="", body=False):
        # Each request is a fresh anonymous session. No redirect, netrc or proxy
        # credential inheritance. Only the caller-approved API origin gets a key.
        headers = {"X-API-Key": key} if key else {}
        if key and origin_of(url) != origin_of(ORIGIN):
            raise ValueError("Credentials may only be sent to ECHOREPO")
        with requests.Session() as session:
            session.trust_env = False
            session.verify = True
            with session.get(url, params=params, headers=headers, timeout=(5, 10),
                             allow_redirects=False, stream=True) as response:
                payload = bytearray()
                if body and response.status_code == 200:
                    for chunk in response.iter_content(65536):
                        payload.extend(chunk)
                        if len(payload) > 2 * MIB:
                            raise ValueError("Response exceeds bounded size")
                return response.status_code, response.headers, list(response.cookies), bytes(payload)

    def run(id_, title, fn):
        try:
            checks.append(fn())
        except (requests.RequestException, ValueError, TypeError, KeyError, AttributeError):
            checks.append(entry(id_, title, "NOT_ASSESSED", {"reason": "request or response validation failed; inspect privately"}))

    def https():
        status, headers, _, _ = get(ORIGIN + "/explore")
        hsts = headers.get("Strict-Transport-Security", "")
        match = re.search(r"(?:^|;)\s*max-age\s*=\s*(\d+)", hsts, re.I)
        return check("WEB01", "HTTPS certificate verification and public response", status == 200,
                     {"tls_verification_enabled": True, "http_status": status, "hsts_positive_max_age": bool(match and int(match[1]) > 0)})
    run("WEB01", "HTTPS certificate verification", https)

    def redirect():
        status, headers, _, _ = get("http://echorepo.quanta-labs.com/explore")
        target = urljoin("http://echorepo.quanta-labs.com/explore", headers.get("Location", ""))
        ok = status in {301, 302, 307, 308} and origin_of(target) == origin_of(ORIGIN)
        return check("WEB02", "HTTP upgrades to official HTTPS origin", ok, {"http_status": status, "same_https_origin": ok})
    run("WEB02", "HTTP redirect", redirect)

    def headers_check():
        status, h, _, _ = get(ORIGIN + "/explore")
        hsts = h.get("Strict-Transport-Security", "")
        match = re.search(r"(?:^|;)\s*max-age\s*=\s*(\d+)", hsts, re.I)
        csp = h.get("Content-Security-Policy", "")
        flags = {"http_status": status, "hsts_positive_max_age": bool(match and int(match[1]) > 0),
                 "csp_present": bool(csp), "csp_report_only_present": bool(h.get("Content-Security-Policy-Report-Only")),
                 "frame_policy_present": bool(h.get("X-Frame-Options")) or "frame-ancestors" in csp.lower(),
                 "nosniff": h.get("X-Content-Type-Options", "").lower() == "nosniff",
                 "referrer_policy_present": bool(h.get("Referrer-Policy")),
                 "permissions_policy_present": bool(h.get("Permissions-Policy"))}
        return check("WEB03", "Selected response-header protections", status == 200 and all(flags[x] for x in (
            "hsts_positive_max_age", "csp_present", "frame_policy_present", "nosniff", "referrer_policy_present")), flags)
    run("WEB03", "Response headers", headers_check)

    def private_page():
        status, h, _, _ = get(ORIGIN + "/my")
        ok = status in {302, 303, 307, 308} and local_login(h.get("Location"))
        return check("AUTH01", "Anonymous personal dashboard redirects to login", ok,
                     {"http_status": status, "expected_local_login_redirect": ok}, "FAIL" if status == 200 else "REVIEW")
    run("AUTH01", "Anonymous dashboard", private_page)

    def cookies():
        status, _, cs, _ = get(ORIGIN + "/login", params={"next": "/my"})
        attrs = cookie_observations(cs, names)
        if not attrs:
            return entry("SES01", "Anonymous session cookie attributes", "NOT_ASSESSED",
                         {"http_status": status, "recognized_session_cookies": 0})
        ok = status == 200 and all(x["secure"] and x["http_only"] and x["same_site"] in {"lax", "strict"} for x in attrs)
        return check("SES01", "Anonymous session cookie attributes", ok, {"http_status": status, "cookies": attrs,
                     "note": "Anonymous session only; opaque shape is not proof of server-side storage."})
    run("SES01", "Session cookie", cookies)

    def research():
        status, h, _, _ = get(ORIGIN + "/internal/jupyterhub-auth")
        identity = bool(h.get("X-ECHOREPO-User"))
        ok = status in {401, 403} and not identity
        return check("AUTH02", "Anonymous research auth check denies access", ok,
                     {"http_status": status, "identity_header_present": identity},
                     "FAIL" if status == 204 or identity else "REVIEW")
    run("AUTH02", "Research authorization", research)

    def samples():
        status, h, _, b = get(ORIGIN + "/api/v1/canonical/samples",
                              params={"format": "json", "limit": 1, "offset": 0}, key=api_key, body=True)
        if status in {401, 403} and not api_key:
            return entry("PRIV01", "Bounded canonical query projection", "NOT_ASSESSED",
                         {"anonymous_access_denied": True, "http_status": status, "note": "Use an authorized key to verify returned projection."})
        obj = json.loads(b) if status == 200 else {}
        meta, rows = obj.get("meta", {}), obj.get("data")
        fields = meta.get("fields")
        shaped = isinstance(fields, list) and isinstance(rows, list) and len(rows) <= 1 and all(isinstance(r, dict) for r in rows)
        exposed = shaped and ("collected_by" in fields or any("collected_by" in r for r in rows))
        ok = status == 200 and shaped and not exposed
        return check("PRIV01", "Bounded canonical query omits collected_by", ok,
                     {"http_status": status, "key_used": bool(api_key), "expected_shape": bool(shaped),
                      "collected_by_exposed": bool(exposed)}, "FAIL" if exposed else "REVIEW")
    run("PRIV01", "Canonical query projection", samples)

    if issuer:
        def discovery():
            status, _, _, b = get(issuer + "/.well-known/openid-configuration", body=True)
            obj = json.loads(b) if status == 200 else {}
            jwks = obj.get("jwks_uri", "")
            consistent = obj.get("issuer", "").rstrip("/") == issuer
            approved = isinstance(jwks, str) and safe_https(jwks) and origin_of(jwks) == origin_of(issuer)
            if not (status == 200 and consistent and approved):
                return check("OIDC01", "Keycloak discovery and public signing keys", False,
                             {"discovery_status": status, "issuer_matches": consistent, "jwks_origin_approved": bool(approved)})
            ks, _, _, kb = get(jwks, body=True)
            keys = json.loads(kb).get("keys", []) if ks == 200 else []
            usable = isinstance(keys, list) and any(isinstance(k, dict) and k.get("kid") and k.get("kty") in {"RSA", "EC"} for k in keys)
            return check("OIDC01", "Keycloak discovery and public signing keys", ks == 200 and usable,
                         {"discovery_status": status, "issuer_matches": True, "jwks_status": ks,
                          "public_key_metadata_present": bool(usable), "issuer_matches_operator_input": True,
                          "note": "No token issued or validated; login is a separate manual check."})
        run("OIDC01", "Keycloak discovery", discovery)
    else:
        checks.append(entry("OIDC01", "Keycloak discovery and public signing keys", "NOT_ASSESSED",
                            {"reason": "No approved issuer supplied"}))
    return report("production HTTPS and access observations", checks, environment="production-readonly",
                  scope="Official HTTPS origin; fresh sessions, no POST, password attempts, logout, bulk exports or redirect following. Login-page GET may create an anonymous session; GETs may generate analytics.")


def privacy_observations(bundle):
    if not bundle.is_file() or bundle.stat().st_size > 100 * MIB:
        raise ValueError("Invalid local canonical ZIP")
    digest = hashlib.sha256()
    with bundle.open("rb") as stream:
        for chunk in iter(lambda: stream.read(MIB), b""):
            digest.update(chunk)
    checks = []
    with zipfile.ZipFile(bundle) as archive:
        infos = archive.infolist()
        if len(infos) > 30 or sum(x.file_size for x in infos) > 250 * MIB:
            raise ValueError("ZIP resource limits exceeded")
        if len(set(archive.namelist())) != len(infos):
            raise ValueError("Duplicate archive names")
        for name in ("samples.csv", "sample_images.csv"):
            with archive.open(name) as raw, io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh, strict=True)
                if "collected_by" not in (reader.fieldnames or []):
                    checks.append(entry("ZIP01-" + name, "Collector identifier field", "REVIEW",
                                        {"column_present": False}))
                    continue
                counts = Counter({"rows": 0, "blank": 0, "email_like": 0,
                                  "uuid_or_hex_like": 0, "other_nonblank": 0})
                for row in reader:
                    counts["rows"] += 1
                    value = (row.get("collected_by") or "").strip()
                    if not value:
                        counts["blank"] += 1
                    elif EMAIL.search(value):
                        counts["email_like"] += 1
                    elif re.fullmatch(r"[a-fA-F0-9]{32,128}|[a-fA-F0-9]{8}(?:-[a-fA-F0-9]{4}){3}-[a-fA-F0-9]{12}", value):
                        counts["uuid_or_hex_like"] += 1
                    else:
                        counts["other_nonblank"] += 1
                checks.append(entry("ZIP01-" + name, "Collector identifier screening", "REVIEW", {
                    "resource": name, **dict(counts),
                    "note": "Pattern classification only. Non-email identifiers may still identify people; inspect provenance and publication authorization. No raw values or hashes of individual values retained.",
                }))
    return report("local canonical export privacy screening", checks, bundle_sha256=digest.hexdigest(),
                  scope="Only collected_by in samples and images. Not a full personal-data, free-text, photo or geographic privacy scan.")


def audit_observations(pip_path, bandit_path, pip_exit, bandit_exit):
    checks = []
    for kind, path, exit_code in (("pip-audit", pip_path, pip_exit), ("bandit", bandit_path, bandit_exit)):
        try:
            if path.stat().st_size > 20 * MIB or exit_code not in (0, 1):
                raise ValueError("Audit did not complete")
            obj = json.loads(path.read_text(encoding="utf-8"))
            if kind == "pip-audit":
                deps = obj.get("dependencies")
                if not isinstance(deps, list) or not deps:
                    raise ValueError("Missing dependency inventory")
                skipped = sum("skip_reason" in d for d in deps)
                scanned = [d for d in deps if "skip_reason" not in d]
                if not scanned or any(not isinstance(d.get("vulns"), list) for d in scanned):
                    raise ValueError("Missing vulnerability data")
                vulnerable = sum(bool(d["vulns"]) for d in scanned)
                findings = sum(len(d["vulns"]) for d in scanned)
                checks.append(check("AUD01", "Installed Python dependency audit",
                                    exit_code == 0 and findings == 0 and skipped == 0,
                                    {"inventory_entries": len(deps), "audited": len(scanned),
                                     "skipped": skipped, "affected_packages": vulnerable, "vulnerability_records": findings,
                                     "exit_code": exit_code, "note": "Known database findings only, no exploitability judgment."}))
            else:
                findings, errors = obj.get("results"), obj.get("errors")
                if not isinstance(findings, list) or not isinstance(errors, list):
                    raise ValueError("Missing Bandit output")
                metrics = obj.get("metrics", {}).get("_totals", {})
                if not isinstance(metrics, dict) or not metrics.get("loc"):
                    raise ValueError("No scanned source lines")
                severity = Counter(f.get("issue_severity", "UNRECOGNIZED") for f in findings)
                confidence = Counter(f.get("issue_confidence", "UNRECOGNIZED") for f in findings)
                # Permit only known categories; never retain issue text, source or paths.
                bins = ("LOW", "MEDIUM", "HIGH", "UNDEFINED")
                by_rule = Counter(f["test_id"] for f in findings
                                  if isinstance(f.get("test_id"), str) and re.fullmatch(r"B\d{3}", f["test_id"]))
                checks.append(check("AUD02", "Python static security analysis",
                                    exit_code == 0 and not findings and not errors,
                                    {"findings": len(findings), "scan_errors": len(errors), "source_lines": metrics["loc"],
                                     "severity": {k: severity[k] for k in bins}, "confidence": {k: confidence[k] for k in bins},
                                     "rule_counts": dict(sorted(by_rule.items())), "exit_code": exit_code,
                                     "note": "Static findings require manual triage; not confirmed vulnerabilities."}))
        except (OSError, ValueError, TypeError, AttributeError, KeyError):
            checks.append(entry("AUD01" if kind == "pip-audit" else "AUD02", kind + " result",
                                "NOT_ASSESSED", {"reason": "missing, incomplete or invalid audit output"}))
    return report("dependency and source audit summary", checks,
                  scope="Python packages of selected container, and Python source checkout. Other containers, OS packages, JS dependencies and runtime exploitability excluded.")


def markdown(result):
    lines = ["# R7-3 — " + result["section"], "", "- UTC: " + result["generated_at_utc"],
             "- Reporting commit: " + result.get("report_commit", "not recorded"),
             "- Scope: " + result["scope"], "", "| ID | Observation | Status | Evidence |", "|---|---|---|---|"]
    for c in result["checks"]:
        evidence = json.dumps(c["evidence"], ensure_ascii=True, sort_keys=True).replace("|", r"\|")
        lines.append(f"| {c['id']} | {c['title']} | {c['status']} | {evidence} |")
    if "bundle_sha256" in result:
        lines += ["", "Bundle SHA-256: " + result["bundle_sha256"]]
    lines += ["", "## Limitations", "", *["- " + x for x in result["limitations"]], ""]
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--commit", default="not recorded")
    p.add_argument("--out", type=Path, help="Write JSON and companion Markdown; omit for JSON stdout")
    sub = p.add_subparsers(dest="mode", required=True)
    live = sub.add_parser("live")
    live.add_argument("--ack", required=True, choices=[ACK])
    live.add_argument("--issuer")
    live.add_argument("--session-cookie")
    live.add_argument("--use-api-key", action="store_true")
    config = sub.add_parser("config")
    config.add_argument("--ack", required=True, choices=["yes-container-env-review"])
    privacy = sub.add_parser("privacy")
    privacy.add_argument("--bundle", type=Path, required=True)
    audit = sub.add_parser("audits")
    audit.add_argument("--pip-json", type=Path, required=True)
    audit.add_argument("--bandit-json", type=Path, required=True)
    audit.add_argument("--pip-exit", type=int, required=True)
    audit.add_argument("--bandit-exit", type=int, required=True)
    render = sub.add_parser("render")
    render.add_argument("--input", type=Path, required=True)
    args = p.parse_args()
    try:
        if args.mode == "live":
            key = os.environ.get("R73_API_KEY", "").strip() if args.use_api_key else ""
            if args.use_api_key and not key:
                raise ValueError("Missing API key")
            result = live_observations(args.issuer, key, args.session_cookie)
        elif args.mode == "config":
            result = config_observations(os.environ)
        elif args.mode == "privacy":
            result = privacy_observations(args.bundle)
        elif args.mode == "audits":
            result = audit_observations(args.pip_json, args.bandit_json, args.pip_exit, args.bandit_exit)
        else:
            # Render only the redacted JSON previously produced by this tool.
            result = json.loads(args.input.read_text(encoding="utf-8"))
            if result.get("kpi") != "R7-3" or result.get("phase") != 3:
                raise ValueError("Expected a Phase 3 report")
            print(markdown(result), end="")
            return 0
        result["report_commit"] = args.commit
        output = json.dumps(result, indent=2) + "\n"
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(output, encoding="utf-8")
            args.out.with_suffix(".md").write_text(markdown(result), encoding="utf-8")
            print("Security observations saved; review all statuses before interpreting.")
        else:
            print(output, end="")
        return int(any(c["status"] == "FAIL" for c in result["checks"]))
    except (OSError, ValueError, TypeError, KeyError, zipfile.BadZipFile, csv.Error, UnicodeError):
        print("Assessment refused or incomplete; check approved arguments and local input files.")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
