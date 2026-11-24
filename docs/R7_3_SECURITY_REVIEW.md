# R7-3 Phase 3 — manual security review and follow-up register

Baseline source review: develop commit c188d629a20e0e7015633766349388229b4971ca. Status: proposed checks, not independently confirmed deployed behavior. R7-1 C01, C02, C06, C08, C11, C12, C13, C14, I02, I07, B10–B12 and B15 supply bounded existing regression/integration evidence (see DOI 10.5281/zenodo.24485700). R7-3 Phase 2 measured authenticated bounded API requests successfully. These do not replace the following review.

## Manual browser/deployment checklist

| ID | Verification | Method / safe evidence | Initial state |
|---|---|---|---|
| MAN01 | Keycloak login | Dedicated consenting user, official HTTPS origin; complete normal login and reach /my. Record success/date; no credentials or personal dashboard screenshot. | NOT_ASSESSED |
| MAN02 | Logged-in session cookie | Browser DevTools Application/Storage: record Secure, HttpOnly, SameSite and whether opaque cookie shape matches configured PostgreSQL backend. Never capture actual values. | NOT_ASSESSED |
| MAN03 | Session logout and revocation | In test browser log out normally; reopen /my, expect login. For stronger revocation/expiry use dedicated dev fixtures and existing automated session tests. Do not retain/replay production session secrets. | NOT_ASSESSED |
| MAN04 | Resource/role isolation | In approved development environment, two synthetic test accounts and owned fixtures; confirm permitted access and rejection of unauthorized resource access. | NOT_ASSESSED |
| MAN05 | Infrastructure confidentiality | Confirm backup/database access restrictions, secrets management, reverse proxy/firewall restrictions and private research upstream access with operator. No public port scans or secret dumps. | NOT_ASSESSED |
| MAN06 | Privacy policy and publication | Verify collector identifier meaning, obfuscation and QA exclusion rules, allowed public fields, free text and image privacy, retention/consent with project owners. | NOT_ASSESSED |
| MAN07 | Dependency failure | Review controlled dev tests of session DB and identity-provider failure; no live fault injection. | NOT_ASSESSED |

For each check, retain date, environment, scope, status (PASS / REVIEW / FAIL / NOT_ASSESSED), sanitized observation, and issue/reference. Use authorized accounts and only test data. A login page screenshot alone does not establish authentication success.

## Source observations for review (not confirmed exploitability)

| ID | Observation at reviewed commit | Recommended disposition |
|---|---|---|
| SRC01 | echorepo/routes/data_api.py require_api_auth supports API keys in query strings as well as headers. URLs may be retained in access logs or history. | Prefer header-only credentials; check compatibility before removing query-string support; review logs/rotation if keys were used in URLs. |
| SRC02 | echorepo/auth/routes.py provides direct password grant and browser-based OIDC login; login template includes direct credential submission. | Review migration toward browser-based authorization code flow and CSRF protections on state-changing forms. Assess client settings with Keycloak owner; do not assume MFA or CSRF coverage from presence of Keycloak. |
| SRC03 | echorepo/auth/tokens.py uses unverified access-token payload as fallback if userinfo is unavailable; refresh failure removes kc but not explicitly user, whereas API auth also accepts session.user. | High-priority code review of consistent fail-closed behavior and verified identity claims. Test failure behavior with synthetic sessions in dev; determine actual impact before classifying severity. |
| SRC04 | echorepo/server_sessions.py defaults to cookie backend unless configured to postgres. The legacy signed-cookie session can contain token data; server-side storage uses opaque IDs and stores token data in database JSONB. | Confirm production backend and legacy migration status; verify cookie flags, revocation, protected database/backup access, and retirement of migration when safe. |
| SRC05 | echorepo/config.py and Compose contain example secret/salt/password defaults. Production Compose maps web service port to 0.0.0.0:8001. | Review actual configuration via redacted screening and operator confirmation of firewall/reverse proxy restrictions. Do not assume the port is publicly reachable or the examples are deployed. |
| SRC06 | Canonical export builders include collected_by while the bounded canonical query explicitly suppresses that field. | Check local aggregate identifier types, exact public publication, consent and provenance. The local authenticated export and public dataset may differ; do not label identifiers anonymous based only on format. |
| SRC07 | /diag/oidc returns endpoint configuration and a boolean indicating secret presence; no secret value is returned in reviewed source. | Review necessity and restriction of diagnostics, avoid publishing unnecessary configuration details. |
| SRC08 | Bearer-token verification checks signature, expiry, issuer and configured audience/client restriction. Audience checking is conditional. | Verify actual configured intended audience/client and token validation behavior with approved dev fixtures. |

Sources: echorepo/auth/routes.py, echorepo/auth/tokens.py, echorepo/auth/decorators.py, echorepo/config.py, echorepo/server_sessions.py, echorepo/routes/data_api.py, echorepo/services/canonical_exports.py, echorepo/templates/login.html, docker-compose.prod.yml, docs/SERVER_SESSION_ROLLOUT.md, all at the pinned baseline above. Implementation and config review are not a penetration test. Record remediation separately with its own tests and release revision. Do not expose raw audit reports or production secrets in GitHub issues, Zenodo or WP reports.
