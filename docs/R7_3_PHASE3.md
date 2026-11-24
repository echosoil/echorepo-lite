# R7-3 Phase 3 — security verification

**Scope:** R73-S01–S05, with supporting evidence for R73-R03 and R73-A01. This is a bounded security assessment, not a penetration test, security certification or GDPR opinion. Reuse the R7-1 auth, session, privacy and research-edge regression results, and R7-3 Phase 2 evidence. No production changes or active attack simulations are required.

## 1. Prepare a private evidence directory

Use the existing development checkout and Python environment. Review and merge this PR (and the Phase 2 results PR separately) first.

    cd /home/echo/ECHO-STORE/echorepo-lite-dev
    git switch develop
    git pull --ff-only
    umask 077
    OUT="$PWD/r73-evidence/phase3-$(date -u +%Y%m%dT%H%M%SZ)"
    mkdir -p "$OUT/private"
    COMMIT="$(git rev-parse HEAD)"
    python -m pytest -q tests/test_r73_phase3.py

Do not put credentials, browser storage state, raw audit source excerpts or private datasets in a public archive. The outputs are intentionally limited, but review them before sharing.

## 2. Inspect deployed transport and access controls

Use a project-approved Keycloak issuer, ideally the same realm verified in R7-1. Check the configured issuer with the identity-service owner. Do not substitute a guessed hostname; omit --issuer if uncertain. The following example uses the existing shell variable from R7-1 if it is still correctly configured.

    ISSUER_ARGS=()
    if [ -n "${R71_KEYCLOAK_ISSUER:-}" ]; then
      ISSUER_ARGS=(--issuer "$R71_KEYCLOAK_ISSUER")
    fi

    python -m tools.r73_security --commit "$COMMIT" \
      --out "$OUT/security-live.json" live --ack yes-production-readonly \
      "${ISSUER_ARGS[@]}"

For an authenticated bounded query projection, rerun in a **new** output file with --use-api-key and R73_API_KEY read using read -rsp (as in Phase 2). Never put a key in a URL, command argument, screenshot or report. Unset it after the run. If the session cookie has a custom name, supply --session-cookie NAME. The probe uses only the official production origin, and the explicitly supplied public HTTPS Keycloak discovery/JWKS origin. No redirect following, no submitted credentials, no logout, no broad export, no credential guessing. The login GET with next=/my may create an anonymous session in PostgreSQL and other GETs may generate analytics entries. Missing header or inaccessible internal routes are review observations, not proof of exploitability.

The HTTPS request verifies the certificate through the client's trust store; it is not a full TLS protocol/cipher/expiry audit. Header checks are presence/basic attribute checks, not proof that a CSP is effective. An anonymous session cookie is not evidence of logged-in session content or revocation. OIDC discovery verifies public metadata and keys, not a completed user login. R7-1 I02 and B11 already cover anonymous ZIP denial; do not regenerate large bundles to retest this.

## 3. Redacted production container environment screening

Confirm the correct running container first:

    docker ps --format '{{.Names}}\t{{.Image}}'

The production Compose file declares echorepo-lite. If yours differs, replace that name only after checking. Execute the standard-library-only script from stdin inside the container, without copying any code or installing anything in it. This checks the new process environment; it does not import or start Flask or access the database.

    docker exec -i echorepo-lite python - --commit "$COMMIT" \
      config --ack yes-container-env-review \
      < tools/r73_security.py > "$OUT/security-config.json"

    python -m tools.r73_security render --input "$OUT/security-config.json" \
      > "$OUT/security-config.md"

The config report contains only classifications and booleans, never secrets, full environment variables, allowlist members or exact secret lengths. Length and example-value checks are conservative project screening rules; a passing result does not prove entropy or rotation. A missing API key may be intentional if bearer authentication is used. Effective Flask session backend should be cross-checked against live cookie attributes and deployment records; no token or session-table content is required.

## 4. Check the exact existing canonical export

Reuse the R7-1 / R7-3 Phase 2 ZIP.

    python -m tools.r73_security --commit "$COMMIT" \
      --out "$OUT/security-privacy.json" privacy \
      --bundle r71-evidence/canonical_all.zip

This counts blank, email-like, UUID/hex-like and other nonblank collector identifiers in samples.csv and sample_images.csv. The counts are row counts (not people); hashed or opaque strings can still be personal data. Check what collector values mean and whether publication is authorized. If email-like values occur in a publicly published version, promptly review that specific record and restrict access where appropriate with the project data protection lead; do not publish the raw sample bundle as security evidence. A local ZIP is not proof that the same contents were published on Zenodo. Separately review free text, image metadata/content and coordinate policy with the relevant owners.

## 5. Python dependency and source audits

Capture the production application's actual installed Python package inventory, not just requirements.txt. No package installation or vulnerability queries occur in production itself.

    docker exec echorepo-lite python -m pip list --format=freeze \
      > "$OUT/private/installed-python.txt"

Inspect that this file is a nonempty list of package==version entries. If the command fails, stop and investigate rather than treating an empty inventory as success. Do not include the raw inventory in public evidence without review.

Install scanners in an isolated local audit venv, not the application container or application venv. Use a workstation if server disk space is constrained.

    python3 -m venv "$OUT/private/audit-venv"
    AUD="$OUT/private/audit-venv/bin/python"
    "$AUD" -m pip install pip-audit bandit
    "$AUD" -m pip_audit --version > "$OUT/private/pip-audit-version.txt"
    "$AUD" -m bandit --version > "$OUT/private/bandit-version.txt"

pip-audit queries an external vulnerability database with package names and versions. Obtain authorization for that disclosure or run the audit through the approved security process. No application credentials are needed. Use the actual pinned inventory with dependency resolution disabled; no automatic fixes. Bandit examines only the local Python application tree (not production files, keys or the full infrastructure).

    PIP_RC=0
    "$AUD" -m pip_audit -r "$OUT/private/installed-python.txt" \
      --no-deps --disable-pip -f json -o "$OUT/private/pip-audit.json" \
      > "$OUT/private/pip-audit.log" 2>&1 || PIP_RC=$?

    BANDIT_RC=0
    "$AUD" -m bandit -r echorepo -f json -o "$OUT/private/bandit.json" \
      > "$OUT/private/bandit.log" 2>&1 || BANDIT_RC=$?

    python -m tools.r73_security --commit "$COMMIT" \
      --out "$OUT/security-audits.json" audits \
      --pip-json "$OUT/private/pip-audit.json" --pip-exit "$PIP_RC" \
      --bandit-json "$OUT/private/bandit.json" --bandit-exit "$BANDIT_RC"

Exit 1 can mean findings were reported, not a failed tool. Missing output, nonstandard exit statuses or incomplete coverage are not a successful audit. Retain raw JSON privately for triage. The sanitized summary includes vulnerability and severity counts, not package names, source excerpts, paths or raw error text. Review actual advisory applicability and fix versions privately before updating dependencies. Bandit findings may be false positives; don't globally suppress rules or force-upgrade production to improve counts. Keycloak, PostgreSQL, MinIO, Jupyter, OS and JavaScript dependency audits are outside this Python-only run.

References: https://github.com/pypa/pip-audit ; https://bandit.readthedocs.io/en/latest/man/bandit.html .

## 6. Manual checks and source review

Use the companion checklist R7_3_SECURITY_REVIEW.md. Use an authorized dedicated test account and normal browser interactions for login/logout, session cookies and resource access. For role isolation use only agreed test fixtures and staging; never inspect another real user's records. Record date, environment, tested revision, observation, outcome, issue disposition and evidence reference, not usernames, cookie values or request headers. Review the configured source findings before proposing remediation; do not demonstrate exploitation in production.

## 7. Retain evidence and report

    ls -lh "$OUT"/*.md "$OUT"/*.json

Review the four redacted Markdown reports: security-live.md, security-config.md, security-privacy.md, security-audits.md (and JSON counterparts). Record the actual deployed image/revision if known; --commit identifies the reporting checkout. Do not include private/ in a public archive. Send only the redacted reports and the completed manual observations for interpretation, then update R7_3_QUALITY_MATRIX.json. A security finding should be triaged and tracked, not concealed or converted into an artificial pass rate.
