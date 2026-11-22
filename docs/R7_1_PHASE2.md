# R7-1 Phase 2 — controlled staging integration validation

**Branch policy:** base changes on `develop`. Do not deploy integration scripts to production.

Phase 1 covered isolated regression and acceptance checks. Phase 2 tests actual staging endpoints and a canonical data bundle without modifying application tables, storage objects or Zenodo depositions. **Read-only API GET requests may still create usage-analytics events.**

## 1. What is tested

The machine-readable list [R7_1_PHASE2_MATRIX.json](R7_1_PHASE2_MATRIX.json) defines ten requirements-linked checks:

| ID | Component | Actual evidence |
|---|---|---|
| I01 | ECHOREPO API | Live `/api/v1/ping` returns documented JSON |
| I02 | API authentication | Anonymous canonical ZIP request returns HTTP 401 |
| I03 | Canonical samples API | Bounded authenticated sample data, pagination, privacy projection |
| I04 | Canonical CSV/count API | CSV header/row count and sample count respond successfully |
| I05 | PostgreSQL | Read-only transaction; expected tables exist; sample count is queryable |
| I06 | API + PostgreSQL | API count equals count in the selected staging database |
| I07 | Keycloak | Issuer discovery and JWKS are publicly reachable and consistent |
| I08 | MinIO | Configured bucket and `canonical/latest/all.zip` metadata exist |
| I09 | Canonical ZIP | Actual local archive passes the existing schema and referential validator |
| I10 | Zenodo preparation | Current publisher generates all six CSVs and `file.json` from a local ZIP with `--dry-run` |

**None of these checks writes data to PostgreSQL, MinIO, or Zenodo.** The database connection is opened read-only, MinIO uses HEAD/stat operations, and Zenodo preparation uses `--source-file`, `--dry-run` and a temporary output directory. The Keycloak check does **not** perform a real user sign-in; login/logout and browser acceptance remain manual evidence.

## 2. Mandatory target safeguards

The suite refuses to run without `R71_PHASE2_RUN=staging`. API, Keycloak, PostgreSQL and MinIO hosts must be either loopback or listed explicitly in `R71_STAGING_HOSTS`. It blocks the known production ECHOREPO hostname even when allowlisted. Remote HTTP (as opposed to HTTPS) additionally needs `R71_ALLOW_HTTP_STAGING=yes`; use this only on an isolated Docker network. The canonical ZIP must be a local file.

**The allowlist cannot prove that an arbitrary hostname or database really points to staging.** Before enabling tests, confirm the intended host and database on your server. This is especially important when production and development share Docker networks. Never set `R71_STAGING_HOSTS` to a production host.

Use a dedicated low-privilege **read-only** PostgreSQL account if possible. The test also sets its transaction read-only, but database-side permissions are an additional safeguard.

## 3. Running on the development server

Run from the project directory after Phase 2 is merged into `develop`:

```bash
cd /home/echo/ECHO-STORE/echorepo-lite-dev
git switch develop
git pull --ff-only

# Use the existing environment/venv; install any missing test dependencies.
python -m pip install -r requirements.txt pytest PyJWT

# Safe examples; replace ports/hosts with YOUR development instance.
export R71_PHASE2_RUN=staging
export R71_API_BASE_URL=http://127.0.0.1:18080
export R71_STAGING_HOSTS="postgres,echorepo-minio,keycloak-dev.quanta-labs.com"
export R71_ALLOW_HTTP_STAGING=yes
```

**API credentials:** supply a development API key or bearer token using `R71_API_KEY` or `R71_BEARER_TOKEN`. Do not put secrets on the command line, in source files or in this guide. For example:

```bash
read -rsp 'Development API key: ' R71_API_KEY; echo
export R71_API_KEY
```

**Database:** supply the staging database via `R71_PG_DSN` and set `R71_PG_EXPECT_DB` to the exact selected database name. The DB hostname must also be in `R71_STAGING_HOSTS` (unless loopback). This DSN must target the development database, not production. Use a password prompt or a private environment file; do not commit the DSN.

```bash
# Example only (set your real hostname/port/name and supply the password securely).
export R71_PG_EXPECT_DB=echorepo_dev
# export R71_PG_DSN='host=postgres port=5432 dbname=echorepo_dev user=read_only_user ...'
```

**Keycloak:** set `R71_KEYCLOAK_ISSUER` to the **staging realm issuer**, including `/realms/<realm>`. Its hostname must be allowlisted. This test only reads discovery and JWK public keys.

**MinIO:** set `R71_MINIO_ENDPOINT` (including `http://` or `https://`), `R71_MINIO_BUCKET`, `R71_MINIO_ACCESS_KEY`, and `R71_MINIO_SECRET_KEY` for the development storage endpoint. The integration test only checks the bucket and stats the latest canonical ZIP.

**Local canonical ZIP:** prepare a development canonical export using the authenticated **development** API, or use an existing local snapshot. Do not point at the production dataset merely to make the test pass.

```bash
mkdir -p r71-evidence
export R71_CANONICAL_BUNDLE="$PWD/r71-evidence/staging_canonical_all.zip"

# Optional: download from LOCAL development API (no redirect following).
curl --fail --silent --show-error --max-redirs 0 \
  -H "X-API-Key: $R71_API_KEY" \
  "$R71_API_BASE_URL/api/v1/canonical/all.zip" \
  --output "$R71_CANONICAL_BUNDLE"
```

If the dev API is reachable only through a Docker-network hostname, use that hostname from an environment with Docker network access. Do **not** substitute `https://echorepo.quanta-labs.com` (production).

## 4. Run tests and create evidence

```bash
python -m pytest -q tests/integration/test_r71_phase2_live.py \
  --junitxml=r71-evidence/phase2-results.xml

python tools/r71_phase2_report.py \
  --xml r71-evidence/phase2-results.xml \
  --matrix docs/R7_1_PHASE2_MATRIX.json \
  --json r71-evidence/phase2-report.json \
  --markdown r71-evidence/phase2-report.md \
  --commit "$(git rev-parse HEAD)" \
  --require-all
```

The second command returns nonzero if **any** of the ten tests failed, was skipped, was not executed or was duplicated. It still writes the report, so the result can be reviewed. Running without `--require-all` returns success after writing even for incomplete runs; use strict mode when collecting evidence for a KPI.

**Interpretation:** PASS = the actual staging check succeeded, SKIPPED = missing explicit configuration, FAIL = acceptance criterion failed, NOT_RUN = test absent from JUnit. Neither a skip nor a non-run check counts toward passing validation.

If a ZIP validator fails, investigate with the existing `tools/validate_echorepo_bundle.py` using a **private** local output file; that detailed output can contain real sample identifiers and must not be posted publicly.

## 5. Limitations and remaining WP5 evidence

- GitHub Actions executes only **offline safety checks**; it never connects automatically to the development deployment. Run Phase 2 manually on the authorised staging host.
- The test's Keycloak discovery check is not proof of complete OAuth login flows or role-based authorisation.
- MinIO object existence is not equivalent to testing upload rights, data integrity on restore, or resilience.
- The Zenodo check validates *local preparation*, not a real Zenodo draft, publication or SoilWise harvest.
- Portal browser acceptance, source-data provenance, GDPR review, actual database migrations and cross-service failure/recovery are not validated by these ten probes.
- The complete R7-1 KPI still requires the consortium's agreed requirements inventory and acceptance sign-off.
- Tests executed now cannot automatically demonstrate M40 (September 2026) results.

The generated `r71-evidence/` folder is excluded by `.gitignore` to avoid leaking environment-specific diagnostics. Store a controlled copy of the report and tested commit SHA in the WP5 evidence folder.
