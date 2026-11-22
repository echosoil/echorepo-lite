# R7-1 — ECHOREPO software component validation

**KPI:** R7-1 — *All software components validated against the requirements*.

**Requested reporting baseline:** M40, September 2026.

**Code baseline for this suite:** GitHub `echosoil/echorepo-lite`, branch `main`, commit `31bbac0b7bf3263c81a1f05a74d5b4702226df48` (20 November 2026). The suite was added later, on an isolated PR branch.

**Development policy:** New R7-1 testing changes now target `develop`; `main` is released separately. The Phase 1 baseline was historically prepared against `main` and later merged into `develop`.

**Phase 2:** For approved read-only staging integration tests, see [R7_1_PHASE2.md](R7_1_PHASE2.md) and [R7_1_PHASE2_MATRIX.json](R7_1_PHASE2_MATRIX.json).

**Phase 3:** Browser-level acceptance for the deployed portal is covered in [R7_1_PHASE3.md](R7_1_PHASE3.md) and [R7_1_PHASE3_MATRIX.json](R7_1_PHASE3_MATRIX.json). Unlike Phase 1 CI, it requires explicit target approval and Playwright Chromium, and separately labels production-readonly observations.

**Important reporting limitation:** A test executed after September 2026 validates the tested current code, not automatically the historical M40 release. To make an M40 claim, retrieve the actual September commit/release, its requirements, test evidence and deployment records.

## Scope and traceability

The machine-readable matrix is [R7_1_VALIDATION_MATRIX.json](R7_1_VALIDATION_MATRIX.json). Each component has a unique ID, a specific acceptance criterion, a list of executable test-case names and outstanding external evidence. Empty test arrays are **NOT COVERED**, never counted as passes.

Included core and optional modules:

- Web UI / auth redirects; REST endpoints and request validation.
- PostgreSQL migration ledger and application usage analytics.
- Canonical CSV/ZIP schemas; soil category harmonisation and QA.
- Privacy jitter / GeoJSON handling.
- CSVW document validation and Zenodo publishing metadata construction.
- MinIO storage operations and laboratory upload limits.
- Biodiversity upload and queueing (reusing existing regression tests).
- PostgreSQL-backed sessions and JupyterHub authentication (existing tests).
- LOESS map integration and i18n (existing tests).

The optional Streamlit data helpers and notebook API client have isolated coverage; the interactive Streamlit interface and full external-integration environment remain *not fully covered*. The standalone ECHOsoil mobile app, external Keycloak, Firebase, Zenodo, SoilWise and EOSC infrastructures are **not implemented by this repository** and need separate owners and evidence. Database migrations, real imports, PDF/UI workflows, deployed reverse proxy and browser experience also need staging/manual acceptance checks.

## How to run

In a fresh checkout of the PR branch using Python 3.11, install test dependencies in a disposable venv:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt pytest PyJWT
export PROJECT_ROOT=. CSV_PATH=data/samples.csv SQLITE_PATH=data/legacy.sqlite
mkdir -p r71-evidence

python -m pytest -q \
  tests/test_r71_api_web.py \
  tests/test_r71_data_contracts.py \
  tests/test_r71_storage_lab.py \
  tests/test_r71_research.py \
  tests/test_biodiversity_api_upload.py \
  tests/test_server_sessions.py \
  tests/test_jupyterhub_auth.py \
  tests/test_loess_converter.py \
  tests/test_loess_photo_urls.py \
  tests/test_loess_i18n.py \
  --junitxml=r71-evidence/results.xml

python tools/r71_validation_report.py \
  --xml r71-evidence/results.xml \
  --matrix docs/R7_1_VALIDATION_MATRIX.json \
  --output r71-evidence/report.json \
  --markdown r71-evidence/report.md \
  --commit "$(git rev-parse HEAD)"
```

**Do not run destructive integration tests against production.** The tests in this set use synthetic fixtures, simulated API clients and mocks instead of changing production PostgreSQL/MinIO/Zenodo resources.

The GitHub Actions workflow [r7-1-validation.yml](../.github/workflows/r7-1-validation.yml) runs the same checks on pull requests targeting develop, and exposes `results.xml`, `pytest.log`, `report.json` and `report.md` in the **r7-1-validation-evidence** workflow artifact. Save the artifact and the exact commit SHA in the KPI evidence folder. A red CI run is not a validated component; inspect the failure.

## Interpretation

- **AUTOMATED_PASS** means every mapped test case for that component ran and passed in the isolated test suite.
- **AUTOMATED_FAIL** means at least one mapped test failed, or duplicate test names made the result ambiguous.
- **INCOMPLETE / NOT_RUN / NOT_COVERED** must never be counted as validated.
- **R7-1 overall result stays NOT YET VERIFIED**, even if all isolated tests pass, until the full component inventory and required external acceptance checks are completed and reviewed.

The report's automated-component percentage is **coverage of the defined suite only**. It is not a certification, production uptime measure or GA completion percentage.

## Additional acceptance evidence to obtain

1. **Requirements baseline**: obtain the GA/D5.1 requirements and agree the component inventory with WP5/WP7; map each requirement to an ID and criterion in the JSON matrix.
2. **Integration**: test live PostgreSQL 16 migrations, REST API pagination/filter/count consistency, Keycloak authorisation, MinIO read/write, and staged data imports with disposable fixtures.
3. **Data products**: validate an immutable M40 canonical ZIP, row counts, referential integrity, missing values and geographic QA; retain bundle hash and DOI.
4. **Publication**: execute a Zenodo dry-run without publishing; record CSVW validator results and independently confirm metadata discoverability where required.
5. **Browser and optional services**: run acceptance tests for public map/search, authenticated /my, Jupyter isolation, Streamlit and localisation; record browser/device, screenshots and defects.
6. **Audit trail**: store dated JUnit reports, commit and deployment hashes, test-run operator, environment, defects and sign-off. Keep historical M40 evidence distinct from current regression checks.

### Suggested WP5 reporting wording

> ECHOREPO has a requirements-traceable automated component regression suite, with isolated acceptance tests for API, access control, export contracts, QA, harmonisation, privacy, storage, ingestion and metadata, combined with existing biodiversity, session, Jupyter and LOESS tests. The suite produces versioned JUnit and machine-readable audit reports. Component validation remains in progress until the production/staging integration evidence, scope sign-off and historical M40 baseline have been verified.

Do **not** replace “in progress” with “all validated” solely because the CI job turns green.
