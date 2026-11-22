# R7-1 Phase 3 — browser and end-to-end acceptance

**Target branch:** `develop` — do not deploy these tests as an application service.

This phase tests actual ECHOREPO pages in **Chromium**, using Playwright. It is
different from the Phase 1 mocked component checks and Phase 2 HTTP/DB/storage
checks. See [R7_1_PHASE3_MATRIX.json](R7_1_PHASE3_MATRIX.json) for all 18 named
acceptance criteria: 14 required public/read-only checks and four optional
authenticated/research-service checks.

Tests exercise the public map, Leaflet initialization, pH and deep-link map
behavior, the search UI and country/pH filters, pagination, identity redaction,
anonymous ZIP access control, /my login redirect, Spanish locale switching,
and mobile layout.

**Safety:** production tests use browser GET navigation, read-only filter form
submission, client-side map interactions and cookie-only locale switching.
They never POST credentials, upload files, accept privacy terms, log out, click
dataset download links or publish anything. GETs can still create analytics
events. Chromium loads the application's ordinary third-party CDN and tile
resources. Optional authenticated checks use a preexisting browser session;
session cookies may be refreshed by the normal app. This is not a guarantee
that the application itself has no GET-side effects.

## 1. Install on the ECHOREPO development host

After merging the PR into `develop`:

```bash
cd /home/echo/ECHO-STORE/echorepo-lite-dev
git switch develop && git pull --ff-only

# In your existing (venv) virtualenv:
python -m pip install playwright
python -m playwright install chromium --only-shell
```

The headless-shell-only install saves disk compared with full Chromium but may still require Linux
shared libraries. If launch fails, inspect the reported missing dependency;
don't run `apt` or `sudo` blindly on a production host. Use an isolated
workstation with a GUI, a preinstalled Chromium binary configured via
`R71_PHASE3_CHROMIUM_PATH`, or an approved Playwright container with enough disk
space. Avoid mixing browser dependencies into the application Docker image.

The tests use Playwright's synchronous Python API; **do not** run them until the
browser is installed. CI only syntax-checks and offline-tests guards; it does
not install Chromium or navigate a live deployment.

## 2. Select the real execution mode — do not reuse Phase 2's staging flag

You already identified the production Flask endpoint on the same Ubuntu host:
`http://127.0.0.1:8001`. Phase 3 has its **own** flag:

```bash
export R71_PHASE3_RUN=production-readonly
export R71_PHASE3_PROD_ACK=yes-production-readonly
export R71_PHASE3_BASE_URL=http://127.0.0.1:8001
```

These are **not** equivalent to `R71_PHASE2_RUN=staging`; Phase 3 ignores the
Phase 2 setting and labels evidence with the actual environment. The guard
refuses to label the known local production port 8001 as staging.

For a real development/staging web port instead:

```bash
export R71_PHASE3_RUN=staging
export R71_PHASE3_BASE_URL=http://127.0.0.1:18080   # example only
unset R71_PHASE3_PROD_ACK
```

If running from your Windows workstation against
`https://echorepo.quanta-labs.com` instead of a loopback port, also set:

```bash
export R71_PHASE3_ALLOWED_HOSTS=echorepo.quanta-labs.com
```

(The URL requires HTTPS in production mode. Export commands shown are for Bash
or Git Bash; adapt to PowerShell as needed.)

## 3. Run real-browser checks and generate evidence

```bash
mkdir -p r71-evidence

python -m pytest -v -rs tests/e2e/test_r71_phase3_browser.py \
  --junitxml=r71-evidence/phase3-results.xml

python tools/r71_phase3_report.py \
  --xml r71-evidence/phase3-results.xml \
  --matrix docs/R7_1_PHASE3_MATRIX.json \
  --json r71-evidence/phase3-report.json \
  --markdown r71-evidence/phase3-report.md \
  --commit "$(git rev-parse HEAD)" \
  --require-core
```

When no authenticated storage state or optional service URLs are configured,
expect **14 public checks** to execute and **4 optional tests to be skipped**.
The report distinguishes these, and `--require-core` returns nonzero if a
required check failed, skipped or was not run. Use `--require-all` only after
configuring and verifying all optional services.

Do not report a skip as passing. A failing browser test may expose an app bug,
an unavailable CDN/map tile dependency, browser compatibility or a configuration
issue; investigate rather than weakening its acceptance criterion.

You may run the public subset alone:

```bash
python -m pytest -v -rs tests/e2e/test_r71_phase3_browser.py \
  -k 'not authenticated and not optional_jupyter and not optional_streamlit'
```

That subset excludes four optional tests. To produce **complete JUnit evidence**,
always rerun the full module before generating the final report.

## 4. Optional authenticated acceptance

`/my` is private and uses Keycloak, so the suite **does not** ask for your
password or automate a login POST. To test /my and the authenticated search
toolbar, provide a storage-state JSON from a **dedicated consenting test
account** (preferably staging):

```bash
export R71_PHASE3_STORAGE_STATE="$PWD/r71-evidence/phase3-auth-state.json"
```

The state must have been captured from **the same browser origin** used in
`R71_PHASE3_BASE_URL`, since cookies for a public HTTPS domain generally
won't be sent to `127.0.0.1`. On a local workstation with a graphical
desktop, you can capture a session interactively with:

```bash
python -m playwright codegen \
  --save-storage=r71-evidence/phase3-auth-state.json \
  https://echorepo.quanta-labs.com/login
```

Sign in yourself in the launched browser, verify /my, then close it. Do **not**
share the state file; it can contain session tokens. Keep it in the ignored
`r71-evidence/` directory, restrict filesystem permissions, and delete after
validation. If a privacy consent dialog appears, do not automate the consent:
resolve it manually with an authorized account outside these tests.

For a production HTTPS origin, use the production-readonly mode and explicitly
allowlist `echorepo.quanta-labs.com` as shown above.

## 5. Optional research services

Only if the actual services are reachable from the test browser:

```bash
# Example when the same reverse proxy exposes Jupyter:
export R71_PHASE3_JUPYTER_URL=http://127.0.0.1:8001/jupyter/

# Streamlit: use a trusted HTTPS endpoint, or a loopback port if locally bound.
# export R71_PHASE3_STREAMLIT_URL=http://127.0.0.1:8501
```

Both are independently gated to approved origins. If the URLs are unavailable,
the tests correctly record SKIPPED. The Jupyter/Streamlit checks verify a
recognizable UI, **not** complete per-user isolation or analytics correctness.

## 6. Interpreting R7-1 evidence

- Phase 1: isolated component regression tests.
- Phase 2: service integration and offline Zenodo preparation.
- Phase 3: actual browser UI acceptance, with clearly separated public and
  authenticated/research scopes.
- All reports identify the tested commit and UTC execution time. Phase 3
  additionally records the explicit environment mode.
- Browser screenshots, any recorded sessions, console logs, and private data
  should never be committed to Git. The `r71-evidence/` folder is gitignored.
- The requirements inventory still needs formal WP5/WP7 mapping and sign-off;
  browser tests are **not a full accessibility audit**, a security penetration
  test, or a substitute for human usability evaluation.
- Running in November 2026 does not retroactively prove the M40 (September
  2026) baseline.

See [R7_1_VALIDATION.md](R7_1_VALIDATION.md) for the overall KPI interpretation.
