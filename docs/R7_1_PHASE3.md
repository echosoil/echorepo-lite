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

## 4. Finish B15/B16: authenticated browser acceptance

These checks require an already-authenticated browser state. The automation
**never** supplies a password or submits a login. Prefer a dedicated test
account that is allowed to view `/my`. You must authenticate interactively
once, with the account holder's consent.

**Important hostname rule:** production login cookies are for
`https://echorepo.quanta-labs.com`, and usually have the Secure flag. They
cannot authenticate `http://127.0.0.1:8001`. The tests therefore need the
public HTTPS origin when using a session captured in the public browser.

On a Windows workstation with graphical Chromium support (Git Bash or
PowerShell; `python` or `py` depending on installation):

```bash
python -m pip install playwright
python -m playwright install chromium
mkdir -p r71-evidence
python -m playwright codegen \
  --save-storage=r71-evidence/phase3-auth-state.json \
  https://echorepo.quanta-labs.com/login
```

Log in **manually** in the browser opened by codegen, navigate to
`https://echorepo.quanta-labs.com/my`, confirm your dashboard is visible,
then close codegen. The browser state JSON will be written on exit.

**The JSON is a live session secret** and must not be committed, printed or
attached to issues/reports. Transfer it to the Ubuntu test host over SSH/SCP
if running Playwright there (or run the tests on the Windows machine).
For example, from Windows Git Bash:

```bash
scp -P YOUR_SSH_PORT -i /path/to/ssh-private-key \
  r71-evidence/phase3-auth-state.json \
  echo@YOUR_SSH_HOST:/home/echo/ECHO-STORE/echorepo-lite-dev/r71-evidence/
```

Then on Ubuntu:

```bash
cd /home/echo/ECHO-STORE/echorepo-lite-dev
chmod 600 r71-evidence/phase3-auth-state.json

export R71_PHASE3_RUN=production-readonly
export R71_PHASE3_PROD_ACK=yes-production-readonly
export R71_PHASE3_BASE_URL=https://echorepo.quanta-labs.com
export R71_PHASE3_ALLOWED_HOSTS=echorepo.quanta-labs.com
export R71_PHASE3_STORAGE_STATE="$PWD/r71-evidence/phase3-auth-state.json"
```

Test only the two authenticated cases first:

```bash
python -m pytest -v -rs tests/e2e/test_r71_phase3_browser.py \
  -k 'r71_b15 or r71_b16'
```

The guard checks that state cookies match the target hostname and that
Secure cookies are not used with an HTTP origin. It does not prove the account
is authorized before the browser makes the first request; expired sessions
will fail with a login redirect.

For a full 18-test report, rerun the entire module afterwards. Avoid retaining
the state file beyond the testing session. The test runner never submits
privacy consent automatically.

## 5. Finish B17/B18: Jupyter and Streamlit

If your production containers still expose the ports reported on the Ubuntu
server, the actual endpoints are expected to be:

- JupyterLab: `http://172.31.5.252:8888/jupyter/`
- Streamlit: `http://172.31.5.252:8501/`

First confirm that the containers are still bound to those ports and that
the server can connect to them:

```bash
docker ps --format 'table {{.Names}}\t{{.Ports}}' | grep -E 'jupyter|streamlit'
curl -sS -L -o /dev/null -w 'Jupyter HTTP %{http_code}\n' \
  --max-time 15 http://172.31.5.252:8888/jupyter/
curl -sS -L -o /dev/null -w 'Streamlit HTTP %{http_code}\n' \
  --max-time 15 http://172.31.5.252:8501/
```

Only the optional research UI checks may use private HTTP targets, and
only when their private IP is explicitly allowlisted **and** the separate
opt-in flag is set. This does not enable HTTP for public production domains
or for the main authenticated browser target:

```bash
export R71_PHASE3_ALLOWED_HOSTS=echorepo.quanta-labs.com,172.31.5.252
export R71_PHASE3_PRIVATE_SERVICE_ACK=yes-private-readonly-services
export R71_PHASE3_JUPYTER_URL=http://172.31.5.252:8888/jupyter/
export R71_PHASE3_STREAMLIT_URL=http://172.31.5.252:8501/
```

The optional-service browser contexts are always anonymous. Jupyter must
render the JupyterLab shell, and Streamlit must display the Streamlit app
root. HTTP 200 or an identity-provider login page alone is not enough.

```bash
python -m pytest -v -rs tests/e2e/test_r71_phase3_browser.py \
  -k 'r71_b17 or r71_b18'
```

If the Jupyter or Streamlit UI is legitimately behind authentication, these
tests will fail until an agreed **read-only browser-based authentication**
method is available; never paste service tokens into URLs just to bypass the
check.

## 5A. Collect the complete Phase 3 evidence

Only after all four optional tests have been tested, run the entire module:

```bash
mkdir -p r71-evidence

python -m pytest -v -rs tests/e2e/test_r71_phase3_browser.py \
  --junitxml=r71-evidence/phase3-results.xml

python tools/r71_phase3_report.py \
  --xml r71-evidence/phase3-results.xml \
  --json r71-evidence/phase3-report.json \
  --markdown r71-evidence/phase3-report.md \
  --commit "$(git rev-parse HEAD)" \
  --require-all
```

With the core 14 and optional 4 checks all passing, the report should show
**14/14 required and 4/4 optional**. If a service cannot be checked, keep
its truthful skipped/failed result rather than downgrading the acceptance
criteria. After recording the evidence, securely remove the session file from
both the workstation and Ubuntu (`rm r71-evidence/phase3-auth-state.json`
on Ubuntu). Do not add the session state to the evidence package.

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
