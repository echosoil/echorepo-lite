# R7-3 Phase 2 — data quality and performance evidence

Scope: extend the ISO/IEC 25010:2023 baseline with quantitative observations. This phase does not calculate an ISO compliance score or certify scientific validity. Keep the R7-1 evidence unchanged. Run these commands from the repository root in the existing Python virtual environment.

## 1. Snapshot data quality (no network)

    cd /home/echo/ECHO-STORE/echorepo-lite-dev
    git switch develop
    git pull --ff-only
    OUT="r73-evidence/phase2-$(date -u +%Y%m%dT%H%M%SZ)"
    mkdir -p "$OUT"
    python -m tools.r73_data_quality \
      --bundle r71-evidence/canonical_all.zip \
      --out-dir "$OUT" \
      --commit "$(git rev-parse HEAD)"

Use the existing immutable canonical ZIP from R7-1 if available. The tool requires all six CSV resources; reports rows, per-column blank counts, selected domain checks, finite numeric and UTC timestamp checks, primary/foreign keys, country-code syntax and parent-country consistency, coordinate ranges, existing QA flags and linked sample coverage. It records hashes for the bundle and its members. It does not emit row data, identifiers, coordinates or error values. Blank values and explicit "no data" categories are not automatically defects. Errors/warnings count rule observations, not distinct defective records. A rule absent from the report has not been exercised. This tool shares the repository's schema/type definitions, and adds aggregate checks; it is not a replacement for the original validator. Structural errors return exit 1; warnings do not. Bad inputs/refusal return exit 2.

The canonical ZIP contains six CSV files, not the separately generated publication file.json. If you possess metadata generated for **this exact bundle**, optionally add:

    --metadata /path/to/matching/file.json

The optional assessment compares table inventory, column names, scalar propertyUrl values and declared SHA-256, row counts and byte sizes. It is not a full W3C CSVW validator. Do not compare a Zenodo v12 file.json against a later live snapshot and interpret differences as a publication failure. Without matching metadata the CSVW check is NOT_ASSESSED. A supplied mismatching/incomplete metadata file returns exit 1 separately from the structural result.

Outputs: data-quality.json and data-quality.md.

## 2. Sequential HTTP performance (explicit production opt-in)

Run against the official public HTTPS origin, during a quiet period with authorization. No concurrency, writes, bulk ZIP export or redirects. GETs may still add usage-analytics entries. The default workload is 10 requests each for seven operations (70 requests, one-second minimum pause); choose 3–20 repetitions. Do not run several instances simultaneously.

Use an authorized API key. Bash prompts without echo; the secret is never in the command line or reports.

    read -rsp 'ECHOREPO API key: ' R73_API_KEY; echo
    export R73_API_KEY
    unset R73_BEARER_TOKEN

    python -m tools.r73_performance \
      --base-url https://echorepo.quanta-labs.com \
      --environment production-readonly \
      --ack yes-production-readonly \
      --country ES \
      --repetitions 10 \
      --delay 1 \
      --out-dir "$OUT" \
      --commit "$(git rev-parse HEAD)"

    unset R73_API_KEY

Alternatively supply R73_BEARER_TOKEN, not both. Add --public-only (no credentials needed) to measure the health, explore and filtered search pages only; the authenticated API operations are then outside that run's scope.

The full default run measures health, explore HTML, filtered search (country, pH 5–8, page 1), API count, bounded JSON page (50 rows), compact country-filtered map (200 features) and bounded CSV (100 rows). Payload sanity checks prevent redirects/login HTML from being treated as successful API responses. No response bodies, credential headers or cookies are retained. Requests do not follow redirects or inherit proxy/netrc credentials. TLS validation stays enabled. The tool stops on HTTP 429/503 or repeated transport failures; reports attempted, successful, error and not-run counts. Response caps are 4 MiB decoded by default. Socket and between-chunk budgets are not server cancellation guarantees. A failed/incomplete attempt set returns 1 after writing reports.

Proposed p95 targets (not contractual): health 500 ms; sample count 1000 ms; bounded JSON 2000 ms; filtered search and explore HTML 2000 ms; compact map 3000 ms. CSV throughput is measurement only. Record target acceptance with reviewers before using comparisons as acceptance criteria. The median, nearest-rank p95 and max are for successful attempts; with ten requests p95 equals the maximum. Do not claim a stable tail latency, availability percentage or browser-rendering benchmark from this run. Transfer throughput uses decoded bytes divided by full elapsed client time; it is not wire bandwidth.

Outputs: performance.json and performance.md.

### Optional filtered ZIP test — staging only

If approved, select your actual staging origin (not the production port 8001). Use --environment staging --ack yes-staging-readonly, and --allowed-host HOSTNAME for a remote HTTPS staging host. Add --include-zip for one filtered country ZIP request (20 MiB decoded limit). This can still cause server-side work before a response is received; only run with an authorized staging target and appropriate data size. A local loopback staging URL can use HTTP, but the known local production port is blocked. The full biodiversity and canonical exports must not be repeatedly generated against production just for benchmarking.

## 3. Review and retain

    ls -lh "$OUT"
    cat "$OUT/data-quality.md"
    cat "$OUT/performance.md"

Review environment, observations and any unexpected issues. Record the tested snapshot date/source, actual deployed version if known, and runner location (e.g. server vs workstation). The --commit field describes the reporting checkout, not necessarily the live revision. Keep outputs in r73-evidence/ (gitignored) and inspect for operational details before publishing a curated archive. If an operation fails or a rule reports warnings, retain the original result, investigate and document any rerun separately; do not edit figures to manufacture success.

The tools do not yet close resource capacity (R73-P03), long-term uptime (R73-R01), restore/recovery (R73-R02), reproducible installation (R73-X01), scalability (R73-X02) or scientific validity (R73-F03). Reuse available dated monitoring and import/benchmark records and assess those separately; controlled experiments belong in development. Phase 3 covers dedicated security verification. Update the quality matrix with observed evidence after execution.

## 4. Offline tests

    python -m pytest -q tests/test_r73_phase2.py

No network or deployment services are used in these tests. The dedicated CI workflow verifies the tool behavior, not the actual ECHOREPO production performance or snapshot quality.
