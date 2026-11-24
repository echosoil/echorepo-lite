# R7-3 Phase 5 — user experience assessment

Status: technical assessment prepared; operator reports that the survey is published and collecting responses. The exact deployed questionnaire, recruitment dates and privacy settings have not been reviewed here. No satisfaction statistic is available. Preserve the instrument and scoring rule actually used before examining response outcomes; see R7_3_USER_SURVEY.md. Do not treat technical test results as survey responses.

## 1. Technical assessment (UX01)

Use a workstation with Playwright Chromium, preferably the same Windows workstation used for the previous audit. No production deployment or Docker changes. Install the optional tools outside the application's runtime. From a Git Bash shell at the repository root, with a local audit environment active:

    python -m pip install playwright
    python -m playwright install chromium
    npm install --prefix "$HOME/echorepo-ux-tools" --ignore-scripts --no-audit --no-fund axe-core@4.10.3

Use a trusted local axe-core script from the pinned npm distribution. The CLI records the engine version and script SHA-256; no external script is fetched by the assessment itself.

    OUT="$PWD/r73-evidence/phase5-$(date -u +%Y%m%dT%H%M%SZ)"
    mkdir -p "$OUT"
    python -m tools.r73_ux --ack yes-production-readonly \
      --axe-js "$HOME/echorepo-ux-tools/node_modules/axe-core/axe.min.js" \
      --commit "$(git rev-parse HEAD)" --out "$OUT/usability.json"

Seven isolated anonymous page/viewport/locale cases: explore, search and login at 1366x900 and 390x844, plus Spanish desktop search via the actual language route. The initial rendered state is scanned using axe-core WCAG 2.1 A/AA rule tags. Record rule and node counts, incomplete rules, document language/title/landmarks, horizontal document overflow, map initialization, skip-link keyboard behavior and mobile navigation opening. Do not assume repeated nodes are unique defects. A missing H1 is reported for contextual review; it is not independently a WCAG failure. The mobile expanded navigation is not included in the initial axe scan.

Production is the fixed public HTTPS origin; explicit opt-in; normal public page GETs may create anonymous sessions or analytics. No authenticated browser state, submissions, logout, bulk export or survey interaction. A conservative allowlist limits ordinary browser requests to known public paths and asset hosts; no service workers. Browser redirect behavior is not a network sandbox. Network failures or blocked critical resources make the case incomplete or REVIEW. No automatic production test in CI.

Reports: usability.json and usability.md. No DOM fragments, cookie values, raw sample identifiers or screenshots included. Exit 0 signifies a report, not all checks passed. A missing browser or axe script is a tooling blocker, not an application defect.

Automated checks do not establish WCAG compliance, accessibility of the map to screen-reader users, or user satisfaction. Perform selected manual checks below and use relevant R7-1 evidence instead of repeating all prior browser tests. Lighthouse is not required for this first focused scan; performance was measured separately in Phase 2.

Offline tests: python -m pytest -q tests/test_r73_phase5.py

## 2. Manual usability observations

Record date, browser/device, status and concise issue description; use consenting accounts/test data when necessary. No screenshots with account details.

| ID | Task | Expected observation |
|---|---|---|
| UX-M01 | Navigate with Tab/Shift+Tab on public explore/search/login | Visible focus, sensible order, no keyboard traps; skip link moves focus into main content. |
| UX-M02 | Use only keyboard to reach search, filter by country/pH, submit search and navigate back | Search can be completed without a mouse; filters and labels remain understandable. |
| UX-M03 | At a narrow mobile viewport, open navigation, use filters and results | No inaccessible controls or unintended page-wide overflow; local table/map scrolling may be appropriate. |
| UX-M04 | Browser zoom 200%, check search/login and map controls | Content remains readable and important actions usable; record map-specific limitations separately. |
| UX-M05 | Switch between English and Spanish (and another supported language if feasible) | Labels, messages and controls are meaningful; distinguish untranslated user-provided text from missing UI translation. |
| UX-M06 | Review a representative screen-reader path if available | Page structure, names, labels and announcements are understandable; if not tested, mark NOT_ASSESSED. |

## 3. User feedback and participatory design (UX03)

See R7_3_CODESIGN_REGISTER.md for a verified public feedback case, proposed dispositions and a template for additional records. A recorded issue is genuine consultation evidence, not evidence that every proposed change was implemented. If no workshop records exist, conduct a short new task-based feedback session with consenting users and document it honestly as new. Do not collect identifiers in the evidence register.

## 4. Survey (UX02)

Retain the published instrument and approved privacy information; record start/end dates, recruitment channels, eligible invitation count if known, actual question codes, missingness, valid-response denominator and duplicate-handling rule. Use the predefined metric only if the instrument supports it. Report aggregate counts and limitations; do not commit raw responses or free text to the repository. An analysis tool can be adapted once the actual export format is known.

## 5. Consolidation

Update R73-I02 only after the technical and manual evidence is received. R73-I04 has partial evidence from documented external participant feedback, with workshop/change verification pending. R73-I03 remains NOT_ASSESSED until actual survey results. No ISO conformity declaration.
