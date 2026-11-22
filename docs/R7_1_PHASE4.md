# R7-1 Phase 4 — consolidate evidence and close external gaps

**Goal:** convert Phases 1–3 into an auditable, requirements-linked technical
evidence package without claiming that the KPI is formally certified.

Phase 3 is already reported by the deployment operator as **14/14 required and
4/4 optional browser checks passed**. The final JUnit XML is needed to attach
that result to an independently inspectable report. Phase 2 was run against
the operating **production** deployment even though the original Phase 2 gate
used the `R71_PHASE2_RUN=staging` opt-in. Accordingly this Phase 4 report
records Phase 2 as *production observations with staging opt-in*: do not
misrepresent it as staging acceptance.

The Phase 1 component matrix has **19** entries; **one** ("External
dependencies and publication interoperability", R71-C18) has no mapped
isolated tests. Even with 18 automated-pass components and all Phase 2/3
checks passing, formal component acceptance requires separate evidence.

## 1. Confirm the evidence files

On the server's development checkout:

```bash
cd /home/echo/ECHO-STORE/echorepo-lite-dev
git switch develop
git pull --ff-only

ls -lh \
  r71-evidence/results.xml \
  r71-evidence/phase2-results.xml \
  r71-evidence/phase3-results.xml \
  r71-evidence/canonical_all.zip
```

Missing paths are reported as **MISSING / INCOMPLETE**, never counted as passing.
You have already generated Phase 3's JUnit XML. If the Phase 1 `results.xml`
is missing, download the `r7-1-validation-evidence` artifact from a passing
GitHub Actions **develop**-targeted PR run, or rerun the offline component
suite (only against synthetic/test fixtures) using the commands from
`docs/R7_1_VALIDATION.md`. Keep the real test commit SHA/CI run URL.
If Phase 2 XML is missing, locate your previous locally generated XML, rather
than re-enabling the **staging** gate against production merely to reproduce
the run. Record the actual environment. Do not copy arbitrary logins/cookies
into the evidence folder.

## 2. Generate the combined report

```bash
python tools/r71_phase4_report.py \
  --phase1-xml r71-evidence/results.xml \
  --phase2-xml r71-evidence/phase2-results.xml \
  --phase3-xml r71-evidence/phase3-results.xml \
  --snapshot r71-evidence/canonical_all.zip \
  --phase2-environment "production observations with staging opt-in" \
  --phase3-environment "production-readonly" \
  --commit "$(git rev-parse HEAD)" \
  --output-json r71-evidence/phase4-consolidated.json \
  --output-md r71-evidence/phase4-consolidated.md \
  --require-technical
```

Outputs:

- `phase4-consolidated.json`: outcomes, 19-component matrix crosswalk,
  external observation register and SHA-256 fingerprints for all evidence inputs.
- `phase4-consolidated.md`: reviewer-friendly report, missing evidence and
  sign-off placeholders.

A complete technical evidence set means all mapped isolated tests for
components **with executable tests** passed, all ten Phase 2 checks passed,
all 18 Phase 3 checks passed and the canonical archive file is present. The
uncovered C18 component stays explicitly open; `--require-technical` does
**not** mark that component or the KPI fully verified.

If the command returns 1, open the report to identify missing or skipped
checks. Do **not** edit a matrix to manufacture a pass.

The `--commit` option records the report-preparation repository commit. The
original test runs may have used different deployed revisions. Preserve the
original workflow run URLs, the report timestamps and the live deployment
version separately; a single commit value must not be read as a proven
identity of all three tested environments.

## 3. External interoperability verification

The external register is
[`R7_1_PHASE4_EXTERNAL.json`](R7_1_PHASE4_EXTERNAL.json).

**Publication (observed):** Zenodo version 12 of the ECHOREPO Soil Dataset is
public at https://zenodo.org/records/23784504
(DOI `10.5281/zenodo.23784504`, published 2026-10-30).
It lists `samples.csv`, `sample_images.csv`,
`sample_parameters.csv`, `sample_biodiversity.csv`,
`parameter_definitions.csv`, `analysis_methods.csv`
and `file.json`. This is public **existence** evidence, not proof that
this Zenodo release is byte-identical to the current live canonical ZIP.

**SoilWise harvesting (reported but not independently linked):** the ECHO
project observes its dataset in SoilWise search. Add a direct public
SoilWise **record** permalink and downloadable machine-readable metadata
for the same version DOI (not just a screenshot or a broad text query).
Verify the original repository source and which metadata fields it includes.

**SoilWise consumption of `file.json` (not demonstrated):** the file is
present on Zenodo, but neither its contents nor special CSVW attributes have
been shown to appear in SoilWise record metadata. Compare a distinctive
file-only property with the SoilWise metadata export and/or request a
harvesting mapping from the SoilWise team.

**EUSO interoperability (not demonstrated):** SoilWise's
https://soilwise-he.eu/faq/ describes the repository as a prototype that may
be adopted by EUSO, while its
https://soilwise-he.eu/repository-road-map/ describes a staged JRC/EUSO
migration. Neither establishes that ECHO's **specific DOI** is present in an
independent EUSO discovery service. Request a JRC/EUSO record identifier,
live query endpoint or technical acceptance statement identifying this DOI.

**Other external components:** R71-C18 still includes Firebase and external
publication interfaces. This evidence is not generated by passing a
Keycloak JWKS request, a Zenodo dry-run or a research dashboard page load.
Document the appropriate owner and external verification for each interface.

## 4. Requirements, M40 and consortium review

The Phase 4 matrix is **not** an official Grant Agreement/D5.1 traceability
approval. The reporting owner must still:

1. Confirm requirements IDs and exact GA/D5.1 version and map the 19 components
   (and any missing external/mobile components) to requirements.
2. Separate **current** November 2026 tests from the historical **M40
   September 2026** baseline, supported by an actual release/deployment SHA,
   immutable dataset, test log and dated records.
3. Review outstanding QA/security/operator evidence against each requirement;
   track issues and approved exceptions.
4. Obtain WP5/WP7 sign-off, with names, roles, date and decision.

Until then, report **"current technical validation complete within the
defined suite; overall R7-1 KPI not yet formally verified."**

The evidence folder must not include `phase3-auth-state.json`, passwords,
API tokens, raw logs with session cookies, or personal data. Securely delete
browser session files once no longer needed.
