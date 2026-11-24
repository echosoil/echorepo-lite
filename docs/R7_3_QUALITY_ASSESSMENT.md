# R7-3 Phase 1 — ECHOREPO software product quality assessment

**KPI:** The overall validation approach will focus on software product quality, security, compatibility with external platforms (e.g., Joint Research Centre), and user experience.

**Baseline:** `develop` at `25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d`; desk assessment dated 2026-11-24.  
**Status:** Phase 1 baseline complete; R7-3 assessment in progress.

**Historical baseline:** This document preserves the Phase 1 desk assessment and its original counts. See [Phase 2 observed results](R7_3_PHASE2_RESULTS.md), [Phase 3 security results](R7_3_PHASE3_RESULTS.md), [Phase 4 interoperability results](R7_3_PHASE4_RESULTS.md) and the updated [quality matrix](R7_3_QUALITY_MATRIX.json) for the latest evidence states.

## Method and scope

The assessment is structured around the nine product-quality characteristics of [ISO/IEC 25010:2023](https://www.iso.org/standard/78176.html). The checklist contains project-specific criteria rather than reproducing the normative text or claiming ISO conformity/certification. [ISO/IEC 25019:2023](https://www.iso.org/standard/78177.html) is a complementary reference for user evaluation in context.

Scope: ECHOREPO's repository, deployed portal/API and supporting data/research services. Independently operated mobile software and external services require their owners' evidence where applicable. WP2's proposed evidence categories (quality matrix, data compliance, response time, security, interoperability, co-design and 80% satisfaction) guide the assessment; any numerical acceptance targets require project agreement.

The baseline uses the project-supplied R7-1 consolidated results (23 November 2026), the associated [Zenodo evidence record](https://zenodo.org/records/24485700), and the current repository's matrices, scripts and runbooks. The archive itself was not independently reopened in this review. R7-1 reported 49/49 mapped automated checks, 10/10 integration checks and 18/18 browser checks passing (77/77); 18 of 19 components have isolated test coverage. Phase 2 of R7-1 was production observations with staging opt-in; Phase 3 was production-readonly. The separate GitHub CI run is not counted again. These results do not constitute an R7-3 pass rate.

**No new deployment tests, benchmarks, security scans or user surveys were performed in this phase.** Repository implementation and documentation are not proof of deployed configuration.

## Assessment summary

| Characteristic | Supported | Partial | Not assessed | Scope review |
|---|---:|---:|---:|---:|
| Functional suitability | 2 | 1 | 0 | 0 |
| Performance efficiency | 0 | 1 | 2 | 0 |
| Compatibility | 2 | 2 | 0 | 0 |
| Interaction capability | 1 | 1 | 2 | 0 |
| Reliability | 0 | 3 | 0 | 0 |
| Security | 1 | 2 | 2 | 0 |
| Maintainability | 1 | 1 | 0 | 0 |
| Flexibility | 0 | 2 | 0 | 0 |
| Safety | 0 | 1 | 0 | 1 |
| **Total (28 criteria)** | **7** | **14** | **6** | **1** |

These counts describe evidence maturity, not ISO compliance percentages. SUPPORTED means bounded existing evidence; PARTIAL means additional verification is needed; NOT_ASSESSED means adequate direct evidence is absent; SCOPE_REVIEW requires a documented scope decision.

## Quality checklist

The machine-readable [quality matrix](R7_3_QUALITY_MATRIX.json) holds the full criterion, evidence references, R7-1 check IDs, limitations and follow-up actions. Reviewers should assess each applicable criterion against agreed project requirements.

### Functional suitability

| ID | Criterion | Evidence state | Next evidence |
|---|---|---|---|
| R73-F01 | Core portal and data API | SUPPORTED | Review agreed requirements and scope, including mobile app ownership. |
| R73-F02 | Canonical data correctness | SUPPORTED | Retain bundle hash, per-rule counts and validation report; quantify missingness and domain QA in Phase 2. |
| R73-F03 | Soil and biodiversity processing | PARTIAL | Collect representative data-quality checks and operational import evidence; clarify Firebase scope. |

### Performance efficiency

| ID | Criterion | Evidence state | Next evidence |
|---|---|---|---|
| R73-P01 | Interactive response time | NOT_ASSESSED | Phase 2: record sample count, median, p95, maximum, errors and response bytes; approve operation-specific targets before measurements. |
| R73-P02 | Export throughput | NOT_ASSESSED | Phase 2: use bounded/approved export workloads, record snapshot and environment. |
| R73-P03 | Resource use and capacity | PARTIAL | Retain existing dated benchmark logs if available; perform controlled dev benchmarks, not production stress tests. |

### Compatibility

| ID | Criterion | Evidence state | Next evidence |
|---|---|---|---|
| R73-C01 | Internal service integration | SUPPORTED | Record exact environment and test dates; retain original XML evidence. |
| R73-C02 | Zenodo data publication | SUPPORTED | Retain public metadata/file listings and publication logs; verify links in Phase 4. |
| R73-C03 | SoilWise and European Data Portal discovery | PARTIAL | Retain dated record captures and SoilWise reply; establish applicable requirements for JRC/EUSO. |
| R73-C04 | External data consumption | PARTIAL | Collect dated refresh status, source link and screenshot of active overlay if applicable. |

### Interaction capability

| ID | Criterion | Evidence state | Next evidence |
|---|---|---|---|
| R73-I01 | Navigation, search and map | SUPPORTED | Retain browser evidence and describe tested browser/device context. |
| R73-I02 | Localization and responsiveness | PARTIAL | Review representative keyboard navigation, contrast and labels; test additional browser/device combinations as appropriate. |
| R73-I03 | User experience and satisfaction | NOT_ASSESSED | Phase 5: use draft survey protocol; report denominator, recruitment, dates and response distribution. |
| R73-I04 | Co-design feedback and improvements | NOT_ASSESSED | Request relevant project records or organize a new documented feedback session; do not relabel it as a historical workshop. |

### Reliability

| ID | Criterion | Evidence state | Next evidence |
|---|---|---|---|
| R73-R01 | Operational availability | PARTIAL | Obtain existing monitoring aggregates and incident history; define observation window. |
| R73-R02 | Data integrity and recoverability | PARTIAL | Verify backup/restore procedure using disposable dev data; record recovery outcomes and limitations. |
| R73-R03 | Failure handling | PARTIAL | Review failure modes and test selected failures in development only. |

### Security

| ID | Criterion | Evidence state | Next evidence |
|---|---|---|---|
| R73-S01 | Authentication and access control | SUPPORTED | Phase 3: check actual deployed HTTPS login and role boundaries with authorized test accounts. |
| R73-S02 | Session confidentiality | PARTIAL | Confirm actual deployed session backend and cookie flags without collecting token or cookie values; verify expiry/revocation in controlled tests. |
| R73-S03 | Transport and security headers | NOT_ASSESSED | Phase 3: conservative public HTTPS checks; record header presence and context, not secrets. |
| R73-S04 | Data privacy and least privilege | PARTIAL | Review canonical exports, private routes, logs, retention and consent; use synthetic users and aggregate results. |
| R73-S05 | Dependency and source security | NOT_ASSESSED | Run pip-audit on recorded environment, Bandit/static review and config review; triage findings without publishing secrets. |

### Maintainability

| ID | Criterion | Evidence state | Next evidence |
|---|---|---|---|
| R73-M01 | Regression testing and traceability | SUPPORTED | Record test revision versus deployment revision; retain evidence archive; expand tests when requirements change. |
| R73-M02 | Modularity and documentation | PARTIAL | Review representative code and documentation consistency, technical debt and release process. |

### Flexibility

| ID | Criterion | Evidence state | Next evidence |
|---|---|---|---|
| R73-X01 | Deployment and configuration | PARTIAL | Reproduce a clean dev deployment or retain documented deployment checks; avoid production changes. |
| R73-X02 | Scalability and extensibility | PARTIAL | Record workload/resource measurements in Phase 2; identify practical scaling constraints. |

### Safety

| ID | Criterion | Evidence state | Next evidence |
|---|---|---|---|
| R73-A01 | Data integrity and safe failure | PARTIAL | Review data-use limitations, failure modes and escalation paths with scientific and technical owners; no claim of safety certification. |
| R73-A02 | Applicability and scientific interpretation | SCOPE_REVIEW | Confirm scope with reviewer; record limitations, warnings and rationale for any criteria considered not applicable. |

## Priority follow-up

See [Phase 5 procedure](R7_3_PHASE5.md) and [user feedback register](R7_3_CODESIGN_REGISTER.md). Technical usability and survey results remain pending.

The [Phase 4 interoperability procedure](R7_3_PHASE4.md) defines the bounded public-record checks and independent catalogue evidence requirements. Results remain pending execution.

- **Phase 2 — Data and performance:** validate a fixed snapshot and report schema/type/key checks, missingness and scientific/geographic QA separately. Record latency (median, p95, max), errors, bytes, workload, revision, date and environment. Set proposed thresholds before measuring. Measure export throughput and biodiversity CPU/memory/storage on controlled workloads. Keep live requests sequential and bounded; concurrency and failure/recovery exercises belong on approved dev systems.
- **Phase 3 — Security:** verify deployed Keycloak/access controls, TLS, cookies, session backend and privacy behavior; audit dependencies and static findings, then triage. Use approved accounts and non-destructive checks. Never store tokens, session state, credentials, private sample data or raw sensitive logs in publication evidence.
- **Phase 4 — Interoperability:** reuse the canonical and 95-sample biodiversity pilot publication references, SoilWise and European Data Portal discovery, and LOESS consumption. Distinguish record discovery, CSVW publication, downstream CSVW interpretation and independent JRC/EUSO indexing. Request confirmation where in scope; no assumption that the publication is byte-identical to the tested snapshot.
- **Phase 5 — User experience:** no existing survey is known to the project operator. Use the proposed [anonymous questionnaire protocol](R7_3_USER_SURVEY.md); obtain project approval and collect actual responses. Seek existing workshop notes or document a new feedback session. Reuse evidence for WI5-3 if its scope matches.
- **Phase 6 — Consolidation:** attach dated measurements, reviewer-approved targets, evidence references and issue dispositions to the matrix and prepare a concise Word report. Do not derive an overall ISO score by averaging checklist states.

## Evidence references

- **R71** — https://zenodo.org/records/24485700: Project-supplied R7-1 evidence archive; 77/77 mapped checks per supplied Phase 4 report dated 2026-11-23. Archive not independently reopened during this review.
- **CI** — https://github.com/echosoil/echorepo-lite/actions/runs/45076519469: Successful isolated CI run, 2026-11-22; separate from the 77 mapped-check denominator.
- **M1** — [docs/R7_1_VALIDATION_MATRIX.json](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/docs/R7_1_VALIDATION_MATRIX.json)
- **M2** — [docs/R7_1_PHASE2_MATRIX.json](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/docs/R7_1_PHASE2_MATRIX.json)
- **M3** — [docs/R7_1_PHASE3_MATRIX.json](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/docs/R7_1_PHASE3_MATRIX.json)
- **P4** — [docs/R7_1_PHASE4.md](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/docs/R7_1_PHASE4.md)
- **DATA** — [tools/validate_echorepo_bundle.py](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/tools/validate_echorepo_bundle.py)
- **API** — [echorepo/routes/data_api.py](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/echorepo/routes/data_api.py)
- **SESSION** — [docs/SERVER_SESSION_ROLLOUT.md](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/docs/SERVER_SESSION_ROLLOUT.md)
- **BIO** — [docs/streaming_biodiversity_import.md](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/docs/streaming_biodiversity_import.md)
- **BIOPUB** — [docs/biodiversity_zenodo_publication.md](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/docs/biodiversity_zenodo_publication.md)
- **ZPUB** — https://zenodo.org/records/23784504
- **ZBIO** — https://zenodo.org/records/22014977
- **SW** — https://catalogue.soilwise-he.eu/resource?id=10.5281%2Fzenodo.23784504
- **EDP** — https://data.europa.eu/data/datasets/oai-zenodo-org-23784504?locale=en
- **COMPOSE** — [docker-compose.prod.yml](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/docker-compose.prod.yml)
- **LOESS** — [tools/refresh_loess_geojson.py](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/tools/refresh_loess_geojson.py)
- **QA** — [echorepo/services/coordinate_qa_exact_overrides.py](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/echorepo/services/coordinate_qa_exact_overrides.py)
- **CI_DEF** — [.github/workflows/r7-1-validation.yml](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/.github/workflows/r7-1-validation.yml)
- **STYLE** — [pyproject.toml](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/pyproject.toml)
- **MIG** — [scripts/run_pg_migrations.py](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/scripts/run_pg_migrations.py)
- **IMPORT** — [tools/import_biodiversity_large.py](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/tools/import_biodiversity_large.py)
- **GUARD** — [docs/R7_1_PHASE2.md](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/docs/R7_1_PHASE2.md)
- **UX** — [docs/R7_1_PHASE3.md](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/docs/R7_1_PHASE3.md)
- **PUB** — [tools/publish_api_file_to_zenodo.py](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/tools/publish_api_file_to_zenodo.py)
- **FIRE** — [echorepo/services/firebase.py](https://github.com/echosoil/echorepo-lite/blob/25d204a035d6b5b8ef10ebffa10d9bfdd374dd0d/echorepo/services/firebase.py)
