# R7-3 Phase 2 — observed data quality and HTTP performance results

**Assessment:** 24 November 2026 (UTC).  
**Reporting checkout:** `c188d629a20e0e7015633766349388229b4971ca`. This identifies the assessment tooling checkout, not necessarily the deployed revision.  
**Status:** Initial quantitative assessment completed; remaining capacity, recovery, metadata and scientific-quality questions are open.

Source: operator-supplied `data-quality.md` (01:42:48 UTC) and `performance.md` (01:45:10–01:46:25 UTC), produced by the Phase 2 tools. This summary records aggregate results, not a new independent test execution. Retain the original Markdown and JSON reports in controlled evidence storage. The Phase 1 assessment is the historical baseline; the JSON matrix has been updated with these observations.

## Canonical snapshot quality

The local canonical ZIP was 4,274,119 bytes, SHA-256 `3dfa5e6d194b91e058a7bfada77b8ea754438b341f7adb3c6b6265b174db778c`, consistent with the fingerprint in the R7-1 consolidation. It contains 11,329 distinct nonblank sample IDs. All six expected resources were present, with matching headers. The reported result was PASS_STRUCTURAL_CHECKS (0 error and 0 warning observations). Across 73 reported rules there were 2,363,065 rule observations (1,447,473 error-severity checks and 915,592 warning-severity checks), with no violations. These are repeated rule evaluations, not independent software tests or a guarantee of scientific accuracy.

| Resource | Rows | Linked distinct samples |
|---|---:|---:|
| samples.csv | 11,329 | — |
| sample_images.csv | 28,683 | 9,418 (83.13%) |
| sample_parameters.csv | 79,601 | 6,443 (56.87%) |
| sample_biodiversity.csv | 100,102 | 2,693 (23.77%) |
| parameter_definitions.csv | 16 | — |
| analysis_methods.csv | 1 | — |
| Total | 219,732 | — |

All 11,329 sample IDs, timestamps, latitude/longitude fields, country codes, obfuscation radii and licences were populated; checked numeric values were finite, coordinates within valid ranges, and the assessed keys, vocabulary and parent-country consistency checks reported no issues. All assessed fields of the laboratory parameter and biodiversity tables were nonblank. The child tables had no orphan sample IDs. This does not establish coordinate/country geographic correctness, validated scientific measurements, completeness of source imports, or anonymity.

### Follow-up observations (not failed structural rules)

- 210 samples (1.85%) carry existing `wrong_coordinates` QA flags. These remain known geographic-quality issues despite valid coordinate ranges. Review flag meanings and downstream exclusion/annotation policy; do not infer improved geographic quality from this snapshot alone.
- Field pH: 21 blanks (0.19%); estimated organic matter: 287 (2.53%); earthworm counts: 95 (0.84%). Record missingness transparently rather than filling unknown measurements. Texture and structure have 180 and 32 blank English categories respectively, plus 20 and 18 explicit "no data" categories.
- The English free-text columns `contamination_other_en`, `observations_en`, `metals_info_en` and `image_description_en` are wholly blank, although their original-language counterparts have 2,496, 5,789, 163 and 4,373 nonblank values respectively. Investigate whether intentionally untranslated, absent upstream or omitted by export mapping; do not infer the cause.
- All 16 `parameter_uri` and `parameter_description` fields and the sole `procedure_uri` field are blank. This is a semantic metadata improvement opportunity; populated parameter names, units and method codes are not equivalent to persistent vocabulary identifiers. Inspect the canonical metadata definitions and publication workflow, and reuse the finding in WI5-2.
- `collected_by` is nonblank for all sample and image rows. The aggregate report does not establish whether the contents are anonymous/pseudonymous, nor whether this ZIP is suitable for public distribution. Verify export privacy separately in Phase 3 without publishing individual values.
- The separate CSVW assessment is NOT_ASSESSED. A matching metadata file was not supplied; compare against the exact publication snapshot rather than pairing unrelated versions. No claim of full CSVW conformance or SoilWise interpretation follows.

## HTTP performance

Environment: official public HTTPS origin `https://echorepo.quanta-labs.com`, production-readonly. Ten sequential requests per operation, one-second spacing, country filter ES for relevant operations. 70/70 attempts successful, no errors and no not-run attempts. All six latency-targeted operations met their proposed p95 targets; bounded CSV had no threshold.

| Operation | Success | Median ms | Observed p95 / max ms | Proposed p95 ms |
|---|---:|---:|---:|---:|
| Health | 10/10 | 20.05 | 197.31 | 500 |
| Explore HTML | 10/10 | 99.74 | 122.62 | 2000 |
| Filtered search | 10/10 | 162.92 | 191.96 | 2000 |
| Canonical sample count | 10/10 | 38.26 | 45.31 | 1000 |
| Bounded samples JSON | 10/10 | 41.17 | 57.70 | 2000 |
| Compact map | 10/10 | 54.02 | 67.50 | 3000 |
| Bounded CSV | 10/10 | 45.95 | 57.70 | Measurement only |

Bounded CSV (limit 100 rows): median decoded response size 5,015 bytes; median effective decoded throughput 0.104 MiB/s. Compact map (limit 200 features): 95,529 median decoded bytes; 1.687 MiB/s. Full ZIP export performance was not measured. All successful measured responses completed within 200 ms in this particular run.

With ten repetitions, nearest-rank p95 equals the maximum. This measures client-observed HTTP elapsed time (including transfer and payload checks), not browser rendering or server-only processing time. No concurrent load, controlled cache state, peak resource capacity, long-term availability or full export throughput was measured. Targets are proposed evaluation criteria, not confirmed contractual thresholds. Record runner location and deployed revision if known before final publication.

## Quality-matrix disposition

- R73-F02: SUPPORTED — exact-snapshot structural and domain-sanity results supplement R7-1 evidence.
- R73-P01: SUPPORTED — initial sequential latency benchmark completed and all proposed thresholds met within the measured scope.
- R73-P02: PARTIAL — bounded CSV throughput measured; full ZIP export not assessed.
- R73-F03, R73-R01, R73-R02, R73-S04 and R73-A01: PARTIAL — additional evidence recorded, but processing provenance/scientific fidelity, uptime, recovery, privacy and safety-specific risk assessment remain open.
- R73-P03, R73-X01 and R73-X02: PARTIAL — resource capacity, repeatable deployment and scalability still require separate evidence.
- Remaining compatibility, security and UX work retains its Phase 1 status.

Phase 2 is sufficient for the initial response-time and structural-data-quality sections of the R7-3 report. Do not run production stress tests or alter scientific data to remove warnings. The next technical phase is security verification; remaining data/metadata improvements may be tracked alongside it and WI5-2. No overall ISO conformity score or formal acceptance is inferred.
