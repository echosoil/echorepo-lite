# R7-3 Phase 4 — observed interoperability results

Assessment: 24 November 2026. Automated report UTC 12:39:26; reporting checkout `2b226e53145adf8ce6d38627d0a33df70788e2e8`. This records operator-supplied interoperability.md and three browser captures supplied the same day; not a new independent execution. Retain original reports and images in controlled evidence storage. The images are not committed because the dashboard capture includes personal account information.

## Automated observations

All nine automated observations passed. The five initially NOT_ASSESSED manual/external observations are separately interpreted below; no original report is overwritten.

| ID | Result | Evidence |
|---|---|---|
| CAN-DOI / CAN-REC | PASS / PASS | 10.5281/zenodo.23784504 resolves to expected record; seven public files, expected inventory and license present. |
| CAN-FILES / CAN-CSVW | PASS / PASS | Seven files match repository sizes and checksums; six CSV tables match metadata inventory, ordered headers, unique headers, row widths, counts, sizes and SHA-256; scalar propertyUrl where present. |
| BIO-DOI / BIO-REC | PASS / PASS | 10.5281/zenodo.22014977 resolves to expected record; two public files, expected inventory and license present. |
| BIO-FILES / BIO-CSVW | PASS / PASS | ZIP and metadata file match repository sizes and checksums; three archived CSV tables match selected metadata checks. |
| LOESS01 | PASS | Served overlay: 352 features, zero invalid observations or duplicate IDs, 77 photo URLs, matching source; generated 2026-11-11T16:20:39.530227+00:00. |

Both Zenodo API version fields were empty in this report; identification is by exact version-specific DOI/record ID. No full CSVW conformance, vocabulary resolution, license appropriateness, scientific validity or live snapshot equivalence is established.

## Published tables

| Canonical resource | Rows | Columns | propertyUrl columns |
|---|---:|---:|---:|
| samples.csv | 10558 | 26 | 20 |
| sample_images.csv | 26673 | 9 | 9 |
| sample_parameters.csv | 79601 | 9 | 7 |
| sample_biodiversity.csv | 45732 | 10 | 8 |
| parameter_definitions.csv | 16 | 6 | 4 |
| analysis_methods.csv | 1 | 4 | 3 |
| Total | 162581 | 64 | 51 |

All 51 propertyUrl annotations counted are absolute HTTP(S) strings; 12 in samples.csv contain the EUSO vocabulary marker. This is a string-pattern count, not verification of ontology meaning or URI resolution.

| Biodiversity resource | Rows | Columns |
|---|---:|---:|
| biodiversity_16S.csv | 40918 | 665 |
| biodiversity_ITS.csv | 23376 | 666 |
| biodiversity_taxonomy.csv | 11233 | 8 |

All three tables pass selected consistency checks, but no propertyUrl annotations are present. Do not infer unique sample counts from matrix widths. Reconcile the earlier pilot description (95 samples) with the actual matrices before reporting a sample count. These published resources differ from the later Phase 2 local snapshot; neither byte equivalence nor matching coverage is assumed.

## Operator browser evidence

| ID | Status | Observed evidence | Limitation |
|---|---|---|---|
| SW01 | PASS | 01_soilwise_catalogue.png: matching title, exact DOI in catalogue URL, ECHO attribution, harvest date 2026-11-01, Zenodo record link visible. | Does not establish file.json parsing. |
| EDP01 | PASS (bounded discovery) | 02_eu_data_portal.png: matching title, record identifier oai-zenodo-org-23784504 and EU Open Research Repository label. | Exact DOI not separately displayed in capture; full exported metadata provenance not inspected. |
| LOESS02 | PASS (bounded browser behavior) | 03_loess_overlay.png: authenticated /my page, enabled LOESS layer, 352 observations, category controls, open popup with photograph and LOESS source link. | Enlargement, refresh scheduling and upstream equality not verified by this capture. |
| EUSO01 | NOT_ASSESSED | Operator reports contacting SoilWise; response pending. | Need record-specific EUSO evidence. |
| SEM01 | NOT_ASSESSED | Same pending correspondence. | Need downstream parsing/mapping or separate consumer evidence. |

Thus 12 of 14 observation IDs have bounded positive evidence, two remain NOT_ASSESSED; not a formal interoperability score. The European Data Portal is not equivalent to JRC/EUSO. Keep the original images privately and redact the logged-in email, account-specific information and unnecessary observation identifiers/locations before external publication.

## Matrix disposition and follow-up

C02 remains SUPPORTED; C03 remains PARTIAL because downstream processing and EUSO remain open; C04 advances to SUPPORTED for the observed data consumption criterion. Proceed to Phase 5 while waiting for external clarification. Reuse verified publication and discovery evidence for RO4-1, RO4-4 and WI5-2 as appropriate, without assuming that each KPI is accepted or that EOSC indexing is established. No deployment or publication changes are necessary for this update.
