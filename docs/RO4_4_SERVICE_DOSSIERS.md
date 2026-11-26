# ECHOREPO — candidate EOSC service descriptions (review drafts)

27 November 2026. Prepared for eligibility discussion, not submitted or accepted catalogue entries. Legal provider, approved support contacts, access policy, service commitments and Node identifiers remain to be filled by authorized representatives. Cross-reference [Phase 2 gap register](RO4_4_PHASE2.md) for common requirements.

## A. ECHOREPO Soil Data Discovery and Access (S01+S02)

**Short description.** ECHOREPO enables discovery and reuse of European citizen-science soil observations, laboratory measurements, photographs and microbial biodiversity summaries through an interactive geographical interface and versioned machine-readable data contracts.

**Users/use cases.** Soil scientists and ecologists can select records by geographic and environmental criteria; agronomists can explore soil property patterns; computational researchers can retrieve harmonised datasets and reproducibly link observations. These are intended use cases, not evidence of adoption by independent disciplines. The pinned canonical publication covers 26 EU country codes and 10,558 identifiers; see RO4_1_PHASE3_RESULTS.md. Avoid presenting this as live service size or representative sampling.

**Interfaces and access.** Public discovery at https://echorepo.quanta-labs.com/explore and /search. Programmatic access base https://echorepo.quanta-labs.com/api/v1 ; /ping, /canonical/samples, /canonical/samples/count, /canonical/sample_images, /canonical/sample_parameters, /canonical/sample_biodiversity, /canonical/map.geojson, /canonical/all.zip. Read-only JSON/CSV/GeoJSON and canonical bundle are the proposed external offering. Some routes require authorized access; current public availability is deployment-configurable, do not promise anonymous bulk/API access. Keep imports, operator snapshots, admin endpoints and personal data outside scope. Docs: docs/ECHOREPO_API_Data_Guide_v4.md (older baseline; verify against current deployment). Versioned public data via https://doi.org/10.5281/zenodo.19722513 ; pinned assessment https://doi.org/10.5281/zenodo.23784504 . Distinguish live and immutable data.

**Technical basis.** Flask/PostgreSQL, object storage, Keycloak/OIDC, Docker Compose. MIT software, curated data licences separate. Public-code repo https://github.com/echosoil/echorepo-lite . R7-1 functional validation, R7-3 Phase 2 all 70 sequential requests passed (including map, search and bounded JSON/CSV); Phase 4 verifies seven canonical publication files and six CSVW tables under selected checks. Phase 3 security and Phase 5 usability are bounded evidence. No formal EOSC AAI, independent capacity or SLA assessment.

**Classification.** Prefer one combined service for web and API initially; if Node determines the data source should be onboarded separately, assess resource registration independently and do not count it as a service automatically. Decide whether a separate machine-facing API offers an independently managed service, not merely a second interface.

## B. ECHOREPO Research and Analytical Environment (S05)

**Short description.** A soil-oriented research workspace offering exploratory notebook workflows and interactive analysis of canonical data, supporting reproducible examination of soil properties and microbial biodiversity.

**Users/use cases.** Researchers, students and authorized project collaborators; country comparisons, soil-property distributions, exploratory biodiversity relationships, data-quality review. User guide: notebooks/README.md; templates for overview, soil properties, biodiversity. Live API data may change; reproducible studies should identify a frozen release and parameters.

**Access.** Existing research interfaces /research/workspace/ and /research/dashboard/ on the documented production origin. Authorized access, subject to current policy and capacity; actual current availability and account isolation require re-verification. No public promise of unlimited compute, persistent private storage, or free external accounts before approval.

**Technical basis.** Existing Jupyter and Streamlit Compose services; Python API client, curated workflows. R7-3 Phase 3 records operator-confirmed authorized workspace access on 24 November. Separate JupyterHub pilot (jupyterhub/README.md) is not proof of deployed multi-user isolation. Before exposing to a broader community, verify independent user workspaces, least-privilege API access, storage and egress policy, resource quotas, cleanup, monitoring, account lifecycle and secure reverse-proxy boundary. Five pilot sessions is not proof of capacity for 50 concurrent users. Use controlled staging/authorized test accounts; keep detailed configuration private. Consider EOSC Interactive Notebooks integration using portable templates/data access as an alternative to hosting compute, but a workflow resource is not automatically an independently integrated service.

**Decision.** Candidate for a separate research service when verified. Ask Node about local versus Node-hosted execution; assess current maturity and support before submission.

## C. FAIR publication workflow (S03) — defer as standalone

Automates canonical/raw biodiversity and derived microbial releases using Zenodo, explicit review, checksums and DOI history; demonstrated FAPROTAX production DOI https://doi.org/10.5281/zenodo.24572403 . Existing scripts and tools are maintainer-operated. Could become reusable documented publication tooling for authorized citizen-science data providers, with explicit privacy and rights review. Currently better described as capability of A, or as software/research workflow resource. Do not count Zenodo itself as an ECHO service. Do not expose privileged tokens or make automatic publication public.

## D. Soil-data quality validation (S04) — defer as standalone

Existing tools/r73_data_quality.py performs structural and selected domain-sanity checks on canonical data. Earlier R7-3 assessment recorded 11,329 IDs in a different local snapshot, not the pinned publication. Potential standalone reusable validator would require public documented input contracts, safe bounded execution, no unauthorized retention, interpretable rule reports and independent users/support. Existing internal audit tool is not evidence of a production service. May publish reusable software/workflow instead; assess eligibility separately.

## Common draft catalogue fields / approval checklist

| Field | Proposed value or required decision |
|---|---|
| Provider | Quanta Systems, S.L. is software maintainer; legal contributor and consortium delegation to confirm |
| Project | ECHO, Horizon Europe 101112869 |
| Scientific domain | Environmental and agricultural soil science, biodiversity; interdisciplinary users to evidence |
| Countries | European scope; actual user-country usage to evidence |
| Language | English and multilingual discovery interface; actual service support languages to confirm |
| Licence | MIT code; versioned datasets under their published licences, live access policy to approve |
| Access | Non-commercial offering proposed; exact eligibility, registration and quotas to approve |
| Operations | Owner, support channel, maintenance, availability target, capacity plan, incident escalation, exit/continuity plan: OPEN |
| Infrastructure | Hosting region, sovereignty, processors and lawful governance: OPEN |
| Security and federation | Existing OIDC; EOSC AAI compliance and Node-specific integration: OPEN |
| Catalogue status | Draft internal description only; no identifier or integration claimed |

All proposed values must be reviewed by the accountable provider. No confidential security findings or personal account details should be placed in a public catalogue.
