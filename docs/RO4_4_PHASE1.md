# RO4-4 — EOSC integration: scope and candidate inventory

Assessment initiated 27 November 2026; source checkout 0c8a1be1dddaa99627a1a1bc27e662395cdde652. No new deployment, EOSC submission, external account action or production test performed. This is a consortium-wide outcome indicator, with an ECHOREPO technical contribution.

## Governing wording and counting

ECHO D1.4, Table 7, printed p. 11 (31 May 2024), states **"+4 services integrated into the European Open Science Cloud (EOSC)"**. Source: https://echosoil.eu/wp-content/uploads/2025/04/D1.4.pdf. Preserve +4 (four additional services), not automatically >4; reconcile any different M40 reporting threshold with the consortium and agree baseline/acceptance rule. Previously reported zero is not evidence that no partner integration exists. Current evidence register: zero verified distinct formally integrated ECHOREPO services; consortium-wide total NOT_ASSESSED.

Proposed conservative counting: distinct ECHO services with a named provider, independent research use, stable access and documented onboarding/acceptance in the EU Node or another approved Federation Node, evidence of production availability and applicable technical/operational compliance. Count each distinct service once, not each route, dataset, DOI, software component, external dependency or catalogue copy. Submission and technical validation are milestones, not completed integration. Confirm the rule with KPI owner. If the consortium interprets integration differently, document the authoritative rule and assess separately.

## Current official pathway

The EU Node contributor pathway permits eligible legal entities to contribute non-commercial resources/services. Register via an authorized representative in Contributors Dashboard; eligibility review precedes service submission; approved submissions proceed through technical onboarding and policy/operational checks before production. An existing Federation Node may also onboard services. Do not assume the legacy Marketplace submission process is current. Eligibility of the proposed provider and free-to-use offerings, sustainability and responsibilities must be established, especially for a commercial legal entity providing non-commercial services.

Sources (reviewed 27 Nov 2026):
- https://open-science-cloud.ec.europa.eu/contributors/node-onboarding
- https://open-science-cloud.ec.europa.eu/contributors
- https://open-science-cloud.ec.europa.eu/support/frequently-asked-questions
- https://eosc.eu/building-the-eosc-federation/eosc-federation-handbook (second edition, 30 Jan 2026, Ch. 6)

The FAQ distinguishes contributing research outputs via OpenAIRE Graph from direct service/resource onboarding. A Zenodo dataset or SoilWise hit may be useful FAIR evidence; it is not by itself a formally integrated ECHO service. Existing Keycloak or PostgreSQL is not EOSC federation. No claim that an EOSC identity integration is mandatory for every service; obtain applicable requirements after onboarding review.

## Candidate inventory (not yet eligible to count)

| ID | Candidate | Existing technical evidence | Key gap / decision |
|---|---|---|---|
| S01 | ECHOREPO soil discovery web portal | Public Explore/search, R7-1 and R7-3 browser and performance evidence | Stable service description, access, support, provider, EOSC acceptance; determine whether S01/S02 are one offering |
| S02 | Soil data API and machine-readable exports | R7-1 API tests; R7-3 Phase 2 bounded JSON/CSV/count checks; canonical CSVW | External API docs, authentication/access terms, sustainability, catalogue classification, formal integration |
| S03 | FAIR publication and DOI bridge | Canonical/raw biodiversity and staged guild publishers; production Zenodo DOI evidence | Internal maintainer workflow today; demonstrate separate externally usable service or package with S02. Do not count Zenodo itself. |
| S04 | Citizen soil data quality assessment | tools/r73_data_quality.py; R7-3 structural checks | Internal audit tool today; separately accessible reusable validation workflow, documented inputs/outputs and operational owner before proposing as service |
| S05 | Research analytical workspace | Existing Jupyter and Streamlit; R7-1 research tests; docker-compose.jupyter.yml and docker-compose.streamlit.yml | Verify production access, isolation, resources, terms and support; JupyterHub README is a pilot, not production proof. Treat dashboard/notebooks as one candidate until separate eligibility justified. |

Priority: S01/S02 public discovery/access and S05 research workspace, subject to avoiding duplication and resolving access/security. S03/S04 are credible capabilities but less mature as standalone services. Five candidates does not imply five qualifying integrations. No service catalogue ID, acceptance ticket or production onboarding proof has been supplied.

## Evidence register / Phase 2

For each consortium candidate record: service name and identifier, provider legal entity and authorized representative, description and distinct users/use case, public URL and API/docs, access and costs, licence/terms, privacy/security, support/contact, monitoring/usage, sustainability, EOSC Node and onboarding path, submission ID/status, acceptance evidence, catalogue ID/link and dated operational access test. Retain only sanitized evidence; no tokens, personal details or private user data. Reuse R7-1 and R7-3 exact evidence with dates, without reclassifying software tests as EOSC integration.

First ask consortium/KPI owner whether a Contributor account or partner Node is already registered, whether any services have been accepted, and who owns each offering. If none, initiate authorized provider registration/eligibility discussion through the official Contributor dashboard and confirm whether the portal/API should be a single service. Use official technical guidance after approval; do not change authentication or production infrastructure merely to anticipate unknown requirements. Prepare S01/S02/S05 service dossiers and gap assessment in Phase 2; retain correspondence and review state. Do not submit without authorized representative approval.

Related evidence: docs/R7_3_PHASE2_RESULTS.md; docs/R7_3_PHASE4_RESULTS.md; docs/R7_3_PHASE5_RESULTS.md; docs/guild_zenodo_release.md; docs/RO4_1_PHASE3_RESULTS.md; jupyterhub/README.md; tools/r73_data_quality.py. No formal KPI achievement declared.
