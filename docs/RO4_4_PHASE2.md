# RO4-4 — service onboarding readiness and action register

Assessment date: 27 November 2026. Repository baseline: 389d1874bbae0b956f30e7876caa9ae01d4fd87e (develop, includes Phase 1). Source/policy review only; no fresh production tests, contributor registration or EOSC application submitted. This is a preparation record, not an eligibility declaration. Consortium-wide integration total remains NOT_ASSESSED; zero ECHOREPO services are verified as formally integrated in available evidence. Confirm KPI baseline and the +4 interpretation with the consortium.

## Authoritative policy and route

Use the EOSC EU Node Resources and Services Onboarding Policy v1.0, §§3.1–3.3, 4 and Annex A:
https://open-science-cloud.ec.europa.eu/support/resources-and-services-onboarding-policy
and the Contributor guide v2.0:
https://open-science-cloud.ec.europa.eu/support/documentation/contr-dashboard-onboarding-user-guide

Section 3.2 is stricter than the general Phase 1 overview. Direct EU Node service eligibility includes TRL >=7, non-commercial access, EOSC AAI and integration-pattern compliance, documented FOSS architecture, recognised service-management practices, accountable support, compliant sovereign infrastructure, and cross-disciplinary/cross-country value. Capacity expectation is up to 500 daily/50 simultaneous users, not a measurement of ECHOREPO. Obtain applicable availability thresholds and integration guidance from the Node. A data source is a different resource category under §3.3.1, and a research product is not itself an integrated service.

For a narrower thematic scope the policy prioritises an appropriate Federation Node. Data Terra is a particularly relevant Earth/environment/biodiversity route: https://eosc.eu/building-the-eosc-federation/eosc-node-data-terra . The EOSC Association records production status approval on 19 November 2026: https://eosc.eu/building-the-eosc-federation/eosc-federation-build-up-group/eosc-node-coordinators-committee-2 . Ask about onboarding an external ECHO service; do not assume acceptance, identical policy, or affiliation. Consider an existing partner Node first if the consortium has one. The Spain national Node was still described as a future proposal in the October 2026 announcement; do not rely on it as an operational pathway.

## Gap register

Statuses: SUPPORTED = bounded relevant evidence, PARTIAL = insufficient for policy declaration, OPEN = no adequate evidence, DECISION = provider/Node must agree. None means formal EOSC compliance.

| ID | Policy area | Evidence and current status | Closure evidence / proposed owner |
|---|---|---|---|
| G01 | Legal contributor and rights (§3.1/3.2) | DECISION. MIT source copyright/maintainer Quanta Systems, S.L.; does not establish authority over consortium data, service ownership or long-term accountability. | Consortium and legal representative designate provider and confirm rights, free access, authority and sustainability. |
| G02 | Scope (§3.1/3.2a) | PARTIAL. 26/27 EU country codes in pinned publication; soil, agriculture, ecology use cases proposed. Country coverage is not user/discipline evidence. | Document actual user groups and distinct cross-disciplinary use cases; ask Node to classify. |
| G03 | Access (§3.2b) | PARTIAL. Public discovery, protected downloads/research; API access configuration differs by route. | Publish approved access policy, registration process, costs (no fees for direct EU Node offer), data licence and restrictions. |
| G04 | Maturity (§3.2c) | PARTIAL. 77/77 mapped R7-1 checks; 70/70 sequential R7-3 requests; actual operational evidence, no formal TRL rating. | Demonstrate each offering in operational environment, confirm deployed revision and scope. |
| G05 | Authentication (§3.2c) | PARTIAL. Keycloak/OIDC and access control tested; not EOSC AAI verified. | Obtain agreed Node integration pattern, map identity/account lifecycle and access rules; test in non-production environment. |
| G06 | Service management (§3.2c) | OPEN. Existing operational documentation and maintenance practices, no FitSM/ISO 20000 alignment assessment. | Adopt proportionate service register, incident/change/release procedures, escalation, continuity, support and service-level reporting; assess against agreed standard. |
| G07 | Security/privacy (§3.2c,4) | PARTIAL. R7-3 bounded security and privacy observations; not comprehensive GDPR or security compliance. | Approved risk assessment, privacy/data-controller review, access isolation, vulnerability and incident response procedures. |
| G08 | Capacity/availability (§3.2c) | OPEN. Sequential latency test is not load/availability evidence; research pilot limit five sessions. | Measure uptime and resource use; test capacity on representative staging with approval, set documented operational limits, agree capacity and SLA with Node. No production stress testing. |
| G09 | Software/docs (§3.2d) | SUPPORTED for MIT source licence and documentation existence; PARTIAL for completeness. | Versioned public API/schema, deployment and management docs, dependency licences and user guide; remove any credential-bearing examples. |
| G10 | Sovereign hosting (§3.2d) | OPEN. Actual hosting region, jurisdiction and supplier/data-processing terms not established. | Obtain authorized hosting inventory and region/provider attestation; evaluate sovereignty with Node. |
| G11 | Continuity/support (§3.1,4) | OPEN. Project-funded development; no approved post-project commitments or support SLA evidenced. | Name owner, support channel, staffing/funding and exit plan; agree responsibilities and targets. |
| G12 | Integration/acceptance | OPEN. No acceptance identifier or service catalogue entry. | Store Node/provider/service identifiers, acceptance date, production endpoint, dated use test and reviewer confirmation; retain confidential correspondence privately. |

## Candidate disposition

See [service dossiers](RO4_4_SERVICE_DOSSIERS.md). Recommend discovery and data access as one initial offering (S01+S02), unless Node confirms independent service classification. Separate analytic environment (S05) only when research access/isolation and capacity are verified. Publication and QA (S03/S04) remain internal capabilities, not separate eligible services today. No artificial splitting to reach four. Ask partners about already integrated services before committing to new development.

## Action sequence

1. Ask KPI owner to confirm count/baseline, partner services, provider authority and preferred Node. Designate authorised representative; decide who can commit long-term operation. No credentials or personal data in Git.
2. In parallel send a pre-application inquiry to EOSC EU Node helpdesk or Data Terra: confirm classification, applicable route, provider eligibility, AAI/management/capacity and acceptance evidence. Send only approved public information. Current UI: User Space > account menu > Join as Contributor; use Save Contributor as Draft if preparing, then Publish Contributor only with authorization and accurate declarations. After approval use Switch profile > contributor > Resources > Add Resource. Guide supports saving draft submissions. Do not attest compliance prematurely.
3. Complete G01/G03/G10/G11 organizational evidence and G05/G06/G08 readiness assessment; maintain register. Reuse dated R7-1/R7-3 results rather than repeat. For technical follow-up use bounded public GET checks, ordinary authorized research sessions, and separately approved staging load tests. No changes to production authentication until Node confirms pattern.
4. Submit one agreed service dossier; record review status and guidance; complete required testing, acceptance, and production verification. Repeat for independently justified offerings; ask consortium partners for other services. Do not count draft or submitted items as integrated.

Evidence sources: README.md, LICENSE, docs/ECHOREPO_API_Data_Guide_v4.md, docs/R7_3_PHASE2_RESULTS.md, docs/R7_3_PHASE3_RESULTS.md, docs/R7_3_PHASE4_RESULTS.md, docs/R7_3_PHASE5_RESULTS.md, docs/RO4_1_PHASE3_RESULTS.md, notebooks/README.md, jupyterhub/README.md. These document source or earlier observed behaviour, not new verification. For official scope use the policy above and https://open-science-cloud.ec.europa.eu/contributors/node-onboarding ; alternate Nodes described in https://eosc.eu/building-the-eosc-federation/eosc-federation-handbook .
