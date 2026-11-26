# RO4-1 — derived microbial datasets and interim evidence

Assessment date: 27 November 2026. This records operator-supplied preparation reports and publication output, not an independent new network or scientific validation run. Retain the original Phase 1/2 artifacts. The governing ECHO D1.4, Table 7, p. 11 (https://echosoil.eu/wp-content/uploads/2025/04/D1.4.pdf) describes RO4-1 in terms of harmonised EU-wide ECHO data supporting EUSO soil perspectives relevant to EU soil policy, CAP, zero pollution and SDGs. Ten DOI-identified and harvested collections was a working proxy, not a confirmed contractual number.

| Collection | Concept DOI suffix | Version DOI suffix | Status |
|---|---|---|---|
| Canonical | 19722513 | 23784504 | Published; SoilWise capture |
| Microbial source data | 22014976 | 22014977 | Published; operator reported SoilWise FOUND 25 Nov |
| Laboratory validation | 23987337 | 24003680 | Published; operator reported SoilWise FOUND 25 Nov; format diagnostic NOT_ASSESSED |
| FAPROTAX derived profiles | 24572402 | 24572403 | Production v1.0 published by operator; independent public read and SoilWise discovery pending |
| FUNGuild derived profiles | — | — | Candidate prepared; reference terms pending |

DOI prefix 10.5281/zenodo. Four distinct published soil-data candidates, three with existing SoilWise evidence of differing strength. No direct EUSO indexing or downstream CSVW processing demonstrated. Do not count Sandbox publications, duplicate versions or unrelated evidence archives. Preserve the October canonical snapshot (10,558 distinct IDs, 26/27 EU member state codes) and the earlier validation format limitations.

## Derived numerical evidence

| Metric | FAPROTAX | FUNGuild |
|---|---:|---:|
| Sample labels | 2730 | 2182 (all observed) |
| Functions / observed macro guilds | 57 | 15 |
| Profile rows | 155610 | 26313 |
| Median retained-read / reconstructed-read annotation percent | 41.53 | 45.49 |

FAPROTAX: percentages of present retained OTUs per overlapping predicted function, separate retained-read coverage. 362 sample profiles sum above 100%, six all-zero. Historical filter settings and exact source lineage not established. FUNGuild: accepted-confidence genus mapping to one macro guild, no display cutoff; denominator all positive reconstructed ITS reads; reference snapshot SHA-256 4031fea05ed979fe714167f04d2afd2266ef74ab4a6050eafe54b9e4275ac8cd, exact reference version unknown. Live reconstruction is not an immutable snapshot. Neither result directly measures microbial activity; do not assert derivation from the pinned canonical DOI.

Both candidates use percentage schema v2; three CSVs plus README, CSVW and provenance. The release workflow checks candidate fingerprints, binds exact files/metadata/target to plan hash, stages before publishing, checks remote metadata and file sizes/MD5, and persists publication state. Sandbox concept 10.5072/zenodo.649428 had versions 649429 and 649717; no pending operation after v1.1. Production publication reported as published: https://zenodo.org/records/24572403, plan SHA-256 48e6b505a05f4a46948251f1bbf5a42564cac4431a7de127675bed285360533e; content SHA-256 a3b5e67e5fa753d46d1465385df6136bcefffd8ab2b4848e8a9f4772e09b99f5. Both awards EC 101112869 and UKRI 10068004 included in reviewed metadata. FUNGuild maintainers contacted; CC BY 4.0 remains intended for derived output, not a statement that reference rights are cleared. Do not bypass reference-review blocker; an authorized release decision is required.

## Remaining

Record dated exact DOI/title catalogue captures, independently read the production record and verify file checksums, retain approval and source limitations, resolve FUNGuild terms and publish if authorized. Confirm scope/counting with consortium. The qualitative policy-use and EUSO aspects require domain and external evidence beyond structural interoperability. Continue RO4-4 separately. No existing public deposition is modified.
