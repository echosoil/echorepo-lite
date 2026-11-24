# R7-3 Phase 4 — external interoperability

## Scope and evidence rules

Pin the public canonical record 23784504 (v12) and 95-sample biodiversity pilot 22014977 (v1). Do not generate or publish a new export. Both public landing pages were observed on 24 November 2026, showing six canonical CSVs + file.json and biodiversity_raw.zip + file.json respectively. DOI resolution, public API records and file integrity are separate observations. Record discovery does not prove interpretation of CSVW or scientific reuse.

## 1. Run the bounded automated assessment

From the development checkout and existing Python environment (requests required):

    cd ~/ECHO-STORE/echorepo-lite-dev
    git switch develop
    git pull --ff-only origin develop
    OUT="$PWD/r73-evidence/phase4-$(date -u +%Y%m%dT%H%M%SZ)"
    (
      umask 077
      mkdir -p "$OUT"
      python -m tools.r73_interoperability --ack yes-public-readonly \
        --commit "$(git rev-parse HEAD)" \
        --download-public-files --out "$OUT/interoperability.json"
    )
    cat "$OUT/interoperability.md"

No tokens, Docker builds, uploads, DB access, LOESS refresh or API exports. Sequential GETs, TLS validation, explicit host allowlist, bounded redirects, one-second request spacing, 80 MiB aggregate decoded HTTP limit, 32 MiB individual public files, 160 MiB uncompressed biodiversity archive cap. The timeout/budget is not a hard server cancellation guarantee. Records total about 29 MB of public files at the observed versions. No raw data is written to disk. Omit --download-public-files for record-level-only observations. Review NOT_ASSESSED/REVIEW explicitly; exit zero signifies report creation, not all checks passing.

The tool verifies repository inventory, license presence (not appropriateness), DOI resolution, published checksums and exact downloaded CSV structure, metadata-declared SHA-256/row counts/size, scalar propertyUrl and counts of URI annotations. Biodiversity CSVW URLs are resolved to members of the pinned ZIP, not assumed to be separate public files. It does not implement the full CSVW specification or validate all values or ontology terms. A missing metadata field is a review observation, not fabricated. The public LOESS overlay is checked structurally; no upstream refresh or image downloads.

## 2. External catalogue browser evidence

Use the following exact links and record date, matching title, DOI/version, visible source repository and outcome. Save a cropped screenshot of record content (not just address bar), or an exported public metadata record, locally under the evidence directory. A JavaScript shell returning 200 is not a successful discovery check.

- SW01: https://catalogue.soilwise-he.eu/resource?id=10.5281%2Fzenodo.23784504
- EDP01: https://data.europa.eu/data/datasets/oai-zenodo-org-23784504?locale=en
- Search by exact version DOI if the permalink does not work. Record a different DOI/version separately; do not silently substitute a concept DOI. If inaccessible or no match, mark REVIEW or NOT_ASSESSED, not a pass.
- LOESS02: open https://echorepo.quanta-labs.com/explore, enable LOESS overlay, verify markers, one popup, attribution and an available image. Do not capture sample identifiers or precise positions in publication evidence.

Record the observations in a Markdown file alongside the automated report. Suggested fields: check ID, UTC date, environment, status, record title, DOI, public URL, evidence filename, limitations. Keep automated and manual evidence separate.

## 3. Downstream semantics and EUSO

Reference SoilWise documentation:
- https://soilwise-he.eu/repository-road-map/
- https://soilwise-he.github.io/soil-observation-data-encodings/CSVW/
- https://soilwise-he.github.io/soil-observation-data-encodings/COMMON/Converters/
- https://soilwise-he.github.io/soilwise-architecture/

The roadmap describes staged infrastructure handover for EUSO. Do not infer independent EUSO indexing of this DOI or an actual data-transfer path merely from project-level integration. Ask SoilWise/JRC for a record-specific permalink or API result, mapping/provenance identifying the source, whether file.json is ingested or only linked, and whether a column property URI is exposed downstream. A local consumer test can prove local interpretation, not catalogue ingestion. Do not submit data to external conversion services without separate authorization. The tool leaves SW01, EDP01, EUSO01, SEM01 and LOESS02 NOT_ASSESSED until evidence is collected.

## 4. Outputs and follow-up

Retain interoperability.json and interoperability.md, plus manual record captures and operator correspondence. The reporting commit identifies the tooling, not deployed software. Verify current published dataset separately if a later version is released. Reuse evidence for compatibility criteria R73-C02–C04, RO4-1, RO4-4 and WI5-2 as applicable, not as automatic acceptance of those KPIs. Update the quality matrix only after actual results. No formal certification or full scientific harmonization claim.

Offline tests: python -m pytest -q tests/test_r73_phase4.py
