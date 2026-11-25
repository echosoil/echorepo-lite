# RO4-1 — scope and public publication inventory

Assessment initiated 24 November 2026. Working interpretation from earlier project KPI discussions: harmonised EU-wide citizen-science soil datasets delivered; proposed measure at least ten DOI-identified datasets published on Zenodo and harvested by SoilWise or EUSO. Confirm the governing KPI wording and numerical acceptance rule with the consortium before declaring attainment. Do not silently replace the outcome with a publication-count proxy.

## Counting policy (proposed)

Count distinct eligible dataset collections once, grouped by concept DOI or verified concept record ID; report version-specific DOI count and file/resource count separately. A new version is not automatically a new dataset. Independently created concept DOIs can still represent redundant copies and require review. Do not count documents, software, validation evidence archives, metadata-only sidecars, or unrelated grants as soil observation datasets merely to meet a number. Pilot and derivative datasets require scope, scientific utility and provenance review. A project-wide output inventory can include datasets that are not eligible for RO4-1. Report wider ECHO datasets separately. The preliminary table of citizen-science initiatives is an example needing scope review.

A qualifying entry needs agreed scientific scope, persistent DOI, published accessible data, documented harmonisation/provenance and record-specific SoilWise or EUSO harvesting/discovery evidence. European Data Portal/OpenAIRE evidence is useful but not a substitute. A catalogue search hit proves discoverability, not that the accompanying CSVW file was consumed or that the data are scientifically harmonised.

## Initial evidence carried forward

| Record | Preliminary scope / evidence |
|---|---|
| https://zenodo.org/records/23784504 | Canonical v12, open dataset; R7-3 Phase 4 verifies file integrity, CSVW structure, and SoilWise discovery of this specific version; EUSO unverified |
| https://zenodo.org/records/22014977 | Microbial source-data example, open dataset; R7-3 verifies archive integrity/CSVW; confirm sample coverage, distinct eligibility, and external discovery |
| https://zenodo.org/records/10218825 | Preliminary table of citizen-science initiatives; ECHO-funded dataset search candidate; scope review |
| https://zenodo.org/records/24485700 | R7-1 validation evidence archive; do not count as citizen-science soil observations |

Search also the ECHO project community (https://zenodo.org/communities/echo_project/records) and grant number 101112869 across Zenodo. Earlier ECHOREPO release DOIs are candidates for grouping, not automatically distinct datasets.

## Run Phase 1

Use an existing Python environment with requests installed; Windows audit checkout or Ubuntu development checkout. No production deployment, token, DOI registration, upload or file downloads.

    cd ~/ECHO-STORE/echorepo-lite-dev
    git switch develop
    git pull --ff-only origin develop
    OUT="$PWD/ro41-evidence/phase1-$(date -u +%Y%m%dT%H%M%SZ)"
    python -m tools.ro41_inventory --ack yes-public-readonly \
      --commit "$(git rev-parse HEAD)" --out "$OUT"
    cat "$OUT/inventory.md"

For additional known public record IDs, append repeatable --record-id ID arguments. The tool queries community, grant-text, structured grant and ECHOREPO title discovery separately; results may be incomplete, the structured query may not be supported or indexed, and the broad search may return unrelated records. Anonymous search page size is 25, capped at eight pages per query; public reader uses bounded response sizes, TLS, request pacing and allowlisted redirects. Review partial/error states; no live network calls in CI.

Outputs: inventory.json (metadata and group provenance), inventory.md (summary), versions.csv (one row per observed version), dataset-review.csv (one row per dataset candidate group, initially REVIEW). Use a new timestamped directory each run. Never overwrite original evidence. CSVW filename presence is not semantic verification. Do not infer distinct concepts where identifiers are missing or contradictory.

## Next phases

1. Review dataset-review.csv against actual scientific scope; choose exact version DOI per qualifying concept. Confirm duplicate concepts and pilot coverage with data owners.
2. Verify DOI resolution, resource availability, licensing, harmonisation, coverage by country and data quality for eligible records. Reuse R7-3 Phase 2 and 4 evidence only for matching snapshots/versions.
3. Capture exact DOI/title, catalogue source and dated evidence for SoilWise or EUSO. Record NOT_ASSESSED if no authoritative evidence is available. Reuse the R7-3 canonical screenshot. Do not fabricate external records.
4. Compute separate distinct publication, version and qualifying harvested counts after review. Compare against agreed KPI target; decide if additional scientifically justified releases are needed.
5. Produce final validation report and evidence archive. Do not publish duplicate or arbitrary country slices solely to inflate counts.

Offline tests: python -m pytest -q tests/test_ro41_inventory.py

References: https://developers.zenodo.org/ ; https://zenodo.org/help/versioning ; docs/R7_3_PHASE4_RESULTS.md ; docs/R7_3_PHASE2_RESULTS.md.
