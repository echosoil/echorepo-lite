# RO4-1 — published coverage and external discovery (interim)

Operator-generated coverage report: 2026-11-25T18:04:11.435040+00:00, reporting revision `3aad77237fd02cba10f831560b134bd8490a771a`. Preserve the original coverage.json and coverage.md. This is an interim evidence record, not final acceptance.

All three pinned version DOIs resolved to the expected Zenodo records. Four downloaded resources matched published sizes and checksums (canonical samples CSV, biodiversity ZIP, validation CSV and README). R7-3 Phase 4 separately verified the full canonical and biodiversity inventories and selected CSVW consistency. The validation CSV parsing returned NOT_ASSESSED / ValueError after successful file integrity checks. The cause is not established by the original report.

## Canonical dataset (10.5281/zenodo.23784504)

10,558 rows and 10,558 unique nonempty sample identifiers; zero missing/malformed two-letter country codes. Forty distinct declared codes; 9,888 rows (93.65%) correspond to 26 of the 27 EU Member State codes, with MT absent; 670 rows have non-EU codes. Derived from the published country counts in the original report. Largest counts: IT 1,407; RO 1,109; ES 985; PL 969; PT 936; FI 873; DE 687; GR 673; GB 370; AT 229. Declared codes do not independently verify actual locations, representativeness or country-level scientific comparability. Rare and unexpected codes need geographical QA; do not conflate this pinned publication with later live snapshots.

## Biodiversity (10.5281/zenodo.22014977)

663 16S columns and 664 ITS columns, all with expected marker suffixes. 663 shared suffix-stripped labels; 664 in the union. These are header labels, not independently verified physical soil samples or canonical links. Reconcile the earlier 95-sample example description with actual published contents before the next release. Do not add both marker counts as unique samples.

## External discovery

The canonical version has existing dated browser evidence from R7-3. On 25 November 2026 the operator reported FOUND for 10.5281/zenodo.22014977 and 10.5281/zenodo.24003680 in SoilWise. Retain record-specific browser captures or catalogue exports if available; the two results are operator-reported, not independently captured in this assessment. All three distinct soil-data candidates now have discovery evidence of differing provenance. No claim of direct EUSO indexing, downstream CSVW processing or scientific harmonisation.

## Targeted follow-up

The validation reader now attempts supported delimiters with exact recognized headers, requires consistent row widths and a known QR field plus at least one complete measurement pair, reports sanitized structural diagnostics, and records the detected encoding/delimiter. Decimal-comma counting is restricted to non-comma-delimited input. Original data bytes are not changed. The published CSV has not been independently inspected in this development environment, so the actual cause of the original ValueError and success of the fix remain to be verified.

Run after merging and passing CI, from the development checkout:

    OUT="$PWD/ro41-evidence/phase2-validation-$(date -u +%Y%m%dT%H%M%SZ)"
    python -m tools.ro41_coverage --ack yes-public-readonly \
      --record-id 24003680 --commit "$(git rev-parse HEAD)" --out "$OUT/coverage.json"
    cat "$OUT/coverage.md"

Keep the original complete report and this targeted follow-up separately. No production deployment or new publication. Scientific coverage assessment, formal acceptance scope and additional scientifically justified publications remain follow-up items.
