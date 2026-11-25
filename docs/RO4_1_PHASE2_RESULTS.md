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


## Refresh for a newer published canonical version

The October version remains historical evidence; do not overwrite its statistics. Use the established `scripts/update_zenodo_bundle.sh` publication workflow, first with `--dry-run --save-prepared-dir` in a private directory. Review the freshly prepared files, schema checks, metadata warnings and privacy/publication authorization before publishing. The normal publication run downloads the source again; verify the exact new published bytes separately. Do not publish a new concept solely to update the count. Check the current deposition state and production/sandbox configuration without exposing credentials.

After publication, record the exact new version record ID (not the concept ID). Run from a checkout containing this change:

    OUT="$PWD/ro41-evidence/canonical-refresh-$(date -u +%Y%m%dT%H%M%SZ)"
    python -m tools.ro41_coverage --ack yes-public-readonly \
      --canonical-record-id NEW_VERSION_RECORD_ID \
      --commit "$(git rev-parse HEAD)" --out "$OUT/coverage.json"

The reader verifies the canonical concept lineage (19722513), the version DOI and samples.csv checksum before deriving counts. A new publication does not inherit evidence of external catalogue discovery or full resource/CSVW validation from the older release; verify those separately. Live counts and published counts can differ even after a refresh.


## Validation subset targeted diagnostic (25 November 2026)

Operator report at 18:29:04 UTC, reporting revision `5428925ee76374c5e6543a281d4a10c7febf6705`. Both resources again match published byte sizes and checksums, DOI resolution matches. The UTF-8 comma reader finds 133 data rows, 17 columns, zero inconsistent row widths and seven unnamed header positions; headers are not unique overall. QR identifier and pH/texture pairs match the expected names; neither expected organic-carbon name matches. Semicolon and tab yield one column each and no recognized expected fields. Original assessment remains NOT_ASSESSED / ValidationFormatError. This is evidence of a machine-readability issue, not proof of a damaged file or invalid scientific results. The unnamed columns' occupancy and exact uniqueness of nonempty headers were not measured in that report.

The follow-up reader permits unambiguous named-field analysis even with unnamed columns, but rejects duplicate named headers, ambiguous delimiters and inconsistent widths. It reports per-unnamed-column nonempty cell counts only; no cell values, sample IDs or guessed mappings are emitted. Missing organic-carbon header names remain unassessed. If unnamed columns or missing expected names are present, the record remains REVIEW even if selected coverage metrics can be computed. No data changes or new Zenodo version are required for this bounded assessment.

The operator elected to retain the pinned October canonical deposition to keep KPI evidence aligned. Do not replace its historical statistics with live portal counts. Retain this diagnostic report separately and repeat only the validation record after the follow-up change. Actual success on the published bytes and scientific interpretation remain to be verified.


## Validation subset final targeted diagnostic

The operator's report dated 2026-11-25T18:37:27.748083+00:00, reporting revision `2e5e506cbf91c262e028affe6371f458548a3847`, confirms that all seven unnamed columns have zero nonempty cells. Ten named columns remain, but the named headers are not unique. All 133 rows have consistent widths. The two expected organic-carbon header strings still do not match; their absence as exact names does not prove the underlying measurements are absent. DOI resolution and both original resource sizes/checksums again match. Detailed QR uniqueness and paired measurement coverage remain NOT_ASSESSED. Retain this report and the prior diagnostics; no further parsing workaround or deposition update is required for the current bounded evidence review. Recommend correcting column names and removing empty columns in a future authorized version. Three distinct soil-data candidates remain published and discoverable in SoilWise (canonical capture plus two operator-reported checks); the proposed numerical target and formal scope still require consortium confirmation.

Proceed with derived microbial publication preparation as a separate task; see [Phase A](guild_publication_phase_a.md).
