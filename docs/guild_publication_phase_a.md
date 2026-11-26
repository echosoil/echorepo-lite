# Derived microbial dataset publication — Phase A

Source audit at develop revision `2e5e506cbf91c262e028affe6371f458548a3847`, 25 November 2026. This is preparation for two independent derived-data collections, not a Zenodo publisher or a scientific acceptance decision. Existing canonical and biodiversity/validation depositions remain unchanged. The new analyses need their own exact source identity; never assert derivation from the October canonical release without proof.

## Source-code audit

| Component | Current behavior | Publication implication |
|---|---|---|
| tools/faprotax_16S_functional_profile.R | Streaming annotation and OTU aggregation; file 8 R row-name CSV with fraction of present retained OTUs per function, rounded to six decimals; zero-signal functions omitted | Rename sample column explicitly, preserve fractions and overlap; never normalize the sum to one. |
| Same R script | File 9 has Sample, Annotated_frac (four decimals), Annotated_pct (two decimals); denominator retained reads, numerator reads assigned at least one function | Separate read coverage table; cannot infer read counts from rounded fractions. Zero retained reads produce zero coverage, not evidence of a measured zero. |
| Same R script | files 8 and 9 written via temporary files and replaced sequentially; no guaranteed joint atomic manifest | Validate label sets and bytes; matching sets do not prove same computation. Capture exact run provenance at the next calculation. |
| tools/generate_biodiversity_piecharts.py | FAPROTAX input generation from current structured raw records; defaults prevalence >=2 and total reads >=50, configurable | Preserve actual run settings; defaults are not proof of the existing files' settings. |
| Same Python script | Bacterial plot uses selected functions aggregated into citizen-friendly guilds; overlapping assignment may sum above 100% | Publish individual functions before display aggregation. |
| Same Python script | FUNGuild source reconstruction uses current upload provenance, historical MinIO archives or structured raw tables; legacy only where no current provenance; inaccessible current archives excluded | Numerical export must enumerate expected, reconstructed and failed source samples, not quietly substitute old data. |
| Same Python script | Genus matching; accepts Probable, Highly Probable and documented misspelling; chooses primary mapped guild, highest confidence, first encountered on equal score | Document mapping/tie rule and version; review ties for reproducibility. |
| Same Python script | Fungal macro guild counts / all positive reconstructed ITS reads, coverage before hiding guilds <1%; no standalone numerical CSV | Extract a shared unfiltered calculation for publication; keep plot cutoff separate. Include unassigned reads and zero/unavailable distinctions. |
| tools/create_funguild_db_once.sh | Local reference JSON downloaded from external endpoint; no pinned release identifier or checksum in script | Record local hash, provenance and terms before use. Do not redistribute database content until permitted. |
| tools/publish_biodiversity_to_zenodo.py and scripts/publish_biodiversity_staged.sh | Saved source, dry run, metadata generation, reviewed publication, remote verification | Reuse low-level Zenodo operations and staged pattern, not the raw-matrix-specific schema or state. |

## Local audit (no network access)

Run from the updated development checkout, with an explicit host-side data directory. If analysis outputs are stored elsewhere, adjust the data directory. The script does not read .env, connect to PostgreSQL/MinIO, run R, generate PNGs or create Zenodo drafts. Do not run while another process is rewriting the files. Standard Python only.

    cd ~/ECHO-STORE/echorepo-lite-dev
    git switch develop
    git pull --ff-only origin develop
    OUT="$PWD/ro41-evidence/guild-audit-$(date -u +%Y%m%dT%H%M%SZ)"
    (
        umask 077
        python3 -m tools.guild_publication_audit \
          --data-dir "$HOME/ECHO-STORE/echorepo-lite/data/biodiversity" \
          --commit "$(git rev-parse HEAD)" --out "$OUT/guild-audit.json"
    )
    cat "$OUT/guild-audit.md"

Send the two aggregate reports. They include row and column counts, normalized label uniqueness/alignment, fraction ranges and overlap statistics, read annotation coverage distribution, reference inventory, file sizes and fingerprints, and source input availability/header alignment. No sample labels or individual data values are emitted. Default hashes cover the small result/reference files; optional --hash-inputs streams the potentially very large input matrix and taxonomy files. A matching hash or modification timestamp does not establish the processing history. OBSERVED is inspection, not acceptance. No source data changes.

## Proposed Phase B/C publication design

FAPROTAX: sample-by-function fractions or explicit long form, separate coverage, methodology and source manifest. Preserve absence of signal, filter settings, and rounding. Capture exact R/microeco/FAPROTAX versions, original input hashes, source sample/feature set, script revision, processing date and run parameters. Existing outputs cannot supply all these facts retrospectively; missing provenance requires review or controlled regeneration. Export taxonomy-to-function mapping only if available and approved; do not fabricate it from fractions.

FUNGuild: use existing matching/mapping and unfiltered positive read counts to generate long-form sample/guild count and fraction, sample totals/assigned/unassigned counts and fractions, matched genus/assignment confidence where authorized, explicit missing-source/zero-data cases and versioned category definitions. Keep the citizen-friendly macro grouping distinct from the original FUNGuild annotation. Validate conservation of reads within the reconstructed denominator; tests must ensure rare guilds are retained in data but hidden only in plots. Consider bounded processing from a frozen source snapshot rather than materializing all source OTUs in memory. Record reference checksum and acquisition/version metadata; do not redistribute reference database without permission.

For both: source manifest, README, CSVW table descriptions and checksums, privacy review of identifiers, separate Zenodo state and logs, staged preparation with immutable hashes and explicit publication approval. Verify exact DOI and SoilWise discovery after release. The pipeline should not update existing depositions. No publication before scientific/provenance review.
