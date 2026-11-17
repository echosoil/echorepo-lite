# Streaming biodiversity import (large files)

This document describes the **server-side** import introduced on branch
\`feature/streaming-biodiversity-large-import\`. It is intentionally separate
from the web \`/lab-import-auto\` endpoint.

## Why a separate importer?

The current web route performs \`uploaded_file.read()\` and the existing MinIO
archiver constructs a compressed ZIP in memory. A 25 GB file therefore cannot
be processed safely by the browser/Flask request route. Upload limits, request
timeouts, the worker lifetime, application RAM and duplicate MinIO storage are
all issues.

The new CLI reads an existing file **directly from a path on the server**.
It does **not** upload a source ZIP to \`biodiversity/raw/\`; only processed
biodiversity data and provenance go to PostgreSQL. There is **no need to split
the abundance table into small import files**.

Use a bind-mounted or otherwise accessible volume for the input. A 25 GB input
does not need to fit on the small Docker root volume if it is mounted externally.

## Two supported input formats

### Mode A: already joined ECHOREPO matrix

A CSV, TSV or XLSX whose header includes \`OTU ID\`, marker-suffixed samples and
the appropriate taxonomy columns:

\`\`\`sh
cd ~/ECHO-STORE/echorepo-lite-dev
python3 -u tools/import_biodiversity_large.py \
  --file /mnt/lab/biodiversity_16S_merged.tsv

# After checking the plan:
python3 -u tools/import_biodiversity_large.py \
  --file /mnt/lab/biodiversity_16S_merged.tsv \
  --apply
\`\`\`

This assumes the source was already filtered according to your laboratory
preprocessing rules. If it is **unsplit** and you explicitly want the importer
to apply the 10-read threshold across the entire file, add \`--post-min10\`.
Do **not** enable this for already split parts.

### Mode B: original OTU table and separate taxonomy

For the original input pair:

\`\`\`text
table_16S_dn_97.tsv
taxa_16S_97.txt
\`\`\`

Run from the repository root (where your production/development \`.env\` is
configured):

\`\`\`sh
python3 -u tools/import_biodiversity_large.py \
  --table /mnt/lab/table_16S_dn_97.tsv \
  --taxonomy /mnt/lab/taxa_16S_97.txt \
  --marker 16S \
  --scratch-dir /mnt/lab/scratch
\`\`\`

The default mode is a **plan only**. After reviewing it:

\`\`\`sh
python3 -u tools/import_biodiversity_large.py \
  --table /mnt/lab/table_16S_dn_97.tsv \
  --taxonomy /mnt/lab/taxa_16S_97.txt \
  --marker 16S \
  --scratch-dir /mnt/lab/scratch \
  --fail-on-missing-taxonomy \
  --apply
\`\`\`

For an ITS table change \`--marker 16S\` to \`--marker ITS\`.

This source mode:

- Reads the taxonomy TXT file into a **temporary SQLite index** under the
  scratch directory, then removes the index after the import.
- Hashes both source files in fixed-size buffers for a stable upload identity.
- Streams each line of the original TSV, retaining only valid ECHOREPO sample
  columns (controls are excluded as in the existing preprocessor).
- Calculates each OTU's read total over the **entire unsplit matrix** and skips
  OTUs below 10 reads by default (\`--min-total-reads\` overrides this).
- Joins taxonomy by OTU ID as each row is processed; no joined 25 GB CSV is
  materialized.
- Reuses the current main-branch taxonomy QC, sparse raw storage, Phylum
  aggregation, current-source replacement and chart invalidation.
- Preserves original TSV source-row and sample-column numbers in the structured
  raw provenance tables.
- Writes **no raw file to MinIO**.

The temporary index may require some extra disk space depending on the size
of \`taxa_*.txt\`. Point \`--scratch-dir\` at a sufficiently large volume (not
the small root filesystem). The large TSV itself is **not copied**.

## What is stored?

The existing tables are retained:

\`\`\`text
biodiversity_uploads
biodiversity_raw_samples
biodiversity_raw_features
biodiversity_raw_abundance
sample_taxon_abundance
\`\`\`

For newly imported archive-free sources,
\`biodiversity_uploads.archive_object_name\` is the empty string. The
\`upload_id\` and \`sha256\` columns still contain a 64-character digest:

- Single-file mode: SHA-256 of the original input file.
- Joined-pair mode: deterministic SHA-256 of a versioned fingerprint containing
  both source-file hashes, the marker and the minimum-read setting. Thus the
  digest is a composite **source-pair identity**, not the SHA-256 of either
  individual raw file.

If a byte-identical upload was previously archived, a re-import preserves its
existing nonempty archive reference rather than discarding it.

**The original lab files must be independently backed up.** PostgreSQL stores
the structured *filtered* data, not an exact copy of every rejected OTU or
every original file byte. Archive-free sources cannot later be reconstructed
byte-for-byte by the old MinIO backfill script.

Existing MinIO objects are **not** deleted automatically. Cleaning older
\`biodiversity/raw/\` archives should be a separate, reviewed, backup-aware
operation.

## Safety and resource requirements

1. The command is **plan-only** unless \`--apply\` is supplied; plan mode checks
   the header/arguments, not every source row.
2. The existing importer performs the import in **one PostgreSQL transaction**;
   failure rolls it back, but the transaction can be long-lived and generate
   significant PostgreSQL WAL, indexes and temporary files. **25 GB of TSV does
   not imply that 25 GB of free database storage will suffice.** Check database
   disk headroom before running.
3. Do not edit either input file during a run. File size and modification time
   are checked before commit.
4. On rerun with the same inputs the content-addressed \`upload_id\` is reused;
   raw rows for that upload are replaced transactionally. Do not run imports
   concurrently for overlapping sample IDs.
5. The importer logs progress every 50,000 OTU source rows.
6. Large files are read from the host filesystem; avoid copying them inside a
   container. If running inside Docker, mount the original directory read-only.
7. Check the selected \`.env\` and database host carefully: this command writes
   to whatever PostgreSQL database its connection configuration selects.
8. Do the first real test with a **small, genuine unsplit sample dataset** in
   development and compare its results with the existing importer before
   processing the full 25 GB dataset.

## Downstream images

- 16S Phylum charts continue to use \`sample_taxon_abundance\`.
- Bacterial guild plots still require rebuilding the current FAPROTAX input
  files and running the R step before images can be regenerated.
- ITS fungal guild plots can now read the structured current raw genus/count
  data from PostgreSQL for archive-free source uploads. Historical uploads
  with MinIO archives retain the existing archive-based path.

After the import, verify the current source counts:

\`\`\`sql
SELECT UPPER(marker) AS marker,
       COUNT(DISTINCT sample_id) AS current_samples,
       COUNT(DISTINCT source_upload_id) AS current_uploads
FROM sample_taxon_abundance
GROUP BY UPPER(marker);
\`\`\`

Then run the existing image regeneration pipeline as appropriate.

## Caveats

The web endpoint still uses its legacy in-memory MinIO archive flow.
The CLI is the supported path for multi-GB inputs. PostgreSQL storage capacity,
write-ahead logs and bulk-insert throughput remain the operational limits. For
extreme data sizes a further phase could use \`COPY\`-based staging and an explicit
job-state table for resumable ingestion; this change does **not** claim to
provide resumable partial commits.
