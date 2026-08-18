# Publishing ECHOREPO Biodiversity Data to Zenodo

This document describes the controlled workflow for publishing the current ECHOREPO microbial biodiversity dataset to Zenodo.

The workflow is deliberately split into checkpoints so that the expensive biodiversity export is generated only once, can be inspected before publication, and can be reused if later validation or Zenodo upload steps fail.

## 1. Published data model

The public scientific biodiversity dataset is exported as wide OTU/feature-by-sample count matrices.

The generated biodiversity bundle contains:

```text
biodiversity_16S.csv
biodiversity_ITS.csv
biodiversity_taxonomy.csv
biodiversity_metadata.json
```

The Zenodo publisher extracts the three CSV resources and generates a SoilWise/CSVW-compatible `file.json`:

```text
biodiversity_16S.csv
biodiversity_ITS.csv
biodiversity_taxonomy.csv
file.json
```

### `biodiversity_16S.csv`

Example:

```csv
OTU ID,taxonomy_id,AACW-5934-16S,AASP-7043-16S,...
ce9224208dbe866d4058dccb9551d44e,123,552,276,...
```

### `biodiversity_ITS.csv`

Same structure, but for ITS samples:

```csv
OTU ID,taxonomy_id,AAUU-9633-ITS,ABYU-1769-ITS,...
13caaca077e645ab6e7e5b109485ec8b,942,5050,3640,...
```

### `biodiversity_taxonomy.csv`

`taxonomy_id` identifies one complete taxonomic lineage:

```csv
taxonomy_id,kingdom,phylum,class,order,family,genus,species
123,Bacteria,Pseudomonadota,Alphaproteobacteria,Hyphomicrobiales,Xanthobacteraceae,Bradyrhizobium,Bradyrhizobium_elkanii
942,Fungi,Ascomycota,Dothideomycetes,Capnodiales,Capnodiales_fam_Incertae_sedis,Capnodiales_gen_Incertae_sedis,Capnodiales_sp
```

The export therefore preserves the original scientific information:

```text
OTU/feature ID
+ sample-level read counts
+ complete taxonomy
```

without publishing the old very large long-form `biodiversity_abundance.csv`.

## 2. Publication workflow

Use the staged publication wrapper:

```bash
./publish_biodiversity_staged.sh
```

The workflow has three phases:

```text
PostgreSQL
    |
    | Phase 1: export
    v
biodiversity_raw.zip
    |
    | checkpoint + manual inspection
    v
Phase 2: prepare / dry run
    |
    v
3 CSV files + file.json
    |
    | checkpoint + manual inspection
    v
Phase 3: publish
    |
    v
Zenodo new version
```

The important rule is:

> Do not regenerate the biodiversity bundle for every retry.

After Phase 1 succeeds, all later steps should use the same saved ZIP via `--source-file`.

---

## 3. One-time configuration

### 3.1 Zenodo environment file

The default environment file is:

```text
.env_zenodo_biodiversity
```

It should contain the credentials and configuration used by the Zenodo publisher.

At minimum, verify the ECHOREPO and Zenodo credentials required by your installation.

Do not commit tokens or API keys to Git.

### 3.2 Existing Zenodo record

If this publication is an update of an existing biodiversity Zenodo record, the publisher **must** receive:

```text
--existing-deposition-id <ID>
```

Without this option, the publisher creates a new independent Zenodo deposition instead of a new version of the existing record.

It is recommended to store the ID in the private environment file:

```bash
BIODIVERSITY_ZENODO_EXISTING_DEPOSITION_ID=12345678
```

and add the following to `publish_biodiversity_staged.sh` after `common_args` is created:

```bash
if [[ -n "${BIODIVERSITY_ZENODO_EXISTING_DEPOSITION_ID:-}" ]]; then
    common_args+=(
        --existing-deposition-id
        "$BIODIVERSITY_ZENODO_EXISTING_DEPOSITION_ID"
    )
fi
```

This prevents accidentally forgetting the ID during the final publication command.

The previous successful publication can also be checked in:

```text
data/zenodo_biodiversity_sync_log.csv
```

For example:

```bash
python3 - <<'PY'
import csv

path = "data/zenodo_biodiversity_sync_log.csv"

with open(path, encoding="utf-8", newline="") as f:
    rows = list(csv.DictReader(f))

successful = [
    row for row in rows
    if row.get("status") == "ok"
]

if not successful:
    print("No successful publication found")
else:
    row = successful[-1]
    for key in (
        "run_at_utc",
        "deposition_id",
        "record_id",
        "version_doi",
        "concept_doi",
        "zenodo_html",
    ):
        print(f"{key}: {row.get(key, '')}")
PY
```

Use the existing publication's deposition ID when creating a new version.

---

## 4. Pre-publication checks

Before generating the bundle, verify that the application is running the latest biodiversity importer/exporter code.

Useful checks:

```bash
git status
git log -1 --oneline
```

Check that the Python modules compile:

```bash
python3 -m py_compile \
    echorepo/services/biodiversity_import.py \
    echorepo/services/biodiversity_raw_exports.py
```

If these modules are baked into the production Docker image, rebuild/restart production before exporting:

```bash
./start-prod.sh
```

### 4.1 Check current structured-raw coverage

The public exporter reconstructs matrices from the current structured raw biodiversity data.

A useful PostgreSQL check is:

```sql
WITH current_samples AS (
    SELECT DISTINCT
        UPPER(sample_id) AS sample_id,
        UPPER(marker) AS marker,
        source_upload_id AS upload_id
    FROM sample_taxon_abundance
    WHERE source_upload_id IS NOT NULL
),
raw_matches AS (
    SELECT DISTINCT
        c.sample_id,
        c.marker
    FROM current_samples AS c
    JOIN biodiversity_raw_samples AS r
      ON r.upload_id = c.upload_id
     AND UPPER(r.sample_id) = c.sample_id
     AND UPPER(r.marker) = c.marker
)
SELECT
    c.marker,
    COUNT(*) AS current_sample_marker_pairs,
    COUNT(r.sample_id) AS pairs_with_structured_raw,
    COUNT(*) - COUNT(r.sample_id) AS missing_structured_raw
FROM current_samples AS c
LEFT JOIN raw_matches AS r
  ON r.sample_id = c.sample_id
 AND r.marker = c.marker
GROUP BY c.marker
ORDER BY c.marker;
```

The expected result is:

```text
missing_structured_raw = 0
```

for both 16S and ITS.

---

## 5. Phase 1 — generate the biodiversity bundle

First inspect status:

```bash
./publish_biodiversity_staged.sh status
```

Then start the controlled export:

```bash
./publish_biodiversity_staged.sh export
```

The export should run in a separate Docker container with explicit CPU and memory limits rather than inside the production Gunicorn request.

Typical conservative limits are:

```bash
EXPORT_CPUS=1.0 \
EXPORT_MEMORY=4g \
./publish_biodiversity_staged.sh export
```

If 4 GB is insufficient, increase only the exporter container:

```bash
EXPORT_CPUS=1.0 \
EXPORT_MEMORY=6g \
./publish_biodiversity_staged.sh export
```

Do not remove resource limits simply to make the export finish faster.

### 5.1 Monitor resource usage

In another terminal:

```bash
docker stats \
    echorepo-biodiversity-export \
    echorepo_prod-postgres-1
```

Also monitor host memory and disk:

```bash
watch -n 5 'free -h; echo; df -h'
```

### 5.2 Expected Phase 1 output

The staged workflow should preserve the generated bundle in a persistent work directory, for example:

```text
biodiversity_zenodo_work/
├── 01_source/
│   ├── biodiversity_raw.zip
│   └── biodiversity_raw.zip.sha256
├── 02_prepared/
├── checkpoints/
│   └── 01_export.ok
└── logs/
    └── 01_export_YYYYMMDD_HHMMSS.log
```

The exact base directory depends on `WORK_DIR`.

Prefer a disk with sufficient free space. Do not use a small Docker root filesystem for large temporary biodiversity data.

---

## 6. Inspect the generated bundle

Do not continue immediately to Zenodo.

Verify the archive:

```bash
unzip -t "$WORK_DIR/01_source/biodiversity_raw.zip"
```

List resources:

```bash
unzip -l "$WORK_DIR/01_source/biodiversity_raw.zip"
```

Expected files:

```text
biodiversity_16S.csv
biodiversity_ITS.csv
biodiversity_taxonomy.csv
biodiversity_metadata.json
```

Inspect the headers:

```bash
unzip -p "$WORK_DIR/01_source/biodiversity_raw.zip" \
    biodiversity_16S.csv | head -3
```

```bash
unzip -p "$WORK_DIR/01_source/biodiversity_raw.zip" \
    biodiversity_ITS.csv | head -3
```

```bash
unzip -p "$WORK_DIR/01_source/biodiversity_raw.zip" \
    biodiversity_taxonomy.csv | head
```

The two matrix headers should have the form:

```text
OTU ID,taxonomy_id,<sample-marker>,<sample-marker>,...
```

Check the saved hash:

```bash
sha256sum "$WORK_DIR/01_source/biodiversity_raw.zip"
```

Keep this ZIP unchanged between prepare and publish.

---

## 7. Phase 2 — dry run and prepare Zenodo resources

Run:

```bash
./publish_biodiversity_staged.sh prepare
```

This phase must use:

```text
--source-file <saved biodiversity_raw.zip>
--dry-run
--save-prepared-dir <persistent directory>
```

It must **not** call `/biodiversity/raw/all.zip` again.

A dry run:

- extracts the three public CSV resources;
- validates their filenames and schemas;
- validates OTU IDs and `taxonomy_id` references;
- validates sample-marker abundance columns;
- validates read-count values;
- builds `file.json`;
- validates the generated CSVW metadata;
- does not create a Zenodo draft.

The expected prepared directory is:

```text
02_prepared/
├── biodiversity_16S.csv
├── biodiversity_ITS.csv
├── biodiversity_taxonomy.csv
├── file.json
└── SHA256SUMS
```

---

## 8. Inspect the dry-run output

List file sizes:

```bash
ls -lh "$WORK_DIR/02_prepared"
```

Inspect `file.json`:

```bash
python3 -m json.tool \
    "$WORK_DIR/02_prepared/file.json" \
    | less
```

Check hashes:

```bash
sha256sum -c \
    "$WORK_DIR/02_prepared/SHA256SUMS"
```

Inspect matrix headers:

```bash
head -2 "$WORK_DIR/02_prepared/biodiversity_16S.csv"
head -2 "$WORK_DIR/02_prepared/biodiversity_ITS.csv"
```

Check taxonomy:

```bash
head \
    "$WORK_DIR/02_prepared/biodiversity_taxonomy.csv"
```

Useful row counts:

```bash
wc -l \
    "$WORK_DIR/02_prepared/biodiversity_16S.csv" \
    "$WORK_DIR/02_prepared/biodiversity_ITS.csv" \
    "$WORK_DIR/02_prepared/biodiversity_taxonomy.csv"
```

Remember that the first line of each CSV is the header.

---

## 9. Phase 3 — publish to Zenodo

Only publish after Phase 1 and Phase 2 have both been inspected.

The staged wrapper should require an explicit confirmation variable:

```bash
CONFIRM_PUBLISH=YES \
./publish_biodiversity_staged.sh publish
```

If the existing deposition ID is not automatically included from `.env_zenodo_biodiversity`, pass it explicitly:

```bash
CONFIRM_PUBLISH=YES \
./publish_biodiversity_staged.sh publish \
    --existing-deposition-id 12345678
```

### What the publisher does

When `--existing-deposition-id` is supplied, the publisher requests a new version draft of the existing Zenodo deposition.

It then:

1. creates/fetches the new-version draft;
2. obtains the draft bucket;
3. removes inherited files when `--replace-draft-files` is enabled;
4. rebuilds `file.json` with the reserved DOI;
5. uploads the three CSV resources and `file.json`;
6. verifies that the remote Zenodo file list exactly matches the prepared upload list;
7. publishes the deposition;
8. records publication metadata in `data/zenodo_biodiversity_sync_log.csv`.

The normal final Zenodo file list should be:

```text
biodiversity_16S.csv
biodiversity_ITS.csv
biodiversity_taxonomy.csv
file.json
```

The source `biodiversity_raw.zip` is not uploaded unless the publisher is explicitly run with:

```text
--keep-source-archive
```

---

## 10. Verify the publication

After a successful run, inspect the last log rows:

```bash
tail -n 3 data/zenodo_biodiversity_sync_log.csv
```

Or print the most recent successful publication:

```bash
python3 - <<'PY'
import csv

path = "data/zenodo_biodiversity_sync_log.csv"

with open(path, encoding="utf-8", newline="") as f:
    rows = list(csv.DictReader(f))

successful = [
    row for row in rows
    if row.get("status") == "ok"
]

if not successful:
    raise SystemExit("No successful Zenodo publication found")

row = successful[-1]

for key in (
    "run_at_utc",
    "deposition_id",
    "record_id",
    "version_doi",
    "concept_doi",
    "zenodo_html",
    "uploaded_files_json",
    "source_sha256",
):
    print(f"{key}: {row.get(key, '')}")
PY
```

Keep at least the following information for reproducibility:

```text
publication time
source ZIP SHA-256
Zenodo deposition/record ID
version DOI
concept DOI
uploaded file list
```

---

## 11. Failure and restart rules

### Export fails

If Phase 1 fails before `01_export.ok` exists:

- inspect its log;
- correct the problem;
- rerun Phase 1.

An incomplete `.part` file must not be treated as a valid bundle.

### Prepare fails

If the saved ZIP and `01_export.ok` are valid:

> Do not rerun the expensive export.

Fix the metadata/publisher problem and rerun:

```bash
./publish_biodiversity_staged.sh prepare
```

using the same source ZIP.

### Zenodo publish fails

If Phase 1 and Phase 2 succeeded:

> Do not regenerate the biodiversity ZIP.

Investigate the Zenodo draft and publication log first.

Be careful when retrying after a Zenodo draft has already been created. A retry may need to reuse, delete, or otherwise handle the existing draft rather than blindly creating another new version.

### Need to rebuild only preparation

Use the wrapper's prepare-reset action if available, for example:

```bash
./publish_biodiversity_staged.sh reset-prepare
```

The saved source ZIP should remain untouched.

### Data changed after Phase 1

If biodiversity data were imported or corrected after the ZIP was generated, the saved ZIP is stale.

Delete/reset the export checkpoint and regenerate Phase 1 before publishing.

---

## 12. Important operational rules

1. **Never publish directly after an expensive export without inspecting it.**
2. **Generate the biodiversity ZIP once and reuse it.**
3. **Do not repeatedly call the live `/biodiversity/raw/all.zip` endpoint during retries.**
4. **Use CPU and memory limits for the export process.**
5. **Store working files on a filesystem with sufficient free space.**
6. **Use `--existing-deposition-id` when updating an existing Zenodo dataset.**
7. **Run `prepare`/`--dry-run` before every real publication.**
8. **Do not commit Zenodo access tokens or ECHOREPO API keys.**
9. **Keep the source ZIP SHA-256 in the publication log.**
10. **If source biodiversity data change, regenerate the source ZIP before publication.**

---

## 13. Quick reference

Check status:

```bash
./publish_biodiversity_staged.sh status
```

Generate a controlled snapshot:

```bash
EXPORT_CPUS=1.0 \
EXPORT_MEMORY=4g \
./publish_biodiversity_staged.sh export
```

Prepare and validate without Zenodo publication:

```bash
./publish_biodiversity_staged.sh prepare
```

Inspect:

```bash
ls -lh "$WORK_DIR/02_prepared"
```

Publish a new version:

```bash
CONFIRM_PUBLISH=YES \
./publish_biodiversity_staged.sh publish
```

If the deposition ID is not configured in the wrapper/environment:

```bash
CONFIRM_PUBLISH=YES \
./publish_biodiversity_staged.sh publish \
    --existing-deposition-id <EXISTING_ZENODO_DEPOSITION_ID>
```

Verify:

```bash
tail -n 3 data/zenodo_biodiversity_sync_log.csv
```

---

## 14. Relevant repository files

Typical relevant files are:

```text
echorepo/services/biodiversity_import.py
echorepo/services/biodiversity_raw_exports.py
tools/publish_biodiversity_to_zenodo.py
metadata/biodiversity/echorepo_biodiversity_columns.json
publish_biodiversity_staged.sh
data/zenodo_biodiversity_sync_log.csv
```

Some installations may use:

```text
tools/publish_biodiversity_zenodo.py
```

instead of `tools/publish_biodiversity_to_zenodo.py`. The wrapper's `PUBLISHER` setting must match the actual repository filename.

## 15. Why this workflow is staged

The expensive operation is reconstructing the wide 16S and ITS OTU matrices from PostgreSQL and compressing them.

Zenodo metadata generation and upload retries should not force that computation to happen again.

The staged design therefore treats the generated ZIP as an immutable publication snapshot:

```text
current PostgreSQL biodiversity state
              |
              v
      biodiversity_raw.zip
              |
       SHA-256 checkpoint
              |
        +-----+------+
        |            |
     validate       publish
        |            |
        +------------+
```

This provides reproducibility, protects the production service from repeated expensive work, and makes failures easier to diagnose.
