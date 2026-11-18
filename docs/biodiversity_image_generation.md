# Biodiversity uploads and image generation (16S + ITS)

This is the end-to-end operator guide for the biodiversity image pipeline on the ECHOREPO **develop** branch. It covers importing original BIOM-derived OTU tables, generating 16S and ITS Phylum pie charts, building 16S FAPROTAX predictions, and producing bacterial and fungal ecological-guild plots.

For complete details of the import service, split algorithm, and worker deployment, see [Large biodiversity uploads through the API](biodiversity_api_upload.md), [Streaming biodiversity import](streaming_biodiversity_import.md), [FAPROTAX input streaming](faprotax_streaming.md), and [Memory-bounded FAPROTAX (R)](faprotax_r_streaming.md).

## 1. What is generated?

| Image family | Input | Stored MinIO object |
| --- | --- | --- |
| 16S taxonomy | Current 16S Phylum summary in PostgreSQL | \`biodiversity/piecharts/16S/Phylum/<SAMPLE_ID>.png\` |
| ITS taxonomy | Current ITS Phylum summary in PostgreSQL | \`biodiversity/piecharts/ITS/Phylum/<SAMPLE_ID>.png\` |
| Bacterial ecological guilds | 16S FAPROTAX file 8 | \`biodiversity/guildplots/bacteria/<SAMPLE_ID>.png\` |
| Fungal ecological guilds | Current ITS genus counts + FUNGuild reference database | \`biodiversity/guildplots/fungi/<SAMPLE_ID>.png\` |

A sample with *both* markers can therefore have up to **four** plots. A guild plot is not guaranteed: missing taxonomic assignments, missing reference annotations, or the plotting thresholds can suppress a plot even when the marker's Phylum chart exists. The generator uploads PNGs to the configured \`MINIO_BUCKET\` (normally \`echorepo-uploads\`) and skips existing objects unless told otherwise.

## 2. Choose an ingestion path

**Yes, the API accepts ITS as well as 16S.** The API is marker-agnostic: it validates the sample columns and taxonomy in the supplied file.

- **Recommended for large remote uploads:** convert an original \`table_ITS_dn_97.tsv\` + \`taxa_ITS_97.txt\` (or 16S equivalents) into joined ECHOREPO CSV parts and send each part to \`POST /api/v1/biodiversity/uploads\`. The API does **not** accept the original OTU table and separate taxonomy file as a two-file request.
- **Alternative when the original pair is on the server:** \`tools/import_biodiversity_large.py --table ... --taxonomy ... --marker ITS --apply\` (or \`16S\`). This avoids the API transfer and joined intermediate parts.
- **Legacy/web imports:** older imports may retain original raw ZIP archives in MinIO. The subsequent ITS guild-generation data source depends on the import path.

Both the API importer and the large-file CLI write current Phylum summaries and structured OTU/raw abundance rows into PostgreSQL. **They do not archive the original upload as a ZIP in MinIO.** Keep the original table and taxonomy files backed up separately. This is intentional and compatible with fungal-guild generation: archive-free imports use the structured PostgreSQL raw tables.

### Before importing

1. Work against the intended **development** deployment first. API imports modify PostgreSQL and may invalidate existing image objects for updated samples.
2. Verify that the \`biodiversity-worker\` service and its migration \`20261117_001_biodiversity_import_jobs.sql\` are deployed if using the API.
3. Confirm database/WAL free space, staging-volume capacity, and HTTPS reverse-proxy upload limits. See [API upload guide](biodiversity_api_upload.md#deployment-configuration-and-disk).
4. Preserve all retained OTUs for each sample **within the same part**. Split by **sample columns**, never by OTU rows. A later upload for the same sample and marker replaces the current data for that pair.
5. Finish *all* relevant parts before generating FAPROTAX profiles or the final image collection.

The provided splitter reads BIOM-style TSV headers beginning with \`#OTU ID\` and discards nonmatching controls such as \`BPCR10-ITS\`. Its implemented sample-column pattern is \`[A-Za-z0-9]{4}-[A-Za-z0-9]{4,}-(16S|ITS)\`, which is **more permissive** than a strict four-letter/four-digit (\`ABCD-1234\`) policy. Independently validate IDs if the stricter policy is required for a particular import.

## 3. Prepare an ITS or 16S file for API upload

Perform preparation on the machine holding the **original** files and a checkout of this repository. Example for ITS (change \`ITS\` to \`16S\` and file names for bacterial data):

~~~bash
python3 -u tools/prepare_split_biodiversity_for_echorepo_premin10.py \
  --table table_ITS_dn_97.tsv \
  --taxonomy taxa_ITS_97.txt \
  --output biodiversity_ITS_import.csv \
  --marker ITS \
  --max-output-mb 2000 \
  --compression gzip \
  --fail-on-missing-taxonomy \
  --list-parts \
  --report-json biodiversity_ITS_parts_plan.json
~~~

This first command **plans** the parts without writing them. The splitter applies its default minimum-10-total-reads OTU filter **globally across the original sample columns**, before splitting. Inspect the report and the number of required parts.

Create the first part:

~~~bash
python3 -u tools/prepare_split_biodiversity_for_echorepo_premin10.py \
  --table table_ITS_dn_97.tsv \
  --taxonomy taxa_ITS_97.txt \
  --output biodiversity_ITS_import.csv \
  --marker ITS \
  --max-output-mb 2000 \
  --compression gzip \
  --fail-on-missing-taxonomy \
  --only-part 1
~~~

The output is named \`biodiversity_ITS_import_part001.csv.gz\`. Repeat with \`--only-part 2\`, etc., *after* importing and checking the preceding part. \`--only-part\` rescans the original table to retain the global filtering semantics.

For verified **unquoted** TSV input, \`--fast-tsv\` is optional; it rejects quoted fields. Omit that flag for the general TSV parser. The output-size planning cap is in *uncompressed CSV bytes*; the transferred gzip size is normally smaller.

Do not use \`--post-min10\` to re-filter an already prepared part: that could produce inconsistent source filtering.

## 4. Send each part through the API

On the computer holding the \`.csv.gz\` part, using Bash or Git Bash:

~~~bash
read -rsp "Biodiversity upload API key: " BIODIV_KEY
echo

BASE_URL="https://YOUR-DEV-HOST"
FILE="biodiversity_ITS_import_part001.csv.gz"

curl --fail-with-body --show-error \
  -X POST "$BASE_URL/api/v1/biodiversity/uploads" \
  -H "X-API-Key: $BIODIV_KEY" \
  -H "X-Filename: $FILE" \
  -H "Content-Type: application/gzip" \
  --data-binary "@$FILE"
~~~

The upload endpoint accepts **raw request bodies**, not multipart form uploads (\`curl -F\`). It accepts \`.csv\`, \`.tsv\`, \`.txt\` and their \`.gz\` variants, but not XLSX. Authentication uses \`X-API-Key\`: the dedicated \`BIODIVERSITY_UPLOAD_API_KEY\`, if configured, otherwise \`API_KEY\`; a web login/session or bearer token is not a substitute.

An accepted transfer returns **HTTP 202** with a \`job_id\` and \`status: queued\`. This confirms upload/queuing, **not** successful import.

Check the returned job ID:

~~~bash
JOB_ID="UUID_FROM_THE_202_RESPONSE"
curl --fail-with-body --show-error \
  -H "X-API-Key: $BIODIV_KEY" \
  "$BASE_URL/api/v1/biodiversity/uploads/$JOB_ID"
~~~

Wait for \`status: succeeded\` before uploading the next part. \`queued\` and \`running\` are intermediate states; \`failed\` contains error details. Monitor the \`biodiversity-worker\` logs on the server if necessary. The worker needs the **same staging mount** as the web application (standard Compose uses \`./data:/data\`). The default upload limit is approximately 2.2 GB *of transmitted bytes*, and the reverse proxy must permit the body.

After the final part succeeds, use the same steps for the 16S data if required. **Do not** compute FAPROTAX halfway through a multi-part 16S import.

### Alternative: ingest the original ITS pair directly on the server

If the original files are already accessible from the server, the large-file importer supports a separate taxonomy file:

~~~bash
# Run from the checkout with the intended .env:
python3 -u tools/import_biodiversity_large.py \
  --table /mnt/lab/table_ITS_dn_97.tsv \
  --taxonomy /mnt/lab/taxa_ITS_97.txt \
  --marker ITS \
  --scratch-dir /mnt/lab/scratch

# Review the plan, then actually import:
python3 -u tools/import_biodiversity_large.py \
  --table /mnt/lab/table_ITS_dn_97.tsv \
  --taxonomy /mnt/lab/taxa_ITS_97.txt \
  --marker ITS \
  --scratch-dir /mnt/lab/scratch \
  --fail-on-missing-taxonomy \
  --apply
~~~

The first invocation is plan-only. Change \`--marker ITS\` to \`--marker 16S\` for the bacterial table. This method also populates structured PostgreSQL raw data and does **not** retain a MinIO source ZIP. See [streaming import details](streaming_biodiversity_import.md).

## 5. Configure the image-generation environment

**Run the image-generation commands on the server, from the root of the checkout whose \`.env\` targets the correct database and MinIO bucket.** The generator loads \`.env\` from the **current working directory**, not from wherever the script file lives. Do not execute a development script while standing in the production checkout.

~~~bash
cd ~/ECHO-STORE/echorepo-lite
source venv/bin/activate

# Check relevant nonsecret configuration, without printing credentials:
grep -E '^(PROJECT_ROOT|DB_HOST_OUTSIDE|DB_HOST_INSIDE|DB_NAME|MINIO_BUCKET|FUNGUILD_DB_JSON|FAPROTAX_FUNCTION_CSV|BUILD_FAPROTAX_INPUTS)=' .env
~~~

Use the actual checkout directory for your environment. The log should show the expected \`PROJECT_ROOT\`, database and MinIO endpoint.

Dependencies: the Python environment needs the project's requirements (including pandas, matplotlib, PostgreSQL and MinIO drivers). The FAPROTAX step requires \`Rscript\` and the R packages **microeco** and **Matrix**.

### FUNGuild database: common path pitfall

The ITS guild plotter needs a local reference file named \`FUNGuild_db.json\`, normally:

~~~text
<PROJECT_ROOT>/data/biodiversity/FUNGuild_db.json
~~~

The JSON database maps fungal genera to predicted ecological guilds. It is a **reference** database, not sample-level ITS data.

If your \`.env\` contains:

~~~dotenv
FUNGUILD_DB_JSON=/data/biodiversity/FUNGuild_db.json
~~~

then a **host-side** Python process will look under the host's root directory \`/data\` (not in \`./data\`). That path might only be correct **inside a Docker container**. Either remove this \`.env\` setting so the script uses its project-root default (also \`unset FUNGUILD_DB_JSON\` if exported in your shell), or override it on the command line with:

~~~bash
FUNGUILD_DB_JSON="$PWD/data/biodiversity/FUNGuild_db.json" \
  python3 -u tools/generate_biodiversity_piecharts.py \
  --marker ITS --fungal-guilds --sample-id ZZXF-1776
~~~

Verify that the file actually exists before a large run:

~~~bash
ls -lh data/biodiversity/FUNGuild_db.json
~~~

The script's \`--dry-run\` previews proposed MinIO objects but **does not prove** that a particular guild plot will pass the annotation and abundance filters.

## 6. Generate ITS taxonomy and fungal guild plots

Once **all ITS imports** have succeeded, no R/FAPROTAX step is needed for ITS.

Run a single-sample test first:

~~~bash
BUILD_FAPROTAX_INPUTS=0 \
FUNGUILD_DB_JSON="$PWD/data/biodiversity/FUNGuild_db.json" \
python3 -u tools/generate_biodiversity_piecharts.py \
  --marker ITS --fungal-guilds --sample-id ZZXF-1776
~~~

Then generate every missing ITS chart and eligible fungal guild image:

~~~bash
BUILD_FAPROTAX_INPUTS=0 \
FUNGUILD_DB_JSON="$PWD/data/biodiversity/FUNGuild_db.json" \
python3 -u tools/generate_biodiversity_piecharts.py \
  --marker ITS --fungal-guilds
~~~

This performs **two families** only: ITS Phylum and ITS fungal guilds. The plotter checks MinIO and skips existing PNGs by default. Use \`--force\` **only** when intentionally regenerating/overwriting existing images.

**How fungal source data are resolved:**

- Current sample-to-upload provenance comes from \`sample_taxon_abundance.source_upload_id\` and \`biodiversity_uploads\`.
- For historical uploads with a nonempty \`archive_object_name\`, the plotter reconstructs ITS genus/read counts by retrieving the source ZIP from MinIO.
- For **API/large-file imports** with an empty archive name, the plotter reconstructs ITS genus/read counts from \`biodiversity_raw_samples\`, \`biodiversity_raw_features\` and \`biodiversity_raw_abundance\` in PostgreSQL.
- The legacy \`sample_otu_counts\` fallback is used only for samples without current source provenance.
- A sample with current provenance but an **inaccessible historical archive** is reported as unreconstructed; the current implementation does **not** automatically fall back to PostgreSQL in that situation. Restore the old archive or reimport the current sample's data.

FUNGuild matching then requires genus-level taxonomy and accepted annotations (the generator keeps \`Probable\`/\`Highly Probable\` and an accepted misspelling). A guild below the current **1%** display threshold will not appear.

## 7. Prepare 16S FAPROTAX inputs (Python)

For bacterial guilds, the generator needs the sample-by-function CSV **file 8**. Build it from all current 16S raw PostgreSQL records only **after all 16S parts succeed**.

~~~bash
GENERATE_FUNGAL_GUILDS=0 \
GENERATE_BACTERIAL_GUILDS=0 \
BUILD_FAPROTAX_INPUTS=0 \
python3 -u tools/generate_biodiversity_piecharts.py \
  --marker 16S --build-faprotax-inputs
~~~

This standalone preparation produces:

~~~text
data/biodiversity/faprotax_work/6_otu_clean_counts_no_blanks.csv
data/biodiversity/faprotax_work/7_taxonomy_clean.csv
~~~

The Python builder streams data from PostgreSQL to avoid an expensive full pandas pivot; the resulting file 6 is nevertheless a **dense** OTU-by-sample CSV and needs free disk space. Current defaults use an OTU prevalence filter of at least 2 samples and total read count of at least 50 (configurable through \`FAPROTAX_MIN_PREV\` and \`FAPROTAX_MIN_TOTAL\`).

**Do not combine** \`--build-faprotax-inputs\` and \`--all-images\`. R must run between those phases.

## 8. Run FAPROTAX (R)

From the same repository root:

~~~bash
FAPROTAX_CHUNK_ROWS=128 \
Rscript tools/faprotax_16S_functional_profile.R \
  data/biodiversity/faprotax_work/6_otu_clean_counts_no_blanks.csv \
  data/biodiversity/faprotax_work/7_taxonomy_clean.csv \
  data/biodiversity
~~~

This writes:

~~~text
data/biodiversity/8_faprotax_samples_x_functions.csv
data/biodiversity/9_faprotax_annotated_fraction.csv
data/biodiversity/7_taxonomy_NA.csv
~~~

**File 8** is the input to the bacterial guild plot generator; **file 9** reports the separate read-weighted coverage of FAPROTAX annotations. File 8 stores fractions of *present OTUs* assigned to each predicted function (not directly measured metabolic activity, and not read-weighted fractions). The revised R implementation processes OTUs in bounded chunks and typically needs far less memory than the historical dense-matrix workflow. Tune \`FAPROTAX_CHUNK_ROWS\` (for example, 32 for less memory or 256 for more speed) if needed.

Check that the output exists before generating bacterial guilds:

~~~bash
ls -lh data/biodiversity/8_faprotax_samples_x_functions.csv
~~~

If file 8 lives elsewhere, set \`FAPROTAX_FUNCTION_CSV\` to the actual path. The default is \`<PROJECT_ROOT>/data/biodiversity/8_faprotax_samples_x_functions.csv\`, **not** the \`faprotax_work/output\` directory.

## 9. Generate 16S bacterial guild plots and taxonomy

~~~bash
BUILD_FAPROTAX_INPUTS=0 \
FAPROTAX_FUNCTION_CSV="$PWD/data/biodiversity/8_faprotax_samples_x_functions.csv" \
python3 -u tools/generate_biodiversity_piecharts.py \
  --marker 16S --bacterial-guilds
~~~

This produces any missing **16S Phylum** and eligible **bacterial guild** charts. It skips existing images. The displayed FAPROTAX functional categories can overlap, so their percentages need not sum to 100%.

## 10. Generate *all four* image families in one command

Once the 16S FAPROTAX file 8 is up to date, the ITS data are imported, and the FUNGuild reference JSON is accessible:

~~~bash
BUILD_FAPROTAX_INPUTS=0 \
FAPROTAX_FUNCTION_CSV="$PWD/data/biodiversity/8_faprotax_samples_x_functions.csv" \
FUNGUILD_DB_JSON="$PWD/data/biodiversity/FUNGuild_db.json" \
python3 -u tools/generate_biodiversity_piecharts.py \
  --all-images
~~~

The \`--all-images\` option performs, in order: 16S Phylum, ITS Phylum, bacterial guilds, fungal guilds. The standalone FAPROTAX Python/R preparation **must already be complete**.

Useful variants:

~~~bash
# Preview which objects are missing (does not prove guild eligibility):
BUILD_FAPROTAX_INPUTS=0 python3 -u \
  tools/generate_biodiversity_piecharts.py --all-images --dry-run

# Limit to one sample while testing:
BUILD_FAPROTAX_INPUTS=0 python3 -u \
  tools/generate_biodiversity_piecharts.py --all-images --sample-id ZZXF-1776

# Overwrite all existing images after a deliberate recalculation or redesign:
BUILD_FAPROTAX_INPUTS=0 python3 -u \
  tools/generate_biodiversity_piecharts.py --all-images --force
~~~

The \`--force\` command may regenerate thousands of PNGs. It does **not** rebuild FAPROTAX file 8; rerun the Python preparation and R workflow separately when 16S source data change.

## 11. Verify results and troubleshoot

Expected object paths for an example sample:

~~~text
biodiversity/piecharts/16S/Phylum/ZZXF-1776.png
biodiversity/piecharts/ITS/Phylum/ZZXF-1776.png
biodiversity/guildplots/bacteria/ZZXF-1776.png
biodiversity/guildplots/fungi/ZZXF-1776.png
~~~

These are naming examples, **not** a claim that all four exist for that sample. The web map checks images individually under the public \`/storage/\` route and may omit unavailable families.

| Symptom | Cause / check |
| --- | --- |
| \`FileNotFoundError: FUNGuild DB JSON not found: /data/...\` | Host versus container path mismatch. Check \`FUNGUILD_DB_JSON\` in \`.env\`, shell, and the actual process filesystem. |
| \`No ITS data available for fungal guild plots\` | Check current ITS provenance, archive reconstruction warnings, and structured raw records. A Phylum image alone does not prove genus data are available. |
| \`Current ITS samples linked ...\` but \`reconstructed: 0\` | Historical MinIO archive may have been deleted, or the raw source cannot be parsed. Archive-free API/CLI imports should use PostgreSQL instead. |
| \`No positive ITS rows reconstructed\` | Inspect sample column headers, counts, and source format; a mapped upload is not necessarily parseable. |
| Fungal data reconstructed but no PNG | The selected genera may not match accepted FUNGuild annotations, or the annotated guild percentages fall below 1%. |
| Missing bacterial guild plots | File 8 is absent/stale, the \`FAPROTAX_FUNCTION_CSV\` path is wrong, or no mapped function passes the plot thresholds. |
| PNG still reflects older data | First check current PostgreSQL import/source; then use \`--force\` deliberately if the old MinIO object remains. API imports normally invalidate affected images. |
| Upload returns 202 but charts show old data | Confirm the import job reached \`succeeded\` rather than merely \`queued\`. |
| API returns 401 / 413 / 415 | Verify \`X-API-Key\`; request/proxy size limits; raw body plus allowed content type (not multipart). |
| Generator touches the wrong environment | Run from the correct checkout; it loads \`.env\` from \`Path.cwd()\`. Inspect printed root, DB and MinIO settings. |

A harmless pandas warning about passing a psycopg2 connection to \`pd.read_sql\` does **not**, by itself, mean the import or image generation failed.

### Quick verification of PostgreSQL coverage

Use a SQL client connected to the **intended** database:

~~~sql
SELECT UPPER(marker) AS marker,
       COUNT(DISTINCT sample_id) AS current_samples,
       COUNT(DISTINCT source_upload_id) AS current_uploads
FROM sample_taxon_abundance
GROUP BY UPPER(marker)
ORDER BY marker;
~~~

If you need to troubleshoot one missing fungal chart, prefer a focused run with \`--marker ITS --fungal-guilds --sample-id SAMPLE_ID\` before regenerating everything.

## 12. Operational and scientific notes

- The Phylum pie charts describe observed sequencing read composition. The guild plots are **inferred ecological roles**, not direct assays of metabolic function or pathogenic activity.
- The bacterial FAPROTAX output uses **OTU presence fractions** for its plotted functional categories; individual guilds may overlap.
- FUNGuild assignments are knowledge-based, confidence-filtered annotations of fungal taxa. Version the local \`FUNGuild_db.json\` reference in your analysis provenance (e.g., checksum and acquisition date).
- Avoid overwriting existing outputs casually; compare results after changes to taxonomy, reference annotation versions, importer thresholds, or plotting rules.
- Preserve original external sequencing files: ECHOREPO's structured raw PostgreSQL records are sufficient for many workflows but are **not** bit-identical backups of original lab matrices.
