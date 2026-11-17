# Large biodiversity uploads through the API (develop)

This workflow uses the current ECHOREPO importer without sending multi-gigabyte
files through the browser. The new API endpoint requires an API key and accepts
**one already-preprocessed sample-column part per request**. The recommended
transfer format is `.csv.gz`: compressed on the local machine and decompressed
in the worker as a stream. Plain CSV is still supported. It does not accept
multipart/form-data. The actual database import runs in the dedicated
`biodiversity-worker` container, so the upload request can return immediately
after transfer without waiting for a long PostgreSQL transaction.

## Why split by sample columns?

For ECHOREPO's *current-source-per-sample-and-marker* model, each uploaded file
must contain **all retained OTUs for the sample IDs in its columns**. The next
file must contain a disjoint set of sample IDs. If you split by **OTU rows**,
a later upload for the same QR sample replaces its earlier phylum aggregate,
so the earlier OTU rows are no longer current.

Each complete part therefore contains:

- The **same full set of retained OTUs** (including their taxonomy).
- A **different subset of QR-code columns**.
- Controls omitted and the global minimum-10-reads filter applied across all
  original sample columns *before* splitting.

The splitter has `--list-parts` and `--only-part N` modes. In one-part mode
it scans the original TSV on every invocation to preserve the correct global
filter; this minimizes local output storage but performs extra work.

### Prepare on the local computer

For `table_16S_97.tsv` + `taxa_16S_97.txt`, using the *develop PR version*:

```bash
python tools/prepare_split_biodiversity_for_echorepo_premin10.py \
  --table table_16S_97.tsv \
  --taxonomy taxa_16S_97.txt \
  --output biodiversity_16S_import.csv \
  --marker 16S \
  --max-output-mb 2000 \
  --compression gzip \
  --fail-on-missing-taxonomy \
  --list-parts \
  --report-json biodiversity_16S_parts_plan.json
```

Read the number of parts. For the first part:

```bash
python tools/prepare_split_biodiversity_for_echorepo_premin10.py \
  --table table_16S_97.tsv \
  --taxonomy taxa_16S_97.txt \
  --output biodiversity_16S_import.csv \
  --marker 16S \
  --max-output-mb 2000 \
  --compression gzip \
  --fail-on-missing-taxonomy \
  --only-part 1
```

This creates **`biodiversity_16S_import_part001.csv.gz`**, compressed directly
in a streaming writer—no uncompressed intermediate file on disk. The
**2,000,000,000-byte cap is calculated on the uncompressed CSV** before
compression, so the transferred gzip file is normally considerably smaller.
`--list-parts` reports *uncompressed upper-bound estimates*; the actual
compressed byte count is only known when a part is generated. For very small
or incompressible files, gzip can add a little overhead; the splitter also
checks the actual resulting file against `--max-output-mb`.

Gzip uses a deterministic timestamp and no embedded source filename, so
re-running `--only-part 1` on unchanged source data produces exactly the same
compressed bytes and SHA-256. To retain the original plain CSV workflow,
omit `--compression gzip`.

## Upload directly from the local computer

Deploy the revised app, the new migration and the `biodiversity-worker`
service to your **development** environment first. The authenticated API
endpoint is:

- `POST /api/v1/biodiversity/uploads` for a raw file body.
- `GET /api/v1/biodiversity/uploads/<job_id>` for status.

Both endpoints require `X-API-Key` and **do not accept web sessions or
bearer tokens**. The configured `BIODIVERSITY_UPLOAD_API_KEY` takes
precedence if set; otherwise your existing `API_KEY` is accepted. Prefer a
separate key in production: the existing API_KEY may have been shared with
read-only data consumers, whereas this endpoint **writes to PostgreSQL**.

Using Bash or Git Bash:

```bash
# Prefer entering the key interactively, not putting it in shell history.
read -rsp "ECHOREPO biodiversity upload key: " BIODIV_KEY
echo

FILE=biodiversity_16S_import_part001.csv.gz
BASE_URL=https://YOUR-DEV-HOST

curl --fail-with-body --show-error \
  -X POST "$BASE_URL/api/v1/biodiversity/uploads" \
  -H "X-API-Key: $BIODIV_KEY" \
  -H "X-Filename: $FILE" \
  -H "Content-Type: application/gzip" \
  --data-binary "@$FILE"
```

A successful upload returns HTTP **202**:

```json
{
  "ok": true,
  "job_id": "uuid-from-server",
  "status": "queued",
  "filename": "biodiversity_16S_import_part001.csv.gz",
  "size_bytes": 123456789,
  "sha256": "64-character-hex-value",
  "status_url": "/api/v1/biodiversity/uploads/uuid-from-server"
}
```

Track the result:

```bash
curl --fail-with-body --show-error \
  -H "X-API-Key: $BIODIV_KEY" \
  "$BASE_URL/api/v1/biodiversity/uploads/JOB_ID"
```

Statuses: `queued`, `running`, `succeeded` or `failed`. Only transfer
the next part after **succeeded**. The server's staging file is deleted after
import success or failure. A failed job retains its error text in PostgreSQL;
you can correct the issue and reupload the local part.

The endpoint accepts raw `.csv`, `.tsv`, `.txt`, `.csv.gz`, `.tsv.gz` or
`.txt.gz`, but not XLSX and not
`curl -F file=@...`. It requires an explicit `Content-Length` and imposes
an upload size limit (default 2.2 GB, on **compressed transfer bytes**). Files
larger than the limit are
rejected without being queued.

### Deployment configuration and disk

The application and worker must share the **same writable** staging volume:

```dotenv
BIODIVERSITY_API_MAX_UPLOAD_BYTES=2200000000
# Expansion limit for gzip parts while importing, not an additional disk copy.
BIODIVERSITY_MAX_UNCOMPRESSED_BYTES=8000000000
BIODIVERSITY_UPLOAD_STAGING_DIR=/data/biodiversity/incoming
# Strongly recommended if API_KEY is distributed:
BIODIVERSITY_UPLOAD_API_KEY=YOUR_SEPARATE_UPLOAD_SECRET
```

In the standard Compose setup, both services mount `./data:/data`.
The production Compose override runs the worker using the same UID (1001)
as the web application. The worker uses a global PostgreSQL advisory lock
to ensure only **one importer** is running at a time.

Rebuild and bring up the development services (adjust project directory and
compose filenames to match your deployment):

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml \
  up -d --build echorepo-lite biodiversity-worker
docker compose -f docker-compose.yml -f docker-compose.dev.yml \
  logs -f biodiversity-worker
```

The new `20261117_001_biodiversity_import_jobs.sql` migration is applied
by the normal application startup migration runner. The worker waits for
this table to become available.

For a gzip part, the staging disk needs room for **one compressed upload**
plus normal filesystem overhead. The database has its own storage, WAL and
indexes; it may need **much more space** than the staged CSV. No 25 GB
intermediate matrix, decompressed CSV, or original-file MinIO ZIP is created. Keep the
original files backed up on your local system.

### Reverse proxy requirements

An Nginx instance with the usual small `client_max_body_size` limit will
reject multi-GB requests *before Flask sees them*. You must configure the
proxy on the **development hostname** to allow this particular API endpoint,
for example:

```nginx
location = /api/v1/biodiversity/uploads {
    client_max_body_size 2300m;
    proxy_request_buffering off;
    proxy_http_version 1.1;
    proxy_read_timeout 900s;
    proxy_send_timeout 900s;
    proxy_pass http://YOUR_EXISTING_APP_UPSTREAM;
}
```

**Adapt** `proxy_pass` to the working upstream on your host, and preserve
whatever `proxy_set_header` rules your existing site requires. Without
`proxy_request_buffering off`, Nginx may first write another full-size
request-body copy to disk, defeating the storage goal.

The import itself is asynchronous *after the upload completes*: the HTTP
request lasts only for file transfer, not for the PostgreSQL import. This
does not implement resume for a disconnected/incomplete HTTP upload; retry
that part if the transfer fails.

### What happens after the last part?

Each part reuses the importer from `echorepo/services/biodiversity_import.py`:

- It validates ECHOREPO sample-marker columns and taxonomy.
- It stores sparse nonzero counts in
  `biodiversity_raw_abundance`, with feature/sample provenance.
- It replaces phylum-level `sample_taxon_abundance` only for the
  current part's sample-marker pairs.
- It preserves the full filter from the local splitter: do **not**
  apply `__POSTMIN10__` again to individual parts.
- It invalidates existing MinIO piechart/guild images for affected samples.

It **does not** archive the uploaded source in `biodiversity/raw/`.
Historical archives are left untouched. The original lab file stays under
your control; filtered PostgreSQL tables are not a bit-for-bit backup.

Once every part has succeeded, run the existing FAPROTAX/R preparation for
16S (if applicable), then regenerate affected images using the usual
`generate_biodiversity_piecharts.py` workflow. Do not regenerate
cross-sample functional profiles halfway through a split import.

**Current limitation:** each import uses one PostgreSQL transaction; the
queue recovers jobs interrupted by a worker restart, but large transactions
can produce substantial WAL and require adequate free database space.
Test with one smaller development part and inspect the output counts before
processing 2 GB parts.
