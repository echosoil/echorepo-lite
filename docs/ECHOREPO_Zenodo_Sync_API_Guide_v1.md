# Publishing API-downloaded files to Zenodo

This document describes how to use the current Zenodo publishing script:

```text
tools/publish_api_file_to_zenodo.py
```

It is based on the current script code, the shell wrappers that load `.env_zenodo`, and the current optional metadata fields implemented in the script.

---

## What the script does

The script:

1. Loads configuration from command-line arguments, environment variables, and an optional `.env_zenodo` file.
2. Downloads a file from an authenticated API endpoint under your ECHOREPO API base URL.
3. Optionally wraps the downloaded file into a new ZIP file.
4. Creates a new Zenodo deposition, or creates a new version draft of an existing deposition.
5. Updates Zenodo metadata.
6. Uploads the file to the Zenodo bucket.
7. Publishes the deposition.
8. Appends a row to a CSV log file.

This script supports both:
- first publication of a dataset
- updates/new versions of an existing Zenodo dataset

If `--existing-deposition-id` is omitted, a new deposition is created.  
If `--existing-deposition-id` is provided, a new version draft is created from that deposition.

---

## Requirements

The script requires Python 3 and `requests`.

Install if needed:

```bash
pip install requests
```

---

## Files involved

Typical related files in the repository:

```text
tools/publish_api_file_to_zenodo.py
scripts/publish_zenodo_bundle.sh
scripts/update_zenodo_bundle.sh
.env_zenodo
.env_zenodo.example
```

---

## Authentication

The script needs credentials for two systems.

### 1. Zenodo

Set:

- `ACCESS_TOKEN`

This is your Zenodo personal access token.

### 2. ECHOREPO API

Use one of:

- `ECHOREPO_API_KEY`
- `ECHOREPO_BEARER_TOKEN`

The script prefers API key if both are provided.

---

## `.env_zenodo`

Example:

```env
ACCESS_TOKEN=myaccesstoken
ZENODO_SANDBOX=true
ECHOREPO_API_KEY=myapikey
ZENODO_API_BASE=https://echorepo.quanta-labs.com/api/v1
ZENODO_API_PATH=/canonical/zenodo_bundle.zip
ZENODO_LOG_FILE=data/zenodo_sync_log.csv
ZENODO_DEPOSITION_ID=123456
ZENODO_TITLE="My Dataset Title"
ZENODO_DESCRIPTION="My Dataset Description"
ZENODO_CREATOR="My Dataset Creator|My Organization|My ORCID"
ZENODO_GRANT=123456789
ZENODO_COPYRIGHT="© 2026 My Soil Project"
ZENODO_KEYWORD="soil,biodiversity,citizen-science"
ZENODO_SUBJECT="Soil science|http://id.loc.gov/authorities/subjects/sh85124022|url"
```

### Notes

- `ZENODO_SANDBOX=true` is used by the shell wrapper to pass `--sandbox`.
- `ZENODO_CREATOR` should use:
  ```text
  Name|Affiliation|ORCID
  ```
- `ZENODO_KEYWORD` may be comma-separated.
- `ZENODO_SUBJECT` should use:
  ```text
  term|identifier|scheme
  ```

---

## How configuration is resolved

The Python script resolves values in this order:

1. command-line argument
2. real environment variable
3. value in `.env_zenodo`
4. built-in default, if any

The shell wrapper separately loads `.env_zenodo` into the shell using:

```bash
set -a
source .env_zenodo
set +a
```

That means shell expressions like:

```bash
"${ZENODO_API_BASE:-https://echorepo.quanta-labs.com/api/v1}"
```

work correctly because `.env_zenodo` is sourced before calling Python.

---

## Main command-line arguments

### Core arguments

- `--env-file`  
  Path to env file. Default:
  ```text
  .env_zenodo
  ```

- `--sandbox`  
  Use Zenodo sandbox instead of production.

- `--api-base`  
  API base URL, for example:
  ```text
  https://echorepo.quanta-labs.com/api/v1
  ```

- `--api-path`  
  Path relative to `--api-base`. Default:
  ```text
  /canonical/zenodo_bundle.zip
  ```

- `--existing-deposition-id`  
  Optional. If provided, the script creates a new version draft instead of a brand-new deposition.

- `--log-file`  
  Path to the CSV log file. Default:
  ```text
  data/zenodo_sync_log.csv
  ```

### Metadata arguments

- `--title`  
  Zenodo record title.

- `--description`  
  Zenodo record description.

- `--creator`  
  Repeatable. Format:
  ```text
  Name|Affiliation|ORCID
  ```
  ORCID is optional.

- `--keyword`  
  Repeatable or comma-separated. Examples:
  ```bash
  --keyword soil --keyword biodiversity
  ```
  or
  ```bash
  --keyword "soil,biodiversity,citizen-science"
  ```

- `--grant`  
  Repeatable grant ID. Accepts either:
  ```text
  101112869
  ```
  or full form:
  ```text
  10.13039/501100000780::101112869
  ```

  If the short form is used, the script normalizes it to:
  ```text
  10.13039/501100000780::<ID>
  ```

- `--subject`  
  Repeatable subject in the form:
  ```text
  term|identifier|scheme
  ```

  Example:
  ```text
  Soil science|http://id.loc.gov/authorities/subjects/sh85124022|url
  ```

- `--copyright`  
  Optional copyright statement. Stored in Zenodo metadata as `notes`, not as a separate native Zenodo field.

- `--communities`  
  Optional list of Zenodo community identifiers.

- `--license`  
  Defaults to:
  ```text
  CC-BY-4.0
  ```

- `--access-right`  
  Defaults to:
  ```text
  open
  ```

- `--version`  
  Optional version string.

### Download filter arguments

These are passed as query parameters to the API endpoint:

- `--from-date`
- `--to-date`
- `--country`
- `--country-code`
- `--bbox`
- `--within`

### Extra query parameters

- `--extra-param key=value`  
  Repeatable. Use this for endpoint-specific query parameters not covered by the built-in filter options.

### File handling arguments

- `--download-name`  
  Override local temporary filename of the downloaded file.

- `--wrap-in-zip`  
  Wrap the downloaded file into a new ZIP before uploading to Zenodo.

- `--zip-member-name`  
  Filename to use inside the wrapper ZIP.

- `--upload-name`  
  Rename the uploaded file before sending it to Zenodo.

---

## Metadata mapping to Zenodo

The script builds Zenodo metadata like this.

### Always included

- `title`
- `upload_type=dataset`
- `description`
- `creators`
- `access_right`
- `license`
- `prereserve_doi=true`

### Included only if specified

- `version`
- `communities`
- `grants`
- `subjects`
- `notes` for copyright
- `keywords`

This design avoids overwriting existing draft metadata unnecessarily during a new-version update.

---

## Creator format

Creators are parsed from:

```text
Name|Affiliation|ORCID
```

Examples:

```bash
--creator "Osychenko, Oleg|Quanta Systems, S.L.|0000-0003-3468-6824"
```

or without ORCID:

```bash
--creator "Osychenko, Oleg|Quanta Systems, S.L."
```

The script sends these fields to Zenodo:

- `name`
- `affiliation`
- `orcid`

It does not send creator roles.

---

## Keywords

Keywords can be supplied either as repeated arguments or as a comma-separated list.

### Repeated form

```bash
--keyword soil --keyword biodiversity --keyword citizen-science
```

### Comma-separated form

```bash
--keyword "soil,biodiversity,citizen-science"
```

### Mixed form

```bash
--keyword "soil,biodiversity" --keyword citizen-science
```

The script splits commas, trims whitespace, and removes duplicates while preserving order.

---

## Grants

Grant IDs can be given in two forms.

### Short form

```bash
--grant 101112869
```

This becomes:

```text
10.13039/501100000780::101112869
```

### Full form

```bash
--grant 10.13039/501100000780::101112869
```

This is sent unchanged.

This is useful for European Commission / Horizon Europe grants.

---

## Subjects

Subjects must be structured as:

```text
term|identifier|scheme
```

Example:

```bash
--subject "Soil science|http://id.loc.gov/authorities/subjects/sh85124022|url"
```

The script sends this as:

```json
{
  "term": "Soil science",
  "identifier": "http://id.loc.gov/authorities/subjects/sh85124022",
  "scheme": "url"
}
```

---

## Copyright

Copyright is optional.

Example:

```bash
--copyright "© 2026 ECHO Horizon Project"
```

The script stores this in Zenodo metadata under `notes`, like:

```text
Copyright: © 2026 ECHO Horizon Project
```

---

## Downloading from API endpoints

The script downloads from:

```text
<api-base><api-path>
```

Examples:

```text
https://echorepo.quanta-labs.com/api/v1/canonical/zenodo_bundle.zip
https://echorepo.quanta-labs.com/api/v1/canonical/all.zip
```

The endpoint must return a file with HTTP 200.

---

## Built-in API filters

The script can send these query parameters:

- `from`
- `to`
- `country`
- `country_code`
- `bbox`
- `within`

Example:

```bash
python3 tools/publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /canonical/zenodo_bundle.zip \
  --country ES \
  --from-date 2025-01-01 \
  --to-date 2025-12-31 \
  --title "ECHOREPO Spain 2025 bundle" \
  --description "Filtered ECHOREPO export bundle" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L.|0000-0003-3468-6824"
```

---

## Using `--extra-param`

For API endpoints that need custom query parameters:

```bash
--extra-param key=value
```

Example:

```bash
python3 tools/publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /some/other/export \
  --extra-param marker=16S \
  --extra-param sample_id=ABCD-1234 \
  --title "Custom export" \
  --description "Custom export for Zenodo" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L."
```

---

## Wrapping the downloaded file in ZIP

If `--wrap-in-zip` is used, the downloaded file is placed inside a new ZIP archive before upload.

This is useful if:
- the endpoint returns a non-ZIP file
- you want a predictable packaged artifact
- you want a custom outer ZIP filename

### Example

```bash
python3 tools/publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /some/report.csv \
  --download-name report.csv \
  --wrap-in-zip \
  --zip-member-name report.csv \
  --upload-name report_package.zip \
  --title "CSV report package" \
  --description "Wrapped CSV export for Zenodo" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L."
```

---

## Publishing a brand-new Zenodo record

If `--existing-deposition-id` is not provided, the script creates a new Zenodo deposition.

Example:

```bash
python3 tools/publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /canonical/zenodo_bundle.zip \
  --log-file data/zenodo_sync_log.csv \
  --title "ECHOREPO Soil Dataset: elementary concentrations, biodiversity, images" \
  --description "ECHOREPO export bundle for Zenodo" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L.|0000-0003-3468-6824" \
  --grant 101112869 \
  --copyright "© 2026 ECHO Horizon Project" \
  --keyword "soil,biodiversity,citizen-science" \
  --subject "Soil science|http://id.loc.gov/authorities/subjects/sh85124022|url"
```

---

## Updating an existing Zenodo record

If `--existing-deposition-id` is provided, the script creates a new version draft from that deposition.

Example:

```bash
python3 tools/publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /canonical/zenodo_bundle.zip \
  --existing-deposition-id 483391 \
  --log-file data/zenodo_sync_log.csv \
  --title "ECHOREPO Soil Dataset: elementary concentrations, biodiversity, images" \
  --description "ECHOREPO export bundle for Zenodo" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L.|0000-0003-3468-6824" \
  --grant 101112869 \
  --copyright "© 2026 ECHO Horizon Project" \
  --keyword "soil,biodiversity,citizen-science" \
  --subject "Soil science|http://id.loc.gov/authorities/subjects/sh85124022|url"
```

---

## Shell wrapper scripts

You mentioned shell wrappers for publish and update. They are nearly identical, except the update wrapper adds:

```bash
--existing-deposition-id "${ZENODO_DEPOSITION_ID:-483391}"
```

### Typical update wrapper

```bash
#!/usr/bin/env bash
set -euo pipefail

set -a
source .env_zenodo
set +a

python3 tools/publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base "${ZENODO_API_BASE:-https://echorepo.quanta-labs.com/api/v1}" \
  --api-path "${ZENODO_API_PATH:-/canonical/zenodo_bundle.zip}" \
  --existing-deposition-id "${ZENODO_DEPOSITION_ID:-483391}" \
  --log-file "${ZENODO_LOG_FILE:-data/zenodo_sync_log.csv}" \
  --title "${ZENODO_TITLE:-ECHOREPO Zenodo bundle publication}" \
  --description "${ZENODO_DESCRIPTION:-ECHOREPO export bundle for Zenodo}" \
  --creator "${ZENODO_CREATOR:-Osychenko, Oleg|Quanta Systems, S.L.}" \
  --grant "${ZENODO_GRANT:-101112869}" \
  --copyright "${ZENODO_COPYRIGHT:-© 2026 ECHO Horizon Project}" \
  --keyword "${ZENODO_KEYWORD:-soil, biodiversity, citizen-science}" \
  --subject "${ZENODO_SUBJECT:-Soil science|http://id.loc.gov/authorities/subjects/sh85124022|url}"
```

### Important note

The shell wrapper uses `source .env_zenodo`, so environment variables from that file are available to shell expansions like `${ZENODO_TITLE:-...}`.

Without `source .env_zenodo`, shell-side defaults would always win unless those variables were already exported in the shell environment.

---

## Output printed by the script

On success, the script prints a JSON summary to stdout.

Typical fields include:

- `ok`
- `sandbox`
- `api_download_url`
- `api_path`
- `deposition_id`
- `record_id`
- `conceptrecid`
- `version_doi`
- `concept_doi`
- `prereserved_doi`
- `zenodo_html`
- `log_file`
- `filters`
- `downloaded_filename`
- `upload_filename`
- `wrapped_in_zip`
- `zip_member_name`
- `downloaded_size_bytes`
- `upload_size_bytes`

This output is useful for:
- checking what happened immediately
- grabbing the deposition ID for future updates
- logging in CI or shell scripts

---

## CSV log file

The script appends one row per run to the CSV log file.

Default:

```text
data/zenodo_sync_log.csv
```

Logged columns:

- `run_at_utc`
- `status`
- `message`
- `api_base`
- `api_path`
- `download_url`
- `filters_json`
- `existing_deposition_id`
- `deposition_id`
- `record_id`
- `conceptrecid`
- `version_doi`
- `concept_doi`
- `prereserved_doi`
- `zenodo_html`
- `latest_draft_html`
- `bucket_url`
- `downloaded_filename`
- `downloaded_size_bytes`
- `upload_filename`
- `upload_size_bytes`
- `wrapped_in_zip`
- `zip_member_name`
- `sandbox`
- `title`

This log is useful for:
- DOI tracking
- finding the deposition ID for later updates
- auditing uploads
- checking which filters were used

---

## Error handling

If an error occurs, the script:

1. appends an `error` row to the CSV log
2. prints the error message to stderr
3. exits with nonzero code

Common failure cases:

### Missing Zenodo access token
```text
ERROR: missing Zenodo access token
```

### Missing ECHOREPO credentials
```text
ERROR: missing ECHOREPO API credentials
```

### Invalid `--extra-param`
If a value does not contain `=`.

### Invalid `--subject`
If it does not contain at least `term|identifier`.

### API file download fails
If the endpoint returns non-200.

### Zenodo create/update/publish fails
If Zenodo returns an unexpected status code.

### Downloaded file is empty
If the API returns a zero-byte file.

---

## Practical workflow

Recommended workflow:

1. Test the API endpoint with `curl`.
2. Test publication in Zenodo sandbox.
3. Confirm metadata in Zenodo UI.
4. Check the generated JSON output and CSV log.
5. Use the logged `deposition_id` for later updates.
6. Only then switch to production Zenodo.

---

## Quick API test with `curl`

Example:

```bash
curl -L \
  -H "X-API-Key: $ECHOREPO_API_KEY" \
  "https://echorepo.quanta-labs.com/api/v1/canonical/zenodo_bundle.zip" \
  -o test_bundle.zip
```

Filtered example:

```bash
curl -L \
  -H "X-API-Key: $ECHOREPO_API_KEY" \
  "https://echorepo.quanta-labs.com/api/v1/canonical/zenodo_bundle.zip?country=ES&from=2025-01-01&to=2025-12-31" \
  -o test_bundle_es_2025.zip
```

---

## Metadata confirmed from Zenodo API

For your current payloads, the deposited metadata has been confirmed to use these fields:

- `creators`
- `keywords`
- `subjects`
- `grants`
- `license`
- `notes`

Notably:
- copyright is stored in `notes`
- creator role is not currently used by this script

---

## Summary

`publish_api_file_to_zenodo.py` is a reusable publication helper that:

- downloads an authenticated API file
- optionally wraps it in ZIP
- publishes it to Zenodo
- supports both first publication and updates
- supports optional metadata for grants, keywords, subjects, and copyright
- logs all runs to a CSV file
