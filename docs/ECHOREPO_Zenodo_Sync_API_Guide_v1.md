# Zenodo sync for API-downloaded files

This document explains how to use `publish_api_file_to_zenodo.py`.

The script downloads a file from an authenticated ECHOREPO API endpoint and publishes that file to Zenodo, either as a brand-new record or as a new version of an existing record.

It is designed to work with `zenodo_bundle.zip`, but it is not limited to that endpoint.

---

## What the script does

`publish_api_file_to_zenodo.py` performs these steps:

1. Downloads a file from an API endpoint under your ECHOREPO API base URL.
2. Optionally wraps the downloaded file into a new ZIP file.
3. Creates a new Zenodo deposition, or creates a new version draft of an existing deposition.
4. Uploads the chosen file to Zenodo.
5. Publishes the deposition.
6. Appends a line to a CSV log file with DOI and deposition information.

---

## Typical use cases

You can use the script for:

- publishing `/canonical/zenodo_bundle.zip` to Zenodo
- publishing `/canonical/all.zip` to Zenodo
- publishing another authenticated API export endpoint
- creating first-time Zenodo datasets
- publishing later updates as new versions
- keeping a local CSV log of DOI history

---

## Script file

```text
publish_api_file_to_zenodo.py
```

---

## Requirements

The script requires Python 3 and the `requests` package.

Install `requests` if needed:

```bash
pip install requests
```

---

## Authentication

The script needs credentials for two systems:

### 1. ECHOREPO API authentication

Use one of these:

- `ECHOREPO_API_KEY`
- `ECHOREPO_BEARER_TOKEN`

### 2. Zenodo authentication

Use:

- `ACCESS_TOKEN`

This is your Zenodo access token.

---

## `.env_zenodo` file

A minimal example:

```env
ACCESS_TOKEN=your_zenodo_sandbox_token
ZENODO_SANDBOX=true
ECHOREPO_API_KEY=your_echorepo_api_key
```

You can also use a bearer token instead of an API key:

```env
ACCESS_TOKEN=your_zenodo_sandbox_token
ZENODO_SANDBOX=true
ECHOREPO_BEARER_TOKEN=your_bearer_token
```

---

## Basic command structure

```bash
python3 publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /canonical/zenodo_bundle.zip \
  --title "ECHOREPO Zenodo bundle" \
  --description "ECHOREPO export bundle for Zenodo" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L."
```

---

## Main arguments

### Required

- `--api-base`  
  Base URL of your API, for example:

  ```text
  https://echorepo.quanta-labs.com/api/v1
  ```

- `--title`  
  Zenodo record title.

- `--description`  
  Zenodo record description.

- `--creator`  
  Repeatable argument. Format:

  ```text
  "Family, Given|Affiliation|ORCID"
  ```

  ORCID is optional.

Examples:

```bash
--creator "Osychenko, Oleg|Quanta Systems, S.L."
--creator "Doe, John|University X|0000-0000-0000-0000"
```

---

### Common optional arguments

- `--sandbox`  
  Use Zenodo sandbox instead of production Zenodo.

- `--api-path`  
  API path to download from. Default:

  ```text
  /canonical/zenodo_bundle.zip
  ```

- `--existing-deposition-id`  
  If given, the script creates a new version of that existing Zenodo record.

- `--keyword`  
  Repeatable Zenodo keyword.

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
  Optional version string for Zenodo metadata.

- `--communities`  
  Optional list of Zenodo community identifiers.

- `--log-file`  
  Path to the CSV log file. Default:

  ```text
  zenodo_sync_log.csv
  ```

---

## Download endpoint configuration

The script does not hard-code the endpoint anymore.

Use:

```bash
--api-path /canonical/zenodo_bundle.zip
```

or:

```bash
--api-path /canonical/all.zip
```

or any other authenticated API file endpoint.

The full URL used by the script is:

```text
<api-base><api-path>
```

Example:

```text
https://echorepo.quanta-labs.com/api/v1/canonical/zenodo_bundle.zip
```

---

## Filters

The script supports the following built-in query parameters:

- `--from-date`
- `--to-date`
- `--country`
- `--country-code`
- `--bbox`
- `--within`

These are sent as query parameters to the API endpoint.

### Example: country filter

```bash
python3 publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /canonical/zenodo_bundle.zip \
  --country ES \
  --title "ECHOREPO Spain bundle" \
  --description "Filtered Spanish bundle" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L."
```

### Example: date range filter

```bash
python3 publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /canonical/zenodo_bundle.zip \
  --from-date 2025-01-01 \
  --to-date 2025-12-31 \
  --title "ECHOREPO 2025 bundle" \
  --description "Filtered 2025 bundle" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L."
```

### Example: bounding box

```bash
python3 publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /canonical/zenodo_bundle.zip \
  --bbox "-2,35,5,45" \
  --title "ECHOREPO bbox bundle" \
  --description "Geographic subset" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L."
```

### Example: radius filter

```bash
python3 publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /canonical/zenodo_bundle.zip \
  --within "41.39,2.17,50" \
  --title "ECHOREPO local bundle" \
  --description "Subset within 50 km" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L."
```

---

## Extra query parameters

For endpoints that need different parameters, use:

```bash
--extra-param key=value
```

This option is repeatable.

Example:

```bash
python3 publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /some/other/export \
  --extra-param marker=16S \
  --extra-param sample_id=ABCD-1234 \
  --title "Custom export" \
  --description "Custom API export" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L."
```

---

## Downloaded file name and uploaded file name

### Downloaded file name

By default, the local filename is inferred from `--api-path`.

Example:

```text
/canonical/zenodo_bundle.zip -> zenodo_bundle.zip
```

You can override it:

```bash
--download-name my_local_file.zip
```

### Uploaded file name

By default, the uploaded Zenodo filename is the same as the downloaded file name, unless wrapping is used.

You can override it with:

```bash
--upload-name release_package.zip
```

---

## Optional wrapping into a new ZIP

Sometimes the API endpoint already returns a ZIP file, but you may still want to upload a wrapper ZIP containing that file.

Use:

```bash
--wrap-in-zip
```

This creates a new ZIP file and places the downloaded file inside it.

### Optional ZIP member name

Use:

```bash
--zip-member-name canonical_zenodo_bundle.zip
```

This controls the filename inside the wrapper ZIP.

### Example

```bash
python3 publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /canonical/zenodo_bundle.zip \
  --wrap-in-zip \
  --zip-member-name canonical_zenodo_bundle.zip \
  --upload-name echorepo_release_package.zip \
  --title "ECHOREPO wrapped bundle" \
  --description "Wrapped bundle for Zenodo" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L."
```

This produces:

- downloaded file: `zenodo_bundle.zip`
- uploaded file: `echorepo_release_package.zip`
- ZIP member inside wrapper: `canonical_zenodo_bundle.zip`

---

## Creating a brand-new Zenodo record

If you do **not** pass `--existing-deposition-id`, the script creates a new deposition.

Example:

```bash
python3 publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /canonical/zenodo_bundle.zip \
  --title "ECHOREPO Zenodo bundle" \
  --description "Initial Zenodo publication" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L."
```

---

## Publishing a new version of an existing Zenodo record

Use:

```bash
--existing-deposition-id 123456
```

Example:

```bash
python3 publish_api_file_to_zenodo.py \
  --sandbox \
  --existing-deposition-id 123456 \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /canonical/zenodo_bundle.zip \
  --title "ECHOREPO Zenodo bundle" \
  --description "Updated Zenodo publication" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L."
```

### Important

`--existing-deposition-id` must be the Zenodo **deposition ID** of the latest published version, not just the concept DOI and not only the concept record id.

You can get this from:

- the JSON output of a previous successful run
- the CSV log file generated by the script

---

## Output printed by the script

On success, the script prints JSON like this:

```json
{
  "ok": true,
  "sandbox": true,
  "api_download_url": "https://echorepo.quanta-labs.com/api/v1/canonical/zenodo_bundle.zip",
  "api_path": "/canonical/zenodo_bundle.zip",
  "deposition_id": "123456",
  "record_id": "123456",
  "conceptrecid": "123450",
  "version_doi": "10.5072/zenodo.123456",
  "concept_doi": "10.5072/zenodo.123450",
  "prereserved_doi": "10.5072/zenodo.123456",
  "zenodo_html": "https://sandbox.zenodo.org/records/123456",
  "log_file": "zenodo_sync_log.csv",
  "filters": {},
  "downloaded_filename": "zenodo_bundle.zip",
  "upload_filename": "zenodo_bundle.zip",
  "wrapped_in_zip": false,
  "zip_member_name": "",
  "downloaded_size_bytes": "12345",
  "upload_size_bytes": "12345"
}
```

---

## Log file

By default, the script appends to:

```text
zenodo_sync_log.csv
```

You can override this:

```bash
--log-file /path/to/zenodo_sync_log.csv
```

### Logged columns

The log contains:

- run timestamp
- status
- error message, if any
- API base
- API path
- full download URL
- serialized filters
- existing deposition id used for versioning
- resulting deposition id
- record id
- concept record id
- version DOI
- concept DOI
- pre-reserved DOI
- Zenodo HTML record URL
- latest draft HTML URL
- Zenodo bucket URL
- downloaded file name and size
- uploaded file name and size
- whether wrapping-in-zip was used
- ZIP member name
- sandbox flag
- title

This file is useful for:
- tracking DOI history
- finding the deposition id to use for the next version
- auditing what exactly was uploaded

---

## Reusing the script for other API endpoints

This script is reusable as long as the endpoint:

- is reachable under `--api-base + --api-path`
- returns a downloadable file with HTTP 200
- accepts your API key or bearer token
- returns content that Zenodo can store as a file

Examples of reusable endpoints:

```bash
--api-path /canonical/zenodo_bundle.zip
--api-path /canonical/all.zip
--api-path /canonical/snapshot/all.zip
```

You can also use custom query parameters with `--extra-param`.

---

## Example: publish `/canonical/all.zip`

```bash
python3 publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /canonical/all.zip \
  --title "ECHOREPO canonical all.zip" \
  --description "Full canonical ZIP export" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L."
```

---

## Example: publish filtered `/canonical/all.zip`

```bash
python3 publish_api_file_to_zenodo.py \
  --sandbox \
  --api-base https://echorepo.quanta-labs.com/api/v1 \
  --api-path /canonical/all.zip \
  --country ES \
  --from-date 2025-01-01 \
  --to-date 2025-12-31 \
  --title "ECHOREPO canonical Spain 2025" \
  --description "Filtered canonical ZIP export for Spain in 2025" \
  --creator "Osychenko, Oleg|Quanta Systems, S.L."
```

---

## Example: publish a non-ZIP endpoint, wrapped into ZIP

```bash
python3 publish_api_file_to_zenodo.py \
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

## Common failure cases

### `ERROR: missing Zenodo access token`

You did not provide `ACCESS_TOKEN` through:

- `--zenodo-access-token`
- environment variable
- `.env_zenodo`

### `ERROR: missing ECHOREPO API credentials`

You did not provide either:

- `ECHOREPO_API_KEY`
- `ECHOREPO_BEARER_TOKEN`

### HTTP 401 during API download

Your API credentials are invalid, missing, or not accepted by the endpoint.

### HTTP 404 during API download

The path in `--api-path` is wrong, or the route is not deployed.

### Downloaded file is empty

The endpoint returned a zero-byte file.

### Zenodo new version action fails

The supplied `--existing-deposition-id` is not valid for versioning, or it is not the expected published deposition.

---

## Recommended workflow

For a stable operational workflow:

1. Make sure the API endpoint works with `curl`.
2. Test the script with `--sandbox`.
3. Confirm that the resulting deposition is correct.
4. Use the CSV log to keep track of:
   - deposition id
   - concept DOI
   - version DOI
5. Reuse the logged deposition id for later versions.

---

## Quick `curl` tests before using the script

### Test API endpoint

```bash
curl -L \
  -H "X-API-Key: $YOUR_API_KEY" \
  "https://echorepo.quanta-labs.com/api/v1/canonical/zenodo_bundle.zip" \
  -o test_bundle.zip
```

### Test filtered API endpoint

```bash
curl -L \
  -H "X-API-Key: $YOUR_API_KEY" \
  "https://echorepo.quanta-labs.com/api/v1/canonical/zenodo_bundle.zip?country=ES&from=2025-01-01&to=2025-12-31" \
  -o test_bundle_es_2025.zip
```

If these work, the script should also work with the same endpoint.

---

## Summary

`publish_api_file_to_zenodo.py` is a reusable publication helper that:

- downloads any authenticated API file endpoint
- optionally wraps it into a ZIP
- uploads it to Zenodo
- publishes it
- logs DOI and deposition history locally

It is suitable for `zenodo_bundle.zip`, but it can also be reused for other export endpoints in ECHOREPO.
