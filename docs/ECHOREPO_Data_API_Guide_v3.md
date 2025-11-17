# ECHOREPO Data API – Canonical & Lab Enrichment (v3)

This document describes the public-facing **ECHOREPO Data API** intended for
external services (e.g. SoilWise / EUSO) and trusted partners.

The focus is on **canonical data in Postgres** and **lab enrichment uploads**.

Base URL examples (adjust to your deployment):

- `https://echorepo.quanta-labs.com/api/v1`
- `https://<your-host>/api/v1`

All endpoints below are relative to `/api/v1`.

---

## 1. Authentication

Every endpoint **requires auth**. Three mechanisms are supported:

1. **API key header** (recommended for system-to-system use)
2. **Bearer token (OIDC / Keycloak)** – `Authorization: Bearer <JWT>`
3. **Logged-in browser session** (for human users using the web UI)

### 1.1. API key

Configured via environment/config as `API_KEY`.

Send one of:

- `X-API-Key: <API_KEY>`
- `x-api-key: <API_KEY>`
- `Authorization: ApiKey <API_KEY>`
- `?api_key=<API_KEY>` query parameter

Example:

```bash
export YOUR_API_KEY="my-api-key"

curl -L   -H "X-API-Key: $YOUR_API_KEY"   "https://echorepo.quanta-labs.com/api/v1/ping"
```

### 1.2. Bearer token (Keycloak)

If you have a valid **access token** issued by Keycloak for this API, send:

```http
Authorization: Bearer <access_token>
```

The token is validated against the configured **OIDC issuer**, audience / client,
and signing keys (JWKS).

### 1.3. Browser session

If you call the API via a browser that is already logged in via the
ECHOREPO UI, the API may accept the existing Flask / Keycloak session cookie.

This is mainly useful for interactive debugging, **not** for automated systems.

---

## 2. Quick health check

### GET `/api/v1/ping`

Simple status endpoint.

- **Auth:** required (API key / bearer / session)
- **Response:**

```json
{
  "ok": true
}
```

Example:

```bash
curl -L   -H "X-API-Key: $YOUR_API_KEY"   "https://echorepo.quanta-labs.com/api/v1/ping"
```

---

## 3. Canonical data model (Postgres)

Canonical tables (Postgres):

- `samples`
- `sample_images`
- `sample_parameters`

Column sets (simplified):

### 3.1. `samples`

```text
sample_id, timestamp_utc, lat, lon, country_code, location_accuracy_m,
ph, organic_carbon_pct, earthworms_count,
contamination_debris, contamination_plastic,
contamination_other_orig, contamination_other_en,
pollutants_count,
soil_structure_orig, soil_structure_en,
soil_texture_orig, soil_texture_en,
observations_orig, observations_en,
metals_info_orig, metals_info_en,
collected_by, data_source, qa_status, licence
```

### 3.2. `sample_images`

```text
sample_id, country_code, image_id, image_url,
image_description_orig, image_description_en,
collected_by, timestamp_utc, licence
```

### 3.3. `sample_parameters`

```text
sample_id, country_code,
parameter_code, parameter_name,
value, uom, analysis_method, analysis_date,
lab_id, created_by, licence, parameter_uri
```

The API exposes **read-only** endpoints over these tables plus one
**write** endpoint for lab enrichment uploads.

---

## 4. Shared filtering options

Several canonical endpoints share a common filter model.

### 4.1. Time window

- `from`: ISO-ish timestamp, compared to `timestamp_utc`
- `to`: same as above

Accepted formats include:

- `YYYY-MM-DD`
- `YYYY-MM-DDTHH:MM`
- `YYYY-MM-DDTHH:MM:SS`
- `YYYY-MM-DDTHH:MM:SS.ssssss`

Examples:

- `from=2025-01-01`
- `from=2025-01-01T00:00`
- `to=2025-12-31`

### 4.2. Country

- `country` or `country_code` – case-insensitive 2-letter code (e.g. `ES`, `PT`)

Example: `country=ES`

### 4.3. Bounding box (samples only)

Used for endpoints that know `lat` / `lon` (currently **canonical samples** and
**canonical ZIP**).

- `bbox=west,south,east,north`

Where:

- `west` / `east` = longitude (x)
- `south` / `north` = latitude (y)
- All in WGS84 degrees

Example (roughly NE Spain / S France):

```text
bbox=-2,35,5,45
```

If `bbox` is supplied but cannot be parsed, the API responds with **400 Bad Request**.

### 4.4. Radius filter (samples only)

- `within=lat,lon,r_km`

Where:

- `lat`, `lon` – center point (degrees)
- `r_km` – radius in kilometers

This is implemented via a **bounding box approximation** using degrees; it is
good for rough filtering, not for exact geodesic calculations.

Example:

```text
within=41.39,2.17,50
```

(≈ 50 km radius around Barcelona)

### 4.5. Pagination

- `limit` – default 100, max 1000
- `offset` – default 0

### 4.6. Format

Depending on endpoint:

- `format=json` (default)
- `format=csv`
- `format=geojson` (only where lat/lon is known)

For **ZIP** endpoint, the format is always **ZIP of CSV files**.

---

## 5. Canonical samples

### 5.1. GET `/api/v1/canonical/samples`

Query canonical samples from Postgres.

**Auth:** required

**Query parameters:**

- `from`, `to` – time window on `timestamp_utc`
- `country` or `country_code`
- `bbox` – `west,south,east,north` on `lon`/`lat`
- `within` – `lat,lon,r_km` (approximate radius)
- `fields` – comma list subset of canonical sample columns, or omit to get all
- `limit`, `offset`
- `order` – one of canonical sample columns (default `timestamp_utc`)
- `dir` – `asc` or `desc` (default `desc`)
- `format` – `json` (default), `csv`, or `geojson`

**Response (JSON):**

```json
{
  "meta": {
    "count": 1234,
    "limit": 100,
    "offset": 0,
    "order": "timestamp_utc",
    "dir": "desc",
    "fields": ["sample_id", "timestamp_utc", "..."]
  },
  "data": [
    {
      "sample_id": "ABCD-1234",
      "timestamp_utc": "2025-11-17T18:00:29.607270+00:00",
      "lat": 41.38,
      "lon": 2.18,
      "country_code": "ES",
      "...": "..."
    }
  ]
}
```

**Example: all ES samples in 2025, CSV:**

```bash
curl -L   -H "X-API-Key: $YOUR_API_KEY"   "https://echorepo.quanta-labs.com/api/v1/canonical/samples?country=ES&from=2025-01-01&to=2025-12-31&format=csv"   -o canonical_samples_ES_2025.csv
```

**Example: samples inside a bbox, GeoJSON:**

```bash
curl -L   -H "X-API-Key: $YOUR_API_KEY"   "https://echorepo.quanta-labs.com/api/v1/canonical/samples?bbox=-2,35,5,45&format=geojson"   -o canonical_samples_bbox.geojson
```

### 5.2. GET `/api/v1/canonical/samples/count`

Same filters as `/canonical/samples`, but returns only the total count:

```json
{ "count": 1234 }
```

Example:

```bash
curl -L   -H "X-API-Key: $YOUR_API_KEY"   "https://echorepo.quanta-labs.com/api/v1/canonical/samples/count?country=ES&from=2025-01-01&to=2025-12-31"
```

---

## 6. Canonical sample images

### GET `/api/v1/canonical/sample_images`

Query canonical sample images.

**Auth:** required

**Query parameters:**

- `sample_id` – filter by sample
- `country` or `country_code`
- `from`, `to` – on `timestamp_utc`
- `fields` – subset of image columns (or omit for all)
- `limit`, `offset`
- `format` – `json` (default) or `csv`

> **Note:** there is **no bbox/within filter** here, since the image table
> does not carry lat/lon.

Example: all images for a single sample, CSV:

```bash
curl -L   -H "X-API-Key: $YOUR_API_KEY"   "https://echorepo.quanta-labs.com/api/v1/canonical/sample_images?sample_id=LFWK-1927&format=csv"   -o canonical_sample_images_LFWK-1927.csv
```

---

## 7. Canonical sample parameters

### GET `/api/v1/canonical/sample_parameters`

Query canonical parameters (lab results, etc.).

**Auth:** required

**Query parameters:**

- `sample_id`
- `country` or `country_code`
- `parameter_code`
- `fields` – subset of parameter columns (or omit for all)
- `limit`, `offset`
- `format` – `json` (default) or `csv`

Example: all parameters for a sample:

```bash
curl -L   -H "X-API-Key: $YOUR_API_KEY"   "https://echorepo.quanta-labs.com/api/v1/canonical/sample_parameters?sample_id=LFWK-1927&format=csv"   -o canonical_sample_parameters_LFWK-1927.csv
```

---

## 8. Canonical ZIP export with filters

### GET `/api/v1/canonical/all.zip`

Returns a **ZIP** file with three CSV files:

- `samples.csv`
- `sample_images.csv`
- `sample_parameters.csv`

The endpoint applies the **same filters** as `/canonical/samples`
to **all three tables**, where the fields exist:

- `from`, `to` – time window on `timestamp_utc`
- `country` / `country_code`
- `bbox` – `west,south,east,north` on `lon`/`lat` (samples only)
- `within` – `lat,lon,r_km` (samples only)

For `sample_images` and `sample_parameters`, the time/country filters are
applied where appropriate; `bbox`/`within` only affect the **sample-level**
subset (i.e. which `sample_id`s are included).

> The endpoint always returns a ZIP; there is **no `format` parameter**.

**Auth:** required

**Query parameters:**

- `from`, `to`, `country` / `country_code`, `bbox`, `within` (as above)

All other paging / field selection is fixed: the ZIP always contains the full
set of canonical columns for the filtered subset.

**Example: all ES samples in 2025 + related images & parameters, as ZIP:**

```bash
curl -L   -H "X-API-Key: $YOUR_API_KEY"   "https://echorepo.quanta-labs.com/api/v1/canonical/all.zip?country=ES&from=2025-01-01&to=2025-12-31"   -o canonical_ES_2025.zip
```

**Example: subset in a geographic bbox:**

```bash
curl -L   -H "X-API-Key: $YOUR_API_KEY"   "https://echorepo.quanta-labs.com/api/v1/canonical/all.zip?bbox=-2,35,5,45"   -o canonical_bbox_subset.zip
```

**Example: subset within 50 km of a point:**

```bash
curl -L   -H "X-API-Key: $YOUR_API_KEY"   "https://echorepo.quanta-labs.com/api/v1/canonical/all.zip?within=41.39,2.17,50"   -o canonical_barcelona_50km.zip
```

---

## 9. Lab enrichment uploads

### POST `/api/v1/lab-enrichment`

Used by labs or automated pipelines to upload **extra lab results** that are
later joined to field samples.

Auth: same as other API endpoints (API key, bearer, or session).

The endpoint accepts **three kinds of payloads**:

1. **JSON** body
2. **CSV/XLSX file** via `multipart/form-data`
3. **Raw CSV/XLSX body** (by content type)

#### 9.1. JSON payloads

Either an **array of row objects**:

```json
[
  {
    "qr_code": "ECHO-ABCD1234",
    "Cd": 0.12,
    "Cd_unit": "mg/kg",
    "Pb": 4.5,
    "Pb_unit": "mg/kg"
  },
  {
    "qr_code": "ECHO-EFGH5678",
    "Cd": 0.22,
    "Cd_unit": "mg/kg"
  }
]
```

or a wrapper object with `"rows"`:

```json
{
  "rows": [
    { "qr_code": "ECHO-ABCD1234", "Cd": 0.12, "Cd_unit": "mg/kg" },
    { "qr_code": "ECHO-EFGH5678", "Cd": 0.22, "Cd_unit": "mg/kg" }
  ]
}
```

Each row is exploded into many `lab_enrichment` records:

- `qr_code` – normalized like in the web UI:
  - remove leading `ECHO-` if present
  - inject dash after 4 chars if missing
- each non-empty column becomes `(qr_code, param, value, unit)`

Columns named like `Cd_unit` are interpreted as units for the corresponding
parameter.

**Example (JSON):**

```bash
curl -L -X POST   -H "X-API-Key: $YOUR_API_KEY"   -H "Content-Type: application/json"   -d '{
        "rows": [
          { "qr_code": "ECHO-ABCD1234", "Cd": 0.12, "Cd_unit": "mg/kg" },
          { "qr_code": "ECHO-EFGH5678", "Pb": 4.5, "Pb_unit": "mg/kg" }
        ]
      }'   "https://echorepo.quanta-labs.com/api/v1/lab-enrichment"
```

**Response:**

```json
{
  "ok": true,
  "processed": 10,
  "skipped": 0
}
```

#### 9.2. CSV / XLSX via multipart/form-data

Form field: `file`

```bash
curl -L -X POST   -H "X-API-Key: $YOUR_API_KEY"   -F "file=@lab_results.xlsx"   "https://echorepo.quanta-labs.com/api/v1/lab-enrichment"
```

The server will detect `.xlsx` or `.csv` and parse accordingly.

#### 9.3. Raw CSV / XLSX body

Set an appropriate `Content-Type`:

- `text/csv` or `application/csv` or `text/plain` – raw CSV body
- `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` – raw XLSX body

Example (raw CSV):

```bash
curl -L -X POST   -H "X-API-Key: $YOUR_API_KEY"   -H "Content-Type: text/csv"   --data-binary @lab_results.csv   "https://echorepo.quanta-labs.com/api/v1/lab-enrichment"
```

---

## 10. Legacy / internal endpoints

There are additional endpoints under `/api/v1` and the main web routes
(such as `/api/v1/samples` backed by SQLite, map GeoJSON endpoints, etc.).

For **external integrations** you should normally use **only**:

- `/api/v1/canonical/samples`
- `/api/v1/canonical/samples/count`
- `/api/v1/canonical/sample_images`
- `/api/v1/canonical/sample_parameters`
- `/api/v1/canonical/all.zip`
- `/api/v1/lab-enrichment`
- `/api/v1/ping`

Other endpoints may change without notice.

---

## 11. Summary of filters by endpoint

| Endpoint                             | Time (`from`,`to`) | Country | `bbox` | `within` | Fields | Format options         |
|-------------------------------------|---------------------|---------|--------|----------|--------|------------------------|
| `/canonical/samples`                | ✅                  | ✅      | ✅     | ✅       | ✅     | json, csv, geojson     |
| `/canonical/samples/count`          | ✅                  | ✅      | ✅     | ✅       | ❌     | json                   |
| `/canonical/sample_images`          | ✅                  | ✅      | ❌     | ❌       | ✅     | json, csv              |
| `/canonical/sample_parameters`      | ❌ (no timestamp)   | ✅      | ❌     | ❌       | ✅     | json, csv              |
| `/canonical/all.zip`                | ✅                  | ✅      | ✅*    | ✅*      | ❌     | zip (3× CSV)           |
| `/lab-enrichment` (POST)            | n/a                 | n/a     | n/a    | n/a      | n/a    | json / csv / xlsx in   |

\* `bbox` / `within` are applied on the **samples** table; the other
files in the ZIP are restricted to the corresponding filtered `sample_id`s.

---

If you need another subset or a new endpoint shape, it is usually best to
build it on top of `/canonical/samples` and the ZIP endpoint so that we keep
the **canonical model** stable.
