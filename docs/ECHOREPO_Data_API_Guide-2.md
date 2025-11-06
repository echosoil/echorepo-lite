# ECHO Data API

This API exposes sample data from a SQLite database and lets you upload “lab enrichment” data that gets joined to samples by QR code.

**Base path** (typical): `/api/v1` — this depends on how the blueprint is registered, e.g.:

```python
app.register_blueprint(data_api, url_prefix="/api/v1")
```

---

## Authentication

Every protected endpoint accepts **one** of the following:

1. **API key**  
   Send **one** of:
   - `X-API-Key: <your-key>`
   - `x-api-key: <your-key>`
   - `Authorization: ApiKey <your-key>`
   - Query string `?api_key=<your-key>`

   The required key is read from:
   - `current_app.config["API_KEY"]`, or
   - env `API_KEY`.

2. **OIDC / Keycloak bearer token**  
   - Send `Authorization: Bearer <JWT>`.
   - The code discovers the issuer from `OIDC_ISSUER_URL` and fetches JWKS.
   - It checks audience/client as configured (`OIDC_AUDIENCE`, `OIDC_CLIENT_ID`).

3. **Flask session user**  
   - If you're logged in with `flask-login`, the request is accepted.

If none of the above is valid, you receive **401 Unauthorized**.

---

## Database discovery

The API connects to SQLite once per request and tries to find the samples table.

- DB path is determined by:
  1. `current_app.config["SQLITE_PATH"]`
  2. env `SQLITE_PATH`
  3. fallback: `<app-root>/../../data/db/data.db`

- Sample table name is determined by:
  1. `current_app.config["SAMPLE_TABLE"]` or env `SAMPLE_TABLE`
  2. else: first match among: `samples`, `sample`, `data`, `records`
  3. else: first table that has a `sampleId` column
  4. else: first table in the database

So in most deployments you **don't** have to hardcode the table name.

---

## Field policy (safe columns)

The API intentionally hides some columns:

- Columns named **`email`** or **`userId`** are **always excluded** from responses.
- Columns whose name ends with **`_state`** are also excluded.
- This applies even if you ask for `fields=*`.

There is also a built-in default list of safe fields for `/samples`:

- `sampleId`
- `collectedAt`
- `GPS_long`
- `GPS_lat`
- `PH_ph`
- `SOIL_TEXTURE_texture`
- `SOIL_STRUCTURE_structure`
- `SOIL_DIVER_earthworms`
- `SOIL_CONTAMINATION_plastic`

If you don't pass `fields=...`, the API will try to return these (but only those that exist in your table).

---

## Endpoints

### 1. `GET /api/v1/ping`

Simple health check.

**Response:**

```json
{ "ok": true }
```

This one does **not** run `require_api_auth()` in the code you shared, so it's open.

---

### 2. `GET /api/v1/samples`

List/filter samples.

**Auth:** required.

**Query parameters:**

- `from` — lower bound for `collectedAt`. Accepts several ISO-ish formats:
  - `YYYY-MM-DD`
  - `YYYY-MM-DDTHH:MM`
  - `YYYY-MM-DDTHH:MM:SS`
  - `YYYY-MM-DDTHH:MM:SS.fff`
- `to` — upper bound for `collectedAt` (same formats).
- `bbox` — geographic bounding box:  
  `west,south,east,north` (i.e. `lon_min,lat_min,lon_max,lat_max`).  
  Only applied if the table has `GPS_long` and `GPS_lat`.
- `within` — approximate circle search:  
  `lat,lon,r_km` → internally converted to a bounding box of `r_km` kilometers around that point.
- `fields` — comma separated list of columns to return, e.g.  
  `fields=sampleId,collectedAt,GPS_long,GPS_lat`  
  Special value: `fields=*` → “all safe columns” (PII + *_state stripped).
- `limit` — default 100, max 1000.
- `offset` — default 0.
- `order` — column to order by. Default `collectedAt` if it exists and is not excluded; otherwise first safe column.
- `dir` — `asc` or `desc`, default `desc`.
- `format` — one of:
  - `json` (default)
  - `csv`
  - `geojson`

**Important safety notes:**

- Even if you pass `fields=*`, the API will remove `email`, `userId`, and any column ending with `_state`.
- If you request non-existing columns, they are ignored.
- If after filtering there are no valid fields, the API falls back to the default safe list.

**Response:**

Default (`format=json`):

```json
{
  "meta": {
    "count": 1234,
    "limit": 100,
    "offset": 0,
    "order": "collectedAt",
    "dir": "desc"
  },
  "data": [
    {
      "sampleId": "abc123",
      "collectedAt": "2025-11-02T10:00:00",
      "GPS_long": 12.34,
      "GPS_lat": 56.78
      // ...
    }
  ]
}
```

- `meta.count` is computed with a separate `COUNT(*)` using the same WHERE if possible.
- `data` is an array of rows.

`format=csv`:

- Returns a streaming CSV with a header matching the selected fields.

`format=geojson`:

- Returns a GeoJSON FeatureCollection.
- Only rows that have both `GPS_long` and `GPS_lat` become features.
- All other selected columns go into `properties`.

**Example:**

```http
GET /api/v1/samples?from=2025-01-01&bbox=10,45,12,47&fields=sampleId,GPS_long,GPS_lat&format=geojson
X-API-Key: YOUR_KEY
```

---

### 3. `GET /api/v1/samples/count`

Returns just the count of samples, optionally filtered by date.

**Auth:** required.

**Query parameters:**

- `from` — lower bound on `collectedAt`
- `to` — upper bound on `collectedAt`

**Response:**

```json
{ "count": 3421 }
```

---

### 4. `POST /api/v1/lab-enrichment`

Upload lab / metals / chemistry data and store it in the normalized `lab_enrichment` table.

**Auth:** required.

This endpoint is flexible about input format — it tries to parse in this order:

1. **`multipart/form-data` with `file=`**  
   - CSV or XLSX
   - uses pandas to read
2. **Raw CSV** (Content-Type `text/csv`, `application/csv`, or `text/plain`)
3. **Raw XLSX** (Content-Type starting with Excel OOXML)
4. **JSON**  
   - array of objects: `[ {...}, {...} ]`
   - or wrapper: `{ "rows": [ {...}, {...} ] }`

If none of the above is recognized → `400`.

---

#### Row shape (JSON example)

```json
[
  {
    "qr_code": "ECHO-ABCD1234",
    "As": 12.2,
    "As_unit": "mg/kg",
    "CaO": 1.201,
    "CaO_unit": "%"
  },
  {
    "QR_qrCode": "TEST-0001",
    "pH": 7.2
  }
]
```

**QR / sample identifier columns it recognizes:**

- `qr_code`
- `QR_qrCode`
- `id`
- `ID`

First non-empty of those is taken as the QR code.

---

#### QR normalization

The QR value is normalized before storing:

1. Leading `ECHO-` is removed (case-insensitive).
2. If there is **no dash** and the length is **≥ 5**, it inserts a dash after the first 4 chars.  
   Example: `ABCD1234` → `ABCD-1234`.

This makes uploads more forgiving.

---

#### How data is stored

There is a table (created if missing):

```sql
CREATE TABLE IF NOT EXISTS lab_enrichment (
  qr_code    TEXT NOT NULL,
  param      TEXT NOT NULL,
  value      TEXT,
  unit       TEXT,
  user_id    TEXT,
  raw_row    TEXT,
  updated_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (qr_code, param)
);
```

For **each** uploaded row:

- The QR is determined and normalized.
- For **each other key** in the JSON/CSV row:
  - if key is in (`qr_code`, `QR_qrCode`, `id`, `ID`) → skipped
  - if value is empty → skipped
  - if key looks like a “unit-only” column (`unit...` or ends with `_unit`) → skipped
  - otherwise it becomes one record with:
    - `qr_code` = normalized QR
    - `param`   = column name
    - `value`   = cell value (stringified)
    - `unit`    = taken from `<column>_unit` in the same row, or from generic `unit` in the same row, if present
    - `user_id` = uploader identifier
    - `raw_row` = JSON dump of the entire original row
- Insert is done with `ON CONFLICT(qr_code, param) DO UPDATE`, so re-uploads update existing values.

**Uploader identification:**

- If request has a valid bearer token → uses `sub` or `preferred_username` from the token.
- Else → header `X-User-Id`
- Else → `"api"`

---

#### Response

```json
{
  "ok": true,
  "processed": 42,
  "skipped": 1
}
```

- `processed` is roughly “how many (qr_code, param) we wrote/updated”.
- `skipped` counts rows that were not dictionaries or had no usable QR.

---

## How this relates to the web/app queries

Elsewhere in your code (e.g. in `db.py`) you do something like:

```sql
WITH lab AS (
  SELECT
    qr_code,
    GROUP_CONCAT(
      CASE
        WHEN (unit IS NOT NULL AND unit <> '')
          THEN param || '=' || value || ' ' || unit
        ELSE param || '=' || value
      END,
      '; '
    ) AS METALS_info
  FROM lab_enrichment
  GROUP BY qr_code
)
SELECT s.*, lab.METALS_info
FROM samples AS s
LEFT JOIN lab
  ON lab.qr_code = s.QR_qrCode
     OR lab.qr_code = s.sampleId
WHERE ...
```

This is how your uploaded enrichment rows turn into a **single string per sample** (often shown as HTML with `<br>` replacing `;`).

So: **API → lab_enrichment → grouped in web → shown in UI**.

---

## Error cases

- `401` — missing or invalid credentials
- `400` — unknown upload format, non-list JSON, unreadable CSV/XLSX
- `500` — DB / config errors

---

## Examples

### Download CSV

```bash
curl -H "X-API-Key: $API_KEY"   "https://yourhost/api/v1/samples?format=csv&limit=500"   -o samples.csv
```

### Upload XLSX

```bash
curl -H "X-API-Key: $API_KEY"      -F "file=@lab_data.xlsx"      https://yourhost/api/v1/lab-enrichment
```

### Upload JSON

```bash
curl -H "X-API-Key: $API_KEY"   -H "Content-Type: application/json"   -d '[
    {"qr_code": "ECHO-TEST0001", "As": 11.2, "As_unit": "mg/kg"},
    {"qr_code": "TEST-0002", "pH": 7.2}
  ]'   https://yourhost/api/v1/lab-enrichment
```

---

## Summary

- Use `/samples` to read data.
- Use `/samples/count` to paginate.
- Use `/lab-enrichment` to push lab results into the DB.
- The app later joins `lab_enrichment` to your main table by QR/sample id and renders a single human-readable field.
