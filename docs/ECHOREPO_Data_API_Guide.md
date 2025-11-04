# ECHOREPO Data API (v1) — Integration Guide

**Base URL:** `https://echorepo.quanta-labs.com/api/v1`  
**Status:** Stable (versioned). Backward‑compatible additions may occur. Breaking changes will use a new version path (e.g., `/api/v2`).

---

## 1) Authentication

You must authenticate **every** request using **one** of the methods below.

### Option A — API Key (simple)
Send the key provided to you by the ECHOREPO team:

- **Header:** `X-API-Key: <your-secret>`  
- **or** Query parameter: `?api_key=<your-secret>` (handy for tools that can’t set headers)

**Example:**
```bash
curl -H "X-API-Key: YOUR_SECRET" \
  "https://echorepo.quanta-labs.com/api/v1/samples?limit=1"
```

### Option B — OAuth2 Client Credentials (Keycloak) (standard)
Use a confidential client in Keycloak to obtain a bearer token.

- **Issuer (realm):** `https://keycloak-dev.quanta-labs.com/realms/echo_realm`
- **Token endpoint:** `.../protocol/openid-connect/token`
- **Client ID (audience):** `echorepo-api`
- **Client Secret:** (obtain from ECHO team)

**Get a token:**
```bash
TOKEN=$(curl -s -X POST \
  -d 'grant_type=client_credentials' \
  -d 'client_id=echorepo-api' \
  -d 'client_secret=YOUR_CLIENT_SECRET' \
  'https://keycloak-dev.quanta-labs.com/realms/echo_realm/protocol/openid-connect/token' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')
```

**Call the API with the token:**
```bash
curl -H "Authorization: Bearer $TOKEN" \
  "https://echorepo.quanta-labs.com/api/v1/samples?limit=1"
```

> You can also use an authenticated **browser session** to call the API from within the app (not common for machine‑to‑machine integrations).

---

## 2) Endpoints

### `GET /ping`
Connectivity health check.
- **200 OK** → `{"ok": true}`

### `GET /samples`
Retrieve samples with filtering, paging, and output format selection.

**Query parameters:**
- `from` — Start timestamp (inclusive). Accepts ISO‑like strings: `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM[:SS[.fff]]`
- `to` — End timestamp (inclusive). Same formats as `from`.
- `bbox` — Bounding box (WGS‑84 lon/lat), **`west,south,east,north`** e.g. `-7.5,43.0,-6.5,43.8`
- `within` — Approximate circle filter, **`lat,lon,r_km`** e.g. `43.41,-7.14,25`
- `fields` — Comma list of columns to include, or `*` for all **non‑excluded** columns.
- `limit` — Page size (default `100`, max `1000`)
- `offset` — Page offset (default `0`)
- `order` — Column to sort by (default `collectedAt` if present)
- `dir` — `asc` or `desc` (default `desc`)
- `format` — `json` (default) | `csv` | `geojson`
- `api_key` — only if you’re sending the API key via query param

**Important field policy (privacy & hygiene):**
- **Always excluded:** `email`, `userId` (PII)
- **Always excluded:** any column whose name ends with `*_state` (e.g., `PH_state`, `PHOTO_state`, …)
- If you request `fields=*` or explicitly include excluded columns, they will still be removed.
- Default field set (subject to availability):  
  `sampleId, collectedAt, GPS_long, GPS_lat, PH_ph, SOIL_TEXTURE_texture, SOIL_STRUCTURE_structure, SOIL_DIVER_earthworms, SOIL_CONTAMINATION_plastic`

**Responses:**
- **JSON** (default):
  ```json
  {
    "meta": { "count": 1234, "limit": 100, "offset": 0, "order": "collectedAt", "dir": "desc" },
    "data": [
      { "sampleId": "...", "collectedAt": "...", "GPS_long": -7.1417, "GPS_lat": 43.4101, "PH_ph": "pH - 8.5", ... }
    ]
  }
  ```
- **CSV**: header = selected fields (excluded columns never appear).
- **GeoJSON**: `FeatureCollection` of `Point` features; coordinates from `GPS_long`/`GPS_lat`; other columns go into `properties`.

### `GET /samples/count`
Fast count of samples matching date filters.
- **Query params:** `from`, `to`
- **Response:** `{"count": <integer>}`

---

## 3) Examples

### Last 7 days (JSON)
```bash
curl -H "X-API-Key: YOUR_SECRET" \
  "https://echorepo.quanta-labs.com/api/v1/samples?from=$(date -I -d '-7 days')&order=collectedAt&dir=desc&limit=100"
```

### Bounding box (GeoJSON)
```bash
curl -H "X-API-Key: YOUR_SECRET" \
  "https://echorepo.quanta-labs.com/api/v1/samples?bbox=-7.5,43.0,-6.5,43.8&format=geojson" \
  -o bbox.geojson
```

### CSV export (selected fields)
```bash
curl -H "X-API-Key: YOUR_SECRET" \
  "https://echorepo.quanta-labs.com/api/v1/samples?fields=sampleId,collectedAt,GPS_lat,GPS_long,PH_ph&format=csv" \
  -o samples.csv
```

### Within radius (25 km)
```bash
curl -H "X-API-Key: YOUR_SECRET" \
  "https://echorepo.quanta-labs.com/api/v1/samples?within=43.41,-7.14,25&format=json&limit=50"
```

### Paging
```bash
# page 1
curl -H "X-API-Key: YOUR_SECRET" \
  "https://echorepo.quanta-labs.com/api/v1/samples?limit=500&offset=0"

# page 2
curl -H "X-API-Key: YOUR_SECRET" \
  "https://echorepo.quanta-labs.com/api/v1/samples?limit=500&offset=500"
```

---

## 4) Tool-specific notes

### QGIS
- Layer → **Add Layer** → **Add Vector Layer** → Protocol = **HTTP(S)**.
- URL (use query param for the key):  
  `https://echorepo.quanta-labs.com/api/v1/samples?bbox=-7.5,43.0,-6.5,43.8&format=geojson&api_key=YOUR_SECRET`

### Python (requests + pandas)
```python
import os, requests, pandas as pd

BASE = "https://echorepo.quanta-labs.com/api/v1"
KEY  = os.getenv("API_KEY", "YOUR_SECRET")
S    = requests.Session(); S.headers["X-API-Key"] = KEY

def fetch_all(params):
    out = []
    limit, offset = 500, 0
    while True:
        r = S.get(f"{BASE}/samples", params={**params, "limit": limit, "offset": offset}, timeout=30)
        r.raise_for_status()
        j = r.json()
        out.extend(j["data"])
        if offset + limit >= j["meta"]["count"]:
            break
        offset += limit
    return out

rows = fetch_all({"from":"2025-05-01","to":"2025-07-31","order":"collectedAt","dir":"desc"})
pd.DataFrame(rows).to_csv("samples_export.csv", index=False)
```

### Excel / Google Sheets
Use CSV with query param key:
- Excel: Data → **From Web** →  
  `https://echorepo.quanta-labs.com/api/v1/samples?from=2025-07-01&format=csv&api_key=YOUR_SECRET`
- Google Sheets:  
  `=IMPORTDATA("https://echorepo.quanta-labs.com/api/v1/samples?format=csv&api_key=YOUR_SECRET")`

---

## 5) Filters: details & caveats

- **Date parsing:** `from`/`to` accept `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM[:SS[.fff]]` (UTC or naive). They are compared lexicographically; ISO‑8601 sorts correctly.
- **`bbox` vs `within`:** `within` uses an **approximate degree window** based on the provided radius and latitude; it’s fast and good for filtering. If you need exact distance (Haversine), ask us to enable an additional precise filter.
- **Coordinates:** `GPS_long` = longitude (x), `GPS_lat` = latitude (y), WGS‑84 (EPSG:4326).

---

## 6) Errors

- **401 Unauthorized** — Missing/invalid credentials.  
  Body example: `{"message":"Missing or invalid credentials"}` (or an HTML error page)
- **400 Bad Request** — Malformed parameters (e.g., invalid bbox format)
- **404 Not Found** — Wrong path or version
- **5xx** — Server error

---

## 7) Privacy & field policy

- **PII** is never returned: `email`, `userId`
- Any column whose name ends with **`_state`** is never returned.
- The default field list avoids personal data; requesting `fields=*` still respects the exclusions.

If your integration requires additional columns, contact the ECHOREPO team to review data‑protection implications.

---

## 8) Changelog

- **2025‑10‑30** — Initial public documentation for `/api/v1`

---

## 9) Support / Contact

- Technical questions & access: **ECHOREPO team (Quanta Systems S.L.)**
- Base URL: `https://echorepo.quanta-labs.com/api/v1`
- Keycloak realm: `echo_realm` (client: `echorepo-api`)

