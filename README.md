# ECHOREPO Lite

Lightweight web interface and data access layer for the ECHOREPO soil citizen science data infrastructure.

ECHOREPO Lite provides a Dockerized Flask application for browsing, visualising, exporting and managing soil observation data. It supports map-based exploration of citizen science soil samples, canonical CSV/ZIP exports, laboratory data enrichment, biodiversity data outputs, multilingual user interfaces and integration with authentication and object-storage services.

The application is part of the ECHO/ECHOREPO digital infrastructure for open soil monitoring, FAIR data publication and long-term findability of citizen science soil observations.

---

## Main features

* Public and authenticated web interface for ECHOREPO soil observations.
* Interactive map-oriented access to soil sampling locations.
* API endpoints for sample data access in JSON, CSV and GeoJSON formats.
* Canonical data exports:

  * `samples.csv`
  * `sample_images.csv`
  * `sample_parameters.csv`
  * `sample_biodiversity.csv`
  * `all.zip`
* PostgreSQL-backed canonical data storage.
* Legacy SQLite compatibility for selected data access paths.
* Laboratory data enrichment workflows for sample parameters.
* Biodiversity OTU abundance export support.
* MinIO-compatible object storage for canonical snapshots and storage assets.
* Keycloak/OIDC and API-key based access modes.
* Multilingual interface support with Flask-Babel and translation overrides.
* Docker Compose deployment profiles for development and production.
* Basic analytics and usage logging for page views, API calls, uploads and downloads.

---

## Repository structure

```text
echorepo-lite/
├── echorepo/                 # Flask application package
│   ├── auth/                 # Authentication routes and helpers
│   ├── routes/               # Web, API, data, storage and admin routes
│   ├── services/             # Database, i18n, validation and permission services
│   ├── templates/            # Jinja templates
│   ├── translations/         # Flask-Babel translations
│   └── wsgi.py               # WSGI entry point
├── migrations/               # PostgreSQL migration files
├── scripts/                  # Startup, migration and utility scripts
├── static/                   # Frontend static assets
├── tools/                    # Auxiliary tools
├── data/                     # Runtime data mount, not intended for source data commits
├── storage/                  # Local storage mount
├── keys/                     # Runtime credentials mount, not committed
├── docker-compose.yml        # Base Compose configuration
├── docker-compose.dev.yml    # Development override
├── docker-compose.prod.yml   # Production override
├── Dockerfile                # Application image build
├── Dockerfile.i18n           # Optional i18n tooling image
├── requirements.txt          # Python dependencies
├── start_dev.sh              # Development startup helper
├── start_prod.sh             # Production startup helper
└── .env.example              # Example environment configuration
```

---

## Technology stack

ECHOREPO Lite is built with:

* Python 3.11
* Flask
* Gunicorn
* PostgreSQL
* Docker and Docker Compose
* Flask-Babel for internationalisation
* Authlib / OIDC integration
* MinIO-compatible object storage
* pandas and openpyxl for tabular data handling
* psycopg2 for PostgreSQL access
* geospatial and geometry utilities such as Shapely
* Matplotlib for selected visualisation or reporting tasks

---

## Quick start

### 1. Clone the repository

```bash
git clone https://github.com/echosoil/echorepo-lite.git
cd echorepo-lite
```

### 2. Prepare the environment file

Copy the example configuration:

```bash
cp .env.example .env
```

Then edit `.env` and set the values required for your deployment.

At minimum, review:

```text
SECRET_KEY
POSTGRES_PASSWORD
DB_NAME
DB_USER
DB_PASSWORD
API_KEY
KEYCLOAK_BASE_URL
KEYCLOAK_REALM
KEYCLOAK_CLIENT_ID
KEYCLOAK_CLIENT_SECRET
MINIO_ENDPOINT
MINIO_ACCESS_KEY
MINIO_SECRET_KEY
MINIO_BUCKET
```

Do not commit your real `.env` file.

### 3. Create the shared Docker network

The Compose setup expects an external network called `echorepo-shared`.

Create it once if it does not already exist:

```bash
docker network create echorepo-shared
```

### 4. Start the development deployment

```bash
chmod +x start_dev.sh
./start_dev.sh
```

The development service exposes:

```text
Application: http://localhost:18080
PostgreSQL:  localhost:5433
debugpy:     127.0.0.1:5678
```

The development container runs PostgreSQL readiness checks, applies migrations and starts Gunicorn with reload enabled.

### 5. Start the production deployment

```bash
chmod +x start_prod.sh
./start_prod.sh
```

The production service exposes:

```text
Application: http://localhost:8001
PostgreSQL:  localhost:5432
```

The production container runs PostgreSQL readiness checks, applies migrations and starts Gunicorn with two workers.

---

## Manual Docker Compose commands

Development:

```bash
COMPOSE_PROJECT_NAME=echorepo_dev \
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build
```

Production:

```bash
COMPOSE_PROJECT_NAME=echorepo_prod \
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

View logs:

```bash
docker compose logs -f echorepo-lite
```

Stop services:

```bash
docker compose down
```

---

## Configuration

The application is configured primarily through environment variables. See `.env.example` for the full list.

Important configuration groups include:

| Area                 | Example variables                                                        |
| -------------------- | ------------------------------------------------------------------------ |
| Flask/core           | `APP_ENV`, `SECRET_KEY`, `FLASK_DEBUG`, `PREFERRED_URL_SCHEME`           |
| Data paths           | `SQLITE_PATH`, `CSV_PATH`, `LOCAL_STORAGE_DIR`, `COUNTRY_SHP_PATH`       |
| PostgreSQL           | `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`                |
| API access           | `API_KEY`, `OIDC_ISSUER_URL`, `OIDC_AUDIENCE`                            |
| Keycloak             | `KEYCLOAK_BASE_URL`, `KEYCLOAK_REALM`, `KEYCLOAK_CLIENT_ID`              |
| MinIO/storage        | `MINIO_ENDPOINT`, `MINIO_BUCKET`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` |
| Privacy and QA       | `PRIVACY_GATE`, `HIDE_WRONG_COORDINATES`, `FILTER_WRONG_COORDINATES`     |
| Internationalisation | `LT_ENDPOINT`, `LT_ENDPOINT_INSIDE`                                      |
| Images               | `MAX_IMAGE_BYTES`, `STRIP_IMAGE_EXIF`, `SAVE_EXIF_SIDECAR`               |

---

## Web interface

The application provides a browser interface for exploring ECHOREPO soil observations.

Typical user-facing functions include:

* viewing the home/explore interface,
* logging in through the configured authentication provider,
* browsing sample records,
* viewing map-based sample locations,
* downloading canonical data exports,
* accessing personal or authorised sample data,
* uploading or managing laboratory enrichment data where permissions allow,
* switching between supported interface languages.

The application supports multilingual labels and translation overrides through the i18n subsystem.

---

## API overview

ECHOREPO Lite exposes API routes under:

```text
/api/v1
```

### Health check

```http
GET /api/v1/ping
```

Returns:

```json
{
  "ok": true
}
```

### Sample query endpoint

```http
GET /api/v1/samples
```

Supported query parameters include:

| Parameter | Description                             |
| --------- | --------------------------------------- |
| `from`    | Start date/time filter                  |
| `to`      | End date/time filter                    |
| `bbox`    | Bounding box as `west,south,east,north` |
| `within`  | Radius filter as `lat,lon,r_km`         |
| `fields`  | Comma-separated field list, or `*`      |
| `limit`   | Page size, maximum 1000                 |
| `offset`  | Pagination offset                       |
| `order`   | Sort column                             |
| `dir`     | `asc` or `desc`                         |
| `format`  | `json`, `csv` or `geojson`              |
| `api_key` | API key, if not passed as a header      |

Example:

```bash
curl "http://localhost:18080/api/v1/samples?limit=100&format=json&api_key=YOUR_API_KEY"
```

GeoJSON example:

```bash
curl "http://localhost:18080/api/v1/samples?bbox=-10,35,30,60&format=geojson&api_key=YOUR_API_KEY"
```

API authentication can use one of the following, depending on deployment configuration:

* `X-API-Key` header,
* `Authorization: ApiKey <key>`,
* `api_key` query parameter,
* `Authorization: Bearer <JWT>` using OIDC/Keycloak,
* authenticated Flask session.

---

## Canonical data exports

ECHOREPO Lite provides canonical exports from PostgreSQL.

Main export routes include:

```text
/download/canonical/samples.csv
/download/canonical/sample_images.csv
/download/canonical/sample_parameters.csv
/download/canonical/sample_biodiversity.csv
/download/canonical/all.zip
```

The canonical data model includes:

### `samples.csv`

Core sample-level observations, including:

```text
sample_id
timestamp_utc
lat
lon
country_code
location_accuracy_m
ph
organic_carbon_pct
earthworms_count
contamination_debris
contamination_plastic
pollutants_count
soil_structure_orig
soil_structure_en
soil_texture_orig
soil_texture_en
observations_orig
observations_en
metals_info_orig
metals_info_en
collected_by
data_source
qa_status
licence
```

### `sample_images.csv`

Image metadata associated with samples, including:

```text
sample_id
country_code
image_id
image_url
image_description_orig
image_description_en
collected_by
timestamp_utc
licence
```

### `sample_parameters.csv`

Laboratory and derived parameter records, including:

```text
sample_id
country_code
parameter_code
parameter_name
value
uom
analysis_method
analysis_date
lab_id
created_by
licence
parameter_uri
```

### `sample_biodiversity.csv`

Biodiversity OTU abundance records, including:

```text
sample_id
marker
otu_id
count
taxa
uploaded_at
uploaded_by
source_file
```

---

## Data and storage

Runtime data are expected to be mounted into the container rather than committed to the repository.

Common runtime mounts include:

```text
./data:/data
./storage:/data/storage
./keys:/keys:ro
```

The application can use MinIO-compatible object storage for:

* canonical snapshots,
* `canonical/latest/` exports,
* dated canonical bundles,
* storage-backed image or asset access.

Example canonical object paths:

```text
canonical/latest/all.zip
canonical/latest/samples.csv
canonical/latest/sample_images.csv
canonical/latest/sample_parameters.csv
canonical/<YYYY-MM-DD>/all.zip
```

---

## Database and migrations

The application uses PostgreSQL for canonical sample, image, parameter and biodiversity data.

On container startup, the configured command runs:

```bash
python scripts/wait_for_postgres.py
python scripts/run_pg_migrations.py
```

before starting Gunicorn.

This ensures that the PostgreSQL service is reachable and that migrations are applied before the web application starts.

---

## Internationalisation

ECHOREPO Lite uses Flask-Babel and translation resources under the application package. It supports multilingual templates and frontend labels.

The default locale list includes languages such as:

```text
en, cs, de, el, es, fi, fr, it, nl, pl, pt, ro, sk
```

Optional development tooling for translations is available through the `devtools` Compose profile, which includes LibreTranslate and an i18n helper container.

---

## Privacy, access control and data protection

ECHOREPO Lite includes several safeguards intended for citizen science data handling:

* configurable privacy gate,
* privacy acceptance tracking,
* API key and OIDC-based access modes,
* Keycloak-compatible login flow,
* suppression of selected personal fields in API outputs,
* optional hiding or filtering of wrong-coordinate samples,
* image EXIF stripping configuration,
* local and object-storage separation for runtime data.

Real secrets, service-account files and production `.env` files must not be committed to this repository.

---

## Development notes

Install Python dependencies locally:

```bash
python3 -m venv venv
source venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

Run the application through Docker Compose for the most reproducible development environment.

Useful development commands:

```bash
./start_dev.sh
docker compose logs -f echorepo-lite
docker compose exec echorepo-lite sh
docker compose exec postgres psql -U echorepo -d echorepo
```

---

## Deployment notes

For production-like deployment:

1. Set `APP_ENV=prod`.
2. Use a strong `SECRET_KEY`.
3. Set real PostgreSQL credentials.
4. Configure Keycloak/OIDC values.
5. Configure MinIO or compatible object storage.
6. Mount credentials read-only under `keys/`.
7. Use HTTPS at the reverse-proxy level.
8. Keep `.env` outside version control.
9. Run the production Compose override:

```bash
./start_prod.sh
```

---

## Citation

If you use this software in research, project reporting or derived infrastructure, please cite the archived software release.

When using Zenodo, cite the version-specific DOI for reproducibility. The Concept DOI can be used to refer to the software project across all versions.

Recommended citation format:

```text
Osychenko, O., Quanta Systems, S.L., and contributors. ECHOREPO Lite: lightweight web interface and data access layer for citizen science soil observations. Version <version>. Zenodo. <DOI>
```

---

## Related work

ECHOREPO Lite is part of the broader ECHOREPO and ECHO digital infrastructure for citizen science soil monitoring, open data publication and FAIR-oriented environmental data access.

Related components may include:

* ECHOREPO repository services,
* ECHO soil citizen science workflows,
* mobile and web-based soil sampling tools,
* Zenodo synchronisation workflows,
* SoilWise/EUSO-oriented findability workflows,
* laboratory enrichment pipelines,
* biodiversity data ingestion workflows.

---


## Maintainer

Quanta Systems, S.L.

Repository: `echosoil/echorepo-lite`
