# ECHOREPO Lite

**ECHOREPO Lite** is the web application, data-access API, and publication tooling for the [ECHO soil citizen-science project](https://echosoil.eu). It brings together field observations, sampling locations, images, laboratory measurements, and microbial biodiversity data under stable sample identifiers.

The repository contains the Flask application, PostgreSQL-backed data services, quality-control utilities, privacy safeguards, machine-readable canonical exports, and tools for versioned FAIR-oriented publication.

**Project:** ECHO — Engaging Citizens in soil science: the road to Healthier sOils · Horizon Europe grant 101112869  
**Software licence:** [MIT](LICENSE) · **Maintainer:** Quanta Systems, S.L.

## Highlights

- **Explore and search:** interactive sampling map, country and biogeographical-region filters, sample details, laboratory results, images, and biodiversity summaries.
- **Structured access:** authenticated /api/v1 endpoints for JSON, CSV, GeoJSON, counts, and downloadable canonical machine bundles.
- **Linked research data:** a common sample ID links citizen observations to imagery, laboratory parameters, and biodiversity results.
- **Quality and privacy:** coordinate issue detection and review; access controls; public-coordinate obfuscation; optional image/EXIF privacy processing. These safeguards require deployment-specific configuration and verification.
- **Reproducible publication:** generate canonical CSV resources plus CSVW metadata and create new versions of the ECHO dataset on Zenodo.
- **Research interfaces:** optional ECHOREPO-specific JupyterLab and Streamlit components, configured separately from the main web application.
- **Open implementation:** Docker Compose deployment, migrations, localisation and data-processing utilities.

The code does **not** imply that ECHOREPO is formally registered as an EOSC service or that every optional component is production-qualified.

## Where to find things

| Directory / file | Purpose |
| --- | --- |
| echorepo/ | Flask application, authentication, routes, templates, and services |
| echorepo/routes/data_api.py | Authenticated machine-facing endpoints |
| echorepo/services/canonical_exports.py | Canonical CSV and ZIP data contracts |
| tools/ | Import, QA, privacy, enrichment, validation, and Zenodo tools |
| scripts/ | Operational and publication wrappers |
| metadata/canonical/ | Parameter and method definitions and curated metadata |
| notebooks/ and jupyter/ | ECHOREPO Jupyter environment and examples |
| streamlit/ | ECHOREPO research explorer |
| docs/ | API guide, biodiversity publication guide, release guidance |
| docker-compose*.yml | Base, development, production, Jupyter and Streamlit configuration |
| .env.example | Illustrative configuration only; not safe for production unchanged |

For the detailed API contract, see [ECHOREPO Data API Guide](docs/ECHOREPO_API_Data_Guide_v4.md). Some older documentation and scripts retain historical field names; prefer the actual canonical export schema for your release.

## Quick start (development)

Requirements: Docker with Compose, Git, and your own authorised access to the relevant ECHO data and external services.

~~~bash
git clone https://github.com/echosoil/echorepo-lite.git
cd echorepo-lite
cp .env.example .env
~~~

Edit .env and replace the example credentials, hostnames and local paths. In particular review SECRET_KEY, API_KEY, PostgreSQL credentials, Keycloak/OIDC, MinIO, privacy configuration and any Firebase access. **Do not use the example secrets or debug settings in production.**

The Compose setup expects a pre-existing external Docker network:

~~~bash
docker network inspect echorepo-shared >/dev/null 2>&1 || docker network create echorepo-shared
./start_dev.sh
~~~

The development deployment is configured to expose the application at http://localhost:18080. Database access and other dependencies depend on your local Compose overrides.

Production deployments use a separate override and environment configuration:

~~~bash
./start_prod.sh
~~~

The production override exposes the application on host port 8001. Configure HTTPS, access control, persistent volumes and backups for your environment. Review the Compose files before deployment; **running these commands does not provision a database or the ECHO data itself**.

For isolated Jupyter and Streamlit components, see docker-compose.jupyter.yml, docker-compose.streamlit.yml and [notebooks/README.md](notebooks/README.md). They rely on an accessible ECHOREPO backend and the shared Docker network.

## API access

The API prefix is /api/v1. The /api/v1/ping health check requires no API credentials. Canonical bundle downloads require authentication, even when some other public access flags are enabled.

An authorised API key is accepted in the X-API-Key header; deployment configuration can also support an OIDC bearer token or an authenticated web session.

~~~bash
export ECHOREPO_API="https://echorepo.quanta-labs.com/api/v1"
# Provide ECHOREPO_API_KEY securely in your shell environment.

curl -fsS "$ECHOREPO_API/ping"

curl -fL -H "X-API-Key: $ECHOREPO_API_KEY" \
  "$ECHOREPO_API/canonical/all.zip" -o canonical_all.zip
~~~

Avoid passing API keys in URL query parameters, where they can be recorded in logs or browser history.

Selected routes:

| Endpoint | Use |
| --- | --- |
| GET /api/v1/canonical/samples | Canonical observations (JSON, CSV and supported geospatial output) |
| GET /api/v1/canonical/samples/count | Filtered sample count |
| GET /api/v1/canonical/sample_images | Sample-linked image metadata |
| GET /api/v1/canonical/sample_parameters | Laboratory measurements |
| GET /api/v1/canonical/sample_biodiversity | Sample-linked microbial taxonomy statistics |
| GET /api/v1/canonical/all.zip | Authenticated six-resource canonical ZIP |
| GET /api/v1/canonical/snapshot/all.zip | Create and store a machine snapshot |
| GET /api/v1/biodiversity/raw/all.zip | Raw biodiversity export |
| POST /api/v1/lab-enrichment | Authorised laboratory enrichment |

Filters and response formats depend on the route. See the API guide and current route implementation for supported parameters.

### Canonical machine bundle

The authenticated /api/v1/canonical/all.zip bundle contains six CSV files:

~~~text
samples.csv
sample_images.csv
sample_parameters.csv
sample_biodiversity.csv
parameter_definitions.csv
analysis_methods.csv
~~~

The public schema intentionally differs in places from the internal PostgreSQL column names:

- samples.csv uses sampling_datetime_utc, soil_ph_field, soil_organic_matter_estimate_pct and coordinate_obfuscation_radius_m.
- sample_images.csv uses image_datetime_utc.
- sample_parameters.csv uses result_value, unit, method_code and analysis_datetime_utc, linked to parameter_definitions.csv and analysis_methods.csv.
- sample_biodiversity.csv uses taxon_rank, scientific_name, read_count, relative_abundance_pct and ingested_datetime_utc.

A publisher-generated file.json adds CSVW-style metadata, table descriptions, relationships and provenance for a Zenodo deposition. **The API's all.zip does not itself include file.json.** The authoritative export logic lives in echorepo/services/canonical_exports.py and is checked by tools/validate_echorepo_bundle.py.

The main canonical bundle contains summarised biodiversity data; separate scripts publish raw biodiversity matrices when appropriate.

## FAIR and Zenodo publication

The Zenodo tooling downloads an authenticated ECHOREPO bundle, validates and normalises the resources, generates machine-readable CSVW metadata, and publishes them as a new dataset version when requested. The publishing step is a controlled operation; the automated workflow should not be interpreted as an automatic approval of data quality or publication rights.

Configuration is kept in a private .env_zenodo file. Start from [.env_zenodo.example](.env_zenodo.example). ECHOREPO_API_KEY in that file must be accepted by the application’s API_KEY setting.

~~~bash
# Validation/preparation, without a new Zenodo version:
./scripts/update_zenodo_bundle.sh --dry-run

# Actual new Zenodo dataset version, after validation and review:
./scripts/update_zenodo_bundle.sh
~~~

For the first deposit, use the corresponding scripts/publish_zenodo_bundle.sh workflow instead. The update wrapper maintains the latest successful deposition ID in a local state file; check that ID before publishing.

Keep separate the **dataset record** (whose collective creator is ECHO Consortium) and the **software release** (this GitHub repository, independently archived through the GitHub–Zenodo connection).

- [ECHOREPO dataset](https://doi.org/10.5281/zenodo.19722513) — published data and metadata.
- [GitHub software releases](https://github.com/echosoil/echorepo-lite/releases) — versioned source code.
- [Biodiversity publication guide](docs/biodiversity_zenodo_publication.md) — staged workflow for large raw biodiversity resources.

Dataset creators, contributors and their roles are configured separately from software citation metadata. Production credentials and publication state files must never be committed.

## Data stewardship and limitations

Citizen-collected coordinates, sample identities and images may contain errors or sensitive content. Apply the project’s QA, consent, access-control and privacy procedures before releasing any data; do not assume that every record is fit for public use.

Canonical exports may exclude, transform or standardise selected fields. In particular, image records expose metadata and image URLs rather than automatically packaging every image byte into the main ZIP. Some measurements and biodiversity data are released in separate products.

Publication through Zenodo, discoverability through SoilWise and formal EOSC service onboarding are **different** interoperability milestones.

## Development

The application primarily targets Python 3.11 in Docker. Dependencies are listed in requirements.txt.

~~~bash
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
python3 -m compileall -q echorepo tools scripts
~~~

The repository currently does not define a GitHub Actions test workflow. A release requires manual checks of the API, data contracts, publication dry-run, database migrations and the intended deployment.

For how to publish source-code versions, see [Releasing ECHOREPO Lite](docs/RELEASING.md).

## Citation, licence and funding

Cite **software** using the version-specific archive DOI associated with the relevant GitHub release. Cite **research datasets** using the DOI of the specific Zenodo dataset version you analysed. These are distinct products and should not be cited interchangeably.

See [CITATION.cff](CITATION.cff) for the software citation metadata. The implementation is licensed under the [MIT License](LICENSE), copyright © 2026 Quanta Systems, S.L.

ECHOREPO is developed within ECHO, funded by the European Union under Horizon Europe grant agreement **101112869**. See [echosoil.eu](https://echosoil.eu).
