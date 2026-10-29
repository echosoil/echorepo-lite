# ECHOREPO Lite v0.3.0 — FAIR data publication and research data workflows

**Status:** proposed release notes. Review the tagged commit and complete the release checks before publishing.

This is the first feature release after [v0.2.0](https://github.com/echosoil/echorepo-lite/releases/tag/v0.2.0) (1 July 2026). It extends ECHOREPO beyond a basic soil-data exploration interface into a more complete research-data workflow, including canonical exports, laboratory/biodiversity integration, quality and privacy controls, and reproducible Zenodo publication.

## Highlights

### FAIR-oriented canonical datasets
- Six linked CSV resources: `samples.csv`, `sample_images.csv`, `sample_parameters.csv`, `sample_biodiversity.csv`, `parameter_definitions.csv`, `analysis_methods.csv`.
- Machine-readable canonical ZIP download at `/api/v1/canonical/all.zip`, with consistent sample identities and controlled reference tables.
- Public export fields are normalised independently of the internal database schema.
- A companion validator checks schema and dataset integrity.

### Versioned Zenodo publication and metadata
- Python publication utility builds CSVW-style `file.json` metadata describing the canonical resources.
- Publication wrappers support initial publication, new versions, metadata validation, a dry-run, and persistence of the last successfully published deposition ID.
- Dataset creator and data-curator contributor are represented separately (the ECHO Consortium can be the creator while the curator receives contributor credit).
- The publication pipeline requires appropriate ECHOREPO credentials and independent Zenodo credentials.

### Quality assurance, geospatial privacy and data access
- Coordinate issue review and correction workflows, including administrative backfill tools.
- Map/search filtering enhancements including country and biogeographical regions.
- Configurable access control for APIs and canonical map endpoints.
- Image privacy redaction tools and EXIF handling, subject to configuration and careful verification.

### Research enrichment and exploration
- Laboratory measurement ingestion and parameter reference data.
- Microbial biodiversity integration, summary exports and separate workflows for raw biodiversity publication.
- ECHOREPO-specific JupyterLab and Streamlit exploration components.
- Broader multilingual coverage and updated translation tools.

## Compatibility and deployment notes

**This release includes many changes since v0.2.0. Review deployment and data contracts before upgrading.**

- The public canonical CSV schemas intentionally rename some columns (for example `sampling_datetime_utc`, `soil_ph_field`, `result_value`, and `taxon_rank`). See [API and Canonical Data Guide](docs/ECHOREPO_API_Data_Guide_v4.md).
- The canonical ZIP download requires authentication using an accepted API key, bearer token or session; do not rely on `CANONICAL_PUBLIC` for `/api/v1/canonical/all.zip`.
- Review environment variables in `.env.example`, database migrations, MinIO configuration, persisted volumes and reverse proxy settings.
- JupyterLab and Streamlit are optional components, not mandatory services of the base deployment.
- Do not replace a live production `.env` with an example or a development configuration without reviewing each value.
- Publication workflows must use the intended Zenodo deposition chain and verified metadata; dataset Zenodo records are separate from source-code releases.
- Review whether any legacy scripts, locally maintained modifications, and manually generated files need to be preserved during upgrades.

## Verification before publication

The following must be checked on the actual release commit, not assumed from source availability:

- [ ] App starts cleanly against a test database and migrations complete.
- [ ] Authenticated `/api/v1/canonical/all.zip` downloads successfully and the bundle validator passes.
- [ ] Map, search, coordinate issue review and privacy behaviour are exercised.
- [ ] Laboratory and biodiversity exports are checked against representative records.
- [ ] Zenodo preparation passes a `--dry-run` (no new deposition).
- [ ] Relevant contributor/creator metadata and URLs are correct.
- [ ] No credentials, runtime dumps or private sample data are committed.
- [ ] The software release DOI is obtained from the GitHub–Zenodo integration, if enabled.

## Documentation

- [README](README.md)
- [API and canonical data guide](docs/ECHOREPO_API_Data_Guide_v4.md)
- [Biodiversity publication workflow](docs/biodiversity_zenodo_publication.md)
- [Release procedure](docs/RELEASING.md)

## Citation and project

This is a **software** release. Its GitHub release and GitHub-connected Zenodo DOI (where archived) are distinct from the **ECHOREPO dataset** and its dataset DOI [10.5281/zenodo.19722513](https://doi.org/10.5281/zenodo.19722513).

ECHO — *Engaging Citizens in soil science: the road to Healthier sOils*, funded under Horizon Europe grant agreement 101112869. Project: https://echosoil.eu.

**Full changes:** https://github.com/echosoil/echorepo-lite/compare/v0.2.0...v0.3.0 (available after creating the tag).
