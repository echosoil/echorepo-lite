# Repository overview: echorepo-lite-dev

## Flask markers found

- run.py
- static/

## Routes (best-effort scan)

- `/debug/whoami`  _(in echorepo/__init__.py)_
- `/i18n/check-overrides`  _(in echorepo/__init__.py)_
- `/i18n/debug`  _(in echorepo/__init__.py)_
- `/i18n/labels.js`  _(in echorepo/__init__.py)_
- `/i18n/labels.json`  _(in echorepo/__init__.py)_
- `/i18n/probe-json`  _(in echorepo/__init__.py)_
- `/i18n/probe-tpl`  _(in echorepo/__init__.py)_
- `/diag/oidc`  _(in echorepo/auth/routes.py)_
- `/login`  _(in echorepo/auth/routes.py)_
- `/login`  _(in echorepo/auth/routes.py)_
- `/logout`  _(in echorepo/auth/routes.py)_
- `/password-reset`  _(in echorepo/auth/routes.py)_
- `/sso/callback`  _(in echorepo/auth/routes.py)_
- `/sso/login`  _(in echorepo/auth/routes.py)_
- `/<lang_code>`  _(in echorepo/i18n.py)_
- `/download/sample_csv`  _(in echorepo/routes/api.py)_
- `/others_geojson`  _(in echorepo/routes/api.py)_
- `/user_geojson`  _(in echorepo/routes/api.py)_
- `/user_geojson_debug`  _(in echorepo/routes/api.py)_
- `/canonical/all.zip`  _(in echorepo/routes/data_api.py)_
- `/canonical/map.count`  _(in echorepo/routes/data_api.py)_
- `/canonical/map.geojson`  _(in echorepo/routes/data_api.py)_
- `/canonical/sample_biodiversity`  _(in echorepo/routes/data_api.py)_
- `/canonical/sample_images`  _(in echorepo/routes/data_api.py)_
- `/canonical/sample_parameters`  _(in echorepo/routes/data_api.py)_
- `/canonical/samples`  _(in echorepo/routes/data_api.py)_
- `/canonical/samples/count`  _(in echorepo/routes/data_api.py)_
- `/canonical/snapshot/all.zip`  _(in echorepo/routes/data_api.py)_
- `/canonical/zenodo_bundle.zip`  _(in echorepo/routes/data_api.py)_
- `/lab-enrichment`  _(in echorepo/routes/data_api.py)_
- `/ping`  _(in echorepo/routes/data_api.py)_
- `/samples`  _(in echorepo/routes/data_api.py)_
- `/samples/count`  _(in echorepo/routes/data_api.py)_
- `/issues`  _(in echorepo/routes/errors.py)_
- `/issues/fix-coords`  _(in echorepo/routes/errors.py)_
- `/issues/why`  _(in echorepo/routes/errors.py)_
- `/admin`  _(in echorepo/routes/i18n_admin.py)_
- `/admin/manual_set`  _(in echorepo/routes/i18n_admin.py)_
- `/admin/set`  _(in echorepo/routes/i18n_admin.py)_
- `/labels.js`  _(in echorepo/routes/i18n_admin.py)_
- `/set-lang/<lang_code>`  _(in echorepo/routes/lang.py)_
- `/exports/canonical/<date>/<filename>`  _(in echorepo/routes/storage.py)_
- `/exports/canonical/<filename>`  _(in echorepo/routes/storage.py)_
- `/storage/<path:relpath>`  _(in echorepo/routes/storage.py)_
- `/`  _(in echorepo/routes/web.py)_
- `/admin/usage`  _(in echorepo/routes/web.py)_
- `/coordinate-issues`  _(in echorepo/routes/web.py)_
- `/coordinate-issues/approve`  _(in echorepo/routes/web.py)_
- `/download/all_csv`  _(in echorepo/routes/web.py)_
- `/download/canonical/<date>/<filename>`  _(in echorepo/routes/web.py)_
- `/download/canonical/all.zip`  _(in echorepo/routes/web.py)_
- `/download/canonical/sample_biodiversity.csv`  _(in echorepo/routes/web.py)_
- `/download/canonical/sample_images.csv`  _(in echorepo/routes/web.py)_
- `/download/canonical/sample_parameters.csv`  _(in echorepo/routes/web.py)_
- `/download/canonical/samples.csv`  _(in echorepo/routes/web.py)_
- `/download/csv`  _(in echorepo/routes/web.py)_
- `/download/sample_csv`  _(in echorepo/routes/web.py)_
- `/download/xlsx`  _(in echorepo/routes/web.py)_
- `/explore`  _(in echorepo/routes/web.py)_
- `/i18n/labels`  _(in echorepo/routes/web.py)_
- `/lab-import`  _(in echorepo/routes/web.py)_
- `/lab-import-auto`  _(in echorepo/routes/web.py)_
- `/lab-import-biodiversity`  _(in echorepo/routes/web.py)_
- `/lab-upload`  _(in echorepo/routes/web.py)_
- `/lab-upload`  _(in echorepo/routes/web.py)_
- `/labels`  _(in echorepo/routes/web.py)_
- `/my`  _(in echorepo/routes/web.py)_
- `/privacy/accept`  _(in echorepo/routes/web.py)_
- `/public/others_geojson`  _(in echorepo/routes/web.py)_
- `/public/sample_image/<sample_id>`  _(in echorepo/routes/web.py)_
- `/public/sample_piechart/<sample_id>`  _(in echorepo/routes/web.py)_
- `/publications/zenodo`  _(in echorepo/routes/web.py)_
- `/search`  _(in echorepo/routes/web.py)_
- `/x`  _(in tools/make_repo_overview.py)_
- `/x`  _(in tools/make_repo_overview.py)_
- `/x`  _(in tools/make_repo_overview.py)_
- `/x`  _(in tools/make_repo_overview.py)_
- `/x`  _(in tools/make_repo_overview.py)_

## Directory tree (depth ≤ 4)

```
├── .github
│   └── .github/workflows
├── data
│   └── .gitkeep
├── docs
│   ├── ECHOREPO_Architecture.md
│   ├── ECHOREPO_Data_API_Guide_v3.html
│   ├── ECHOREPO_Data_API_Guide_v3.md
│   ├── ECHOREPO_Zenodo_Sync_API_Guide_v1.md
│   ├── translate_all.html
│   └── translate_all.md
├── echorepo
│   ├── echorepo/auth
│   ├── echorepo/routes
│   ├── echorepo/services
│   ├── echorepo/templates
│   ├── echorepo/translations
│   ├── echorepo/utils
│   ├── __init__.py
│   ├── analytics.py
│   ├── config.py
│   ├── extensions.py
│   ├── i18n.py
│   └── wsgi.py
├── keys
│   └── .gitkeep
├── migrations
│   └── migrations/postgres
├── scripts
│   ├── .gitkeep
│   ├── compile_translations.sh
│   ├── feature.sh
│   ├── make_release.sh
│   ├── publish_zenodo_bundle.sh
│   ├── run_pg_migrations.py
│   ├── translate_all.sh
│   ├── update_zenodo_bundle.sh
│   └── wait_for_postgres.py
├── static
│   ├── static/css
│   ├── static/fonts
│   ├── static/img
│   ├── static/js
│   └── static/privacy
├── storage
│   └── .gitkeep
├── tools
│   ├── tools/biodiversity
│   ├── tools/sql
│   ├── auto_translate.py
│   ├── check_lab_enrichment_qrs.py
│   ├── create_funguild_db_once.sh
│   ├── create_indexes.py
│   ├── find_default_coords.py
│   ├── firebase_kc_sync.py
│   ├── generate_biodiversity_piecharts.py
│   ├── i18n_override.py
│   ├── make_repo_overview.py
│   ├── publish_api_file_to_zenodo.py
│   ├── pull_and_enrich_samples.py
│   ├── refresh_sqlite.py
│   ├── run_sql.py
│   ├── run_sql_pg.py
│   └── translate_pg_en.py
├── .dockerignore
├── .env.example
├── .env_zenodo.example
├── .gitignore
├── .pre-commit-config.yaml
├── Dockerfile
├── Dockerfile.i18n
├── Makefile
├── REPO_OVERVIEW.md
├── REPO_OVERVIEW_tmp.html
├── babel.cfg
├── docker-compose.dev.yml
├── docker-compose.prod.yml
├── docker-compose.storage.yml
├── docker-compose.yml
├── project_paths.py
├── pyproject.toml
├── requirements-dev.txt
├── requirements.txt
├── run.py
├── start_dev.sh
├── start_prod.sh
├── start_translate_containers.sh
├── stop_dev.sh
├── stop_prod.sh
└── stop_translate_containers.sh
```
