# Repository overview: echorepo-lite-dev

## Flask markers found

- run.py
- static/

## Routes (best-effort scan)

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
- `/sso/callback`  _(in echorepo/auth/routes.py)_
- `/sso/login`  _(in echorepo/auth/routes.py)_
- `/<lang_code>`  _(in echorepo/i18n.py)_
- `/download/sample_csv`  _(in echorepo/routes/api.py)_
- `/others_geojson`  _(in echorepo/routes/api.py)_
- `/user_geojson`  _(in echorepo/routes/api.py)_
- `/user_geojson_debug`  _(in echorepo/routes/api.py)_
- `/issues`  _(in echorepo/routes/errors.py)_
- `/issues/fix-coords`  _(in echorepo/routes/errors.py)_
- `/admin`  _(in echorepo/routes/i18n_admin.py)_
- `/admin/set`  _(in echorepo/routes/i18n_admin.py)_
- `/labels.js`  _(in echorepo/routes/i18n_admin.py)_
- `/set-lang/<lang_code>`  _(in echorepo/routes/lang.py)_
- `/`  _(in echorepo/routes/web.py)_
- `/download/all_csv`  _(in echorepo/routes/web.py)_
- `/download/csv`  _(in echorepo/routes/web.py)_
- `/download/sample_csv`  _(in echorepo/routes/web.py)_
- `/download/xlsx`  _(in echorepo/routes/web.py)_
- `/i18n/labels`  _(in echorepo/routes/web.py)_
- `/x`  _(in tools/make_repo_overview.py)_
- `/x`  _(in tools/make_repo_overview.py)_
- `/x`  _(in tools/make_repo_overview.py)_
- `/x`  _(in tools/make_repo_overview.py)_
- `/x`  _(in tools/make_repo_overview.py)_

## Directory tree (depth ≤ 3)

```
├── .github
│   └── .github/workflows
├── data
│   └── .gitkeep
├── echorepo
│   ├── echorepo/auth
│   ├── echorepo/routes
│   ├── echorepo/services
│   ├── echorepo/templates
│   ├── echorepo/translations
│   ├── echorepo/utils
│   ├── __init__.py
│   ├── config.py
│   ├── extensions.py
│   ├── i18n.py
│   └── wsgi.py
├── keys
│   └── .gitkeep
├── scripts
│   └── find_default_coords.py
├── static
│   ├── static/css
│   ├── static/fonts
│   ├── static/img
│   └── static/js
├── tools
│   ├── auto_translate.py
│   ├── feature.sh
│   ├── i18n_override.py
│   ├── make_release.sh
│   ├── make_repo_overview.py
│   ├── pull_and_enrich_samples.py
│   ├── pull_and_enrich_samples_dev.py
│   ├── rebuild_babel_catalog.sh
│   ├── refresh_sqlite.py
│   └── refresh_sqlite_dev.py
├── .env.example
├── .gitignore
├── Dockerfile
├── Dockerfile.i18n
├── REPO_OVERVIEW.md
├── babel.cfg
├── docker-compose.dev.yml
├── docker-compose.nosplit.yml
├── docker-compose.prod.yml
├── docker-compose.yml
├── requirements.txt
├── run.py
├── start_dev.sh
├── start_prod.sh
├── stop_dev.sh
└── stop_prod.sh
```
