# Repository overview: echorepo-lite

## Flask markers found

- run.py
- static/

## Routes (best-effort scan)

- `/<lang_code>`  _(in echorepo/i18n.py)_

## Directory tree (depth ≤ 3)

```
├── .env
├── .env.example
├── .github
│   └── workflows
│       └── i18n.yml
├── .gitignore
├── Dockerfile
├── Dockerfile.i18n
├── REPO_OVERVIEW.md
├── babel.cfg
├── data
│   ├── .gitkeep
│   ├── db
│   │   └── .gitkeep
│   ├── X.csv
│   ├── Y.csv
│   └── Z.csv
├── docker-compose.yml
├── echorepo
│   ├── __init__.py
│   ├── auth
│   │   ├── __init__.py
│   │   ├── decorators.py
│   │   ├── keycloak.py
│   │   ├── routes.py
│   │   └── tokens.py
│   ├── config.py
│   ├── extensions.py
│   ├── i18n.py
│   ├── routes
│   │   ├── __init__.py
│   │   ├── api.py
│   │   ├── errors.py
│   │   ├── lang.py
│   │   └── web.py
│   ├── services
│   │   ├── __init__.py
│   │   ├── db.py
│   │   ├── firebase.py
│   │   ├── planned.py
│   │   └── validation.py
│   ├── templates
│   │   ├── base.html
│   │   ├── issues.html
│   │   ├── login.html
│   │   └── results.html
│   ├── translations
│   │   ├── cs
│   │   │   └── LC_MESSAGES
│   │   ├── de
│   │   │   └── LC_MESSAGES
│   │   ├── el
│   │   │   └── LC_MESSAGES
│   │   ├── es
│   │   │   └── LC_MESSAGES
│   │   ├── fi
│   │   │   └── LC_MESSAGES
│   │   ├── fr
│   │   │   └── LC_MESSAGES
│   │   ├── it
│   │   │   └── LC_MESSAGES
│   │   ├── messages.pot
│   │   ├── nl
│   │   │   └── LC_MESSAGES
│   │   ├── pl
│   │   │   └── LC_MESSAGES
│   │   ├── pt
│   │   │   └── LC_MESSAGES
│   │   ├── ro
│   │   │   └── LC_MESSAGES
│   │   └── sk
│   │       └── LC_MESSAGES
│   ├── utils
│   │   ├── __init__.py
│   │   ├── coords.py
│   │   ├── country.py
│   │   ├── data
│   │   │   └── planned.xlsx
│   │   ├── geo.py
│   │   ├── load_csv.py
│   │   ├── planned.py
│   │   ├── pull_and_enrich_samples.py
│   │   ├── refresh_sqlite.py
│   │   ├── table.py
│   │   └── users.py
│   └── wsgi.py
├── instance
│   ├── .gitkeep
│   └── docker-compose.libretranslate.yml
├── keys
│   ├── .gitkeep
│   └── firebase-sa.json
├── requirements.txt
├── run.py
├── scripts
│   └── find_default_coords.py
├── static
│   ├── css
│   │   └── site.css
│   ├── fonts
│   │   ├── FuturaCyrillicBold.ttf
│   │   ├── FuturaCyrillicBook.ttf
│   │   ├── FuturaCyrillicDemi.ttf
│   │   ├── FuturaCyrillicExtraBold.ttf
│   │   ├── FuturaCyrillicHeavy.ttf
│   │   ├── FuturaCyrillicLight.ttf
│   │   └── FuturaCyrillicMedium.ttf
│   ├── img
│   │   ├── echorepo_logo_light.svg
│   │   └── favicon.ico
│   └── js
│       └── map.js
├── tools
│   ├── auto_translate.py
│   ├── make_repo_overview.py
│   ├── pull_and_enrich_samples.py
│   └── refresh_sqlite.py
└── translations
    ├── cs
    │   └── LC_MESSAGES
    │       ├── messages.mo
    │       └── messages.po
    ├── de
    │   └── LC_MESSAGES
    │       ├── messages.mo
    │       └── messages.po
    ├── el
    │   └── LC_MESSAGES
    │       ├── messages.mo
    │       └── messages.po
    ├── es
    │   └── LC_MESSAGES
    │       ├── messages.mo
    │       └── messages.po
    ├── fi
    │   └── LC_MESSAGES
    │       ├── messages.mo
    │       └── messages.po
    ├── fr
    │   └── LC_MESSAGES
    │       ├── messages.mo
    │       └── messages.po
    ├── it
    │   └── LC_MESSAGES
    │       ├── messages.mo
    │       └── messages.po
    ├── nl
    │   └── LC_MESSAGES
    │       ├── messages.mo
    │       └── messages.po
    ├── pl
    │   └── LC_MESSAGES
    │       ├── messages.mo
    │       └── messages.po
    ├── pt
    │   └── LC_MESSAGES
    │       ├── messages.mo
    │       └── messages.po
    ├── ro
    │   └── LC_MESSAGES
    │       ├── messages.mo
    │       └── messages.po
    └── sk
        └── LC_MESSAGES
            ├── messages.mo
            └── messages.po
```
