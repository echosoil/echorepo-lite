# Free-text English translations

Run on the Docker host, with the project's Python environment. The default is
read-only counts; use the explicit database port and --apply to write. For
production, use the production checkout/.env (port 5432); development uses 5433.

```bash
bash scripts/translate_data.sh --db-port 5432
LT_COMPOSE_DIR="$HOME/ECHO-STORE/echorepo-lite-dev" \
  bash scripts/translate_data.sh --db-port 5432 --limit 10 --apply
LT_COMPOSE_DIR="$HOME/ECHO-STORE/echorepo-lite-dev" \
  bash scripts/translate_data.sh --db-port 5432 --apply
```

LT_COMPOSE_DIR selects the existing Compose checkout (not the database). If the
existing Compose project has an explicit name, set LT_COMPOSE_PROJECT too.
TRANSLATION_PYTHON may specify the host virtualenv interpreter.
The wrapper starts only libretranslate, checks loopback port 5001, waits for
readiness, and stops only that service on exit, even after failures or signals.
It deliberately stops a pre-existing LibreTranslate in that project too; do not
run concurrently with the UI catalogue translator. A host-user lock prevents
overlapping backfills. It never runs compose down or starts PostgreSQL.

Only missing contamination_other, observations, metals_info and
image_description English fields are written. No soil texture/structure fields
or existing translations are modified. Failed/unchanged unconfirmed strings
remain missing. Successful updates commit individually and guard against changes
to the source or destination during translation. The script does not fix
previously populated incorrect or stale translations.

Map pop-ups show original text first and a translation icon when a distinct
English translation is available. Hover for the browser tooltip or click/tap
the text to expand; keyboard users can focus the summary and use Enter/Space.
The personal table reads canonical translations for the authenticated user's
own sample/image rows; source text must match. It never adds English columns.
Structured lab concentration values retain the existing formatting and precedence.
The existing UI catalogue translation workflow is independent.
