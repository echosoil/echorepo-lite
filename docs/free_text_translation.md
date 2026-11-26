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

## Photo captions

Canonical image descriptions include the nonempty photo option and comment (in
that order, on separate lines if distinct). This preserves participant text
such as an option-only Italian caption. All flattened photo path slots are
included in canonical ingestion; the personal table still displays its first
three photo columns and the map shows its existing representative photo.
When an image description changes, ingestion clears only that image's English
translation so the next translation backfill can regenerate it. To repair old
records, run the regular ingestion with PostgreSQL upsert enabled, then run
`scripts/translate_data.sh` against the same database. Refresh any cached
snapshots separately; existing Zenodo depositions are unchanged.


## Country-guided source language selection

The default `--language-strategy hybrid` uses country as a prior, not a claim
about the participant's language. The available models are discovered at runtime
from LibreTranslate /languages (English target required); no models are downloaded
or enabled automatically.

- `auto`: previous automatic source selection.
- `country`: force the primary country language if supported; otherwise fall
  back to detection. An English prior in this explicit mode preserves the text.
- `hybrid`: prioritize the supported country language on ambiguous text.
  A different detected language can override it at >=95% for short texts
  (<=3 words or <15 letters) and >=85% for longer text, with a 15-point
  lead over the next candidate. Alternative national languages have lower
  thresholds (85% short / 75% long). Strongly detected unsupported languages
  are left missing, rather than translated with an unrelated model.
  Unknown countries and unavailable priors fall back to detection/auto;
  English is not inferred solely from an English-speaking country.

Thresholds are initial heuristics, not measured accuracy or calibrated
probabilities. Review the comparison CSV before tuning them. The cache keys
include country and strategy. Only missing translations are backfilled; prior
English values and both soil category fields remain untouched.

```bash
LT_COMPOSE_DIR="$HOME/ECHO-STORE/echorepo-lite-dev" \
  bash scripts/translate_data.sh --db-port 5432 --language-strategy hybrid --limit 10 --apply
```

### Read-only comparison

Run a deterministic, deduplicated sample of up to 50 unique source strings per
country across all four free-text fields, including previously translated text.
Compare auto, forced-country and hybrid candidates side by side without
changing PostgreSQL. The wrapper manages the same LibreTranslate container.

```bash
mkdir -p "$HOME/translation-review"
LT_COMPOSE_DIR="$HOME/ECHO-STORE/echorepo-lite-dev" \
  bash scripts/translate_data.sh --db-port 5432 \
    --compare-country IT --compare-country ES --compare-country DE \
    --compare-per-country 50 \
    --compare-output "$HOME/translation-review/language-comparison.csv"
```

Omit the repeated country filters to sample all countries (including missing
country codes). The report includes the detected language, confidence, hybrid
decision, translations and per-strategy status. It is a review aid, not an
automated quality score. It contains participant free text, so keep it private.
The CSV is created with owner-only permissions and never overwrites a file;
formula-like text is prefixed with an apostrophe for spreadsheet safety.
Failed rows remain visible with error status. Existing translations are not
rewritten. A separate explicit retranslation workflow would be needed for that.
