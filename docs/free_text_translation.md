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

By default, only missing contamination_other, observations, metals_info and
image_description English fields are written. No soil texture/structure fields
are modified in any mode. Existing translations are preserved unless an explicit
retranslation scope is selected. Failed/unchanged unconfirmed strings
remain missing. Successful updates commit individually and guard against changes
to the source or destination during translation. Use the explicit retranslation workflow below for previously populated values.

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
Failed rows remain visible with error status. Comparison never rewrites existing translations.


## Controlled retranslation of existing English values

Use a new private journal path for each run. Preview counts without Docker or writes:

```bash
bash scripts/translate_data.sh --db-port 5432 --retranslate-all
bash scripts/translate_data.sh --db-port 5432 --retranslate-country IT
```

Pilot (at most ten attempted eligible values per field, including existing values):

```bash
mkdir -p "$HOME/translation-review"
LT_COMPOSE_DIR="$HOME/ECHO-STORE/echorepo-lite-dev" \
  bash scripts/translate_data.sh --db-port 5432 --apply \
  --retranslate-country IT --limit 10 \
  --backup-output "$HOME/translation-review/italy-pilot.csv"
```

Full run:

```bash
LT_COMPOSE_DIR="$HOME/ECHO-STORE/echorepo-lite-dev" \
  bash scripts/translate_data.sh --db-port 5432 --apply --retranslate-all \
  --backup-output "$HOME/translation-review/retranslation.csv"
```

Repeat `--retranslate-country` to select multiple countries, or use
`--retranslate-all` (mutually exclusive). Hybrid is still the default.
Retranslation includes both missing and populated English values. It never
clears an English value before translation and never replaces it on failed,
unsupported or unconfirmed results. Equal results are recorded without an
UPDATE. The UPDATE guards against concurrent changes to source, country and
previous English value. The usual incremental command is unchanged.

The owner-only CSV journal contains a scope record (database connection
coordinates, strategy, countries) and exact JSON-encoded keys, source, country,
previous and proposed English values. JSON preserves NULL, empty strings,
newlines and quotation marks. Keep the journal private: it contains original
participant text and sample IDs. Each pending record is flushed and fsynced
before the PostgreSQL write; a committed record follows successful commit.
A pending record alone is NOT proof of a completed write. This is a recovery
and audit journal, not an automatic rollback utility. Keep it outside Git.

If interrupted, use the same command, same scope and path, adding `--resume`.
For a limited pilot, remove `--limit` to continue beyond it with the same scope
and journal. Resume skips completed/unchanged records only when their current
source, country and English match the journal. It reconciles pending writes
whose proposed value is already present and retries pending writes whose old
value is still present. Changed records are reported as conflicts and not
overwritten. Failed/unsupported/unconfirmed translations remain eligible for
retry. A truncated journal or mismatching scope fails closed; do not edit it
blindly. Schedule retranslation separately from ingestion where practical.

Normal snapshots and previously published Zenodo versions are not rewritten by
this operation; regenerate live snapshots through the usual workflow if desired.


## Manual English corrections

Open /i18n/admin?scope=manual_en as an authorized editor. Set I18N_ADMIN_EDITORS
to a comma-separated list of allowed emails/usernames; if unset, the existing
COORDINATE_QA_EDITORS list is used. Empty lists deny access. The new Postgres
migration must be applied first (normal application startup). The Page texts
and JS keys tabs retain their previous behavior.

Search is case-insensitive, literal substring matching in original and current
English text, with 50 grouped results per page and no initial list. Each group
is an exact original string, country and free-text field; saving replaces all
matching canonical English values and creates persistent protection in
manual_en_overrides. Other fields/countries are not changed. Saving briefly locks writes to the relevant
canonical table (reads remain available). Removing protection
does not erase the current English value. Automatic backfill and retranslation
skip protected groups; database triggers also preserve corrections during
ingestion and legacy translator writes. No soil categories are editable here.

The former manual_overrides.json is not automatically imported: it has no
country/field scope, so applying its entries globally could change unrelated
records. Keep it as a reference and re-enter verified corrections using search.
Live PostgreSQL exports reflect changes; regenerate snapshots if needed.
