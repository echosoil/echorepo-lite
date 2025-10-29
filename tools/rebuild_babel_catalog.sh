#!/usr/bin/env bash
set -euo pipefail

# Config
LOCALES=(cs de el es fi fr it nl pl pt ro sk)
TRANS_DIR="echorepo/translations"
POT="$TRANS_DIR/messages.pot"
LT_URL_DEFAULT="http://host.docker.internal:5001"

# 0) Stop app (optional)
docker compose stop echorepo-lite || true

# 1) Clean catalogs
find "$TRANS_DIR" -name 'messages.po' -delete || true
find "$TRANS_DIR" -name 'messages.mo' -delete || true
rm -f "$POT"

# 2) Extract
docker compose --profile devtools run --rm i18n \
  pybabel extract -F babel.cfg -o "$POT" .

# 3) Init locales
for lang in "${LOCALES[@]}"; do
  docker compose --profile devtools run --rm i18n \
    pybabel init -i "$POT" -d "$TRANS_DIR" -l "$lang"
done

# 4) (Optional) repair-only pass (safe placeholder cleanup)
docker compose --profile devtools run --rm i18n \
  python tools/auto_translate.py \
    --trans-dir "$TRANS_DIR" \
    --langs "${LOCALES[@]}" \
    --repair-only --verbose

# 5) Machine translate empties via LT
docker compose --profile devtools run --rm -e LT_URL="${LT_URL:-$LT_URL_DEFAULT}" i18n \
  python tools/auto_translate.py \
    --trans-dir "$TRANS_DIR" \
    --langs "${LOCALES[@]}" \
    --batch 60 --verbose

# 6) Compile .mo (writes to host; app reads via RO mount)
docker compose --profile devtools run --rm i18n \
  pybabel compile -d "$TRANS_DIR" -D messages

# 7) Restart app (pick up new .mo files)
docker compose up -d --force-recreate echorepo-lite
