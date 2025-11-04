#!/usr/bin/env bash
set -euo pipefail

# --------------------------------------------------
# CONFIG – adjust if your names differ
# --------------------------------------------------
COMPOSE="docker compose"             # or 'docker-compose'
LT_PROFILE="--profile devtools"
LT_SERVICE="libretranslate"
I18N_SERVICE="i18n"

# where translations live INSIDE the app container
APP_TRANSL_DIR="/app/echorepo/translations"

# allow forcing from CLI: APP_CONTAINER=echorepo-lite ./tools/translate_all.sh
APP_CONTAINER="${APP_CONTAINER:-}"

LT_HOST_URL="${LT_HOST_URL:-http://localhost:5001}"
LT_CONTAINER_URL="http://libretranslate:5000"

echo "[1/5] Starting libretranslate + i18n..."
$COMPOSE $LT_PROFILE up -d "$LT_SERVICE" "$I18N_SERVICE"

echo "[2/5] Waiting for LibreTranslate at $LT_HOST_URL ..."
for i in {1..200}; do
  if curl -sf "$LT_HOST_URL/languages" >/dev/null 2>&1; then
    echo "LibreTranslate is up."
    break
  fi
  echo "  ... still waiting ($i)"
  sleep 5
done

if ! curl -sf "$LT_HOST_URL/languages" >/dev/null 2>&1; then
  echo "ERROR: LibreTranslate did not become ready. Aborting."
  exit 1
fi

echo "[3/5] Collecting languages from ./echorepo/translations ..."
if [ ! -d ./echorepo/translations ]; then
  echo "ERROR: ./echorepo/translations not found in current dir"
  exit 1
fi

LANGS=""
for d in ./echorepo/translations/*; do
  [ -d "$d" ] || continue
  lang="$(basename "$d")"
  if [ "$lang" != "en" ]; then
    LANGS+="$lang "
  fi
done

echo "Will translate into: $LANGS"

echo "[4/5] Running auto_translate.py in i18n container..."
$COMPOSE $LT_PROFILE run --rm "$I18N_SERVICE" \
  python tools/auto_translate.py \
    --trans-dir /work/echorepo/translations \
    --endpoint "$LT_CONTAINER_URL" \
    --source en \
    --langs $LANGS
echo "Translation step done."

# --------------------------------------------------
# find the REAL app container to compile inside
# --------------------------------------------------
if [ -z "$APP_CONTAINER" ]; then
  # 1) try explicit name you showed earlier
  candidate=$(docker ps --filter "name=echorepo-lite" --format '{{.Names}}' | head -n1 || true)

  # 2) if it picked the i18n one, discard it
  if echo "$candidate" | grep -qi "i18n"; then
    candidate=""
  fi

  # 3) try any container whose IMAGE contains 'echorepo-lite'
  if [ -z "$candidate" ]; then
    candidate=$(docker ps --format '{{.Names}} {{.Image}}' \
      | grep 'echorepo-lite' \
      | grep -vi 'i18n' \
      | awk '{print $1}' \
      | head -n1 || true)
  fi

  APP_CONTAINER="$candidate"
fi

if [ -z "$APP_CONTAINER" ]; then
  echo "WARN: could not autodetect app container."
  echo "Run this yourself in the app container:"
  echo "  docker exec -it <app-container> pybabel compile -d $APP_TRANSL_DIR"
else
  echo "[5/5] Compiling translations inside container: $APP_CONTAINER ..."
  docker exec "$APP_CONTAINER" pybabel compile -d "$APP_TRANSL_DIR"
  echo "Compilation done."
fi

echo "Stopping temporary i18n services..."
$COMPOSE $LT_PROFILE down

echo "All done ✅"
