#!/usr/bin/env bash
set -euo pipefail

# 0) where the code is inside the container
WORK_DIR="/work"
TRANS_DIR="$WORK_DIR/echorepo/translations"
LT_HOST_URL="http://localhost:5001"       # host side
LT_CONTAINER_URL="http://libretranslate:5000"

echo "[1/6] Starting libretranslate + i18n..."
docker compose up -d libretranslate i18n

echo "[2/6] Waiting for LibreTranslate at $LT_HOST_URL ..."
for i in {1..30}; do
  if curl -s "$LT_HOST_URL/languages" >/dev/null 2>&1; then
    echo "LibreTranslate is up."
    break
  fi
  sleep 1
done

echo "[3/6] Running pybabel extract in i18n container..."
docker compose run --rm i18n \
  pybabel extract -F babel.cfg -o "$TRANS_DIR/messages.pot" "$WORK_DIR"

echo "[4/6] Running pybabel update in i18n container..."
docker compose run --rm i18n \
  pybabel update -i "$TRANS_DIR/messages.pot" -d "$TRANS_DIR"

echo "[5/6] Running auto_translate.py in i18n container..."
docker compose run --rm i18n \
  python tools/auto_translate.py --trans-dir "$TRANS_DIR" --endpoint "$LT_CONTAINER_URL"

echo "[6/6] Compiling translations in i18n container..."
docker compose run --rm i18n \
  pybabel compile -d "$TRANS_DIR"

echo "Stopping libretranslate..."
docker compose stop libretranslate

echo "✅ Translation pipeline done."
