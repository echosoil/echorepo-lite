#!/usr/bin/env bash
set -euo pipefail

# Path layout inside the i18n container
WORK_DIR="/work"
TRANS_DIR="$WORK_DIR/echorepo/translations"

echo "[1/2] Compiling translations with pybabel..."
docker compose run --rm i18n \
  pybabel compile -d "$TRANS_DIR"

echo "✅ Translation compilation done."
