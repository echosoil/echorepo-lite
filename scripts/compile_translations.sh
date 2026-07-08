#!/usr/bin/env bash
set -euo pipefail

WORK_DIR="/work"
TRANS_DIR="$WORK_DIR/echorepo/translations"

echo "[1/2] Compiling translations with pybabel..."

docker compose run --no-deps --rm i18n \
  pybabel compile -d "$TRANS_DIR"

echo "✅ Translation compilation done."