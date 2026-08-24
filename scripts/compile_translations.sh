#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_ROOT"

export LOCAL_UID="$(id -u)"
export LOCAL_GID="$(id -g)"

TRANS_DIR="/work/echorepo/translations"

echo "[1/2] Removing stale compiled translation files..."

docker compose run \
  --no-deps \
  --rm \
  --user "${LOCAL_UID}:${LOCAL_GID}" \
  i18n \
  sh -c "find '$TRANS_DIR' -type f -name '*.mo' -delete"

echo "[2/2] Compiling translations with pybabel..."

docker compose run \
  --no-deps \
  --rm \
  --user "${LOCAL_UID}:${LOCAL_GID}" \
  i18n \
  pybabel compile -d "$TRANS_DIR"

echo "✅ Translation compilation done."