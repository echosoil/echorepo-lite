#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-echorepo_dev}"

docker compose \
    -f docker-compose.yml \
    -f docker-compose.dev.yml \
    --profile devtools \
    stop i18n libretranslate