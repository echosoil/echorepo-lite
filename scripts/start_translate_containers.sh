#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-echorepo_dev}"

export LOCAL_UID="$(id -u)"
export LOCAL_GID="$(id -g)"

# echorepo-shared is declared external, so Docker Compose will not create it.
if ! docker network inspect echorepo-shared >/dev/null 2>&1; then
    echo "Creating external Docker network: echorepo-shared"
    docker network create echorepo-shared
fi

echo "Starting LibreTranslate and i18n containers..."

docker compose \
    -f docker-compose.yml \
    -f docker-compose.dev.yml \
    --profile devtools \
    up -d libretranslate i18n

echo "Translation containers started."