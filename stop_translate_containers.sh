#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/home/quanta/echorepo-lite-dev"
cd "$BASE_DIR"

COMPOSE="docker compose -p echorepo_dev -f docker-compose.yml -f docker-compose.dev.yml"

# stop only these two services
$COMPOSE stop libretranslate i18n

# remove these two services
$COMPOSE rm libretranslate i18n