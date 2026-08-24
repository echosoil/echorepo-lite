#!/usr/bin/env bash
set -euo pipefail

export COMPOSE_PROJECT_NAME=echorepo_dev

docker compose \
  -f docker-compose.yml \
  -f docker-compose.dev.yml \
  -f docker-compose.jupyter.yml \
  up -d --build
