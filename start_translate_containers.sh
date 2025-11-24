COMPOSE_PROJECT_NAME=echorepo_dev \
docker compose -f docker-compose.yml -f docker-compose.dev.yml \
  --profile devtools up -d libretranslate i18n