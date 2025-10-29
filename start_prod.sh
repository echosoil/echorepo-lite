COMPOSE_PROJECT_NAME=echorepo_prod \
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
