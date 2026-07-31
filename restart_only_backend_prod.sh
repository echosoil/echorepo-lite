#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${ECHOREPO_CONTAINER:-echorepo-lite}"

echo "Restarting production ECHOREPO container: $CONTAINER"

if ! docker inspect "$CONTAINER" >/dev/null 2>&1; then
  echo "Error: container '$CONTAINER' does not exist." >&2
  echo
  echo "Available containers:"
  docker ps -a --format '  {{.Names}}'
  exit 1
fi

docker restart "$CONTAINER" >/dev/null

echo "Waiting for the container to start..."

for attempt in {1..30}; do
  status="$(
    docker inspect \
      --format '{{.State.Status}}' \
      "$CONTAINER"
  )"

  if [[ "$status" == "running" ]]; then
    echo "ECHOREPO production container is running."
    docker ps \
      --filter "name=^/${CONTAINER}$" \
      --format 'Name: {{.Names}}\nStatus: {{.Status}}\nPorts: {{.Ports}}'
    exit 0
  fi

  sleep 1
done

echo "Error: container did not reach the running state." >&2
echo
docker logs --tail 50 "$CONTAINER"
exit 1
