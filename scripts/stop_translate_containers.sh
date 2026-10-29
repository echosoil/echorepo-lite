
#!/usr/bin/env bash
set -Eeuo pipefail

# Resolve repository root regardless of current directory.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Use the same Compose project as the startup script.
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-echorepo_dev}"

export LOCAL_UID="$(id -u)"
export LOCAL_GID="$(id -g)"

COMPOSE=(
    docker compose
    -p "$COMPOSE_PROJECT_NAME"
    -f "$REPO_ROOT/docker-compose.yml"
    -f "$REPO_ROOT/docker-compose.dev.yml"
    --profile devtools
)

echo "========================================"
echo " Stopping ECHOrepo translation containers"
echo "========================================"
echo "Project: $COMPOSE_PROJECT_NAME"

# Stop only translation containers, leaving the rest running.
"${COMPOSE[@]}" stop i18n libretranslate

echo
echo "=== Translation container status ==="
"${COMPOSE[@]}" ps -a i18n libretranslate

echo
echo "Translation containers stopped."
