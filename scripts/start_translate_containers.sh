
#!/usr/bin/env bash
set -Eeuo pipefail

# Resolve repository root regardless of current directory.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Always use the intended development Compose project by default.
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-echorepo_dev}"

# Host user/group IDs for mounted files.
export LOCAL_UID="$(id -u)"
export LOCAL_GID="$(id -g)"

# Use the same Compose configuration everywhere.
COMPOSE=(
    docker compose
    -p "$COMPOSE_PROJECT_NAME"
    -f "$REPO_ROOT/docker-compose.yml"
    -f "$REPO_ROOT/docker-compose.dev.yml"
    --profile devtools
)

echo "========================================"
echo " ECHOrepo translation containers"
echo "========================================"
echo "Project: $COMPOSE_PROJECT_NAME"
echo "UID/GID: $LOCAL_UID:$LOCAL_GID"

# Create external shared network only if missing.
if ! docker network inspect echorepo-shared >/dev/null 2>&1; then
    echo "Creating external Docker network: echorepo-shared"
    docker network create echorepo-shared
fi

echo
echo "Starting LibreTranslate and i18n containers..."

if ! "${COMPOSE[@]}" up -d libretranslate i18n; then
    echo
    echo "ERROR: Failed to start translation containers." >&2

    echo "=== Container status ==="
    "${COMPOSE[@]}" ps || true

    echo "=== LibreTranslate logs ==="
    "${COMPOSE[@]}" logs --tail=50 libretranslate || true

    echo "=== Existing translation containers ==="
    docker ps -a \
        --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' \
        | grep -E 'NAMES|libretranslate|i18n' || true

    exit 1
fi

echo
echo "=== Translation container status ==="
"${COMPOSE[@]}" ps libretranslate i18n

echo
echo "Translation containers started successfully."
