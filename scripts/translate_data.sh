#!/usr/bin/env bash
# Host-side PostgreSQL backfill; manage only the LibreTranslate service.
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PYTHON="${TRANSLATION_PYTHON:-python3}"
if [[ -z "${TRANSLATION_PYTHON:-}" && -x "$ROOT/venv/bin/python" ]]; then
    PYTHON="$ROOT/venv/bin/python"
elif [[ -z "${TRANSLATION_PYTHON:-}" && -x "$ROOT/../venv/bin/python" ]]; then
    PYTHON="$ROOT/../venv/bin/python"
fi

# The caller must explicitly select the database port and opt in to writes.
# Count-only runs do not need Docker.
needs_lt=0
for arg in "$@"; do
    case "$arg" in
        --apply|--compare-output|--compare-output=*)
            needs_lt=1
            ;;
        --endpoint|--endpoint=*|--db-host|--db-host=*)
            echo "Use the local PostgreSQL and LibreTranslate endpoints with this wrapper." >&2
            exit 2
            ;;
    esac
done
"$PYTHON" -m tools.translate_free_text_en --help >/dev/null
if [[ "$needs_lt" == 0 ]]; then
    exec "$PYTHON" -u -m tools.translate_free_text_en "$@"
fi

# Validate arguments and database connectivity without writes or report creation.
"$PYTHON" -u -m tools.translate_free_text_en "$@" --preflight

COMPOSE_DIR="$(cd "${LT_COMPOSE_DIR:-$ROOT}" && pwd)"
COMPOSE=(docker compose --project-directory "$COMPOSE_DIR" -f "$COMPOSE_DIR/docker-compose.yml" --profile devtools)
if [[ -n "${LT_COMPOSE_PROJECT:-}" ]]; then
    COMPOSE+=(-p "$LT_COMPOSE_PROJECT")
fi

# Serialize runs across production and development checkouts on this host/user.
LOCK_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/echorepo"
mkdir -p "$LOCK_DIR"
exec 9>"$LOCK_DIR/free-text-translation.lock"
if ! flock -n 9; then
    echo "Another free-text translation run is active." >&2
    exit 1
fi

cleanup() {
    local status=$?
    trap - EXIT
    if ! "${COMPOSE[@]}" stop libretranslate; then
        echo "Could not stop LibreTranslate; check Docker manually." >&2
        [[ "$status" != 0 ]] || status=1
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${COMPOSE[@]}" up -d --no-recreate libretranslate
# Do not accidentally use a different project's service on port 5001.
port="$("${COMPOSE[@]}" port libretranslate 5000)"
if [[ "$port" != "127.0.0.1:5001" ]]; then
    echo "Expected LibreTranslate on 127.0.0.1:5001, found $port" >&2
    exit 1
fi

ready=0
for ((i=0; i<${LT_WAIT_ATTEMPTS:-180}; i++)); do
    if curl --noproxy '*' -fsS --max-time 3 http://127.0.0.1:5001/languages >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 2
done
if [[ "$ready" != 1 ]]; then
    echo "LibreTranslate did not become ready." >&2
    "${COMPOSE[@]}" logs --tail=50 libretranslate >&2 || true
    exit 1
fi

"$PYTHON" -u -m tools.translate_free_text_en "$@"
