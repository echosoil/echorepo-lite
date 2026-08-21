#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-echorepo_dev}"

COMPOSE=(
    docker compose
    -f docker-compose.yml
    -f docker-compose.dev.yml
    --profile devtools
)

echo "========================================"
echo " ECHOrepo translation pipeline"
echo "========================================"

#
# 1. Make sure translation containers exist
#

echo "[1/7] Starting translation containers..."

"$REPO_ROOT/scripts/start_translate_containers.sh"


#
# 2. Wait for LibreTranslate
#

echo "[2/7] Waiting for LibreTranslate..."

READY=0

for i in $(seq 1 60); do
    if "${COMPOSE[@]}" exec -T i18n python - <<'PY' >/dev/null 2>&1
import urllib.request

try:
    with urllib.request.urlopen(
        "http://libretranslate:5000/languages",
        timeout=3,
    ) as response:
        if response.status != 200:
            raise RuntimeError(response.status)
except Exception:
    raise SystemExit(1)
PY
    then
        READY=1
        break
    fi

    echo "  LibreTranslate not ready yet ($i/60)..."
    sleep 2
done

if [[ "$READY" != "1" ]]; then
    echo "ERROR: LibreTranslate did not become ready." >&2
    echo
    echo "LibreTranslate logs:"
    "${COMPOSE[@]}" logs --tail=100 libretranslate
    exit 1
fi

echo "LibreTranslate is ready."


#
# 3. Extract
#

echo "[3/7] Extracting translatable messages..."

"${COMPOSE[@]}" exec -T i18n \
    pybabel extract \
        -F babel.cfg \
        -o echorepo/translations/messages.pot \
        echorepo \
        static


#
# 4. Update PO files
#

echo "[4/7] Updating translation catalogues..."

"${COMPOSE[@]}" exec -T i18n \
    pybabel update \
        -i echorepo/translations/messages.pot \
        -d echorepo/translations


#
# 5. Automatically translate
#

echo "[5/7] Translating empty/fuzzy entries..."

"${COMPOSE[@]}" exec -T i18n \
    python tools/auto_translate.py \
        --trans-dir echorepo/translations \
        --endpoint http://libretranslate:5000


#
# 6. Compile
#

echo "[6/7] Compiling catalogues..."

"${COMPOSE[@]}" exec -T i18n \
    pybabel compile \
        -d echorepo/translations


#
# 7. Summary
#

echo "[7/7] Translation changes:"

git diff --stat -- echorepo/translations || true

echo
echo "Remaining fuzzy entries:"

grep -Rhs '^#,.*fuzzy' \
    echorepo/translations/*/LC_MESSAGES/messages.po \
    2>/dev/null \
    | wc -l || true

echo
echo "✅ Translation pipeline completed."