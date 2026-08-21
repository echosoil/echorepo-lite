#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-echorepo_dev}"

COMPOSE=(
    docker compose
    -f docker-compose.yml
    --profile devtools
)

echo "========================================"
echo " ECHOrepo translation pipeline"
echo "========================================"

echo "[1/8] Starting translation containers..."
"$REPO_ROOT/scripts/start_translate_containers.sh"

echo "[2/8] Waiting for LibreTranslate..."

READY=0

for i in $(seq 1 90); do
    if "${COMPOSE[@]}" exec -T i18n python - <<'PY' >/dev/null 2>&1
import urllib.request

with urllib.request.urlopen(
    "http://libretranslate:5000/languages",
    timeout=3,
) as response:
    if response.status != 200:
        raise SystemExit(1)
PY
    then
        READY=1
        break
    fi

    echo "  waiting ($i/90)..."
    sleep 2
done

if [[ "$READY" != "1" ]]; then
    echo "ERROR: LibreTranslate did not become ready." >&2
    "${COMPOSE[@]}" logs --tail=100 libretranslate
    exit 1
fi

echo "LibreTranslate is ready."


echo "[3/8] Extracting translatable messages..."

"${COMPOSE[@]}" exec -T i18n \
    pybabel extract \
        -F babel.cfg \
        -o echorepo/translations/messages.pot \
        echorepo static


echo "[4/8] Reading supported locales from echorepo/i18n.py..."

# Read SUPPORTED_LOCALES directly from Python source using AST.
# This avoids maintaining another independent language list here.
mapfile -t LANGUAGES < <(
    "${COMPOSE[@]}" exec -T i18n python - <<'PY' | tr -d '\r'
import ast
from pathlib import Path

path = Path("echorepo/i18n.py")
tree = ast.parse(path.read_text(encoding="utf-8"))

for node in tree.body:
    if isinstance(node, ast.Assign):
        for target in node.targets:
            if (
                isinstance(target, ast.Name)
                and target.id == "SUPPORTED_LOCALES"
            ):
                values = ast.literal_eval(node.value)

                for lang in values:
                    if lang != "en":
                        print(lang)

                raise SystemExit(0)

raise SystemExit(
    "SUPPORTED_LOCALES not found in echorepo/i18n.py"
)
PY
)

echo "Target languages:"
printf '  %s\n' "${LANGUAGES[@]}"


echo "[5/8] Initialising missing translation catalogues..."

for lang in "${LANGUAGES[@]}"; do
    PO="$REPO_ROOT/echorepo/translations/$lang/LC_MESSAGES/messages.po"

    if [[ ! -f "$PO" ]]; then
        echo "  NEW: $lang"

        "${COMPOSE[@]}" exec -T i18n \
            pybabel init \
                -i echorepo/translations/messages.pot \
                -d echorepo/translations \
                -l "$lang"
    else
        echo "  exists: $lang"
    fi
done


echo "[6/8] Updating all translation catalogues..."

"${COMPOSE[@]}" exec -T i18n \
    pybabel update \
        -i echorepo/translations/messages.pot \
        -d echorepo/translations


echo "[7/8] Automatically translating empty/fuzzy entries..."

"${COMPOSE[@]}" exec -T i18n \
    python tools/auto_translate.py \
        --trans-dir echorepo/translations \
        --endpoint http://libretranslate:5000 \
        --source en


echo "[8/8] Compiling translation catalogues..."

"${COMPOSE[@]}" exec -T i18n \
    pybabel compile \
        -d echorepo/translations


echo
echo "Translation directories now present:"
find echorepo/translations \
    -mindepth 1 \
    -maxdepth 1 \
    -type d \
    -printf '%f\n' \
    | sort

echo
echo "Translation changes:"
git diff --stat -- echorepo/translations || true

echo
echo "✅ Translation pipeline completed."