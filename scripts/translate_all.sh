#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
VENV="$REPO_ROOT/.venv-i18n"
TRANS_DIR="$REPO_ROOT/echorepo/translations"
LT_URL="${LT_URL:-http://127.0.0.1:5001}"

if [[ ! -x "$VENV/bin/python" ]]; then
    echo "ERROR: i18n virtual environment not found: $VENV" >&2
    echo "Create it with:" >&2
    echo "  python3 -m venv .venv-i18n" >&2
    echo "  .venv-i18n/bin/pip install libretranslate Babel polib requests" >&2
    exit 1
fi

# Activate the environment so pybabel and Python dependencies are available.
source "$VENV/bin/activate"

cd "$REPO_ROOT"

echo "[1/5] Checking LibreTranslate at $LT_URL ..."

if ! curl --fail --silent "$LT_URL/languages" >/dev/null; then
    echo "ERROR: LibreTranslate is not running at $LT_URL" >&2
    echo "Start it with:" >&2
    echo "  systemctl --user start libretranslate" >&2
    exit 1
fi

echo "[2/5] Extracting translatable messages..."
pybabel extract \
  -F babel.cfg \
  -o "$TRANS_DIR/messages.pot" \
  "$REPO_ROOT"

echo "[3/5] Updating translation catalogues..."
pybabel update \
  -i "$TRANS_DIR/messages.pot" \
  -d "$TRANS_DIR"

echo "[4/5] Automatically translating messages..."
python tools/auto_translate.py \
  --trans-dir "$TRANS_DIR" \
  --endpoint "$LT_URL"

echo "[5/5] Compiling translation catalogues..."
pybabel compile \
  -d "$TRANS_DIR"

echo "✅ Translation pipeline completed."