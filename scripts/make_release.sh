#!/usr/bin/env bash
set -euo pipefail

# figure out paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"                  # echorepo-lite-dev
PROD_REPO_DIR="$(cd "$DEV_REPO_DIR/../echorepo-lite" && pwd)" # echorepo-lite

TMPDIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

echo "[INFO] dev repo : $DEV_REPO_DIR"
echo "[INFO] prod repo: $PROD_REPO_DIR"

# ---------------------------------------------------------------------------
# 1) stash from DEV
# ---------------------------------------------------------------------------
# 1a) .env
if [[ -f "$DEV_REPO_DIR/.env" ]]; then
  cp "$DEV_REPO_DIR/.env" "$TMPDIR/.env"

  # remove literal "-dev" substrings (your earlier requirement)
  sed -i 's/-dev//g' "$TMPDIR/.env"

  # change APP_ENV=dev → APP_ENV=prod
  sed -i 's/^APP_ENV=dev$/APP_ENV=prod/' "$TMPDIR/.env"

  echo "[INFO] copied and sanitized .env from dev"
else
  echo "[WARN] no .env in $DEV_REPO_DIR"
fi

# 1b) compiled translations
mkdir -p "$TMPDIR/echorepo/translations"
rsync -a \
  --prune-empty-dirs \
  --include '*/' \
  --include 'messages.mo' \
  --exclude '*' \
  "$DEV_REPO_DIR/echorepo/translations/" \
  "$TMPDIR/echorepo/translations/"
echo "[INFO] copied .mo files from dev into tmp"

# ---------------------------------------------------------------------------
# 2) go to PROD and do git stuff
# ---------------------------------------------------------------------------
cd "$PROD_REPO_DIR"

echo "[INFO] fetching + switching to main..."
git fetch --all
git switch main

echo "[INFO] merging origin/develop into main..."
git merge --no-ff origin/develop

# ---------------------------------------------------------------------------
# 3) restore into PROD
# ---------------------------------------------------------------------------
if [[ -f "$TMPDIR/.env" ]]; then
  cp "$TMPDIR/.env" .env
  echo "[INFO] restored .env into prod"
fi

rsync -a "$TMPDIR/echorepo/translations/" echorepo/translations/
echo "[INFO] restored .mo files into prod"

# ---------------------------------------------------------------------------
# 4) tag + push
# ---------------------------------------------------------------------------
TAG="v$(date +%Y.%m.%d-%H%M)"
git tag -a "$TAG" -m "Release"
git push --follow-tags
echo "[INFO] Release done: $TAG"
#!/usr/bin/env bash
set -euo pipefail

# figure out paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"                  # echorepo-lite-dev
PROD_REPO_DIR="$(cd "$DEV_REPO_DIR/../echorepo-lite" && pwd)" # echorepo-lite

TMPDIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

echo "[INFO] dev repo : $DEV_REPO_DIR"
echo "[INFO] prod repo: $PROD_REPO_DIR"

# ---------------------------------------------------------------------------
# 1) stash from DEV
# ---------------------------------------------------------------------------
if [[ -f "$DEV_REPO_DIR/.env" ]]; then
  cp "$DEV_REPO_DIR/.env" "$TMPDIR/.env"

  # 1) APP_ENV=dev -> APP_ENV=prod
  sed -i 's/^APP_ENV=dev$/APP_ENV=prod/' "$TMPDIR/.env"

  # 2) remove literal "-dev" everywhere (for paths like echorepo-lite-dev, etc.)
  sed -i 's/-dev//g' "$TMPDIR/.env"

  # 3) but restore the real Keycloak host, which *must* have -dev
  sed -i 's/keycloak\.quanta-labs\.com/keycloak-dev.quanta-labs.com/g' "$TMPDIR/.env"

  echo "[INFO] copied and sanitized .env from dev"
else
  echo "[WARN] no .env in $DEV_REPO_DIR"
fi

# 1b) compiled translations (.mo)
mkdir -p "$TMPDIR/echorepo/translations"
rsync -a \
  --prune-empty-dirs \
  --include '*/' \
  --include 'messages.mo' \
  --exclude '*' \
  "$DEV_REPO_DIR/echorepo/translations/" \
  "$TMPDIR/echorepo/translations/"
echo "[INFO] copied .mo files from dev into tmp"

# ---------------------------------------------------------------------------
# 2) go to PROD and do git stuff
# ---------------------------------------------------------------------------
cd "$PROD_REPO_DIR"

echo "[INFO] fetching + switching to main..."
git fetch --all
git switch main

echo "[INFO] merging origin/develop into main..."
git merge --no-ff origin/develop

# ---------------------------------------------------------------------------
# 3) restore into PROD
# ---------------------------------------------------------------------------
if [[ -f "$TMPDIR/.env" ]]; then
  cp "$TMPDIR/.env" .env
  echo "[INFO] restored .env into prod"
fi

rsync -a "$TMPDIR/echorepo/translations/" echorepo/translations/
echo "[INFO] restored .mo files into prod"

# ---------------------------------------------------------------------------
# 4) tag + push
# ---------------------------------------------------------------------------
TAG="v$(date +%Y.%m.%d-%H%M)"
git tag -a "$TAG" -m "Release"
git push --follow-tags
echo "[INFO] Release done: $TAG"
