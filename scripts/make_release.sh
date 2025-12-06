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

  # normalize .env for prod
  sed -i 's/^APP_ENV=dev$/APP_ENV=prod/' "$TMPDIR/.env"
  
  # 2) remove literal "-dev" everywhere (for paths like echorepo-lite-dev, etc.)  
  sed -i 's/-dev//g' "$TMPDIR/.env"

  # 3) but restore the real Keycloak host, which *must* have -dev
  sed -i 's/keycloak\.quanta-labs\.com/keycloak-dev.quanta-labs.com/g' "$TMPDIR/.env"

  # 4) normalize .env for prod
  sed -i 's|localhost:18080|echorepo.quanta-labs.com|g' "$TMPDIR/.env"
  sed -i 's|echorepo_dev|echorepo_prod|g' "$TMPDIR/.env"
  sed -i 's|5433|5432|g' "$TMPDIR/.env"

  echo "[INFO] copied and sanitized .env from dev"
else
  echo "[WARN] no .env in $DEV_REPO_DIR"
fi

# compiled translations
# mkdir -p "$TMPDIR/echorepo/translations"
# rsync -a \
#   --prune-empty-dirs \
#   --include '*/' \
#   --include 'messages.mo' \
#   --exclude '*' \
#   "$DEV_REPO_DIR/echorepo/translations/" \
#   "$TMPDIR/echorepo/translations/"
# echo "[INFO] copied .mo files from dev into tmp"

# ---------------------------------------------------------------------------
# 2) go to PROD and do git stuff
# ---------------------------------------------------------------------------
cd "$PROD_REPO_DIR"

echo "[INFO] fetching..."
git fetch --all

# bail if repo is mid-merge
if [ -f .git/MERGE_HEAD ]; then
  echo "[ERROR] This repo is currently in the middle of a merge."
  echo "        Run 'git status' and either 'git merge --abort' or finish the merge."
  exit 1
fi

echo "[INFO] switching to main..."
git switch main

echo "[INFO] merging origin/develop into main..."
if ! git merge --no-ff origin/develop; then
  echo "[WARN] merge had conflicts, trying to auto-resolve docker-compose.prod.yml by keeping main version..."
  # keep main’s version of the prod compose file
  git checkout --ours docker-compose.prod.yml
  git add docker-compose.prod.yml
  git commit -m "Merge origin/develop into main (auto-resolve docker-compose.prod.yml)"
fi
# ---------------------------------------------------------------------------
# 3) restore into PROD
# ---------------------------------------------------------------------------
if [[ -f "$TMPDIR/.env" ]]; then
  cp "$TMPDIR/.env" .env
  echo "[INFO] restored .env into prod"
fi

# rsync -a "$TMPDIR/echorepo/translations/" echorepo/translations/
# echo "[INFO] restored .mo files into prod"

# ---------------------------------------------------------------------------
# 4) tag + push
# ---------------------------------------------------------------------------
TAG="v$(date +%Y.%m.%d-%H%M)"

# check if tag already exists (e.g. when you re-run the script in the same minute)
if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  echo "[ERROR] tag '$TAG' already exists. Run again in a minute or bump manually."
  exit 1
fi

git tag -a "$TAG" -m "Release"
git push --follow-tags
echo "[INFO] Release done: $TAG"
