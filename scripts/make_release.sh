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

# ----------------------------------------------------------------------------
# 2) remove existing libretranslate container (if any)
# ----------------------------------------------------------------------------
if docker ps -a --format '{{.Names}}' | grep -qx libretranslate; then
  echo "[INFO] removing existing libretranslate container"
  docker rm -f libretranslate
fi

# ---------------------------------------------------------------------------
# 3) go to PROD and do git stuff
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

echo "[INFO] updating local main from origin/main..."
if ! git merge --ff-only origin/main; then
  echo "[ERROR] local main cannot be fast-forwarded to origin/main."
  echo "        Please inspect the prod repo manually:"
  echo "        cd $PROD_REPO_DIR && git status && git log --oneline --graph --decorate --all -20"
  exit 1
fi

echo "[INFO] merging origin/develop into main..."

if ! git merge --no-ff origin/develop; then
  echo "[WARN] merge has conflicts."

  # ------------------------------------------------------------
  # docker-compose.prod.yml is production-specific:
  # keep MAIN's version ("ours").
  # ------------------------------------------------------------
  if git diff --name-only --diff-filter=U \
      | grep -qx 'docker-compose.prod.yml'; then

    echo "[INFO] resolving docker-compose.prod.yml using main version..."
    git checkout --ours -- docker-compose.prod.yml
    git add docker-compose.prod.yml
  fi

  # ------------------------------------------------------------
  # Translation catalogues are generated/maintained in DEVELOP:
  # take DEVELOP's version ("theirs") when releasing.
  # ------------------------------------------------------------
  while IFS= read -r file; do
    [[ -z "$file" ]] && continue

    case "$file" in
      echorepo/translations/*)
        echo "[INFO] resolving translation conflict from develop: $file"
        git checkout --theirs -- "$file"
        git add "$file"
        ;;
    esac
  done < <(git diff --name-only --diff-filter=U)

  # ------------------------------------------------------------
  # IMPORTANT:
  # Never blindly commit while unresolved conflicts remain.
  # ------------------------------------------------------------
  REMAINING_CONFLICTS="$(git diff --name-only --diff-filter=U)"

  if [[ -n "$REMAINING_CONFLICTS" ]]; then
    echo
    echo "[ERROR] Some merge conflicts require manual resolution:"
    echo
    echo "$REMAINING_CONFLICTS"
    echo
    echo "The repository has intentionally been left in merge state."
    echo "Resolve the files above, then run:"
    echo
    echo "  git add <resolved-files>"
    echo "  git commit"
    echo
    echo "Or abort with:"
    echo
    echo "  git merge --abort"
    exit 1
  fi

  echo "[INFO] all known conflicts resolved automatically."

  git commit \
    -m "Merge origin/develop into main (release)"
fi
# ---------------------------------------------------------------------------
# 4) restore into PROD
# ---------------------------------------------------------------------------
if [[ -f "$TMPDIR/.env" ]]; then
  cp "$TMPDIR/.env" .env
  echo "[INFO] restored .env into prod"
fi

# rsync -a "$TMPDIR/echorepo/translations/" echorepo/translations/
# echo "[INFO] restored .mo files into prod"

# ---------------------------------------------------------------------------
# 5) tag + push
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
