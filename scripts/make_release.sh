#!/usr/bin/env bash
set -euo pipefail

# This script MUST live in /home/echo/ECHO-STORE/echorepo-lite-dev/scripts/.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
DEV_REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd -P)"
PROD_REPO_DIR="$(cd "$DEV_REPO_DIR/../echorepo-lite" && pwd -P)"

echo "[INFO] dev repo : $DEV_REPO_DIR"
echo "[INFO] prod repo: $PROD_REPO_DIR"

# Never release into the same checkout. This was the root cause of the reported log.
if [[ "$DEV_REPO_DIR" == "$PROD_REPO_DIR" ]]; then
  echo "[ERROR] Development and production paths resolve to the SAME directory." >&2
  echo "[ERROR] Run the copy of this script located in echorepo-lite-dev/scripts/." >&2
  echo "[ERROR] Expected dev: /home/echo/ECHO-STORE/echorepo-lite-dev" >&2
  echo "[ERROR] Expected prod: /home/echo/ECHO-STORE/echorepo-lite" >&2
  exit 1
fi

for repo in "$DEV_REPO_DIR" "$PROD_REPO_DIR"; do
  if [[ ! -d "$repo/.git" ]] && [[ ! -f "$repo/.git" ]]; then
    echo "[ERROR] Not a Git checkout: $repo" >&2
    exit 1
  fi
done

SOURCE_REMOTE="${SOURCE_REMOTE:-origin}"
MAIN_REF="$SOURCE_REMOTE/main"
DEVELOP_REF="$SOURCE_REMOTE/develop"
echo "[INFO] release source: $SOURCE_REMOTE (main and develop)"

TMPDIR="$(mktemp -d)"
cleanup() { rm -rf -- "$TMPDIR"; }
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 1) Save and sanitize the development .env for production.
# ---------------------------------------------------------------------------
if [[ -f "$DEV_REPO_DIR/.env" ]]; then
  cp -p -- "$DEV_REPO_DIR/.env" "$TMPDIR/.env"

  sed -i 's/^APP_ENV=dev$/APP_ENV=prod/' "$TMPDIR/.env"
  sed -i 's/-dev//g' "$TMPDIR/.env"
  sed -i 's/keycloak\.quanta-labs\.com/keycloak-dev.quanta-labs.com/g' "$TMPDIR/.env"
  sed -i 's|localhost:18080|echorepo.quanta-labs.com|g' "$TMPDIR/.env"
  sed -i 's|echorepo_dev|echorepo_prod|g' "$TMPDIR/.env"
  sed -i 's|5433|5432|g' "$TMPDIR/.env"
  echo "[INFO] copied and sanitized .env from dev"
else
  echo "[WARN] no .env in $DEV_REPO_DIR; production .env will not be replaced"
fi

# ---------------------------------------------------------------------------
# 2) Update production main and merge develop.
# ---------------------------------------------------------------------------
cd "$PROD_REPO_DIR"

# Do not interfere with an existing unfinished merge or a modified tracked file.
MERGE_HEAD_PATH="$(git rev-parse --git-path MERGE_HEAD)"
if [[ -f "$MERGE_HEAD_PATH" ]]; then
  echo "[ERROR] A merge is already in progress in production. Resolve/abort it first." >&2
  exit 1
fi
if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
  echo "[ERROR] Production has modified/staged tracked files. Resolve before releasing:" >&2
  git status --short
  exit 1
fi

echo "[INFO] fetching..."
git fetch --all

if ! git show-ref --verify --quiet "refs/remotes/$SOURCE_REMOTE/main" || \
   ! git show-ref --verify --quiet "refs/remotes/$SOURCE_REMOTE/develop"; then
  echo "[ERROR] Expected $MAIN_REF and $DEVELOP_REF after fetch." >&2
  exit 1
fi

echo "[INFO] switching to main..."
git switch main

echo "[INFO] updating local main from $MAIN_REF..."
if ! git merge --ff-only "$MAIN_REF"; then
  echo "[ERROR] Production main cannot be fast-forwarded to $MAIN_REF." >&2
  echo "        Inspect: cd '$PROD_REPO_DIR' && git status && git log --oneline --graph --all -20" >&2
  exit 1
fi

# A manually generated untracked GeoJSON in prod blocks merging the tracked
# version from develop. Preserve it OUTSIDE Git before the merge; never delete it.
OVERLAY_REL="static/geo/biogeo_regions_2016.geojson"
if [[ -e "$OVERLAY_REL" ]] && \
   ! git ls-files --error-unmatch -- "$OVERLAY_REL" >/dev/null 2>&1 && \
   git cat-file -e "$DEVELOP_REF:$OVERLAY_REL" 2>/dev/null; then
  BACKUP_DIR="$HOME/echorepo-release-backups"
  mkdir -p -- "$BACKUP_DIR"
  BACKUP_PATH="$BACKUP_DIR/biogeo_regions_2016.geojson.prod-$(date +%Y%m%d-%H%M%S)-$$"
  mv -- "$OVERLAY_REL" "$BACKUP_PATH"
  echo "[WARN] Saved untracked production GeoJSON to: $BACKUP_PATH"
  echo "[INFO] The version tracked in $DEVELOP_REF will be merged into main."
fi

echo "[INFO] merging $DEVELOP_REF into main..."
if git merge --no-ff --no-edit "$DEVELOP_REF"; then
  echo "[INFO] merge finished successfully (or already up to date)."
else
  # An untracked-file collision/precondition error doesn't start a merge.
  # Never pretend it was a resolvable conflict; never call git commit here.
  if [[ ! -f "$(git rev-parse --git-path MERGE_HEAD)" ]]; then
    echo "[ERROR] Git refused to start the merge (possibly another untracked file)." >&2
    echo "[ERROR] No merge commit was created; inspect 'git status' above." >&2
    exit 1
  fi

  echo "[WARN] Merge has actual file conflicts; resolving known paths."

  # docker-compose.prod.yml: keep production main's version.
  if git diff --name-only --diff-filter=U | grep -qx 'docker-compose.prod.yml'; then
    echo "[INFO] resolving docker-compose.prod.yml using main version..."
    git checkout --ours -- docker-compose.prod.yml
    git add -- docker-compose.prod.yml
  fi

  # Translation catalogs: use develop's version.
  while IFS= read -r file; do
    [[ -z "$file" ]] && continue
    case "$file" in
      echorepo/translations/*)
        echo "[INFO] resolving translation conflict from develop: $file"
        git checkout --theirs -- "$file"
        git add -- "$file"
        ;;
    esac
  done < <(git diff --name-only --diff-filter=U)

  REMAINING_CONFLICTS="$(git diff --name-only --diff-filter=U)"
  if [[ -n "$REMAINING_CONFLICTS" ]]; then
    echo "[ERROR] Manual merge resolution required:" >&2
    echo "$REMAINING_CONFLICTS" >&2
    echo "        Resolve and commit, or use: git merge --abort" >&2
    exit 1
  fi

  echo "[INFO] all merge conflicts resolved; completing the merge..."
  git commit -m "Merge origin/develop into main (release)"
fi

# ---------------------------------------------------------------------------
# 3) Apply the sanitized .env only AFTER a successful merge.
# ---------------------------------------------------------------------------
if [[ -f "$TMPDIR/.env" ]]; then
  if [[ -f .env ]]; then
    BACKUP_DIR="$HOME/echorepo-release-backups"
    mkdir -p -- "$BACKUP_DIR"
    cp -p -- .env "$BACKUP_DIR/env.prod-$(date +%Y%m%d-%H%M%S)-$$"
  fi
  cp -- "$TMPDIR/.env" .env
  echo "[INFO] restored sanitized .env into prod"
fi

# Only touch this container once the source release has succeeded.
if docker ps -a --format '{{.Names}}' | grep -qx libretranslate; then
  echo "[INFO] removing existing libretranslate container"
  docker rm -f libretranslate
fi

# ---------------------------------------------------------------------------
# 4) Tag and push.
# ---------------------------------------------------------------------------
TAG="v$(date +%Y.%m.%d-%H%M)"
if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  echo "[ERROR] Tag '$TAG' already exists. Re-run later or choose another tag." >&2
  exit 1
fi

git tag -a "$TAG" -m "Release"
git push --follow-tags
echo "[INFO] Release done: $TAG"
