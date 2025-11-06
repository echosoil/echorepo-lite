#!/usr/bin/env bash
set -euo pipefail

# path to your main repo
REPO_DIR="../echorepo-lite"

cd "$REPO_DIR"

# temp place to stash the files from develop
TMPDIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

echo "Fetching origin..."
git fetch --all

# ---------------------------------------------------------------------------
# 1) find where 'develop' is actually checked out
# ---------------------------------------------------------------------------
DEV_WORKTREE=""
while IFS= read -r line; do
  case "$line" in
    worktree\ *)
      wt_path="${line#worktree }"
      current_wt="$wt_path"
      ;;
    branch\ refs/heads/develop)
      DEV_WORKTREE="$current_wt"
      ;;
  esac
done < <(git worktree list --porcelain)

SWITCHED_TO_DEVELOP=0

if [[ -n "$DEV_WORKTREE" && "$DEV_WORKTREE" != "$(pwd)" ]]; then
  echo "develop is checked out in another worktree: $DEV_WORKTREE"
  SRC_DIR="$DEV_WORKTREE"
else
  # develop is NOT checked out elsewhere, so we can just switch here
  echo "develop is not checked out elsewhere, switching in this repo..."
  git switch develop
  SWITCHED_TO_DEVELOP=1
  SRC_DIR="$(pwd)"
fi

# ---------------------------------------------------------------------------
# 2) copy the untracked stuff from that develop tree
# ---------------------------------------------------------------------------
# 2a) .env
if [[ -f "$SRC_DIR/.env" ]]; then
  cp "$SRC_DIR/.env" "$TMPDIR/.env"
fi

# 2b) compiled translations: echorepo/translations/**/LC_MESSAGES/messages.mo
mkdir -p "$TMPDIR/echorepo/translations"
rsync -a \
  --prune-empty-dirs \
  --include '*/' \
  --include 'messages.mo' \
  --exclude '*' \
  "$SRC_DIR/echorepo/translations/" "$TMPDIR/echorepo/translations/"

# if we temporarily switched to develop in this repo, go back to main now
if [[ $SWITCHED_TO_DEVELOP -eq 1 ]]; then
  git switch main
else
  # we're already in the main repo root but maybe on some branch; make sure
  git switch main
fi

# ---------------------------------------------------------------------------
# 3) merge origin/develop into main
# ---------------------------------------------------------------------------
git merge --no-ff origin/develop

# ---------------------------------------------------------------------------
# 4) restore the files into main's working tree
# ---------------------------------------------------------------------------
if [[ -f "$TMPDIR/.env" ]]; then
  cp "$TMPDIR/.env" .env
fi

rsync -a "$TMPDIR/echorepo/translations/" echorepo/translations/

# ---------------------------------------------------------------------------
# 5) tag + push
# ---------------------------------------------------------------------------
TAG="v$(date +%Y.%m.%d-%H%M)"
git tag -a "$TAG" -m "Release"
git push --follow-tags

echo "Release done: $TAG"
