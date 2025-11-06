#!/usr/bin/env bash
set -euo pipefail

# path to your repo (same as you had)
REPO_DIR="../echorepo-lite"

cd "$REPO_DIR"

# we'll park the untracked files from develop here
TMPDIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

# get latest refs
git fetch --all

# 1) go to develop to copy the untracked stuff
git switch develop

# 1a) copy .env if it exists
if [[ -f .env ]]; then
  cp .env "$TMPDIR/.env"
fi

# 1b) copy only messages.mo from translations, keeping the structure
# this uses rsync because it's the least painful way to only take certain files
mkdir -p "$TMPDIR/echorepo/translations"
rsync -a \
  --prune-empty-dirs \
  --include '*/' \
  --include 'messages.mo' \
  --exclude '*' \
  echorepo/translations/ "$TMPDIR/echorepo/translations/"

# 2) now switch to main and merge develop into it
git switch main
git merge --no-ff origin/develop

# 3) restore the untracked files into main's working tree
if [[ -f "$TMPDIR/.env" ]]; then
  cp "$TMPDIR/.env" .env
fi

# put the .mo files back in place
rsync -a "$TMPDIR/echorepo/translations/" echorepo/translations/

# 4) tag + push like before
TAG="v$(date +%Y.%m.%d-%H%M)"
git tag -a "$TAG" -m "Release"
git push --follow-tags
