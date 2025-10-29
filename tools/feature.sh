#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Config (can be overridden via env or flags)
# -----------------------------
REMOTE="${REMOTE:-origin}"
BASE="${BASE:-develop}"
PREFIX="${PREFIX:-feat/}"
REPO="${REPO:-.}"

# behavior flags (can be overridden with flags)
DELETE_REMOTE="${DELETE_REMOTE:-1}"   # 1=yes, 0=no (on close)
NOFF_MERGE="${NOFF_MERGE:-1}"        # 1=--no-ff, 0=fast-forward allowed

usage() {
  cat <<EOF
Usage:
  $(basename "$0") open  <name> [--repo DIR] [--base BRANCH] [--remote ORIGIN] [--prefix feat/]
  $(basename "$0") close <name> [--repo DIR] [--base BRANCH] [--remote ORIGIN] [--prefix feat/] [--keep-remote] [--ff]

Examples:
  $(basename "$0") open privacy_policy_alert
  $(basename "$0") close privacy_policy_alert

Env overrides:
  REPO=.  REMOTE=origin  BASE=develop  PREFIX=feat/  DELETE_REMOTE=1  NOFF_MERGE=1
EOF
}

say() { printf "\033[1;32m%s\033[0m\n" "$*"; }
warn(){ printf "\033[1;33m%s\033[0m\n" "$*"; }
err() { printf "\033[1;31m%s\033[0m\n" "$*" >&2; }

need_git_repo() {
  if ! git -C "$REPO" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    err "Not a git repo: $REPO"
    exit 1
  fi
}

parse_common_flags() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --repo)   REPO="$2"; shift 2;;
      --base)   BASE="$2"; shift 2;;
      --remote) REMOTE="$2"; shift 2;;
      --prefix) PREFIX="$2"; shift 2;;
      --keep-remote) DELETE_REMOTE=0; shift;;
      --ff) NOFF_MERGE=0; shift;;
      -h|--help) usage; exit 0;;
      *) break;;
    esac
  done
  echo "$@"
}

branch_exists_local() { git -C "$REPO" show-ref --verify --quiet "refs/heads/$1"; }
branch_exists_remote(){ git -C "$REPO" show-ref --verify --quiet "refs/remotes/$REMOTE/$1"; }

cmd_open() {
  local NAME="$1"
  [[ -z "$NAME" ]] && err "Branch name required" && exit 1
  local BRANCH="${PREFIX}${NAME}"

  need_git_repo
  say "➡ Opening feature branch: $BRANCH (base: $BASE, remote: $REMOTE) in $REPO"
  git -C "$REPO" fetch --all --prune

  # Ensure base exists and is current
  git -C "$REPO" switch "$BASE" >/dev/null 2>&1 || git -C "$REPO" switch -c "$BASE" "refs/remotes/$REMOTE/$BASE"
  git -C "$REPO" pull "$REMOTE" "$BASE"

  if branch_exists_local "$BRANCH"; then
    warn "Local branch $BRANCH already exists. Switching to it."
    git -C "$REPO" switch "$BRANCH"
  elif branch_exists_remote "$BRANCH"; then
    warn "Remote branch $REMOTE/$BRANCH exists. Creating local tracking branch."
    git -C "$REPO" switch -c "$BRANCH" --track "$REMOTE/$BRANCH"
  else
    say "Creating new branch $BRANCH from $BASE"
    git -C "$REPO" switch -c "$BRANCH"
    say "Pushing and setting upstream"
    git -C "$REPO" push -u "$REMOTE" "$BRANCH"
  fi

  say "✅ Ready. Do your edits/commits in VS Code on branch: $BRANCH"
}

cmd_close() {
  local NAME="$1"
  [[ -z "$NAME" ]] && err "Branch name required" && exit 1
  local BRANCH="${PREFIX}${NAME}"

  need_git_repo
  say "➡ Closing feature branch: $BRANCH → merge into $BASE (remote: $REMOTE) in $REPO"
  git -C "$REPO" fetch --all --prune

  # Ensure base is current
  git -C "$REPO" switch "$BASE"
  git -C "$REPO" pull "$REMOTE" "$BASE"

  # Choose merge ref: prefer remote/$BRANCH if present; else local
  local MERGE_REF="$BRANCH"
  if branch_exists_remote "$BRANCH"; then
    MERGE_REF="refs/remotes/$REMOTE/$BRANCH"
  elif ! branch_exists_local "$BRANCH"; then
    err "Feature branch not found locally or remotely: $BRANCH"
    exit 1
  fi

  if [[ "$NOFF_MERGE" == "1" ]]; then
    say "Merging (no-ff) $MERGE_REF → $BASE"
    git -C "$REPO" merge --no-ff --log "$MERGE_REF"
  else
    say "Merging (fast-forward allowed) $MERGE_REF → $BASE"
    git -C "$REPO" merge --log "$MERGE_REF"
  fi

  say "Pushing $BASE"
  git -C "$REPO" push "$REMOTE" "$BASE"

  # Cleanup branches
  if branch_exists_local "$BRANCH"; then
    say "Deleting local branch $BRANCH"
    git -C "$REPO" branch -D "$BRANCH" || true
  fi
  if [[ "$DELETE_REMOTE" == "1" ]] && branch_exists_remote "$BRANCH"; then
    say "Deleting remote branch $REMOTE/$BRANCH"
    git -C "$REPO" push "$REMOTE" --delete "$BRANCH" || true
  else
    warn "Keeping remote branch $REMOTE/$BRANCH (use --keep-remote to keep, default deletes)."
  fi

  say "✅ Closed. Feature merged into $BASE."
}

main() {
  local cmd="${1:-}"; shift || true
  case "$cmd" in
    open)
      # parse flags then remaining args
      args=$(parse_common_flags "$@"); set -- $args
      cmd_open "${1:-}";;
    close)
      args=$(parse_common_flags "$@"); set -- $args
      cmd_close "${1:-}";;
    -h|--help|"")
      usage;;
    *)
      err "Unknown command: $cmd"; usage; exit 1;;
  esac
}

main "$@"
