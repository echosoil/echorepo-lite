#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------
# Config (env overrides allowed)
# ---------------------------------------
REMOTE="${REMOTE:-origin}"
BASE="${BASE:-develop}"
PREFIX="${PREFIX:-feat/}"
REPO="${REPO:-.}"

# Behavior flags
DELETE_REMOTE="${DELETE_REMOTE:-1}"   # 1=yes on 'close', 0=no
NOFF_MERGE="${NOFF_MERGE:-1}"        # 1=use --no-ff on 'close', 0=allow FF
RECREATE="${RECREATE:-0}"            # 1=delete existing local/remote on 'open'
ASSUME_YES="${ASSUME_YES:-0}"        # 1=auto-confirm on 'nuke'

usage() {
  cat <<EOF
Usage:
  $(basename "$0") open  <name> [--repo DIR] [--base BRANCH] [--remote ORIGIN] [--prefix feat/] [--recreate]
  $(basename "$0") close <name> [--repo DIR] [--base BRANCH] [--remote ORIGIN] [--prefix feat/] [--keep-remote] [--ff]
  $(basename "$0") nuke  <name> [--repo DIR] [--remote ORIGIN] [--prefix feat/] [--yes]

Examples:
  $(basename "$0") open privacy_policy_alert
  $(basename "$0") open privacy_policy_alert --recreate
  $(basename "$0") close privacy_policy_alert
  $(basename "$0") nuke  privacy_policy_alert --yes

Env overrides:
  REPO=.  REMOTE=origin  BASE=develop  PREFIX=feat/
  DELETE_REMOTE=1  NOFF_MERGE=1  RECREATE=0  ASSUME_YES=0
EOF
}

say()  { printf "\033[1;32m%s\033[0m\n" "$*"; }
warn() { printf "\033[1;33m%s\033[0m\n" "$*"; }
err()  { printf "\033[1;31m%s\033[0m\n" "$*" >&2; }

need_git_repo() {
  if ! git -C "$REPO" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    err "Not a git repo: $REPO"
    exit 1
  fi
}

parse_common_flags() {
  # Echo back non-flag args to become "$@" of the caller.
  local args=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --repo)   REPO="$2"; shift 2;;
      --base)   BASE="$2"; shift 2;;
      --remote) REMOTE="$2"; shift 2;;
      --prefix) PREFIX="$2"; shift 2;;
      --keep-remote) DELETE_REMOTE=0; shift;;
      --ff) NOFF_MERGE=0; shift;;
      --recreate) RECREATE=1; shift;;
      --yes) ASSUME_YES=1; shift;;
      -h|--help) usage; exit 0;;
      *) args+=("$1"); shift;;
    esac
  done
  printf '%s\n' "${args[@]+"${args[@]}"}"
}

branch_exists_local()  { git -C "$REPO" show-ref --verify --quiet "refs/heads/$1"; }
branch_exists_remote() { git -C "$REPO" show-ref --verify --quiet "refs/remotes/$REMOTE/$1"; }

ensure_base_current() {
  git -C "$REPO" fetch --all --prune
  git -C "$REPO" switch "$BASE" >/dev/null 2>&1 || git -C "$REPO" switch -c "$BASE" "refs/remotes/$REMOTE/$BASE"
  git -C "$REPO" pull "$REMOTE" "$BASE" --ff-only || true
}

ensure_not_on_branch() {
  local b="$1"
  local cur
  cur="$(git -C "$REPO" rev-parse --abbrev-ref HEAD)"
  if [[ "$cur" == "$b" ]]; then
    say "Currently on $b — switching to $BASE"
    git -C "$REPO" switch "$BASE"
  fi
}

confirm() {
  local prompt="$1"
  if [[ "$ASSUME_YES" == "1" ]]; then
    return 0
  fi
  read -r -p "$prompt [y/N] " reply
  [[ "$reply" == "y" || "$reply" == "Y" ]]
}

cmd_open() {
  local NAME="$1"; [[ -z "${NAME:-}" ]] && err "Branch name required" && exit 1
  local BRANCH="${PREFIX}${NAME}"

  need_git_repo
  say "➡ Opening feature branch: $BRANCH (base: $BASE, remote: $REMOTE) in $REPO"
  ensure_base_current

  if [[ "$RECREATE" == "1" ]]; then
    warn "Recreate: deleting $BRANCH locally/remotely if present…"
    branch_exists_local "$BRANCH"  && git -C "$REPO" branch -D "$BRANCH" || true
    branch_exists_remote "$BRANCH" && git -C "$REPO" push "$REMOTE" --delete "$BRANCH" || true
    git -C "$REPO" fetch --prune
    say "Creating new $BRANCH from $BASE"
    git -C "$REPO" switch -c "$BRANCH"
    say "Pushing and setting upstream"
    git -C "$REPO" push -u "$REMOTE" "$BRANCH"
    say "✅ Ready. On branch: $BRANCH"
    return
  fi

  if branch_exists_local "$BRANCH"; then
    warn "Local $BRANCH exists — switching."
    git -C "$REPO" switch "$BRANCH"
  elif branch_exists_remote "$BRANCH"; then
    warn "Remote $REMOTE/$BRANCH exists — creating local tracking."
    git -C "$REPO" switch -c "$BRANCH" --track "$REMOTE/$BRANCH"
  else
    say "Creating $BRANCH from $BASE"
    git -C "$REPO" switch -c "$BRANCH"
    say "Pushing and setting upstream"
    git -C "$REPO" push -u "$REMOTE" "$BRANCH"
  fi
  say "✅ Ready. Do your edits/commits on: $BRANCH"
}

cmd_close() {
  local NAME="$1"; [[ -z "${NAME:-}" ]] && err "Branch name required" && exit 1
  local BRANCH="${PREFIX}${NAME}"

  need_git_repo
  say "➡ Closing feature: $BRANCH → merge into $BASE (remote: $REMOTE) in $REPO"
  ensure_base_current

  # Prefer remote ref if available
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
    say "Merging (FF allowed) $MERGE_REF → $BASE"
    git -C "$REPO" merge --log "$MERGE_REF"
  fi

  say "Pushing $BASE"
  git -C "$REPO" push "$REMOTE" "$BASE"

  # Cleanup
  ensure_not_on_branch "$BRANCH"
  if branch_exists_local "$BRANCH"; then
    say "Deleting local $BRANCH"
    git -C "$REPO" branch -D "$BRANCH" || true
  fi
  if [[ "$DELETE_REMOTE" == "1" ]] && branch_exists_remote "$BRANCH"; then
    say "Deleting remote $REMOTE/$BRANCH"
    git -C "$REPO" push "$REMOTE" --delete "$BRANCH" || true
  else
    warn "Keeping remote branch (use --keep-remote to keep; default deletes)."
  fi

  say "✅ Closed. Feature merged into $BASE."
}

cmd_nuke() {
  local NAME="$1"; [[ -z "${NAME:-}" ]] && err "Branch name required" && exit 1
  local BRANCH="${PREFIX}${NAME}"

  need_git_repo
  say "⚠ Nuke branch: $BRANCH (local + remote) in $REPO"
  ensure_not_on_branch "$BRANCH"

  if ! branch_exists_local "$BRANCH" && ! branch_exists_remote "$BRANCH"; then
    warn "Nothing to delete. Branch not found locally or remotely."
    return 0
  fi

  if confirm "Delete branch '$BRANCH' locally and on '$REMOTE'? This does NOT merge."; then
    branch_exists_local "$BRANCH"  && git -C "$REPO" branch -D "$BRANCH" || true
    branch_exists_remote "$BRANCH" && git -C "$REPO" push "$REMOTE" --delete "$BRANCH" || true
    git -C "$REPO" fetch --prune
    say "✅ Nuked $BRANCH."
  else
    warn "Aborted."
  fi
}

main() {
  local cmd="${1:-}"; shift || true
  case "$cmd" in
    open)
      set +e
      local rest; rest="$(parse_common_flags "$@")"; set -e
      # shellcheck disable=SC2086
      set -- $rest
      cmd_open "${1:-}";;
    close)
      set +e
      local rest; rest="$(parse_common_flags "$@")"; set -e
      # shellcheck disable=SC2086
      set -- $rest
      cmd_close "${1:-}";;
    nuke)
      set +e
      local rest; rest="$(parse_common_flags "$@")"; set -e
      # shellcheck disable=SC2086
      set -- $rest
      cmd_nuke "${1:-}";;
    -h|--help|"")
      usage;;
    *)
      err "Unknown command: $cmd"; usage; exit 1;;
  esac
}

main "$@"
