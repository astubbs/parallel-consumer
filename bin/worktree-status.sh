#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# worktree-status — show who/what owns each git worktree.
# Reads the local-only `.worktree-owner` marker in each worktree and cross-checks
# live process holders via lsof. The marker convention answers the question the
# Claude UI can't: which agent/session is working in which worktree.
# See AGENTS.md "Worktree ownership".
#
# Usage: bash bin/worktree-status.sh
set -euo pipefail
export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$PATH"

cd "$(git rev-parse --show-toplevel)"

git worktree list --porcelain | awk '
  /^worktree /{path=$2}
  /^branch /{sub("refs/heads/","",$2); print path"\t"$2}
  /^detached/{print path"\tdetached"}
' | while IFS=$'\t' read -r path branch; do
  name=$(basename "$path")
  marker="$path/.worktree-owner"
  if [ -f "$marker" ]; then
    owner=$(grep -E '^owner:'  "$marker" | head -1 | cut -d: -f2- | sed 's/^ *//')
    st=$(grep -E '^status:' "$marker" | head -1 | cut -d: -f2- | sed 's/^ *//')
    work=$(grep -E '^work:'   "$marker" | head -1 | cut -d: -f2- | sed 's/^ *//')
  else
    owner="<no marker>"; st="<unmarked>"; work=""
  fi
  holder=$(lsof +D "$path" 2>/dev/null | awk 'NR>1{print $1}' | sort -u | paste -sd, - || true)
  printf '\n\033[1m%s\033[0m  [%s]\n' "$name" "$branch"
  printf '  owner:  %s\n' "$owner"
  printf '  status: %s\n' "$st"
  [ -n "$work" ] && printf '  work:   %s\n' "$work"
  printf '  live:   %s\n' "${holder:-idle}"
done
