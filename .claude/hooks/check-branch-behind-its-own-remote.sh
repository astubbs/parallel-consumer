#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Two arms against ONE failure: working from a remote-tracking ref that is out of date.
#
#   SessionStart  - fetch every remote, pruned, so every ref this session reads is real
#   PreToolUse    - DENY `git merge` / `git rebase` while this branch is BEHIND its own
#                   `origin/<branch>`, naming the commits it has not got
#
# THE INCIDENT. On 2026-08-26 a session picked up astubbs/parallel-consumer#205, fetched
# `origin/master`, and merged it in. It never fetched the BRANCH's own ref, which had been two weeks
# stale since the package-rename sweep pushed five commits to it. So the session re-did the rename
# from scratch - taking the tooling, clearing the legacy-token residue, moving 239 files - resolved
# 43 merge conflicts on top of it, and only found out at `git push`, which rejected the
# non-fast-forward. Everything after the first command was wasted, and the recovery was to throw the
# local work away and start again from the published tip.
#
# WHY `origin/master` BEING FRESH IS NOT ENOUGH, and why this is a separate hook from
# remind-master-drift-on-push.sh. That one fetches and reports what MASTER has gained; it is about
# the base moving under you. This is the mirror image - YOUR OWN BRANCH moving under you, from
# another session, another machine, or a sweep that touched every open branch at once. A repo where
# several agents run at once has that happen routinely, and nothing was looking: the divergence is
# invisible until a push is rejected, by which time the cost is already paid.
#
# WHY THIS ONE DENIES, when the other push hooks only report. `AGENTS.md` says a hook blocks a
# VIOLATION and reports a SITUATION, and "should I merge master now" is a situation with no wrong
# answer. Merging into a branch you know is behind its own published tip is not that: the result
# cannot be pushed without discarding somebody else's commits, so there is no outcome the agent
# wanted. Denying costs one fetch; allowing cost an hour, once, measurably.
#
# IT ONLY DENIES WHAT IT CAN PROVE. No branch, no upstream, no network, a detached HEAD, a fetch
# that fails - every one of those exits silent. The deny fires on exactly one answer: the
# remote-tracking ref contains commits HEAD does not. `git push` is deliberately NOT an arm, because
# git already refuses that case itself and the refusal is legible.
#
# NOT `git fetch` ITSELF ON EVERY TOOL CALL. The SessionStart arm is throttled on a stamp file so
# that a session which starts, stops and resumes does not turn into a fetch loop; the PreToolUse arm
# fetches ONE ref, which is cheap enough to pay for every merge.
set -uo pipefail

payload="$(cat 2>/dev/null || true)"
[ -n "$payload" ] || exit 0

hook_lib="${BASH_SOURCE[0]%/*}/lib/hook-common.sh"
[ -r "$hook_lib" ] || exit 0
# shellcheck source=.claude/hooks/lib/hook-common.sh
. "$hook_lib"

event="$(printf '%s' "$payload" | python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("hook_event_name",""))
except Exception: pass' 2>/dev/null || true)"

root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$root" ] || exit 0
cd "$root" 2>/dev/null || exit 0

# GIT_TERMINAL_PROMPT=0 on every fetch below: a credential prompt inside a hook hangs the tool call
# with nothing on screen to explain it, which is the one failure worse than not fetching.
export GIT_TERMINAL_PROMPT=0

# ------------------------------------------------------------------------------------------------
# SessionStart: fetch everything, once per throttle window
# ------------------------------------------------------------------------------------------------
FETCH_FLOOR_SECONDS="${BRANCH_FRESHNESS_FETCH_FLOOR:-300}"

if [ "$event" = "SessionStart" ]; then
    stamp="$(hook_stamp_path pc-fetch-all "$(git rev-parse --git-common-dir 2>/dev/null || echo repo)")"
    now="$(date +%s)"
    last="$(hook_file_mtime "$stamp")"
    case "$last" in ''|*[!0-9]*) last=0 ;; esac
    if [ $((now - last)) -ge "$FETCH_FLOOR_SECONDS" ]; then
        : > "$stamp" 2>/dev/null || true
        # --prune so a deleted remote branch stops answering queries as though it still existed.
        # Backgrounded is NOT an option: a later ref read in the same session would race it and get
        # the stale answer this exists to prevent.
        git fetch --all --prune --quiet >/dev/null 2>&1 || true
    fi
    exit 0
fi

# ------------------------------------------------------------------------------------------------
# PreToolUse: refuse a merge or rebase onto a branch that is behind its own published tip
# ------------------------------------------------------------------------------------------------

# Cheap bail before paying for python3 on EVERY Bash call.
case "$payload" in
    *merge*|*rebase*) ;;
    *) exit 0 ;;
esac

hook_git_runs "$payload" merge || hook_git_runs "$payload" rebase || exit 0

# The documented escape hatch, read from the PAYLOAD rather than this process's environment:
# a hook does not inherit the variables an agent prefixes onto its own command, so testing
# "$BRANCH_FRESHNESS_OVERRIDE" here would look right and never fire.
case "$payload" in *BRANCH_FRESHNESS_OVERRIDE=1*) exit 0 ;; esac

branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
[ -n "$branch" ] && [ "$branch" != "HEAD" ] || exit 0

# The branch's OWN counterpart, not whatever @{upstream} happens to point at - a branch tracking
# origin/master would otherwise be compared against the base, which the other hook already covers.
remote_ref="origin/$branch"
git rev-parse --verify --quiet "$remote_ref" >/dev/null 2>&1 || exit 0

git fetch --quiet origin "$branch" >/dev/null 2>&1 || true

behind="$(git rev-list --count "HEAD..$remote_ref" 2>/dev/null || true)"
case "$behind" in ''|*[!0-9]*) exit 0 ;; esac
[ "$behind" -gt 0 ] || exit 0

subjects="$(git log --oneline --max-count=10 "HEAD..$remote_ref" 2>/dev/null || true)"

printf '%s' "$payload" | BEHIND="$behind" BRANCH="$branch" SUBJECTS="$subjects" python3 -c '
import json, os
behind = os.environ["BEHIND"]
branch = os.environ["BRANCH"]
subjects = os.environ["SUBJECTS"]
reason = (
    "origin/" + branch + " has " + behind + " commit(s) this checkout does not have, so this "
    "branch is BEHIND its own published tip:\n\n" + subjects + "\n\n"
    "Merging or rebasing now builds on a stale ref. Whatever you produce cannot be pushed without "
    "discarding those commits, and you will not find out until the push is rejected - which is "
    "exactly how an hour of duplicated package-rename work was thrown away once already.\n\n"
    "Run `git fetch --all --prune` first, then reconcile with the published tip before you build on "
    "it. If the remote commits are genuinely to be abandoned, say so explicitly and re-run with "
    "BRANCH_FRESHNESS_OVERRIDE=1."
)
print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
    "permissionDecision": "deny",
    "permissionDecisionReason": reason}}))
' 2>/dev/null || true
