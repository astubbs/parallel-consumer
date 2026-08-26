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
# from scratch - taking the tooling, clearing the legacy-token residue, moving the package tree -
# resolved the merge conflicts on top of it, and only found out at `git push`, which rejected the
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
# WHAT IT MUST NEVER DENY, because a guard that blocks its own remedy gets switched off:
#
#   - THE RECONCILIATION ITSELF. `git merge origin/<this-branch>`, `git rebase origin/<this-branch>`,
#     `@{upstream}`, `FETCH_HEAD` - the commands the deny message tells you to run. Review found the
#     first version denying all of them, so the only way to obey the message was to lie to the
#     override, whose documented meaning is the opposite. It also meant that ON `master`,
#     `origin/<branch>` IS `origin/master`, so `git merge origin/master` - the most routine command
#     in this repo - was denied every time master advanced.
#   - THE WAY OUT OF A CONFLICTED TREE. `--abort`, `--continue`, `--skip`, `--quit`. A sibling
#     session pushing mid-rebase must not trap the agent between a rebase it cannot finish and one it
#     cannot abandon. check-history-rewrite.sh already exempts these; this departed from it.
#
# IT ONLY DENIES WHAT IT CAN PROVE. No branch, no `origin/<branch>`, a detached HEAD, a fetch that
# fails - every one of those exits silent. The deny fires on one answer: the remote-tracking ref
# contains commits HEAD does not, and the command is not one of the two exemptions above.
# `git push` is deliberately NOT an arm, because git already refuses that case itself and legibly.
#
# KNOWN IMPRECISIONS, stated the way check-shallow-history.sh states its own, because an
# undocumented one reads as a bug to the next person who trips it:
#
#   - `git -C <dir>` IS NOT READ AS A REDIRECT. A merge aimed at another repository is judged
#     against this one. The tokeniser skips the flag so the SUBCOMMAND is found correctly; nothing
#     re-points the comparison.
#   - A DELETED REMOTE BRANCH KEEPS DENYING until something prunes. The per-merge fetch of a ref
#     origin no longer has fails into silence, and this arm does not prune. The SessionStart arm
#     does, and the deny message names `git fetch --all --prune`, so it self-heals within a session.
#   - A BRANCH THIS CHECKOUT HAS NEVER FETCHED IS INVISIBLE. With no `origin/<branch>` locally the
#     hook exits silent - which is the state where the cache is MOST stale. That is the SessionStart
#     arm's job, and it is why the stamp key below is a correctness matter rather than a tidy-up.
#
# FAIL LOUDLY, NOT OPEN, WHEN THE GUARD ITSELF IS BROKEN. Missing `python3` or a missing shared
# library are not evidence about the branch; they mean the check could not run, and a silent exit
# there is indistinguishable from a healthy quiet hook - the defect class this whole harness exists
# to police. Both say so on stderr. Likewise, once the branch is PROVEN behind, a failure to emit
# the verdict must not degrade into an allow: it exits 2, which blocks with the reason on stderr.
#
# CHEAP BAIL FIRST. This is registered on `Bash`, so it runs on every tool call the agent makes;
# review measured the first version at several times the cost of its siblings because it paid a
# python3 and a git process before its pre-filter. The pre-filter may only SKIP work, never decide -
# every payload either arm can act on contains one of its substrings, and a false positive falls
# through to exactly the logic below. NOTE: it gates EVERY arm, so registering this script on a
# third event means adding that event's marker to the case, and nothing checks that for you.
set -uo pipefail

payload="$(cat 2>/dev/null || true)"
[ -n "$payload" ] || exit 0

case "$payload" in
    *merge*|*rebase*|*SessionStart*) ;;
    *) exit 0 ;;
esac

# RUN IT, do not merely FIND it. `command -v python3` is an existence test, so an interpreter that
# is on PATH and exits non-zero on every invocation passes it - and this guard then goes silently
# dead, which is the failure it is here to announce. The self-test's shim is exactly that shape, and
# it defeated the `command -v` form. Same reasoning as hook-common.sh's `stat` probe: ask whether the
# tool WORKS, never whether it is present.
python3 -c '' >/dev/null 2>&1 || {
    echo "check-branch-behind-its-own-remote: python3 is missing or non-functional - this guard CANNOT RUN and is not passing." >&2
    exit 0
}

hook_lib="${BASH_SOURCE[0]%/*}/lib/hook-common.sh"
[ -r "$hook_lib" ] || {
    echo "check-branch-behind-its-own-remote: $hook_lib is unreadable - this guard CANNOT RUN and is not passing." >&2
    exit 0
}
# shellcheck source=.claude/hooks/lib/hook-common.sh
. "$hook_lib"

root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$root" ] || exit 0
cd "$root" 2>/dev/null || exit 0

# GIT_TERMINAL_PROMPT=0 on every fetch below: a credential prompt inside a hook hangs the tool call
# with nothing on screen to explain it, which is the one failure worse than not fetching. A network
# STALL has the same shape and needs its own bound - `timeout(1)` is GNU-only and absent here, so
# the ceiling is set through git's own transport knobs, which cover both halves of a hang: a
# connection that never establishes, and one that establishes and then delivers nothing.
export GIT_TERMINAL_PROMPT=0
export GIT_SSH_COMMAND="${GIT_SSH_COMMAND:-ssh} -o ConnectTimeout=10 -o BatchMode=yes"
bounded_fetch() { git -c http.lowSpeedLimit=1000 -c http.lowSpeedTime=20 fetch "$@" >/dev/null 2>&1 || true; }

# ------------------------------------------------------------------------------------------------
# SessionStart: fetch everything, once per throttle window
# ------------------------------------------------------------------------------------------------
if [ "$(printf '%s' "$payload" | python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("hook_event_name",""))
except Exception: pass' 2>/dev/null || true)" = "SessionStart" ]; then
    # ABSOLUTE, ALWAYS. `--git-common-dir` answers a RELATIVE `.git` from a main checkout and an
    # absolute path from a linked worktree, so keying on it raw put every clone on this machine
    # onto one stamp - one clone's session start then suppressed another's fetch for the whole
    # window, leaving it reading exactly the stale refs this hook exists to refresh. Measured twice,
    # independently, in review. The `cd` above means a relative answer is relative to $root.
    common="$(git rev-parse --git-common-dir 2>/dev/null || echo .git)"
    case "$common" in /*) ;; *) common="$root/$common" ;; esac
    stamp="$(hook_stamp_path pc-fetch-all "$common")"
    if hook_throttle_expired "$stamp" "${BRANCH_FRESHNESS_FETCH_FLOOR:-300}"; then
        : > "$stamp" 2>/dev/null || true
        # --prune so a deleted remote branch stops answering queries as though it still existed.
        # Backgrounded is NOT an option: a later ref read in the same session would race it and get
        # the stale answer this exists to prevent.
        bounded_fetch --all --prune --quiet
    fi
    exit 0
fi

# ------------------------------------------------------------------------------------------------
# PreToolUse: refuse a merge or rebase onto a branch that is behind its own published tip
# ------------------------------------------------------------------------------------------------

hook_git_runs_any "$payload" merge rebase || exit 0

branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
[ -n "$branch" ] && [ "$branch" != "HEAD" ] || exit 0

# The branch's OWN counterpart, not whatever @{upstream} happens to point at - a branch tracking
# origin/master would otherwise be compared against the base, which the other hook already covers.
remote_ref="origin/$branch"
git rev-parse --verify --quiet "$remote_ref" >/dev/null 2>&1 || exit 0

# THE ESCAPE HATCH IS A TOKEN, NOT A SUBSTRING, and this is the one place the distinction has teeth:
# the deny message below TEACHES the agent the exact string, so an agent that reads it and then
# writes `git commit -m "note BRANCH_FRESHNESS_OVERRIDE=1" && git merge ...`, or merely paraphrases
# it into the payload's agent-written `description` field, silently bypassed the guard. Review
# confirmed all three. Matched against the parsed command's tokens, and against a genuinely exported
# variable too, the way check-shallow-history.sh honours both forms of its own override.
[ "${BRANCH_FRESHNESS_OVERRIDE:-}" = "1" ] && exit 0
printf '%s' "$payload" | python3 -c '
import json, shlex, sys
try:
    cmd = json.load(sys.stdin).get("tool_input", {}).get("command", "")
    toks = shlex.split(cmd)
except Exception:
    sys.exit(1)
sys.exit(0 if any(t == "BRANCH_FRESHNESS_OVERRIDE=1" for t in toks) else 1)
' 2>/dev/null && exit 0

# EXEMPT THE REMEDY AND THE WAY OUT - see WHAT IT MUST NEVER DENY in the header. Every merge/rebase
# invocation is inspected; the guard stands only if at least one of them is neither.
exempt="$(hook_git_invocations "$payload" | REMOTE_REF="$remote_ref" BRANCH="$branch" python3 -c '
import os, sys
remote_ref = os.environ["REMOTE_REF"]
branch = os.environ["BRANCH"]
# The refs that NAME this branch published tip. Merging or rebasing onto any of them IS the
# reconciliation the deny message asks for, so denying it would block the fix and nothing else.
REMEDY = {remote_ref, "FETCH_HEAD", branch + "@{upstream}", "@{upstream}", "@{u}"}
# The forms that finish or abandon an in-progress operation. Trapping an agent between a rebase it
# cannot continue and one it cannot abort is worse than any staleness.
CONTROL = {"--abort", "--continue", "--skip", "--quit", "--edit-todo", "--show-current-patch"}
guarded = False
for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    sub, args = parts[0], parts[1:]
    if sub not in ("merge", "rebase"):
        continue
    if CONTROL & set(args):
        continue
    if REMEDY & set(args):
        continue
    guarded = True
print("guarded" if guarded else "exempt")
' 2>/dev/null || true)"
[ "$exempt" = "guarded" ] || exit 0

bounded_fetch --quiet origin "$branch"

behind="$(git rev-list --count "HEAD..$remote_ref" 2>/dev/null || true)"
case "$behind" in ''|*[!0-9]*) exit 0 ;; esac
[ "$behind" -gt 0 ] || exit 0

subjects="$(git log --oneline --max-count=10 "HEAD..$remote_ref" 2>/dev/null || true)"

# The branch is now PROVEN behind, so a failure to emit must not become an allow: exit 2 blocks the
# call with stderr as the reason, which is the honest fallback for a verdict already established.
BEHIND="$behind" BRANCH="$branch" SUBJECTS="$subjects" python3 -c '
import json, os
behind = os.environ["BEHIND"]
branch = os.environ["BRANCH"]
subjects = os.environ["SUBJECTS"]
reason = (
    "origin/" + branch + " has " + behind + " commit(s) this checkout does not have, so this "
    "branch is BEHIND its own published tip:\n\n" + subjects + "\n\n"
    "Merging or rebasing something ELSE into it now builds on a stale ref. Whatever you produce "
    "cannot be pushed without discarding those commits, and you will not find out until the push is "
    "rejected - which is exactly how an hour of duplicated package-rename work was thrown away "
    "once already.\n\n"
    "Run `git fetch --all --prune`, then reconcile with the published tip - merging or rebasing "
    "onto origin/" + branch + " itself is NOT blocked, and is the way out. If those remote commits "
    "are genuinely to be abandoned, say so explicitly and re-run prefixed with "
    "BRANCH_FRESHNESS_OVERRIDE=1."
)
print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
    "permissionDecision": "deny",
    "permissionDecisionReason": reason}}))
' || {
    echo "check-branch-behind-its-own-remote: $branch is behind $remote_ref, but the verdict could not be emitted - blocking rather than allowing a merge already proven unsafe." >&2
    exit 2
}
exit 0
