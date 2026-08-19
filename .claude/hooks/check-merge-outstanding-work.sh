#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# PreToolUse hook: refuse a `gh pr merge` while this session still has background work in flight.
#
# THE TRAP. A PR is green, a human asks "ready?", and the agent answers from the PR's state alone -
# while a subagent it spawned is still working on something that belongs IN that PR. Merging then
# is not recoverable from inside the PR: the work has to become a second PR, and anything the
# merged description or the inflight notes said about the now-closed gap goes stale on master the
# moment it lands.
#
# That is not hypothetical. On 2026-08-19 astubbs#31 merged roughly ten minutes before a spawned
# agent finished building the broker-level reproduction of confluentinc#909 - the exact gap that
# PR's own description declared open under "Known gap". The fix and the evidence proving it ended
# up in different PRs, and docs/inflight/test-909-not-reproducible-by-existing-chaos-scenario.md
# was stale before anyone read it.
#
# WHY A HOOK AND NOT A LINE IN A DOCUMENT. docs/merge-checklist.md already lists what to confirm
# before merging, and it was loaded - injected into that very turn by inject-merge-checklist.sh.
# It did not help, because the question the agent asked itself was "is this PR ready?" and the
# thing it needed to remember was somewhere else entirely: a background task it had started an
# hour earlier. A checklist cannot prompt for what you have forgotten you are waiting on.
#
# THE SIGNAL. Claude Code writes each background task's output to
# <scratch>/<project>/$CLAUDE_CODE_SESSION_ID/tasks/<id>.output and appends to it while the task
# runs. A file touched within the window below means something is still producing output right
# now. Scoped by session id so a sibling session's work never blocks this one.
#
# ITS LIMIT, STATED PLAINLY: a STALLED agent writes nothing and will not be detected. Stalls are
# common enough here that this must not be read as proof of quiescence - it catches the live case,
# which is the one that bit us, and nothing more. `ListAgents` is the check a human should still
# run when the answer matters.
#
# ALSO CHECKED: a live maven build under .claude/worktrees, which is the other shape of "the thing
# that would have changed this PR has not finished yet".
#
# THE OVERRIDE IS DELIBERATE AND LOUD. Set MERGE_DESPITE_OUTSTANDING_WORK=1 to proceed. There are
# legitimate cases - the background work belongs to a different PR, or you have decided to follow
# up separately - and the point of the guard is that the decision is made rather than skipped.
#
# FAILS OPEN otherwise, deliberately: no session id, no scratch dir, no python3, unparseable JSON.
# A hook that blocks on its own bug jams the tool call shut.

set -euo pipefail

WINDOW_SECONDS="${MERGE_OUTSTANDING_WINDOW_SECONDS:-300}"

payload="$(cat)" || exit 0

# TOKENS, NOT SUBSTRINGS - the same rule check-squash-subject.sh states, for the same reason. A
# grep for "gh pr merge" fires on `gh pr comment --body "remember to run gh pr merge later"`, and a
# guard that blocks ordinary commands gets routed around. shlex splits the command the way the
# shell would, so the three words must appear as consecutive ARGV tokens; an unbalanced quote makes
# shlex raise, and that fails open.
is_merge="$(printf '%s' "$payload" | python3 -c '
import json, shlex, sys
try:
    d = json.load(sys.stdin)
    cmd = (d.get("tool_input") or {}).get("command") or ""
    toks = shlex.split(cmd)
except Exception:
    sys.exit(0)
for i in range(len(toks) - 2):
    if toks[i] == "gh" and toks[i + 1] == "pr" and toks[i + 2] == "merge":
        print("yes")
        break
' 2>/dev/null)" || exit 0

[ "$is_merge" = "yes" ] || exit 0

[ "${MERGE_DESPITE_OUTSTANDING_WORK:-}" = "1" ] && exit 0
[ -n "${CLAUDE_CODE_SESSION_ID:-}" ] || exit 0

now="$(date +%s)"
live_tasks=""
while IFS= read -r f; do
    [ -f "$f" ] || continue
    mtime="$(stat -c %Y "$f" 2>/dev/null || echo 0)"
    [ "$mtime" -eq 0 ] && continue
    age=$(( now - mtime ))
    if [ "$age" -lt "$WINDOW_SECONDS" ]; then
        live_tasks="${live_tasks}  - $(basename "$f" .output) (wrote ${age}s ago)"$'\n'
    fi
done < <(find "/tmp/claude-$(id -u)" -maxdepth 4 -path "*/${CLAUDE_CODE_SESSION_ID}/tasks/*.output" 2>/dev/null || true)

live_builds="$(pgrep -af 'MavenWrapperMain' 2>/dev/null | grep -c '\.claude/worktrees' || true)"
[ -z "$live_builds" ] && live_builds=0

if [ -n "$live_tasks" ] || [ "$live_builds" -gt 0 ]; then
    # The repo's hook contract is a PreToolUse `permissionDecision: deny` payload on STDOUT, the
    # same shape check-squash-subject.sh emits - not a bare exit 2. The reason string is what the
    # model actually reads, so it names the work found and the one legitimate way past.
    REASON="Background work from this session is still in flight, so this PR may be missing something that belongs in it."
    [ -n "$live_tasks" ] && REASON="$REASON Tasks that wrote output in the last ${WINDOW_SECONDS}s: $(printf '%s' "$live_tasks" | tr -d '\n' | sed 's/^  - //; s/  - /, /g')."
    [ "$live_builds" -gt 0 ] && REASON="$REASON Live maven build(s) under .claude/worktrees: ${live_builds}."
    REASON="$REASON Work that belongs in this PR cannot be added after the merge - it becomes a second PR, and whatever the description or the inflight notes claimed about it goes stale on master. Establish what each one is doing first. NOTE a stalled agent writes nothing and is not detected here, so this is not proof of quiescence - run ListAgents if the answer matters. If the outstanding work genuinely does not belong in this PR, re-run with MERGE_DESPITE_OUTSTANDING_WORK=1."

    REASON="$REASON" python3 -c '
import json, os
print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "permissionDecision": "deny",
        "permissionDecisionReason": os.environ["REASON"] + " See docs/merge-checklist.md.",
    }
}))'
    exit 0
fi

exit 0
