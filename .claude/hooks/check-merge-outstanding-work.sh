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
# up in different PRs, and the inflight note tracking the gap was stale before anyone read it (that
# note was later retired by the doc that settled it,
# docs/solutions/logic-errors/909-needs-a-saturated-pipeline-the-third-precondition-2026-08-19.md).
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
# ITS LIMITS, STATED PLAINLY. A STALLED agent writes nothing and will not be detected. Stalls are
# common enough here that this must not be read as proof of quiescence - it catches the live case,
# which is the one that bit us, and nothing more. `ListAgents` is the check a human should still
# run when the answer matters. And a merge wrapped in another interpreter - `bash -c "gh pr merge
# ..."` - reaches the token scan below as ONE opaque token and is not seen: shlex cannot unwrap a
# nested shell without executing it, and the squash guard shares the same gap.
#
# WHAT IS DELIBERATELY NOT CHECKED: a live maven build. An earlier version scanned the process table
# for one, and it was dropped rather than scoped. Any build this harness launched is already a
# background task and shows up in the arm above; a build started OUTSIDE the harness is not this
# hook's business, and matching the process table by pattern brought its own bug - it counted the
# hook's own subshell and every wait-loop mentioning the pattern, reporting six live builds when
# none were running. One signal, honestly scoped, beats two where the second needs a caveat.
#
# THE OVERRIDE IS DELIBERATE AND LOUD. Prefix the merge command itself with
# MERGE_DESPITE_OUTSTANDING_WORK=1 to proceed. From inside a session that prefix arrives as part
# of the COMMAND TEXT, never as this hook's environment - hooks run with the harness's own env,
# which an agent cannot reach - so the token scan below honors the prefix form; the process-env
# form also works, for a human driving the harness from a shell that exports it. There are
# legitimate cases - the background work belongs to a different PR, or you have decided to follow
# up separately - and the point of the guard is that the decision is made rather than skipped.
#
# FAILS OPEN otherwise, deliberately: no session id, no scratch dir, no python3, unparseable JSON.
# A hook that blocks on its own bug jams the tool call shut.

set -euo pipefail

WINDOW_SECONDS="${MERGE_OUTSTANDING_WINDOW_SECONDS:-300}"

payload="$(cat)" || exit 0

# CHEAP PRE-FILTER before the interpreter spawn: a tokenised `gh pr merge` necessarily contains the
# literal substring "merge" (JSON escaping never rewrites letters), so anything without it can skip
# the python3 startup this hook would otherwise cost EVERY Bash call. It can only skip work, never
# decide - the token check below still makes every decision.
case "$payload" in
    *merge*) ;;
    *) exit 0 ;;
esac

# TOKENS, NOT SUBSTRINGS - the same rule check-squash-subject.sh states, for the same reason. A
# grep for "gh pr merge" fires on `gh pr comment --body "remember to run gh pr merge later"`, and a
# guard that blocks ordinary commands gets routed around. shlex splits the command the way the
# shell would, so the three words must appear as consecutive ARGV tokens; an unbalanced quote makes
# shlex raise, and that fails open. The gh token is matched by BASENAME - /usr/local/bin/gh is the
# same binary, and docs/agent-harness.md names that exact shape as one this guard exists for.
is_merge="$(printf '%s' "$payload" | python3 -c '
import json, re, shlex, sys
try:
    d = json.load(sys.stdin)
    cmd = (d.get("tool_input") or {}).get("command") or ""
    toks = shlex.split(cmd)
except Exception:
    sys.exit(0)
verdict = ""
for i in range(len(toks) - 2):
    if toks[i].rsplit("/", 1)[-1] == "gh" and toks[i + 1] == "pr" and toks[i + 2] == "merge":
        # The documented override, typed as an env prefix on the merge command, arrives HERE - as
        # command tokens - never in this hook process env (see THE OVERRIDE above). Walk the
        # NAME=VALUE assignments immediately preceding this gh; anything after it (an option value,
        # a --body string) is not a prefix and does not count.
        j = i - 1
        override = False
        while j >= 0 and re.match(r"[A-Za-z_][A-Za-z0-9_]*=", toks[j]):
            if toks[j] == "MERGE_DESPITE_OUTSTANDING_WORK=1":
                override = True
            j -= 1
        if not override:
            verdict = "merge"
            break
        verdict = "override"
print(verdict)
' 2>/dev/null)" || exit 0

[ "$is_merge" = "merge" ] || exit 0

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

if [ -n "$live_tasks" ]; then
    # The repo's hook contract is a PreToolUse `permissionDecision: deny` payload on STDOUT, the
    # same shape check-squash-subject.sh emits - not a bare exit 2. The reason string is what the
    # model actually reads, so it names the work found and the one legitimate way past.
    REASON="Background work from this session is still in flight, so this PR may be missing something that belongs in it."
    [ -n "$live_tasks" ] && REASON="$REASON Tasks that wrote output in the last ${WINDOW_SECONDS}s: $(printf '%s' "$live_tasks" | tr -d '\n' | sed 's/^  - //; s/  - /, /g')."
    REASON="$REASON Work that belongs in this PR cannot be added after the merge - it becomes a second PR, and whatever the description or the inflight notes claimed about it goes stale on master. Establish what each one is doing first. NOTE a stalled agent writes nothing and is not detected here, so this is not proof of quiescence - run ListAgents if the answer matters. If the outstanding work genuinely does not belong in this PR, re-run the merge command prefixed with MERGE_DESPITE_OUTSTANDING_WORK=1."

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
