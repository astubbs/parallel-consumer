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
# nested shell without executing it, and the squash guard shares the same gap. A merge through the
# REST API - `gh api repos/<owner>/<repo>/pulls/<n>/merge -X PUT` - is not seen either: the scan
# looks for the `pr merge` subcommand, and widening it to API paths is a policy call deliberately
# not taken here (astubbs#324 review); ListAgents before an unusual merge route remains the human
# check.
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
# letters of "merge" (JSON escaping never rewrites letters), so anything without them can skip the
# python3 startup this hook would otherwise cost EVERY Bash call. Quote characters are stripped
# FIRST, because shlex joins mer""ge / mer'ge' / mer\ge back into the token `merge` - a raw
# substring test exited on those spellings before the token scan ran, making the pre-filter the
# decider (found by the astubbs#324 review). Stripped, it can only skip work, never decide - the
# token check below still makes every decision. Parameter expansions, not tr: no fork per Bash call.
stripped=${payload//\"/}
stripped=${stripped//\'/}
stripped=${stripped//\\/}
case "$stripped" in
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
def skip_repo_flags(j):
    # Only --repo/-R is skipped (with its value, attached or split). Any other token stops the
    # walk and leaves the command unmatched, which fails open - the same posture as the sibling
    # hook when its regex does not match.
    while j < len(toks):
        t = toks[j]
        if t in ("-R", "--repo"):
            j += 2
        elif t.startswith("--repo=") or (t.startswith("-R") and len(t) > 2):
            j += 1
        else:
            break
    return j

def subcommand_after(i):
    # gh accepts --repo/-R on EITHER side of `pr`: `gh -R owner/repo pr merge` AND
    # `gh pr -R owner/repo merge` both merge (live-verified against gh), so a bare three-token
    # adjacency check misses both - while `gh pr merge --repo owner/repo` is caught. The leading
    # form is what house style produces (AGENTS.md qualifies every gh command with -R); the
    # mid-position gap was found by the astubbs#324 review after the leading fix landed.
    j = skip_repo_flags(i + 1)
    if j < len(toks) and toks[j] == "pr":
        k = skip_repo_flags(j + 1)
        return ["pr"] + toks[k:k + 1]
    return toks[j:j + 2]

verdict = ""
for i in range(len(toks)):
    if toks[i].rsplit("/", 1)[-1] == "gh" and subcommand_after(i) == ["pr", "merge"]:
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
# PORTABLE MTIME. `stat -c %Y` is GNU; BSD/macOS stat rejects `-c` and this returned nothing while
# still exiting 0, so every caller silently read "no mtime". Branch on the platform rather than
# falling back: on Linux `stat -f` is --file-system and SUCCEEDS with a number about the filesystem,
# so a blind `-c || -f` fallback would hand back a wrong answer instead of no answer.
#
# `|| true` IS LOAD-BEARING, not tidiness: this script runs under `set -e`, and `mtime="$(_mtime ...)"`
# takes the substitution's status, so a failing stat killed the hook AT that line - never reaching the
# fail-closed branch below, and exiting non-zero, which PreToolUse reads as a non-blocking error and
# ALLOWS the merge. The fail-closed arm was unreachable until this was here. `_mtime` therefore never
# fails: it prints the mtime, or nothing.
if stat -c %Y . >/dev/null 2>&1; then
    _mtime() { stat -c %Y "$1" 2>/dev/null || true; }      # GNU coreutils
else
    _mtime() { stat -f %m "$1" 2>/dev/null || true; }      # BSD / macOS
fi

live_tasks=""
while IFS= read -r f; do
    [ -f "$f" ] || continue
    mtime="$(_mtime "$f")"
    # FAIL CLOSED. A file matched the session's tasks glob, so something is there; being unable to
    # date it is not evidence that nothing is running. The old code skipped it, which is how this
    # guard came to allow every merge on macOS.
    if [ -z "$mtime" ]; then
        live_tasks="${live_tasks}  - $(basename "$f" .output) (mtime unreadable - assuming live)"$'\n'
        continue
    fi
    age=$(( now - mtime ))
    if [ "$age" -lt "$WINDOW_SECONDS" ]; then
        live_tasks="${live_tasks}  - $(basename "$f" .output) (wrote ${age}s ago)"$'\n'
    fi
done < <(find "/tmp/claude-$(id -u)" -maxdepth 4 -path "*/${CLAUDE_CODE_SESSION_ID}/tasks/*.output" 2>/dev/null || true)

# SECOND ARM: THIS PR'S OWN INFLIGHT NOTE, surfaced at the one moment it matters.
#
# A note saying "still open: X" is written precisely so X is not forgotten, and is then read by
# nobody at the moment of merging - which is the only moment it could still change the outcome.
# Work that belonged in the PR becomes a second PR; a caveat the note recorded becomes a claim on
# master that nothing tested.
#
# NOT a post-commit hook: commits happen constantly and the note is dozens of lines, so it would
# bury the work it is meant to protect and train everyone to scroll past it. Fires once, here.
#
# Prints only what is above the first "Already fixed" heading - the convention those notes follow -
# so a note that has accumulated a long resolved section stays short at the point of use.
pr_num=""
case "$payload" in
    *"pr merge "*) pr_num="$(sed -n 's/.*pr merge \([0-9][0-9]*\).*/\1/p' <<<"$payload" | head -1)" ;;
esac
if [ -z "$pr_num" ]; then
    branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
    [ -n "$branch" ] && pr_num="$(gh pr list --head "$branch" --json number --jq '.[0].number' 2>/dev/null || true)"
fi
outstanding=""
if [ -n "$pr_num" ]; then
    # EVERY matching note. `pr-322-*` matches more than one, and `head -1` surfaced whichever sorted
    # first - so the guard could quote the split plan while the file recording what is actually open
    # went unread. Same flaw found in bin/check-pr-ready.sh and fixed in the same commit.
    while IFS= read -r n; do
        [ -n "$n" ] && [ -f "$n" ] || continue
        chunk="$(awk '/^## Already fixed/ {exit} {print}' "$n" 2>/dev/null)"
        [ -n "$chunk" ] && outstanding="${outstanding}
=== ${n} ===
${chunk}"
    done <<< "$(find docs/inflight -maxdepth 1 -name "pr-${pr_num}-*.md" 2>/dev/null | sort)"
fi

# BOTH ARMS, NOT THE FIRST ONE THAT MATCHES. This used to require `-z "$live_tasks"`, so a session
# with live tasks AND an outstanding note saw only the task message - and the documented response to
# that message is to re-run with MERGE_DESPITE_OUTSTANDING_WORK=1, which exits above before either
# arm runs. The note was then never shown at all, at the one moment it exists for. Found in review of
# astubbs#324. The note now rides along with the task deny instead of losing to it.
if [ -n "$outstanding" ] && [ -z "$live_tasks" ]; then
    export REASON="astubbs#${pr_num} has an inflight note recording what is still open on it. Read it before merging - this is the last moment any of it can still land in THIS PR rather than becoming a follow-up. If every item is genuinely resolved or genuinely belongs elsewhere, delete or update the note as part of the merge, then re-run prefixed with MERGE_DESPITE_OUTSTANDING_WORK=1."
    export NOTE_BODY="$outstanding"
    python3 -c '
import json, os
print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
    "permissionDecision": "deny",
    "permissionDecisionReason": os.environ["REASON"] + "\n\n" + os.environ["NOTE_BODY"] + "\n\nSee docs/merge-checklist.md."}}))
'
    exit 0
fi

if [ -n "$live_tasks" ]; then
    # The repo's hook contract is a PreToolUse `permissionDecision: deny` payload on STDOUT, the
    # same shape check-squash-subject.sh emits - not a bare exit 2. The reason string is what the
    # model actually reads, so it names the work found and the one legitimate way past.
    REASON="Background work from this session is still in flight, so this PR may be missing something that belongs in it."
    REASON="$REASON Tasks that wrote output in the last ${WINDOW_SECONDS}s: $(printf '%s' "$live_tasks" | tr -d '\n' | sed 's/^  - //; s/  - /, /g')."
    REASON="$REASON Work that belongs in this PR cannot be added after the merge - it becomes a second PR, and whatever the description or the inflight notes claimed about it goes stale on master. Establish what each one is doing first. NOTE a stalled agent writes nothing and is not detected here, so this is not proof of quiescence - run ListAgents if the answer matters. If the outstanding work genuinely does not belong in this PR, re-run the merge command prefixed with MERGE_DESPITE_OUTSTANDING_WORK=1."

    # Carry the note here too - see the both-arms comment above.
    [ -n "$outstanding" ] && REASON="$REASON

astubbs#${pr_num} ALSO has an inflight note recording what is still open on it, and this is the last
moment any of it can land in THIS PR rather than becoming a follow-up:
${outstanding}"

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
