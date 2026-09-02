#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# SessionStart and pre-push hook: say so when a file this repo means to decompose has become cheap
# to decompose, and stay quiet the rest of the time.
#
# WHY THIS EXISTS
#
# docs/refactoring.md says its entries are to be picked up "when things are quiet". That condition
# was never evaluated by anything, so the entries aged instead - AbstractParallelEoSStreamProcessor
# is recorded there at 1533 lines and is now 2405. `bin/inflight.mjs refactor-window` can now answer
# the question, but docs/agent-harness.md's standing distinction applies: a rule in a file fires only
# if somebody opens the file, and nobody opens a backlog speculatively to ask whether today is the
# day. A hook fires whether or not anyone remembers it exists.
#
# REGISTERED TWICE, ONE SCRIPT. SessionStart reaches a person or agent starting work, which is when
# picking up a refactor is a live option; the pre-push arm reaches the moment a piece of work is
# finishing, which is when the next thing gets chosen. Neither covers the other: an agent that never
# starts a fresh session still pushes, and a person who never pushes still opens sessions.
# .claude/hooks/check-branch-behind-its-own-remote.sh is the precedent for the double registration
# and for reading `hook_event_name` to tell the arms apart.
#
# IT DOES NOT REACH SUBAGENTS on the SessionStart arm, and that is measured rather than assumed:
# .claude/hooks/inject-branch-context.sh records that SessionStart does NOT fire for an agent spawned
# via the Task tool. A subagent that pushes gets the other arm; one that only edits gets
# nudge-refactor-candidate.sh. Closing the gap entirely would need a third registration keyed on
# `agent_type`, which is deliberately not done here - see the plan's deferred work.
#
# SILENCE IS EARNED, NEVER ASSUMED. `--if-open` prints nothing when the signal RAN and no candidate
# is open. It prints when anything failed, and so does this - a hook whose correct output is silence
# is byte-identical to a hook that is broken, which is the rule inject-branch-context.sh states as
# "DEGRADED READS ARE LOUD, NEVER SHORT". So a non-zero exit from the command becomes a loud line
# rather than the quiet that means "go ahead and refactor". The one silence left is this script
# failing to start at all - a dead hook cannot announce itself, and jamming the tool call would be
# worse than the miss.
#
# THROTTLED ON CONTENT, NOT ON TIME, and the distinction is the whole design. R8 says the report
# repeats until the work is done, and that stands - but repeating the IDENTICAL paragraph at every
# session start and every push for days is how CONCEPTS.md says an advisory reminder trains its
# reader to skip it, which is the same end state as never firing. So the stamp holds a digest of what
# was last said: unchanged, it stays quiet for the window; changed - a window newly opened, a new
# blocker named, a failure appearing - it speaks at once. A plain timer would have delayed a newly
# opened window by up to the whole window, which is the one moment this exists for.
#
# THIS IS NOT THE STATE THE DESIGN REFUSES TO KEEP. Nothing here is a stored verdict: the answer is
# recomputed from the refs on every run, and the stamp only decides whether to repeat it. The command
# itself is never throttled - anything invoked by hand always answers.
#
# shell-justified: this is a hook, matching every other script in this directory; bin/AGENTS.md's
# Node-default rule is scoped to bin/ and does not reach here. The work itself is Node - this only
# decides whether to ask and how to deliver the answer.
#
# Never fails a session or a push: any error prints nothing and exits 0.

set -uo pipefail

payload="$(cat 2>/dev/null || true)"
[ -n "$payload" ] || exit 0

# Cheap bail before anything is sourced or forked. Every payload either arm can act on contains one
# of these substrings; a false positive costs the real check below and nothing else.
case "$payload" in
    *SessionStart*|*push*) ;;
    *) exit 0 ;;
esac

hook_lib="${BASH_SOURCE[0]%/*}/lib/hook-common.sh"
[ -r "$hook_lib" ] || exit 0
# shellcheck source=lib/hook-common.sh
. "$hook_lib"

command -v python3 >/dev/null 2>&1 || exit 0
event="$(hook_event_name "$payload")"

# The pre-tool arm has to prove the command really pushes. `hook_git_runs_any` is token-aware and
# reads EVERY git invocation in the command, which the naive test does not: hook-common.sh's header
# records `git -C /path push` silently defeating the version that stopped at the first subcommand.
if [ "$event" != "SessionStart" ]; then
    hook_git_runs_any "$payload" push || exit 0
fi

# The payload's cwd wins over the session's - hook_project_root's header owns why. The `cd` that
# used to sit here was inert (the old ordering overrode it) AND load-bearing-looking, which is the
# worst combination: with CLAUDE_PROJECT_DIR absent it let the payload choose which bin/inflight.mjs
# ran. Resolving the root first and invoking by ABSOLUTE path removes that entirely.
# KNOWN LIMIT, recorded rather than papered over: for `git -C /other/worktree push` this measures
# the session's repository, not the one being pushed. The obvious fix - read the -C target from the
# already-tokenised command - is not available: `hook_git_invocations` emits the subcommand and its
# arguments, and -C is a git GLOBAL option that precedes the subcommand, so the helper drops it.
# Getting it would mean widening that helper's output contract, which three other hooks consume.
# Left as an open thread on the pull request rather than decided here.
root="$(hook_project_root "$payload")"
[ -n "$root" ] || exit 0
inflight="$root/bin/inflight.mjs"
[ -f "$inflight" ] || exit 0
command -v node >/dev/null 2>&1 || exit 0
cd "$root" 2>/dev/null || exit 0

# STDERR IS KEPT SEPARATE AND ONLY USED ON FAILURE. Folding it into stdout unconditionally would put
# any future node warning into every session; discarding it would lose the one thing R18 requires -
# an unreadable config reports its reason on stderr and prints nothing at all on stdout, so a hook
# that dropped stderr would answer a broken configuration with the silence that means "all quiet".
# THE SCRATCH FILE IS AN ENHANCEMENT, THE LOUD LINE IS THE CONTRACT. `|| exit 0` here meant a full
# or read-only TMPDIR turned a failed measurement into the exact bytes a quiet tree produces - a
# second silence, created by the failure-reporting machinery itself, and correlated with trouble.
err="$(mktemp 2>/dev/null || true)"
[ -n "$err" ] && trap 'rm -f "$err"' EXIT
report="$(node "$inflight" refactor-window --if-open 2>"${err:-/dev/null}")"
rc=$?
if [ "$rc" -ne 0 ]; then
    reason="$([ -n "$err" ] && cat "$err" 2>/dev/null)"
    # APPENDED, NOT SUBSTITUTED. The command reports per candidate and still exits non-zero when any
    # one of them failed, so overwriting the captured stdout threw away the candidates that DID
    # answer - reinstating one layer up the all-or-nothing behaviour the library is written against.
    report="refactor-window could not answer for everything, so this is NOT a quiet tree - it is a partly unknown one.
${reason:-the command failed with no message}${report:+

${report}}"
fi
[ -n "$report" ] || exit 0

# Same content as last time, inside the window? Then this is a repeat, not news.
: "${REFACTOR_WINDOW_REPEAT_SECONDS:=43200}"
stamp="$(hook_stamp_path pc-refactor-window "$root")"
digest="$(printf '%s' "$report" | cksum | awk '{print $1}')"
# ONLY A SUCCESSFUL REPORT IS ELIGIBLE. Throttling on content alone silenced a repeated
# measurement FAILURE for twelve hours, and the second session then received the exact silence
# reserved for a completed run over a quiet tree - reintroducing, in the throttle, the collapse of
# "could not look" into "nothing to say" that the rest of this script is built to prevent.
if [ "$rc" -eq 0 ] && [ -r "$stamp" ] && [ "$(cat "$stamp" 2>/dev/null)" = "$digest" ] \
   && ! hook_throttle_expired "$stamp" "$REFACTOR_WINDOW_REPEAT_SECONDS"; then
    exit 0
fi
[ "$rc" -eq 0 ] && printf '%s' "$digest" > "$stamp" 2>/dev/null || true

if [ "$event" = "SessionStart" ]; then
    printf '%s\n' "$report"
    exit 0
fi

# The pre-tool arm cannot just print: a PreToolUse hook reaches the model only through the
# `additionalContext` envelope, which is the shape remind-inflight-on-push.sh demonstrates.
# THE PREAMBLE MUST MATCH THE OUTCOME. Every non-empty report got the success wording, so a
# failure-only report told the model a file "is currently cheap to decompose" when no open
# candidate had been established at all.
export REPORT_BODY="$report"
if [ "$rc" -eq 0 ]; then
    export REPORT_PREAMBLE="A file this repo has decided to decompose is currently cheap to decompose. This is a reminder while you are between pieces of work, not a blocker on this push - and it will keep appearing until the work is done or the entry leaves bin/refactor-candidates.json."
else
    export REPORT_PREAMBLE="The refactor-window check could not answer for everything, so the state of the files this repo means to decompose is UNKNOWN rather than quiet. Not a blocker on this push; reported because a measurement that failed must not read as one that found nothing."
fi
python3 -c '
import json, os
print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
    "additionalContext": os.environ["REPORT_PREAMBLE"] + "\n\n" + os.environ["REPORT_BODY"]}}))
' 2>/dev/null || true
exit 0
