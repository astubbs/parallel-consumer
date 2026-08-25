#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# PUSH-TIME reminder of what this PR's own inflight note says is still open.
#
# WHY PUSH, given check-merge-outstanding-work.sh already guards the merge. That guard is the
# backstop: it fires at `gh pr merge`, by which point the work is mentally finished and re-opening it
# is expensive, so the honest outcome is often "acknowledge, override, merge". Push is the moment the
# agent is still IN the work, frequent enough to catch drift and rare enough not to be noise. The two
# are complements - this one informs while it can still change what gets built; that one prevents the
# merge happening in ignorance.
#
# NOT post-commit, which was the first idea and is worse than nothing: commits happen constantly and
# these notes run to dozens of lines, so it would bury the work it exists to protect and train
# everyone to scroll past it - the same failure as a check that is always red.
#
# NON-BLOCKING BY DESIGN. It emits `additionalContext`, the channel inject-merge-checklist.sh already
# uses, never a `deny`. A guard that blocks pushes would be routed around within a day, and the thing
# being surfaced is a reminder rather than a violation.
#
# THROTTLED per branch, because a push loop would otherwise repeat the whole note every time and
# teach the reader to skip it.
#
# THE PUSH DETECTION AND THE MTIME READ ARE SHARED with remind-master-drift-on-push.sh, in
# .claude/hooks/lib/hook-common.sh, which owns the reasoning for both. Each was got wrong once in a
# way that made this hook silently stop working, and a second copy hides the next such bug until
# somebody re-runs the same experiment on the same platform.
set -uo pipefail

payload="$(cat 2>/dev/null || true)"
[ -n "$payload" ] || exit 0

# Cheap bail before paying for python3 on EVERY Bash call.
case "$payload" in
    *push*) ;;
    *) exit 0 ;;
esac

# Resolved before it is sourced, and the hook stays SILENT if the helper is missing rather than
# erroring into the agent's transcript - bin/lib/node-gate.sh's header owns that reasoning for the
# gates, and a non-blocking reminder has even less business failing loudly.
hook_lib="${BASH_SOURCE[0]%/*}/lib/hook-common.sh"
[ -r "$hook_lib" ] || exit 0
# shellcheck source=.claude/hooks/lib/hook-common.sh
. "$hook_lib"

[ "$(hook_git_subcommand "$payload")" = "push" ] || exit 0

root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$root" ] || exit 0
cd "$root" 2>/dev/null || exit 0

branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
[ -n "$branch" ] && [ "$branch" != "HEAD" ] || exit 0

# THROTTLE. Same branch, same hour, one reminder.
stamp="${TMPDIR:-/tmp}/pc-push-reminder-$(printf '%s' "$branch" | tr '/' '_')"
if [ -f "$stamp" ]; then
    # Portable mtime - hook-common.sh owns why this probes the platform instead of chaining `-c`
    # into `-f`. On a branch's first push there is no stamp and this never runs.
    last="$(hook_file_mtime "$stamp")"
    # ANYTHING THAT IS NOT A TIMESTAMP MEANS REMIND, not stay silent - the safe direction for a
    # reminder, where the guards in check-merge-outstanding-work.sh and bin/check-pr-ready.sh must
    # instead assume live work. Reminding twice costs a paragraph; skipping loses the only prompt
    # there is. Testing the shape and not just emptiness matters for the same reason it does there:
    # `$(( now - last ))` on prose evaluates it as an expression and `set -u` would abort the hook.
    case "$last" in ''|*[!0-9]*) last=0 ;; esac
    now="$(date +%s)"
    [ $(( now - last )) -lt "${INFLIGHT_PUSH_REMINDER_SECONDS:-3600}" ] && exit 0
fi

pr_num="$(gh pr list --head "$branch" --json number --jq '.[0].number' 2>/dev/null || true)"
[ -n "$pr_num" ] || exit 0

note="$(find docs/inflight -maxdepth 1 -name "pr-${pr_num}-*.md" 2>/dev/null | head -1)"
[ -n "$note" ] && [ -f "$note" ] || exit 0

# Only what is above the first "Already fixed" heading - a note whose resolved section has grown must
# not bury the lines that still matter.
outstanding="$(awk '/^## Already fixed/ {exit} {print}' "$note" 2>/dev/null)"
[ -n "$outstanding" ] || exit 0

: > "$stamp" 2>/dev/null || true

export NOTE_BODY="$outstanding"
export NOTE_PATH="$note"
export PR_NUM="$pr_num"
python3 -c '
import json, os
print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
    "additionalContext": (
        "READINESS IS THE OPERATOR\u2019S CALL, NOT YOURS. Do not tell them this PR is ready, "
        "mergeable or good to go. `MERGEABLE/CLEAN` from gh is a GIT fact - it means no conflicts - "
        "and saying it in prose reaches them earlier than any guard can fire, because a hook can "
        "intercept a tool call and not a sentence. Report what is outstanding and let them decide. "
        "`bin/check-pr-ready.sh <n>` enumerates the blockers it can measure.\n\n"
        "You are pushing to astubbs/parallel-consumer#" + os.environ["PR_NUM"] + ", which has an "
        "inflight note recording what is still open on it (" + os.environ["NOTE_PATH"] + "). This is "
        "a reminder while the work is still open, not a blocker - the merge guard is the backstop. "
        "If an item below is now done, update the note in this push; if one has become someone "
        "else'"'"'s, say so there rather than leaving it to be rediscovered.\n\n"
        + os.environ["NOTE_BODY"])}}))
' 2>/dev/null || true
exit 0
