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
set -uo pipefail

payload="$(cat 2>/dev/null || true)"
[ -n "$payload" ] || exit 0

# Cheap bail before paying for python3 on EVERY Bash call.
case "$payload" in
    *push*) ;;
    *) exit 0 ;;
esac

# TOKENS, NOT SUBSTRINGS - the rule check-squash-subject.sh and check-merge-outstanding-work.sh both
# state. `git commit -m "ready to push"` must not fire this. git is matched by BASENAME so
# /usr/bin/git counts; an unbalanced quote makes shlex raise, and that fails open.
is_push="$(printf '%s' "$payload" | python3 -c '
import json, shlex, sys
try:
    data = json.load(sys.stdin)
    cmd = data.get("tool_input", {}).get("command", "")
    toks = shlex.split(cmd)
except Exception:
    sys.exit(0)
for i, t in enumerate(toks):
    if t.rsplit("/", 1)[-1] == "git":
        # NO APOSTROPHES ANYWHERE IN THIS BLOCK: it lives inside a single-quoted shell string, so
        # one quote ends the string and bash then parses Python as shell. That is how this comment
        # was first written, and the whole hook died with a syntax error on line 53.
        #
        # Global git flags take a SEPARATE value token, and dropping only the flag leaves the value
        # where the subcommand should be: `git -C /path push` put "/path" at rest[0], so the reminder
        # never fired for the form an agent is most likely to use. It was silently dead for most
        # pushes in the very session that wrote it. Consume each value with its flag, the way
        # skip_repo_flags in the sibling hook does for -R and --repo.
        VALUE_FLAGS = ("-C", "-c", "--git-dir", "--work-tree", "--namespace", "--exec-path")
        j, rest = i + 1, []
        while j < len(toks):
            t = toks[j]
            if t in VALUE_FLAGS:
                j += 2; continue
            if t.startswith("-"):
                j += 1; continue
            rest.append(t); break
        if rest and rest[0] == "push":
            print("push"); break
' 2>/dev/null || true)"
[ "$is_push" = "push" ] || exit 0

root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$root" ] || exit 0
cd "$root" 2>/dev/null || exit 0

branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
[ -n "$branch" ] && [ "$branch" != "HEAD" ] || exit 0

# THROTTLE. Same branch, same hour, one reminder.
stamp="${TMPDIR:-/tmp}/pc-push-reminder-$(printf '%s' "$branch" | tr '/' '_')"
if [ -f "$stamp" ]; then
    # PORTABLE MTIME, read where it is used - on a branch's first push there is no stamp and this
    # never runs. `stat -c %Y` is GNU; BSD/macOS stat rejects `-c` and returned nothing while still
    # exiting 0, so the throttle silently read "no mtime". Probe the platform rather than falling
    # back: on Linux `stat -f` is --file-system and SUCCEEDS with a number about the FILESYSTEM, so a
    # blind `-c || -f` would hand back a wrong answer instead of no answer.
    if stat -c %Y . >/dev/null 2>&1; then
        last="$(stat -c %Y "$stamp" 2>/dev/null)"   # GNU coreutils
    else
        last="$(stat -f %m "$stamp" 2>/dev/null)"   # BSD / macOS
    fi
    # An unreadable stamp reminds rather than staying silent - the safe direction for a reminder,
    # where the guards in check-merge-outstanding-work.sh and bin/check-pr-ready.sh must instead
    # assume live work. Reminding twice costs a paragraph; skipping loses the only prompt there is.
    now="$(date +%s)"
    [ $(( now - ${last:-0} )) -lt "${INFLIGHT_PUSH_REMINDER_SECONDS:-3600}" ] && exit 0
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
