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
    # exiting 0, so the throttle silently read "no mtime". PROBE the platform once rather than
    # falling back arm to arm: on GNU, `stat -f %m FILE` exits 1 while PRINTING filesystem prose to
    # stdout, so a blind `-c || -f` returns a string, not a number.
    if stat -c %Y . >/dev/null 2>&1; then
        last="$(stat -c %Y "$stamp" 2>/dev/null)"   # GNU coreutils
    else
        last="$(stat -f %m "$stamp" 2>/dev/null)"   # BSD / macOS
    fi
    # ANYTHING THAT IS NOT A TIMESTAMP MEANS REMIND, not stay silent - the safe direction for a
    # reminder, where the guards in check-merge-outstanding-work.sh and bin/check-pr-ready.sh must
    # instead assume live work. Reminding twice costs a paragraph; skipping loses the only prompt
    # there is. Testing the shape and not just emptiness matters for the same reason it does there:
    # `$(( now - last ))` on prose evaluates it as an expression and `set -u` would abort the hook.
    case "$last" in ''|*[!0-9]*) last=0 ;; esac
    now="$(date +%s)"
    [ $(( now - last )) -lt "${INFLIGHT_PUSH_REMINDER_SECONDS:-3600}" ] && exit 0
fi

# WHICH REPOSITORY, AND WHETHER THE ANSWER IS AN ANSWER. `gh pr list --head "$branch" 2>/dev/null
# || true` got both of these wrong at once, and silently: it left the repository to gh, which in
# this fork prefers `upstream` and answers for confluentinc/parallel-consumer - a PR number from
# THERE would have been matched against `docs/inflight/pr-<n>-*.md` here and quoted a completely
# unrelated note - and it discarded gh's exit status, so an unauthenticated or rate-limited lookup
# was indistinguishable from a branch with no PR. Both render as silence, and silence from a
# reminder is exactly what nobody notices.
#
# The slug is derived from `origin`, never hardcoded and never left to gh; the lookup is bounded in
# python3 rather than by `timeout(1)`, which is GNU-only. `.claude/hooks/check-history-rewrite.sh`
# states the full reasoning in this tree, and `.claude/hooks/inject-branch-context.sh` states it at
# "THE REPO IS DERIVED FROM `origin`" - but that file arrives with astubbs#350 and is not here yet,
# so grep it on that branch. This is the same lookup in its smallest form.
command -v python3 >/dev/null 2>&1 || exit 0
lookup="$(python3 - "$branch" <<'PY'
import re
import subprocess
import sys

BRANCH = sys.argv[1] if len(sys.argv) > 1 else ""


def run(args, secs):
    try:
        p = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=secs)
    except FileNotFoundError:
        return None, "`%s` is not on PATH" % args[0]
    except subprocess.TimeoutExpired:
        return None, "`%s` did not answer within %ds" % (args[0], secs)
    except Exception as exc:
        return None, "`%s` could not be run (%s)" % (args[0], exc.__class__.__name__)
    if p.returncode != 0:
        why = " ".join(p.stderr.decode("utf-8", "replace").split())
        return None, (why[:200] or "`%s` exited %d without saying why" % (args[0], p.returncode))
    return p.stdout.decode("utf-8", "replace").strip(), None


try:
    url, problem = run(["git", "remote", "get-url", "origin"], 5)
    hosted = url and (re.match(r"^(?:https?|ssh|git)://", url) or re.match(r"^[^/]+@[^/:]+:", url))
    m = re.search(r"[:/]([^/:]+)/([^/]+?)(?:\.git)?/?$", url) if hosted else None
    if m is None:
        print("failed\tthe repository could not be derived from the `origin` remote (%s), and the "
              "lookup was not retried without `-R`, which in this fork answers for "
              "confluentinc/parallel-consumer" % (problem or "it is not a hosted remote URL"))
        sys.exit(0)
    slug = "%s/%s" % (m.group(1), m.group(2))
    number, problem = run(["gh", "pr", "list", "-R", slug, "--head", BRANCH,
                           "--json", "number", "--jq", ".[0].number"], 10)
    if number is None:
        print("failed\tthe PR lookup against %s failed - %s" % (slug, problem))
    elif number.isdigit():
        print("found\t%s" % number)
    else:
        # gh exits 0 printing nothing when no open PR has this head branch. A measured absence, and
        # the only case in which saying nothing is honest.
        print("none\t")
except Exception as exc:
    print("failed\tthe PR lookup could not be completed (%s)" % exc.__class__.__name__)
PY
)"
lookup_status="${lookup%%$'\t'*}"
lookup_info="${lookup#*$'\t'}"

# AN ANSWER THAT IS NONE OF THE THREE IS A FAILURE, not a fourth quiet way of saying "no PR". Every
# path the block above can reach prints `found`, `failed` or `none`, so anything else means the
# interpreter never got to print - killed for memory, or a BaseException its `except Exception`
# cannot catch. Without this arm that empty string matched neither test below and fell through to
# the same silent exit as a measured absence, which is the defect this hook was just fixed for,
# arriving one level down. check-history-rewrite.sh already had the equivalent backstop.
case "$lookup_status" in
    found|failed|none) ;;
    *) lookup_status="failed"
       lookup_info="the lookup returned no recognizable answer - whatever ran it did not print one" ;;
esac

# A LOOKUP THAT COULD NOT RUN IS NOT "NO PR". Staying silent here would report the same nothing as a
# branch with no PR, on a hook whose entire output is a reminder - so the failure is said out loud,
# once, under the same throttle. Still `additionalContext`, still never a deny.
if [ "$lookup_status" = "failed" ]; then
    : > "$stamp" 2>/dev/null || true
    export LOOKUP_PROBLEM="$lookup_info"
    python3 -c '
import json, os
print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
    "additionalContext": (
        "This branch may have an inflight note recording what is still open on it, and this hook "
        "could not find out: " + os.environ["LOOKUP_PROBLEM"] + ". That is not the same as there "
        "being nothing outstanding - it is no answer at all. If you are pushing to a PR, read "
        "docs/inflight/pr-<number>-*.md yourself before you treat this push as routine.")}}))
' 2>/dev/null || true
    exit 0
fi
[ "$lookup_status" = "found" ] || exit 0
pr_num="$lookup_info"
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
