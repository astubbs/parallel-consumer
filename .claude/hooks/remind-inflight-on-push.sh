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

# ONE tokeniser spawn answers both of this hook's questions - "is this a push?" here, and "which
# branch does it name?" below. `hook_git_runs "$payload" push` would pay python3 a second time to
# walk the same token list, the economy hook-common.sh's `hook_git_runs_any` records. A push
# invocation is a line reading `push` or `push<TAB>args...`.
invocations="$(hook_git_invocations "$payload")"
case "$invocations" in push|push$'\t'*|*$'\n'push|*$'\n'push$'\t'*) ;; *) exit 0 ;; esac

root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$root" ] || exit 0
cd "$root" 2>/dev/null || exit 0

# WHICH BRANCH IS BEING PUSHED - the command's refspec first, this directory's HEAD only as a
# fallback. A hook does not run in the directory its guarded command runs in, and this repository
# keeps many worktrees checked out at once, so HEAD alone answers about whichever branch the SESSION
# sits on: `git push origin other-branch` from here would look up THIS branch's PR and quote
# `docs/inflight/pr-<n>-*.md` for work the push does not touch. Observed on 2026-08-31 in the sibling
# guard, twice, on two different branches - .claude/hooks/check-history-rewrite.sh records both under
# "WHICH BRANCH". `hook_push_head_ref` owns the refspec rules.
inferred_branch=0
branch="$(hook_push_head_ref "$invocations")"
if [ -z "$branch" ]; then
    inferred_branch=1
    branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
fi
[ -n "$branch" ] && [ "$branch" != "HEAD" ] || exit 0

# THROTTLE. Same branch, same hour, one reminder.
stamp="$(hook_stamp_path pc-push-reminder "$branch")"
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
# "THE REPO IS DERIVED FROM `origin`". This is the same lookup in its smallest form.
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
# SAY WHEN THE BRANCH WAS A GUESS. A bare `git push` names no refspec, so the branch came from this
# directory's HEAD - and "You are pushing to astubbs/parallel-consumer#N" is then a claim the hook
# cannot support. The reminder is still worth making; presenting it as a fact is not.
if [ "$inferred_branch" = 1 ]; then
    export BRANCH_CAVEAT="The command names no branch, so this was looked up for \`$branch\`, the current HEAD of $root. If you are pushing something else, ignore all of this and read that branch's own note. "
else
    export BRANCH_CAVEAT=""
fi
python3 -c '
import json, os
print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
    "additionalContext": (
        os.environ["BRANCH_CAVEAT"] +
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
