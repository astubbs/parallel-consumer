#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# REFUSES A HISTORY REWRITE WHILE A REVIEW IS IN FLIGHT.
#
# A force-push re-anchors every inline review comment and destroys the incremental diff a reviewer
# works from - so it throws away the REVIEWER's effort, not git's. While a review is running it is
# worse: findings land against a SHA that no longer exists and threads are orphaned before anyone
# reads them.
#
# WHY A HOOK AND NOT A RULE. docs/merge-checklist.md already says re-cut "right at the end before
# merging". That did not stop it happening twice in one session - once losing the diff on a PR under
# review, once starting a re-cut with three reviews mid-flight. A rule fires when someone remembers
# it; this fires whether or not anyone does.
#
# IT CHECKS RATHER THAN ASKS. A guard that always asks "are you sure?" becomes noise and gets waved
# through reflexively, which is worse than no guard because it manufactures the habit of overriding.
# This one looks up the branch's PR and names what would actually be lost - open review threads, a
# review running right now. With nothing found it still stops, but says so honestly - and says WHICH
# nothing it found, because "this branch has no PR" and "the lookup never answered" are different
# facts that one message used to report identically.
set -uo pipefail

payload="$(cat 2>/dev/null || true)"
[ -n "$payload" ] || exit 0

# Cheap bail before paying for python3 on every Bash call.
case "$payload" in
    # `-f` matched with a LEADING space only: `git push -f` has nothing after it, and requiring a
    # trailing space let exactly that through - proven by the self-test below. Widening costs a
    # python3 call on commands like `grep -f`, which the token scan then rejects; the pre-filter may
    # only ever skip work, never decide.
    *rebase*|*--force*|*" -f"*|*--amend*|*filter-branch*|*filter-repo*|*reset*|*update-ref*|*" -B"*|*" -C"*|*--delete*|*" :"*) ;;
    *) exit 0 ;;
esac

[ "${REWRITE_HISTORY_CONFIRMED:-}" = "1" ] && exit 0

# TOKENS, NOT SUBSTRINGS - the rule the sibling hooks state. `git commit -m "rebase notes"` and
# `gh pr comment --body "we should force-push"` must not fire. An unbalanced quote makes shlex raise,
# and that fails open.
verdict="$(printf '%s' "$payload" | python3 -c '
import json, shlex, sys
try:
    data = json.load(sys.stdin)
    cmd = data.get("tool_input", {}).get("command", "")
    toks = shlex.split(cmd)
except Exception:
    sys.exit(0)

FORCE = {"--force", "-f", "--force-with-lease", "--force-if-includes"}
for i, t in enumerate(toks):
    if t.rsplit("/", 1)[-1] != "git":
        continue
    rest = toks[i+1:]
    sub = next((x for x in rest if not x.startswith("-")), None)
    flags = set(rest)
    # An env-prefixed override reaches here as a token, not as process env.
    if any(x == "REWRITE_HISTORY_CONFIRMED=1" for x in toks):
        sys.exit(0)
    if sub == "push" and (flags & FORCE or any(x.startswith("--force-with-lease=") for x in rest)):
        print("force-push"); break
    if sub == "rebase" and "--abort" not in flags and "--continue" not in flags and "--skip" not in flags:
        print("rebase"); break
    if sub == "commit" and "--amend" in flags:
        print("amend"); break
    if sub in ("filter-branch", "filter-repo"):
        print(sub); break
    # EVERY OTHER WAY TO MOVE A REF AND LOSE COMMITS. Found by probing the first version, which
    # caught only the obvious four - a guard that reaches just the shapes you thought of is a
    # documented bypass.
    if sub == "reset":
        # Forward sync to a remote ref is routine. Going BACKWARDS - HEAD~n, a bare SHA - drops
        # commits and needs a force-push afterwards, which is the thing being guarded.
        tgt = next((x for x in rest[rest.index("reset")+1:] if not x.startswith("-")), None)
        if tgt and not tgt.startswith("origin/") and not tgt.startswith("upstream/"):
            import re as _re
            if _re.search(r"[~^]", tgt) or _re.fullmatch(r"[0-9a-f]{7,40}", tgt):
                print("reset-backwards"); break
    if sub == "branch" and "-f" in flags or (sub == "branch" and "--force" in flags):
        print("branch -f"); break
    if sub in ("checkout", "switch"):
        moved = "-B" in flags or "-C" in flags
        # `-B name` alone just points at HEAD; `-B name <start>` moves the ref somewhere else.
        if moved:
            after = rest[rest.index("-B") + 1:] if "-B" in rest else rest[rest.index("-C") + 1:]
            if len([x for x in after if not x.startswith("-")]) >= 2:
                print("branch reset via " + sub); break
    if sub == "update-ref" and any(x.startswith("refs/heads/") for x in rest):
        print("update-ref"); break
    if sub == "push" and ("--delete" in flags or "-d" in flags or any(x.startswith(":") and len(x) > 1 for x in rest)):
        print("remote branch deletion"); break
' 2>/dev/null || true)"
[ -n "$verdict" ] || exit 0

root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$root" ] || exit 0
cd "$root" 2>/dev/null || exit 0
branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"

# WHAT WOULD ACTUALLY BE LOST. Best-effort: a missing PR, a missing gh or a dead network all fall
# through to the refusal rather than letting the rewrite past - the point is the pause, the detail is
# a bonus. But WHICH of those happened is not a detail, it is the difference between "there is
# nothing here to lose" and "I could not look", and the first version printed one sentence for both.
#
# THREE ANSWERS, NEVER ONE. `2>/dev/null || true` on the lookup discarded gh's exit status and its
# stderr together, so "this branch has no PR", "gh is not installed / not authenticated /
# rate-limited" and "the lookup answered for the WRONG REPOSITORY" all rendered as "No PR was found
# for this branch". Observed twice in one day on astubbs/parallel-consumer#356, from two different
# causes, with the operator told the same thing each time.
#
# THE REPO IS DERIVED FROM `origin`, NOT LEFT TO gh AND NOT HARDCODED - the same reasoning
# .claude/hooks/inject-branch-context.sh states at "THE REPO IS DERIVED FROM `origin`": a bare `gh`
# in this fork resolves to confluentinc/parallel-consumer, because gh prefers the `upstream` remote
# and the fix (`gh repo set-default`) writes `remote.origin.gh-resolved` into a LOCAL, uncommitted
# config that a CI runner or a fresh sandbox does not have. Hardcoding the slug would be wrong the
# moment someone works in their own fork. When `origin` cannot be read the lookup is NOT retried
# unqualified: a wrong answer that resolves is worse than no answer.
#
# BOUNDED, and bounded in python3 rather than with `timeout(1)`, which is GNU-only and absent on
# macOS - the portability rule this directory already follows. An unbounded lookup would hang the
# tool call it is guarding.
detail=""
if ! command -v python3 >/dev/null 2>&1; then
    detail="This hook could not look the branch up: python3 is not available, so nothing could be measured - which is not the same as nothing being at risk."
elif [ -z "$branch" ] || [ "$branch" = "HEAD" ]; then
    detail="HEAD is detached here, so there is no branch to look a pull request up by and nothing could be measured - which is not the same as nothing being at risk."
else
    detail="$(python3 - "$branch" <<'PY'
import re
import subprocess
import sys

BRANCH = sys.argv[1] if len(sys.argv) > 1 else ""
GH_SECONDS = 10
GIT_SECONDS = 5


def run(args, secs):
    """(stdout, problem). stdout is None whenever the command did not answer."""
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


def origin_slug():
    url, problem = run(["git", "remote", "get-url", "origin"], GIT_SECONDS)
    if url is None:
        return None, "the `origin` remote could not be read - %s" % problem
    # A HOSTED REMOTE, not any path and not any scheme: a clone whose origin is a local directory
    # otherwise yields a slug built from the last two path segments, and gh is then asked about a
    # repository that does not exist. inject-branch-context.sh carries the worked case, including
    # why `file://` has to be excluded by allowlisting the schemes rather than requiring one.
    hosted = re.match(r"^(?:https?|ssh|git)://", url) or re.match(r"^[^/]+@[^/:]+:", url)
    m = re.search(r"[:/]([^/:]+)/([^/]+?)(?:\.git)?/?$", url) if hosted else None
    if not m:
        return None, "`origin` is %s, which is not a hosted remote URL, so there is no repository to ask about" % (
            ("`%s`" % url) if url else "unset")
    return "%s/%s" % (m.group(1), m.group(2)), None


def emit(text):
    print(text)
    sys.exit(0)


try:
    slug, slug_problem = origin_slug()
    if slug is None:
        emit("The pull-request lookup DID NOT RUN: %s. It was not retried without `-R`, because gh "
             "prefers the `upstream` remote in this fork and an unqualified lookup answers for "
             "confluentinc/parallel-consumer instead - a wrong answer that resolves is worse than "
             "none. Nothing could be measured, which is NOT the same as nothing being at risk." % slug_problem)

    number, problem = run(["gh", "pr", "list", "-R", slug, "--head", BRANCH,
                           "--json", "number", "--jq", ".[0].number"], GH_SECONDS)
    if number is None:
        emit("The pull-request lookup FAILED against %s: %s. Nothing could be measured, and a "
             "lookup that never answered is NOT evidence that this branch has no PR - a rewrite "
             "here could still be destroying review work." % (slug, problem))
    if not number.isdigit():
        if number:
            emit("The pull-request lookup against %s answered `%s`, which is not a PR number, so "
                 "nothing could be measured - which is not the same as nothing being at risk."
                 % (slug, number[:80]))
        # gh exits 0 and prints nothing for a head branch with no open PR, so this - and only this -
        # is a measured absence. Say which repository was asked, or the reader cannot tell this
        # apart from the wrong-repo answer that used to be possible here.
        emit("The lookup ran against %s and came back empty: no open pull request has `%s` as its "
             "head branch. So nothing could be measured, which is not the same as nothing being at "
             "risk - an unpushed branch still carries commits a rewrite drops." % (slug, BRANCH))

    threads, threads_problem = run(["gh", "api", "repos/%s/pulls/%s/comments" % (slug, number),
                                    "--jq", "length"], GH_SECONDS)
    runs, runs_problem = run(["gh", "run", "list", "-R", slug, "--branch", BRANCH,
                              "--json", "status,name", "--jq",
                              '[.[] | select(.status=="in_progress" or .status=="queued")] | length'],
                             GH_SECONDS)

    parts = ["This branch is %s#%s." % (slug, number)]
    found = False
    unmeasured = []
    if threads_problem:
        unmeasured.append("its inline review comments could not be counted (%s)" % threads_problem)
    elif threads not in ("", "0"):
        found = True
        parts.append("It has %s inline review comment(s), which a force-push re-anchors or orphans." % threads)
    if runs_problem:
        unmeasured.append("its runs in progress could not be counted (%s)" % runs_problem)
    elif runs not in ("", "0"):
        found = True
        parts.append("%s check/review run(s) are IN PROGRESS against the current head - their "
                     "findings would land on a SHA that no longer exists." % runs)

    # NOTHING FOUND IS NOT PERMISSION, and must not READ as permission. A quiet PR is the most
    # dangerous message this hook can send: a reviewer who has read the diff and not yet commented
    # has exactly zero threads and zero running jobs, and loses the most from a rewrite. What is
    # measurable here is a lower bound on the damage, never the absence of it. And a count that
    # FAILED is not a count of zero - saying "none were found" for a request that never answered is
    # the same defect one level down.
    if unmeasured:
        parts.append("However %s - so what a rewrite would cost here is UNMEASURED, not absent."
                     % " and ".join(unmeasured))
    elif not found:
        parts.append("No open review comments and no runs in progress were found - which is NOT "
                     "evidence that a rewrite is safe. A reviewer part-way through the diff, with "
                     "nothing posted yet, looks exactly like this and loses the most. Only the "
                     "operator saying now is evidence.")
    emit(" ".join(parts))
except Exception as exc:
    print("The pull-request lookup could not be completed (%s), so nothing could be measured - "
          "which is not the same as nothing being at risk." % exc.__class__.__name__)
PY
)"
fi
[ -n "$detail" ] || detail="The pull-request lookup produced no answer at all, so nothing could be measured - which is not the same as nothing being at risk."

export VERDICT="$verdict"
export DETAIL="$detail"
python3 -c '
import json, os
print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
    "permissionDecision": "deny",
    "permissionDecisionReason": (
        "This is a history rewrite (" + os.environ["VERDICT"] + "). " + os.environ["DETAIL"] +
        "\n\nRe-cutting, rebasing and force-pushing are the LAST step before a merge - after the "
        "reviews are in and their fixes are made, never while one is running. Ask the operator "
        "whether now is the right time, and say what would be lost.\n\n"
        "If a merge cannot do the job - removing a commit from the ancestry of a branch, e.g. detaching a "
        "stacked PR - say so and re-run prefixed with REWRITE_HISTORY_CONFIRMED=1. Updating a moved "
        "base is a MERGE. Removing content from a branch is an ordinary revert commit. "
        "See docs/merge-checklist.md.")}}))
'
exit 0
