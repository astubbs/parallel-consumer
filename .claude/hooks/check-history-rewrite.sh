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
# review running right now. With nothing found it still stops, but says so honestly.
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

# WHAT WOULD ACTUALLY BE LOST. Best-effort: no PR, no gh, or no network all fall through to the
# generic refusal rather than letting the rewrite past - the point is the pause, the detail is a bonus.
detail=""
if [ -n "$branch" ] && [ "$branch" != "HEAD" ]; then
    pr="$(gh pr list --head "$branch" --json number --jq '.[0].number' 2>/dev/null || true)"
    if [ -n "$pr" ]; then
        threads="$(gh api "repos/{owner}/{repo}/pulls/${pr}/comments" --jq 'length' 2>/dev/null || echo "")"
        running="$(gh run list --branch "$branch" --json status,name \
                     --jq '[.[] | select(.status=="in_progress" or .status=="queued")] | length' 2>/dev/null || echo "")"
        detail="This branch is astubbs/parallel-consumer#${pr}."
        found=""
        [ -n "$threads" ] && [ "$threads" != "0" ] && { found=1; detail="$detail It has ${threads} inline review comment(s), which a force-push re-anchors or orphans."; }
        [ -n "$running" ] && [ "$running" != "0" ] && { found=1; detail="$detail ${running} check/review run(s) are IN PROGRESS against the current head - their findings would land on a SHA that no longer exists."; }
        # NOTHING FOUND IS NOT PERMISSION, and must not READ as permission. A quiet PR is the most
        # dangerous message this hook can send: a reviewer who has read the diff and not yet commented
        # has exactly zero threads and zero running jobs, and loses the most from a rewrite. What is
        # measurable here is a lower bound on the damage, never the absence of it.
        [ -z "$found" ] && detail="$detail No open review comments and no runs in progress were found - which is NOT evidence that a rewrite is safe. A reviewer part-way through the diff, with nothing posted yet, looks exactly like this and loses the most. Only the operator saying now is evidence."
    fi
fi
[ -n "$detail" ] || detail="No PR was found for this branch, so nothing could be measured - which is not the same as nothing being at risk."

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
