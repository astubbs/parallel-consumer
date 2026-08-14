#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# PreToolUse hook: refuse a `gh pr merge --subject` whose subject does not carry the RIGHT PR number.
#
# THE TRAP. `gh pr merge --squash` normally lands a subject ending `... (#265)` - GitHub appends the
# PR number itself. It does that ONLY when the subject is not overridden. Pass `--subject "..."` and
# your text is used verbatim, so the number silently never appears. AGENTS.md reserves that trailing
# slot for exactly this, and every neighbouring commit on master has it.
#
# It is a good candidate for a hook rather than a rule because it is invisible at the point of
# failure: the merge succeeds, the message reads fine, and the omission only shows up later next to
# its neighbours in `git log`. That is what happened on astubbs#206, and rewriting a commit already
# on master is not a fix anyone should need.
#
# WHY THE PARSER IS SHLEX AND NOT A REGEX. The first version read the FIRST `--subject` with a
# regex over the whole command line. `gh` honours the LAST one, and the whole command line includes
# text that is not part of the merge at all. Both halves of that were wrong, in both directions:
#
#   - `--subject "ok (#299)" --subject "bad"` was ALLOWED - the bad subject is the one that lands.
#   - `echo --subject "x (#1)" ; gh pr merge 299 --subject "bad"` was ALLOWED - the decoy matched.
#   - `--subject "thing (#206)"` while merging 299 was ALLOWED - any `(#N)` satisfied it, so the
#     exact astubbs#206 shape this hook is named after walked straight through.
#   - `--subject 'don'\''t drop it (#299)'` was DENIED - the escaped apostrophe truncated the
#     capture before the suffix.
#   - `--body "we discussed --subject" --subject "thing (#299)"` was DENIED - the first `--subject`
#     is inside the body text.
#
# A PreToolUse deny is hard, so the last two are the more expensive class: a false positive stops a
# legitimate merge and the agent cannot argue with it. So: slice the command at each `gh pr merge`,
# `shlex.split` that slice so quoting is handled by something that knows shell quoting, take the
# LAST `--subject` the way `gh` does, and cross-check the number against the PR being merged.
#
# FAIL OPEN, ALWAYS. Any parse failure exits 0. A hook that blocks on its own bug is worse than no
# hook: the gate below it (docs/merge-checklist.md, and review) still catches a bad subject, but
# nothing catches a hook that has jammed the tool call shut. `bin/test-check-agent-hooks.sh` is the
# negative control - it asserts each shape above, in both directions.
#
# BOUNDARY, STATED. The `(#N)` may sit anywhere in the subject, not only at the end, so
# `"port (#299) to master"` passes. Requiring the trailing slot would be truer to the convention
# but turns every unusual-but-fine subject into a hard block, and the cost of the two error
# directions is not symmetric. Likewise, a merge with no PR argument (`gh pr merge --squash`, which
# resolves the PR from the branch) cannot be cross-checked, so any `(#N)` is accepted there.
#
# PreToolUse CAN feed text back to the model - a deny reason is delivered, and so is
# `hookSpecificOutput.additionalContext`. This hook uses the deny reason. See docs/agent-harness.md.

set -euo pipefail

payload=$(cat)

python3 - "$payload" <<'PY'
import json, re, shlex, sys

try:
    tool = json.loads(sys.argv[1])
except Exception:
    sys.exit(0)                      # unparseable: never block on our own bug

if tool.get("tool_name") != "Bash":
    sys.exit(0)

cmd = tool.get("tool_input", {}).get("command", "")

MERGE = re.compile(r"\bgh\s+pr\s+merge\b")

# One command line can carry more than one `gh pr merge` (`a && b`, `a ; b`). Each is judged on its
# own slice, running to the start of the next one, so a good merge cannot vouch for a bad one.
starts = [m.start() for m in MERGE.finditer(cmd)]
if not starts:
    sys.exit(0)
bounds = list(zip(starts, starts[1:] + [len(cmd)]))


def subjects_and_pr(slice_):
    """The `--subject` values and the PR number in one `gh pr merge ...` slice.

    Raises ValueError from shlex on unbalanced quotes, which the caller turns into an allow.
    """
    tokens = shlex.split(slice_)
    values, pr = [], None
    seen_merge = False
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token == "--subject" and i + 1 < len(tokens):
            values.append(tokens[i + 1])
            i += 2
            continue
        if token.startswith("--subject="):
            values.append(token[len("--subject="):])
            i += 1
            continue
        if token == "merge":
            seen_merge = True
        elif seen_merge and pr is None and token.isdigit():
            pr = token
        i += 1
    return values, pr


for start, end in bounds:
    try:
        subjects, pr = subjects_and_pr(cmd[start:end])
    except ValueError:
        continue                     # our own parse limit, not the author's mistake - allow

    if not subjects:
        continue                     # no override, so GitHub appends the number itself - fine

    subject = subjects[-1]           # gh honours the LAST occurrence, so that is the one judged
    found = re.findall(r"\(#(\d+)\)", subject)
    pr_hint = f"(#{pr})" if pr else "(#<pr>)"

    if not found:
        reason = (
            "This --subject has no PR-number suffix, and passing --subject suppresses the "
            f"{pr_hint} that GitHub would otherwise append. "
        )
    elif pr is not None and pr not in found:
        reason = (
            f"This --subject carries (#{found[-1]}) but the merge is of PR #{pr}. A number that "
            "points at the wrong PR - or at an issue - is worse than none: it reads as correct and "
            "links a future reader somewhere else. "
        )
    else:
        continue                     # carries the right number - fine

    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": (
                reason
                + "The commit would land out of step with every neighbour on master, and it is not "
                "fixable afterwards without rewriting a pushed commit (this happened on "
                f"astubbs#206). Either end the subject with ' {pr_hint}', or drop --subject "
                "entirely and let the PR title be used - --body-file alone does not affect the "
                "subject. See docs/merge-checklist.md."
            ),
        }
    }))
    break
PY
