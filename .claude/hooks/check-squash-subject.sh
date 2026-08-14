#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# PreToolUse hook: refuse `--subject` on `gh pr merge`.
#
# `gh pr merge --squash` lands a subject ending `... (#265)` because GitHub appends the PR number -
# but only when the subject is NOT overridden. Pass `--subject` and your text is used verbatim, the
# number silently never appears, and the commit lands out of step with every neighbour on master.
# That happened on astubbs#206 and needed a force-push to master to correct.
#
# So the rule is DON'T OVERRIDE THE SUBJECT, not "override it correctly". If the PR title is wrong,
# fix the PR title - it is what reviewers saw, and AGENTS.md already asks for it to be kept in step.
# `--body-file` alone does not touch the subject, so a hand-written message still works.
#
# The first version tried to police correct use: parse the flag, take the last one the way gh does,
# cross-check the number against the PR being merged. It was 343 lines with a 400-line test suite,
# and review found it wrong in both directions - allowing a wrong PR number through, and denying a
# legitimate subject containing an escaped apostrophe. Refusing the flag needs no parser, cannot be
# fooled by quoting, and has no false-positive class to defend against.
#
# Fails open on anything it cannot parse: a hook that blocks on its own bug is worse than no hook.

set -euo pipefail

payload=$(cat)

python3 - "$payload" <<'PY'
import json, re, sys

try:
    tool = json.loads(sys.argv[1])
except Exception:
    sys.exit(0)                      # unparseable: never block on our own bug

if tool.get("tool_name") != "Bash":
    sys.exit(0)

cmd = tool.get("tool_input", {}).get("command", "")
if not re.search(r"\bgh\s+pr\s+merge\b", cmd) or not re.search(r"--subject\b", cmd):
    sys.exit(0)

print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "permissionDecision": "deny",
        "permissionDecisionReason": (
            "Don't pass --subject to gh pr merge. It suppresses the (#N) GitHub would otherwise "
            "append, so the commit lands without its PR number and out of step with every "
            "neighbour on master - and that is not fixable afterwards without rewriting a pushed "
            "commit (astubbs#206). Drop the flag and the PR title is used. If the title is wrong, "
            "fix the title: it is what reviewers saw. --body-file alone does not affect the "
            "subject. See docs/merge-checklist.md."
        ),
    }
}))
PY
