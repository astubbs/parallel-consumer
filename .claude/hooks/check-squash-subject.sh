#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# PreToolUse hook: refuse a `gh pr merge --subject` whose subject does not carry the PR number.
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
# PRECISE, NOT NAGGY. It fires only when `--subject` is present AND the subject lacks a `(#N)`
# suffix - i.e. only when the mistake is actually being made. A subject that already carries the
# number passes silently, and so does a merge that does not override the subject at all.
#
# PreToolUse cannot inject context (stdout never reaches the model), so this denies with a reason,
# which IS fed back - see docs/agent-harness.md for what each layer can and cannot do.

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

# Only a merge that overrides the subject can hit the trap.
if not re.search(r"\bgh\s+pr\s+merge\b", cmd) or not re.search(r"--subject\b", cmd):
    sys.exit(0)

# Pull the subject argument, tolerating single quotes, double quotes or a bare token.
m = re.search(r"--subject(?:=|\s+)(\"(?:[^\"\\]|\\.)*\"|'[^']*'|\S+)", cmd)
subject = m.group(1) if m else ""
if subject[:1] in ("\"", "'"):
    subject = subject[1:-1]

if re.search(r"\(#\d+\)", subject):
    sys.exit(0)                      # already carries the number - fine

pr = re.search(r"\bgh\s+pr\s+merge\s+(\d+)", cmd)
pr_hint = f"(#{pr.group(1)})" if pr else "(#<pr>)"

print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "permissionDecision": "deny",
        "permissionDecisionReason": (
            "This --subject has no PR-number suffix, and passing --subject suppresses the "
            f"{pr_hint} that GitHub would otherwise append. The commit would land out of step with "
            "every neighbour on master, and it is not fixable afterwards without rewriting a "
            "pushed commit (this happened on astubbs#206). Either append "
            f"' {pr_hint}' to the subject, or drop --subject entirely and let the PR title be used "
            "- --body-file alone does not affect the subject. See docs/merge-checklist.md."
        ),
    }
}))
PY
