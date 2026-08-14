#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# PreToolUse hook: run `.githooks/pre-commit` before the agent's `git commit`, and honour
# `--no-verify` the way git itself does.
#
# WHY THIS EXISTS AT ALL. `core.hooksPath` cannot be committed, so a fresh clone has no git hooks
# until someone runs the config command once. This covers Claude Code in that window. It is
# belt-and-braces, not the primary mechanism - the git hook binds every process that runs `git`,
# including humans and other agents; this binds one tool.
#
# WHY IT IS A SCRIPT AND NOT `... || exit 2` INLINE. The inline form never reads the hook payload,
# so it could not see the command it was gating - which meant `git commit --no-verify` ran the
# gates anyway and blocked. That directly contradicts the pre-commit hook's own header: "a gate
# people cannot skip when they have a reason is a gate they disable permanently". An agent facing a
# red gate it cannot bypass has exactly one move left, which is to stop working; a human in that
# spot deletes the hook. The escape hatch is the thing that keeps the gate installed.
#
# WHY EXIT 2 AND NOT A JSON DENY. Exit 2 is PreToolUse's documented block, and it forwards stderr to
# the model - so the failing gate's own output becomes the explanation. A bare `exit 2` with nothing
# on stderr produces "hook error: No stderr output", which tells the agent it was blocked and
# nothing about why; that was the observed behaviour of the inline form.
#
# FAIL OPEN ON OUR OWN BUG. If the payload does not parse, or the gate script is missing, this exits
# 0. The git hook and CI both still gate the same commit.
#
# Negative control: bin/test-check-agent-hooks.sh.

set -euo pipefail

payload=$(cat)

project_dir="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
gate="$project_dir/.githooks/pre-commit"
[ -x "$gate" ] || exit 0

# Does this command carry a real `--no-verify` argument? `shlex` so that a commit MESSAGE mentioning
# the flag (`git commit -m "document --no-verify"`) is not mistaken for the flag itself; a word-
# boundary search only as the fallback when shlex cannot parse the line, because refusing to decide
# would mean gating a commit the author explicitly asked not to gate.
#
# Only the long spelling counts. `git commit -n` means the same thing to git, but `-n` is a common
# token in a command line that merely CONTAINS a commit (`echo -n`, an unquoted `$(...)`), and a
# bypass triggered by accident is a gate that silently stopped running. The long form is what the
# hook headers and docs tell people to type, and it is unambiguous.
if python3 - "$payload" <<'PY'
import json, re, shlex, sys
try:
    cmd = json.loads(sys.argv[1]).get("tool_input", {}).get("command", "")
except Exception:
    sys.exit(0)                      # unparseable payload: treat as bypass, never block on our bug
try:
    bypass = "--no-verify" in shlex.split(cmd)
except ValueError:
    bypass = re.search(r"(?<!\S)--no-verify(?!\S)", cmd) is not None
sys.exit(0 if bypass else 1)
PY
then
    exit 0
fi

if ! output=$("$gate" 2>&1); then
    printf '%s\n' "$output" >&2
    printf '\nBlocked by the repo pre-commit gate (.githooks/pre-commit). Fix the gate(s) above, or\n' >&2
    printf 'commit with --no-verify if you have a reason - the bypass is deliberate, not an oversight.\n' >&2
    exit 2
fi

exit 0
