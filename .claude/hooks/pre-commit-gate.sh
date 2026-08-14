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
# WHICH REPOSITORY IT GATES - THE SESSION'S, NOT THE COMMAND'S. Both the gate path below and the
# gate's own `git rev-parse --show-toplevel` resolve from this hook process, so what gets checked is
# the checkout the SESSION is rooted in. Raised in review as a worktree hazard: a session in the
# primary checkout running `cd .claude/worktrees/task && git commit` would be gated against the
# wrong tree. It cannot reach here - `if: Bash(git commit *)` matches the command as WRITTEN, so
# that command does not fire this hook at all (docs/agent-harness.md, "Known gaps"). Every command
# that does fire it is a bare `git commit ...` in the session's own cwd, where session repo and
# commit repo are the same one. The residual case - a `cd`'d or `git -C` commit into another
# worktree - is covered by `.githooks/pre-commit`, which git runs inside the target repository. That
# is why the git hook is the primary mechanism and this is belt-and-braces.
#
# FAIL OPEN ON OUR OWN BUG. If the payload does not parse, or the gate script is missing, this exits
# 0. The git hook and CI both still gate the same commit.
#
# Negative control: bin/test-check-agent-hooks.sh.

set -euo pipefail

# THE PAYLOAD ARRIVES BY FILE, NOT BY ARGV. Linux caps a single argv string at ~128 KiB
# (MAX_ARG_STRLEN), and a hook payload carries the whole prompt or command - a pasted diff or log
# clears that easily. Passing it as an argument then fails with "Argument list too long" BEFORE
# python starts, and since these hooks are built to fail open, the failure is silent: the hook
# simply stops doing its job on exactly the large inputs a human is most likely to be mid-decision
# on. A temp file has no such limit. mktemp is 0600 and the trap removes it.
payload_file=$(mktemp)
trap 'rm -f "$payload_file"' EXIT
cat > "$payload_file"

project_dir="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
gate="$project_dir/.githooks/pre-commit"
[ -x "$gate" ] || exit 0

# NO PYTHON, NO GATE. The bypass below cannot be detected without parsing the payload, and the
# repo's documented build requirements are JDK 17, Docker and the Maven wrapper - python is not
# among them. Falling through on a missing interpreter meant `git commit --no-verify` ran the gates
# and was blocked at exit 2 with no way to argue: the escape hatch the header calls load-bearing,
# absent on exactly the machine that has no other way out. Fail open, like every other limit here;
# `.githooks/pre-commit` and CI still gate the same commit.
command -v python3 >/dev/null 2>&1 || exit 0

# Does THIS COMMIT carry a real `--no-verify` argument? Three things are load-bearing:
#
#   - `shlex`, so a commit MESSAGE mentioning the flag (`git commit -m "document --no-verify"`) is
#     not mistaken for the flag itself;
#   - the search is scoped to the `git commit` command, not the whole payload. It used to scan the
#     entire line, so `git commit -m x && echo --no-verify` bypassed a red gate for a commit that
#     never asked - the violation lands, and the later command is what "requested" it;
#   - a word-boundary search over the line only as the fallback when the line cannot be lexed,
#     because refusing to decide would mean gating a commit the author explicitly asked not to gate.
#
# Only the long spelling counts. `git commit -n` means the same thing to git, but `-n` is a common
# token in a command line that merely CONTAINS a commit (`echo -n`, an unquoted `$(...)`), and a
# bypass triggered by accident is a gate that silently stopped running. The long form is what the
# hook headers and docs tell people to type, and it is unambiguous.
if python3 - "$payload_file" <<'PYGATE'
import json, re, shlex, sys

OPERATORS = {"&&", "||", ";", ";;", "|", "&", "(", ")"}
ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
GIT_VALUE_FLAGS = {"-C", "-c", "--git-dir", "--work-tree"}

try:
    with open(sys.argv[1], encoding="utf-8") as fh:
        cmd = json.load(fh).get("tool_input", {}).get("command", "")
except Exception:
    sys.exit(0)                      # unparseable payload: treat as bypass, never block on our bug


def commit_requests_bypass(line):
    """True when a `git commit` in COMMAND POSITION on this line carries --no-verify itself."""
    lexer = shlex.shlex(line, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    tokens = list(lexer)
    i, at_command = 0, True
    while i < len(tokens):
        token = tokens[i]
        if token in OPERATORS:
            at_command = True
            i += 1
            continue
        if at_command and ASSIGNMENT.match(token):
            i += 1
            continue
        if at_command and (token == "git" or token.endswith("/git")):
            j = i + 1
            while j < len(tokens) and tokens[j] in GIT_VALUE_FLAGS:
                j += 2               # git global flags that consume a value
            if j < len(tokens) and tokens[j] == "commit":
                end = j
                while end < len(tokens) and tokens[end] not in OPERATORS:
                    end += 1
                if "--no-verify" in tokens[j:end]:
                    return True
                i = end
                at_command = True
                continue
        at_command = False
        i += 1
    return False


try:
    bypass = any(commit_requests_bypass(line) for line in cmd.splitlines())
except ValueError:
    bypass = re.search(r"(?<!\S)--no-verify(?!\S)", cmd) is not None
sys.exit(0 if bypass else 1)
PYGATE
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
