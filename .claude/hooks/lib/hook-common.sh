#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Shared helpers for the `PreToolUse` hooks in `.claude/hooks/`. Sourced, never executed.
#
# WHY IT EXISTS. Two hooks now fire on a `git push` - remind-inflight-on-push.sh and
# remind-master-drift-on-push.sh - and both need the same two answers: "is this payload actually a
# push?" and "when was this stamp file last written?". Both answers were got wrong once already, in
# ways that made a hook silently stop working rather than fail:
#
#   - `git -C /path push` put "/path" where the subcommand should be, so the reminder never fired
#     for the form an agent uses most (astubbs#324 review found it).
#   - `stat -c %Y` is GNU; BSD/macOS stat rejects `-c`, so the throttle read no mtime at all
#     (astubbs#341, the BSD portability sweep).
#
# A copy of either bug is invisible until someone re-runs the same experiment on the same platform,
# which is the argument for one implementation rather than two that agree today. `bin/AGENTS.md`
# makes the same call for `bin/lib/node-gate.sh`.
#
# FAIL OPEN, ALWAYS. Everything here is used by non-blocking reminders. A helper that cannot answer
# returns nothing and the caller stays silent - a guard that jams the tool call shut when it is
# itself broken is worse than the mistake it was written to prevent (docs/agent-harness.md).

# Prints the git SUBCOMMAND a hook payload runs (`push`, `commit`, ...), or nothing when the payload
# is not a git invocation at all. The caller compares - `[ "$(hook_git_subcommand "$payload")" = push ]`.
#
# TOKENS, NOT SUBSTRINGS, which is the whole point: `git commit -m "ready to push"` contains the
# word and must not fire. git is matched by BASENAME so /usr/bin/git counts, and an unbalanced quote
# makes shlex raise, which fails open.
hook_git_subcommand() { # <payload-json>
    printf '%s' "$1" | python3 -c '
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
        # one quote ends the string and bash then parses Python as shell. That is how the original
        # of this block was first written, and the whole hook died with a syntax error.
        #
        # Global git flags take a SEPARATE value token, and dropping only the flag leaves the value
        # where the subcommand should be: `git -C /path push` put "/path" at rest[0], so the
        # reminder never fired for the form an agent is most likely to use. Consume each value with
        # its flag, the way skip_repo_flags in check-merge-outstanding-work.sh does for -R/--repo.
        VALUE_FLAGS = ("-C", "-c", "--git-dir", "--work-tree", "--namespace", "--exec-path")
        j, rest = i + 1, []
        while j < len(toks):
            t = toks[j]
            if t in VALUE_FLAGS:
                j += 2; continue
            if t.startswith("-"):
                j += 1; continue
            rest.append(t); break
        if rest:
            print(rest[0]); break
' 2>/dev/null || true
}

# Prints a file's mtime as a unix timestamp, or nothing when it cannot be read.
#
# PROBE THE PLATFORM, never fall back arm to arm: on GNU, `stat -f %m FILE` exits 1 while PRINTING
# filesystem prose to stdout, so a blind `-c || -f` chain returns a string rather than a number and
# the arithmetic that consumes it then aborts the hook under `set -u`. Callers must still treat a
# non-numeric answer as "no timestamp" - see the shape test in remind-inflight-on-push.sh.
hook_file_mtime() { # <path>
    [ -f "$1" ] || return 0
    if stat -c %Y . >/dev/null 2>&1; then
        stat -c %Y "$1" 2>/dev/null   # GNU coreutils
    else
        stat -f %m "$1" 2>/dev/null   # BSD / macOS
    fi
}
