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

# Prints EVERY git subcommand the payload's command runs, one per line - `git add -A && git push`
# prints `add` then `push`. Use `hook_git_runs` rather than reading the first line.
#
# ALL OF THEM, NOT THE FIRST, and the singular version of this was a live regression. The inline
# code this replaced stopped only at a git invocation whose subcommand was `push`, so it scanned past
# earlier ones; the first extraction dropped that condition and returned the first git subcommand
# unconditionally. `git add -A && git commit -m x && git push` then reported `add`, and BOTH push
# hooks silently did nothing on a real push - the exact silently-stops-working class this file was
# created to stop being duplicated, reintroduced by the extraction meant to prevent it. Caught by
# review on astubbs/parallel-consumer#357 and reproduced before being believed; `bin/test-check-agent-hooks.sh`
# now carries compound-command fixtures for both hooks.
#
# TOKENS, NOT SUBSTRINGS, which is the whole point: `git commit -m "ready to push"` contains the
# word and must not fire. git is matched by BASENAME so /usr/bin/git counts, and an unbalanced quote
# makes shlex raise, which fails open.
hook_git_subcommands() { # <payload-json>
    printf '%s' "$1" | python3 -c '
import json, shlex, sys
try:
    data = json.load(sys.stdin)
    cmd = data.get("tool_input", {}).get("command", "")
    # OPERATOR-AWARE, because plain shlex.split() splits on whitespace and quotes ONLY. It leaves
    # `&&` and `;` fused to whatever touches them, so `git add -A&&git push` tokenised as
    # [git, add, -A&&git, commit, ...] - the later `git` never matched, and the hook went silent on a
    # real push. `git push; echo done` was worse and commoner: the token is `push;`, so even the
    # SPACED form missed. punctuation_chars makes the operators their own tokens, which is exactly
    # what the git-token scan below assumes it is walking.
    lex = shlex.shlex(cmd, posix=True, punctuation_chars=True)
    lex.whitespace_split = True
    toks = list(lex)
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
            print(rest[0])
' 2>/dev/null || true
}

# True when the payload runs `git <subcommand>` ANYWHERE in its command, including after another git
# invocation in a compound one. This is the predicate both push hooks actually want; reading a single
# subcommand is what made a chained push invisible.
hook_git_runs() { # <payload-json> <subcommand>
    local want="$2" sub found=1
    while IFS= read -r sub; do
        if [ "$sub" = "$want" ]; then found=0; fi
    done <<EOF
$(hook_git_subcommands "$1")
EOF
    return "$found"
}

# Prints a file's mtime as a unix timestamp, or nothing when it cannot be read.
#
# PROBE THE PLATFORM, never fall back arm to arm: on GNU, `stat -f %m FILE` exits 1 while PRINTING
# filesystem prose to stdout, so a blind `-c || -f` chain returns a string rather than a number and
# the arithmetic that consumes it then aborts the hook under `set -u`. Callers must still treat a
# non-numeric answer as "no timestamp" - see the shape test in remind-inflight-on-push.sh.
#
# `|| true` ON BOTH ARMS, though neither current caller needs it. Two OTHER copies of this probe
# exist (`_mtime` in .claude/hooks/check-merge-outstanding-work.sh and in bin/check-pr-ready.sh), and
# collapsing them onto this one is queued in docs/refactoring.md. The merge guard runs under `set -e`,
# where a failing `stat` without this would abort the script instead of reaching its documented
# fail-closed branch - so the safe version has to be here BEFORE anything is pointed at it, or the
# consolidation that removes a duplicate silently introduces a bug.
hook_file_mtime() { # <path>
    [ -f "$1" ] || return 0
    # hazard-ok: this IS the platform probe - it asks whether GNU stat exists before anything uses it
    if stat -c %Y . >/dev/null 2>&1; then
        # hazard-ok: the probe above already established GNU stat is present
        stat -c %Y "$1" 2>/dev/null || true   # GNU coreutils
    else
        # hazard-ok: the probe above rejected GNU stat, so this is the BSD branch
        stat -f %m "$1" 2>/dev/null || true   # BSD / macOS
    fi
}

# Path of a per-branch throttle stamp, given a prefix. Branch names contain `/`, which is the only
# real content here: the substitution keeps a path from a name, and doing it in one place stops the
# two hooks from disagreeing about how a branch is spelled on disk.
hook_stamp_path() { # <prefix> <branch>
    printf '%s/%s-%s' "${TMPDIR:-/tmp}" "$1" "$(printf '%s' "$2" | tr '/' '_')"
}
