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
hook_git_invocations() { # <payload-json>
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
            # ALSO THE ARGUMENTS, stopping at the next shell operator. `hook_git_subcommands` throws
            # them away, but a caller that must tell `git rebase origin/x` from `git rebase --abort`
            # cannot: the flag it needs was consumed by the loop above, and the ref it needs was
            # never emitted. Re-tokenising in the caller is what this file exists to prevent, so the
            # ONE tokeniser emits both and the two views below differ only in what they cut.
            #
            # STOP AT ANY OPERATOR TOKEN, not a hand-listed few. `punctuation_chars=True` makes shlex
            # emit `> >> < << ( ) ; ;; | || & &&` as tokens of their own, and an earlier version
            # named only the command SEPARATORS. That left redirections inside the argument list, so
            # `git merge origin/master > out.log` walked args as ["origin/master", ">", "out.log"] -
            # and a redirect target named like a control flag or a remedy ref could then spoof the
            # exemption test in the consumer. Testing that every character is punctuation catches the
            # whole family, including operators nobody has thought of, and cannot catch a real
            # argument: refs, paths and flags all contain at least one non-operator character.
            OPERATORS = set("();<>|&")
            k, args = j + 1, []
            while k < len(toks) and not (toks[k] and all(c in OPERATORS for c in toks[k])):
                args.append(toks[k]); k += 1
            print("\t".join([rest[0]] + args))
' 2>/dev/null || true
}

# Prints only the subcommand of each git invocation - the original contract, kept because both push
# hooks and their fixtures depend on it verbatim.
hook_git_subcommands() { # <payload-json>
    hook_git_invocations "$1" | cut -f1
}

# Prints the REMOTE-side branch name that the payload's `git push` names, or nothing when it names
# none. Nothing is a real answer here: it means the caller must fall back to a working directory,
# and must SAY that it did.
#
# WHY THE COMMAND AND NOT `HEAD`. A hook process does not run in the directory its guarded command
# runs in, and this repository keeps many worktrees checked out at once - so
# `git rev-parse --abbrev-ref HEAD` answers about whichever branch the SESSION happens to sit on,
# confidently and wrongly. On 2026-08-31 that made check-history-rewrite.sh report
# "no open pull request has `docs/god-branch-decomposition-plan` as its head branch" while refusing a
# force-push of `feats/proxy-verdict-free-return`, which had an open PR with review history - the
# exact thing that hook exists to name. When the command spells the refspec out, that is the only
# authoritative answer available, and it is free.
#
# THE PR HEAD IS THE DESTINATION, NOT THE SOURCE: `git push origin src:dst` publishes `dst`, so
# `dst` is what a pull request has as its head branch. `+src` is the force spelling of `src`, and
# `refs/heads/x` is the long spelling of `x`.
#
# FOUR SHAPES DELIBERATELY RETURN NOTHING, because each names something this cannot read as a branch:
# a push with no refspec (`git push -f`), `HEAD` (which means the command's own directory, the thing
# a hook cannot see), a bare `refs/`-something (a tag or a note), and `tag <name>`.
#
# A MULTI-REFSPEC PUSH IS ANSWERED WITH ITS FIRST REFSPEC, which is incomplete rather than wrong -
# that branch really is one of the branches being pushed, so a refusal naming it is about work the
# command actually touches. Falling back to the working directory instead would trade an incomplete
# answer for an unrelated one.
#
# THE SAME RULE IS SPELLED OUT A SECOND TIME, in python, inside check-history-rewrite.sh - which
# refuses tool calls and therefore may not depend on a library it might fail to source
# (astubbs/parallel-consumer#341). That duplication is deliberate and is tracked with the rest of it
# in docs/inflight/ci-pr-lookup-is-copied-into-three-hooks.md; change one and change the other.
#
# TAKES THE TOKENISER'S OUTPUT, NOT THE PAYLOAD. The caller has already spawned
# `hook_git_invocations` to ask whether the payload is a push at all; reading that list rather than
# re-deriving it keeps the whole hook to ONE python3 spawn - the same economy `hook_git_runs_any`
# exists for, one function down.
hook_push_head_ref() { # <invocations - the output of hook_git_invocations>
    local line args t spec dst count skip
    while IFS= read -r line; do
        case "$line" in push|push$'\t'*) ;; *) continue ;; esac
        # `hook_git_invocations` already stopped this invocation's arguments at the next shell
        # operator, so everything here belongs to this push and nothing to the command after it.
        args="${line#push}"
        count=0
        spec=""
        skip=0
        while IFS= read -r t; do
            [ -n "$t" ] || continue
            if [ "$skip" = 1 ]; then skip=0; continue; fi
            case "$t" in
                # Push options that consume a SEPARATE value token. Dropping only the flag would
                # leave its value where the repository or the refspec should be - the same class of
                # bug this file records above for `git -C /path push`.
                -o|--push-option|--receive-pack|--exec|--repo) skip=1; continue ;;
                -?*) continue ;;
            esac
            count=$((count + 1))
            # positional 1 is the repository, positional 2 is the first refspec.
            if [ "$count" = 2 ]; then spec="$t"; break; fi
        done <<EOF
$(printf '%s' "$args" | tr '\t' '\n')
EOF
        [ -n "$spec" ] || continue
        [ "$spec" = "tag" ] && continue
        case "$spec" in
            *:*) dst="${spec#*:}" ;;
            *)   dst="$spec" ;;
        esac
        dst="${dst#+}"
        dst="${dst#refs/heads/}"
        case "$dst" in ''|HEAD|refs/*) continue ;; esac
        printf '%s\n' "$dst"
        return 0
    done <<EOF
$1
EOF
    return 0
}

# True when the payload runs ANY of the named git subcommands. One tokeniser spawn for the whole
# question: `hook_git_runs a || hook_git_runs b` paid python3 twice to walk the same token list.
hook_git_runs_any() { # <payload-json> <subcommand>...
    local payload="$1" sub want found=1
    shift
    while IFS= read -r sub; do
        for want in "$@"; do
            if [ "$sub" = "$want" ]; then found=0; fi
        done
    done <<EOF
$(hook_git_subcommands "$payload")
EOF
    return "$found"
}

# True when <stamp> is missing, unreadable, or older than <seconds>.
#
# THE THROTTLE TRIAD, NOT JUST THE `stat`. `hook_file_mtime` already removed one copy of the
# platform probe; the three lines that CONSUME it - now, mtime, numeric-shape guard, subtract,
# compare - were then copied into every hook that needed a floor, which is the same defect class one
# level up. The shape guard is load-bearing: without it a non-numeric answer aborts the arithmetic
# under `set -u`, and the hook dies rather than throttling.
hook_throttle_expired() { # <stamp-path> <seconds>
    local last now floor="$2"
    case "$floor" in ''|*[!0-9]*) floor=0 ;; esac
    last="$(hook_file_mtime "$1")"
    case "$last" in ''|*[!0-9]*) last=0 ;; esac
    now="$(date +%s)"
    [ $(( now - last )) -ge "$floor" ]
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

# Path of a throttle stamp, given a prefix and a KEY. The key is usually a branch name but need not
# be - one caller keys by git-common-dir - and either way it contains `/`, which is the only real
# content here: the substitution keeps a path from a name, and doing it in one place stops the hooks
# from disagreeing about how a key is spelled on disk. A caller passing a PATH must make it absolute
# first: `git rev-parse --git-common-dir` answers `.git` from a main checkout and an absolute path
# from a linked worktree, so keying on it raw collides every clone on the machine onto one stamp.
hook_stamp_path() { # <prefix> <key>
    printf '%s/%s-%s' "${TMPDIR:-/tmp}" "$1" "$(printf '%s' "$2" | tr '/' '_')"
}
