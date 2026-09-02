#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for the hooks in `.claude/hooks/`. Feeds each one a crafted hook payload on stdin
# and asserts its verdict.
#
# WHY IT EXISTS. docs/agent-harness.md's own rule 3 is "give it a negative control - make it go red
# on purpose before you trust it". The harness shipped without applying that rule to itself, and a
# review found six defects in one 25-line parser, four of which let the exact mistake the hook is
# named after straight through while two blocked legitimate merges. Every one of those is a case
# below. A hook is unusually easy to get wrong this way: it runs inside another process, its output
# is consumed by a model rather than a person, and a broken one looks identical to a quiet one.
#
# WHY THE `test-check-` PREFIX, given there is no `bin/check-agent-hooks.sh`. bin/AGENTS.md pairs
# `test-check-X.sh` with `check-X.sh`, and this is a deliberate exception: the prefix is also what
# grants a script to the review agent by pattern, and a self-test for the harness is exactly the
# thing a reviewer should be able to run. It stays inside the grant's boundary - read-only, no
# network, no writes outside its own mktemp directory.
#
# Run: bin/test-check-agent-hooks.sh

set -uo pipefail

# Resolved from $0, never from the caller's cwd. inject-merge-checklist.sh finds its checklist via
# CLAUDE_PROJECT_DIR or `git rev-parse --show-toplevel`, so running this script from a DIFFERENT
# checkout of this repo - the primary one, a sibling worktree on another branch - silently pointed
# the hook at that tree's docs/ instead of this branch's. On a branch without the checklist the
# hook exits 0 and the assertions below read the absence as "not injected". That is the failure
# this file exists to catch, in this file: pinning the root makes the test measure the branch it
# ships with, wherever it is run from.
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOOKS="$REPO_ROOT/.claude/hooks"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

failures=0

assert() { # <description> <expected> <actual>
    if [ "$2" = "$3" ]; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (expected '$2', got '$3')"
        failures=$((failures + 1))
    fi
}

# The `--head` value a stubbed `gh` was asked about, read from that stub's argv log. Awk rather
# than `grep -o ... | head -1`, which is the early-exiting-reader pipeline shape bin/AGENTS.md
# warns about - and this suite runs under pipefail.
stub_head_arg() { # <argv-log>
    awk '{for (i = 1; i < NF; i++) if ($i == "--head") { print $(i+1); exit }}' "$1"
}

# ---------------------------------------------------------------------------------------------
# check-squash-subject.sh
#
# The rule is now "no --subject on gh pr merge at all", so there is nothing to parse and nothing
# to get subtly wrong. The previous version policed CORRECT use of the flag - last-occurrence
# semantics, PR-number cross-check, shell quoting - in 343 lines that review found wrong in both
# directions. These cases exist to stop that creeping back.
# ---------------------------------------------------------------------------------------------

echo
echo "--- check-squash-subject.sh ---"


fails=${fails:-0}

# Which hook verdict()/expect() drive - each section points it at its own hook, so the harness
# below is written once (it was cloned per hook until review flagged the drift risk).
HOOK_UNDER_TEST="$HOOKS/check-squash-subject.sh"

verdict() { # <bash-command> -> ALLOW | DENY, from $HOOK_UNDER_TEST
    # The command goes to the JSON builder on STDIN, never argv: one case here is a 150 KB command,
    # and passing that as an argument hits the same E2BIG the case exists to detect - the harness
    # would die and the failure would read as the hook's.
    local out tmp
    tmp=$(mktemp)
    printf '%s' "$1" | VERDICT_EVENT="${VERDICT_EVENT:-PreToolUse}" python3 -c 'import json,os,sys; print(json.dumps({"tool_name":"Bash","hook_event_name":os.environ["VERDICT_EVENT"],"tool_input":{"command":sys.stdin.read()}}))' > "$tmp"
    # VERDICT_CWD lets a section drive its hook from inside a fixture repo. Default `.` keeps every
    # existing caller running exactly where it did.
    out=$(cd "${VERDICT_CWD:-.}" && "$HOOK_UNDER_TEST" < "$tmp" 2>/dev/null)
    rm -f "$tmp"
    case "$out" in
        *'"deny"'*) echo DENY ;;
        *)          echo ALLOW ;;
    esac
}

expect() { # <expected> <name> <command>
    local got; got=$(verdict "$3")
    if [ "$got" = "$1" ]; then echo "ok:   $2"; else echo "FAIL: $2 (expected $1, got $got)"; fails=$((fails + 1)); fi
}

expect DENY  "--subject with no (#N)"              'gh pr merge 2999 --squash --subject "foo"'
expect DENY  "-t, the documented SHORT form"       'gh pr merge 2999 --squash -t "foo"'
expect DENY  "--subject= equals form"              'gh pr merge 2999 --squash --subject=foo'
expect DENY  "-t= equals form"                     'gh pr merge 2999 --squash -t=foo'
expect DENY  "-tVALUE attached short form"         'gh pr merge 2999 --squash -tfoo'
expect ALLOW "-tVALUE attached, with (#N)"         'gh pr merge 2999 --squash -t"foo (#2999)"'
expect DENY  "(#N) present but not at the END"     'gh pr merge 2999 --squash --subject "port (#2999) to master"'

# Squash-body sign-off guard: a squash body override must carry the Co-authored-by trailer and
# must not carry a Claude-Session line (docs/merge-checklist.md owns both). Non-squash merges and
# merges without a body override are out of scope; an unreadable --body-file fails open because
# gh itself then fails loudly.
body_ok=$(mktemp); printf '%s\n%s\n' "Real explanation." "Co-authored-by: Claude Opus 5 (1M context) <noreply@anthropic.com>" > "$body_ok"
body_bare=$(mktemp); printf '%s\n' "Real explanation, no trailer." > "$body_bare"
body_sess=$(mktemp); printf '%s\n%s\n%s\n' "Real explanation." "Co-authored-by: Claude Opus 5 (1M context) <noreply@anthropic.com>" "Claude-Session: https://claude.ai/code/session_x" > "$body_sess"
expect DENY  "squash --body without co-authored trailer"    'gh pr merge 2999 --squash --body "no trailer here"'
expect ALLOW "squash --body with co-authored trailer"       'gh pr merge 2999 --squash --body "why.
Co-authored-by: Claude Opus 5 (1M context) <x@y>"'
expect DENY  "trailer mid-line is not a trailer"            'gh pr merge 2999 --squash --body "why. Co-authored-by: Claude Opus 5 (1M context) <x@y>"'
expect DENY  "squash body with Claude-Session line"         "gh pr merge 2999 --squash --body-file $body_sess"
expect ALLOW "squash --body-file with trailer"              "gh pr merge 2999 --squash --body-file $body_ok"
expect DENY  "squash --body-file without trailer"           "gh pr merge 2999 --squash --body-file $body_bare"
expect ALLOW "squash --body-file unreadable fails open"     'gh pr merge 2999 --squash --body-file /nonexistent/nope.txt'
expect ALLOW "non-squash merge body needs no trailer"       'gh pr merge 2999 --merge --body "no trailer needed"'
rm -f "$body_ok" "$body_bare" "$body_sess"
# The same global-flag gap the outstanding-work guard had, in the other implementation: this one
# detects the command with a regex, so `gh -R owner/repo pr merge` was not a merge as far as it was
# concerned and the subject went unchecked. Found by sweeping for the defect CLASS after fixing the
# sibling (AGENTS.md -> "look for other instances of the same defect"), astubbs#324.
expect DENY  "leading -R, --subject with no (#N)"  'gh -R astubbs/parallel-consumer pr merge 2999 --squash --subject "foo"'
expect DENY  "leading --repo, --subject no (#N)"   'gh --repo astubbs/parallel-consumer pr merge 2999 --squash --subject "foo"'
expect DENY  "attached --repo=, --subject no (#N)" 'gh --repo=astubbs/parallel-consumer pr merge 2999 --squash --subject "foo"'
expect DENY  "attached -RVALUE, squash body bare"  'gh -Rastubbs/parallel-consumer pr merge 2999 --squash --body "no trailer here"'
expect ALLOW "leading -R, subject ends with (#N)"  'gh -R astubbs/parallel-consumer pr merge 2999 --squash --subject "foo (#2999)"'

expect ALLOW "leading -R on a non-merge command"   'gh -R astubbs/parallel-consumer pr view 2999 --json title'
# gh also accepts the flag BETWEEN `pr` and `merge` (`gh pr -R owner/repo merge`), and the first
# global-flag fix only covered the leading position - found by the astubbs#324 review, live-proven
# against gh itself. Same defect class, third position.
expect DENY  "mid-position -R, --subject no (#N)"  'gh pr -R astubbs/parallel-consumer merge 2999 --squash --subject "foo"'
expect ALLOW "mid-position -R on a non-merge cmd"  'gh pr -R astubbs/parallel-consumer view 2999 --json title'

expect ALLOW "--subject ending with (#N)"          'gh pr merge 2999 --squash --subject "foo (#2999)"'
expect ALLOW "-t ending with (#N)"                 'gh pr merge 2999 --squash -t "foo (#2999)"'
expect ALLOW "--subject mentioned inside --body"   'gh pr merge 2999 --squash --body "why --subject matters
Co-authored-by: Claude Opus 5 (1M context) <noreply@anthropic.com>"'
expect ALLOW "escaped apostrophe, number present"  'gh pr merge 2999 --squash --subject '"'"'don'"'"'\'"'"''"'"'t drop it (#2999)'"'"''
expect ALLOW "--body-file does not touch subject"  'gh pr merge 2999 --squash --body-file b.txt'
expect ALLOW "no subject override at all"          'gh pr merge 2999 --squash'
expect ALLOW "not a merge command"                 'gh pr view 2999 --json title'
expect ALLOW "the word --subject in other text"    'echo "we discussed --subject and why"'

# A 150 KB command used to hit E2BIG and exit having printed nothing, which reads as ALLOW.
big=$(python3 -c "print('gh pr merge 2999 --squash --subject bad # ' + 'x'*150000)")
if [ "$(verdict "$big")" = DENY ]; then
    echo "ok:   a 150 KB command does not fail open"
else
    echo "FAIL: a 150 KB command fails open"; fails=$((fails + 1))
fi

# Negative control: prove it can deny. A guard that has never fired proves nothing (bin/AGENTS.md).
if [ "$(verdict 'gh pr merge 2999 --squash --subject "x"')" = DENY ] \
   && [ "$(verdict 'gh pr merge 2999 --squash')" = ALLOW ]; then
    echo "ok:   the guard distinguishes overridden from not"
else
    echo "FAIL: the guard does not distinguish overridden from not"; fails=$((fails + 1))
fi

# ---------------------------------------------------------------------------------------------
# check-upstream-map-merged.sh
#
# Refuses `gh pr merge <N>` while upstream-map.yaml still records that PR as `status: pr-open`.
# It reads the manifest from the CWD, so each case runs inside a fixture directory rather than
# against the live manifest - a test whose expected verdict changes when someone edits the real
# manifest is a test that will be deleted the first time it goes red for the wrong reason.
#
# It shipped with no self-test and reproduced two defects its siblings had already fixed: the
# regex missed `gh -R owner/repo pr merge`, and the URL form fell open with the number in plain
# sight. Those are the first cases below, and they are why this section exists.
# ---------------------------------------------------------------------------------------------

echo
echo "--- check-upstream-map-merged.sh ---"

HOOK_UNDER_TEST="$HOOKS/check-upstream-map-merged.sh"

umm_fixture=$(mktemp -d)
mkdir -p "$umm_fixture/src/docs/development"
cat > "$umm_fixture/src/docs/development/upstream-map.yaml" <<'YAML'
entries:
  - id: still-open
    fork:
      prs: [2999]
      status: pr-open
  - id: already-merged
    fork:
      prs: [2998]
      status: merged
YAML
umm_prev_pwd=$PWD
cd "$umm_fixture"

expect DENY  "bare form, entry still pr-open"      'gh pr merge 2999 --squash'
expect DENY  "leading -R (the astubbs#324 gap)"    'gh -R astubbs/parallel-consumer pr merge 2999 --squash'
expect DENY  "leading --repo long form"            'gh --repo astubbs/parallel-consumer pr merge 2999 --squash'
expect DENY  "attached --repo= form"               'gh --repo=astubbs/parallel-consumer pr merge 2999 --squash'
expect DENY  "-R between pr and merge"             'gh pr -R astubbs/parallel-consumer merge 2999 --squash'
expect DENY  "PR URL instead of a bare number"     'gh pr merge https://github.com/astubbs/parallel-consumer/pull/2999 --squash'

# The PR argument is the first POSITIONAL, not the first digit-shaped word: an unquoted numeric
# flag value ahead of it used to win. Both directions of that bug are pinned - denying the wrong
# PR, and quietly checking a PR nobody is merging.
expect DENY  "numeric --body value before the PR"  'gh pr merge --body 2998 2999 --squash'
expect ALLOW "numeric --body value, merged PR"     'gh pr merge --body 2999 2998 --squash'
# Passes on the pre-fix code too - `--body=2998` is one token, so the old first-digit-wins scan
# already skipped it. Kept as a guard on the attached form rather than as proof of this fix: a
# future refactor that starts splitting `--flag=value` would break it, and this is where that shows.
expect DENY  "attached --body= before the PR"      'gh pr merge --body=2998 2999 --squash'
expect DENY  "-t value before the PR"              'gh pr merge -t 2998 2999 --squash'

# Negative controls - each names the reason the hook must NOT fire, so a future change that makes
# it deny everything shows up here rather than by jamming somebody's merge shut.
expect ALLOW "entry already says merged"           'gh pr merge 2998 --squash'
expect ALLOW "PR absent from the manifest"         'gh pr merge 2997 --squash'
expect ALLOW "no PR number: current branch's PR"   'gh pr merge --squash'
expect ALLOW "not a merge command at all"          'gh pr view 2999'
expect ALLOW "merge in prose, not a gh command"    'echo "remember to merge 2999 later"'

cd "$umm_prev_pwd"
rm -rf "$umm_fixture"

# Fails open rather than jamming the tool shut when it cannot know: no manifest in this directory.
umm_empty=$(mktemp -d); umm_prev_pwd=$PWD; cd "$umm_empty"
expect ALLOW "no manifest here - fails open"       'gh pr merge 2999 --squash'
cd "$umm_prev_pwd"; rm -rf "$umm_empty"

# ---------------------------------------------------------------------------------------------
# pre-commit-gate.sh
#
# CLAUDE_PROJECT_DIR points at a fixture holding a stub `.githooks/pre-commit`, so the gate's
# pass/fail is controlled by the test rather than by the state of the real tree.

# ---------------------------------------------------------------------------------------------

echo
echo "--- pre-commit-gate.sh ---"

make_project() { # <gate-exit-code> -> project dir
    local dir="$TMP/proj$RANDOM$RANDOM"
    mkdir -p "$dir/.githooks"
    printf '#!/bin/sh\necho "STUB GATE SPOKE"\nexit %s\n' "$1" > "$dir/.githooks/pre-commit"
    chmod +x "$dir/.githooks/pre-commit"
    echo "$dir"
}

gate_rc() { # <project-dir> <bash-command>
    local payload
    payload=$(python3 -c 'import json,sys; print(json.dumps({"tool_name":"Bash","tool_input":{"command":sys.argv[1]}}))' "$2")
    printf '%s' "$payload" | CLAUDE_PROJECT_DIR="$1" "$HOOKS/pre-commit-gate.sh" >/dev/null 2>&1
    echo $?
}

red=$(make_project 1)
green=$(make_project 0)

assert "green gate lets the commit through" 0 \
    "$(gate_rc "$green" 'git commit -m "ordinary"')"

# The block is exit 2 - PreToolUse's documented deny - not 1, which is merely a non-blocking error.
assert "red gate blocks with exit 2" 2 \
    "$(gate_rc "$red" 'git commit -m "ordinary"')"

# THE POINT OF THE WHOLE SCRIPT. The first version was `pre-commit || exit 2` inline, which never
# read the payload, so --no-verify was ignored and a red gate locked the agent out of committing at
# all - including the commit that would have fixed the gate.
assert "--no-verify bypasses a red gate" 0 \
    "$(gate_rc "$red" 'git commit --no-verify -m "I have a reason"')"

# ...but only as a real argument. A commit MESSAGE that merely mentions the flag is not a bypass,
# or the escape hatch could be triggered by writing about it.
assert "--no-verify inside a quoted message is NOT a bypass" 2 \
    "$(gate_rc "$red" 'git commit -m "document the --no-verify escape hatch"')"

# The blocked call must carry the reason, or the agent sees "hook error: No stderr output" and has
# nothing to act on - the observed behaviour of the inline form.
stderr_text=$(python3 -c 'import json,sys; print(json.dumps({"tool_name":"Bash","tool_input":{"command":"git commit -m x"}}))' \
    | CLAUDE_PROJECT_DIR="$red" "$HOOKS/pre-commit-gate.sh" 2>&1 >/dev/null)
case "$stderr_text" in
    *"STUB GATE SPOKE"*) got=forwarded ;;
    *)                   got="swallowed: $stderr_text" ;;
esac
assert "the failing gate's own output reaches stderr" forwarded "$got"


# Round six. The bypass search scanned the WHOLE payload, so a later, unrelated command could
# request a bypass the commit never asked for - and the violation lands.
assert "--no-verify in a LATER command is not this commit's bypass" 2 \
    "$(gate_rc "$red" 'git commit -m x && echo --no-verify')"
assert "--no-verify in an EARLIER command is not this commit's bypass" 2 \
    "$(gate_rc "$red" 'echo --no-verify && git commit -m x')"
# TWO REAL COMMITS, one asking for a bypass and one not. The bypass used to be judged over the
# whole payload, so the second commit's flag exempted the first - which in a clone with no
# core.hooksPath meant that first commit landed with no gate run at all.
assert "a later commit's --no-verify does not exempt an earlier one" 2 \
    "$(gate_rc "$red" 'git commit -m first; git commit --no-verify -m second')"
assert "both commits asking for a bypass is honoured" 0 \
    "$(gate_rc "$red" 'git commit --no-verify -m a; git commit --no-verify -m b')"
# A quoted MULTILINE message containing the flag. Lexing line-by-line split this down the middle,
# the first line raised ValueError, and the fallback then found the flag in the message text.
assert "a multiline message mentioning the flag is not a bypass" 2 \
    "$(gate_rc "$red" 'git commit -m "line1
--no-verify
line3"')"
assert "git -C <path> commit --no-verify is still a bypass" 0 \
    "$(gate_rc "$red" 'git -C /some/path commit --no-verify -m x')"

# No python, no way to read the payload - so no way to see --no-verify. Running the gate anyway
# blocked the documented escape hatch on the one machine with no other way out. Python is not among
# the repo's build requirements (JDK 17, Docker, the Maven wrapper), so this is a real setup.
nopython="$TMP/nopython"; mkdir -p "$nopython"
for b in bash cat mktemp rm sh dirname env; do
    resolved=$(command -v "$b" 2>/dev/null) && ln -sf "$resolved" "$nopython/$b"
done
nopython_rc=$(printf '%s' "$(python3 -c 'import json; print(json.dumps({"tool_name":"Bash","tool_input":{"command":"git commit -m x"}}))')" \
    | PATH="$nopython" CLAUDE_PROJECT_DIR="$red" "$HOOKS/pre-commit-gate.sh" >/dev/null 2>&1; echo $?)
assert "a missing python3 fails OPEN rather than blocking the bypass" 0 "$nopython_rc"

# Fail open: no gate script in the project at all must not block every commit.
empty="$TMP/empty$RANDOM"; mkdir -p "$empty"
assert "absent gate script fails OPEN" 0 \
    "$(gate_rc "$empty" 'git commit -m "ordinary"')"

# NOT A COMMIT AT ALL: the hook must self-filter. The registration's `if: Bash(git commit *)` is
# supposed to scope it, but the script cannot rely on that - observed live (astubbs#324 babysit):
# with the gate red, a plain `ls` and a read-only `cat` of the gate itself were blocked with the
# gate's own error, because "no commit found" fell into "run the gate". Self-filtering in the
# script is the contract every other hook in this directory follows.
assert "a non-commit command is not gated" 0 \
    "$(gate_rc "$red" 'ls -la')"
assert "a read-only command naming the gate is not gated" 0 \
    "$(gate_rc "$red" 'cat .githooks/pre-commit')"
# The NASTY shape that made the misfire visible: shell keywords and a command substitution, and
# not a commit anywhere in it. The near-miss for every case below - one `git` away from being
# gated, and it must stay green.
assert "a compound non-commit command with if/for/\$() is not gated" 0 \
    "$(gate_rc "$red" 'for f in $(ls); do if [ -f "$f" ]; then echo "$f"; fi; done')"

# ...and the other half of that pair, which the self-filter above quietly broke. Only OPERATORS
# reopened command position, so `then`, `do`, `{` and `!` swallowed it and the commit behind them
# counted as zero - gated by accident while zero meant "run the gate", silently EXEMPT the moment
# zero meant "skip". A gate that stops firing looks exactly like a gate with nothing to say.
assert "a commit inside if/then is still gated" 2 \
    "$(gate_rc "$red" 'if true; then git commit -m x; fi')"
assert "a commit inside a for loop is still gated" 2 \
    "$(gate_rc "$red" 'for f in a b; do git commit -m "$f"; done')"
assert "a commit inside a brace group is still gated" 2 \
    "$(gate_rc "$red" 'git status && { git commit -m x; }')"
assert "a commit behind a ! negation is still gated" 2 \
    "$(gate_rc "$red" '! git commit -m x')"
# The escape hatch reaches inside those constructs too, or the fix above would have taken it away
# from exactly the shapes it just started gating.
assert "--no-verify inside if/then is still a bypass" 0 \
    "$(gate_rc "$red" 'if true; then git commit --no-verify -m x; fi')"
# A keyword is only a keyword in COMMAND POSITION. As an argument it is text, so this is an echo.
assert "a keyword in argument position does not make a commit" 0 \
    "$(gate_rc "$red" 'echo do git commit -m x')"

# The same defect class one lexer layer down: an unquoted NEWLINE separates statements exactly
# like `;`, but shlex's default whitespace swallowed it - no token, no reset - so `at_command`
# carried over from the previous line and a commit on line two was invisible. Every case above
# passes only because it joins with `;`; multi-line payloads are the natural agent shape.
assert "a commit on the second line is still gated" 2 \
    "$(gate_rc "$red" 'git add -A
git commit -m wip')"
assert "a commit inside a multiline for loop is still gated" 2 \
    "$(gate_rc "$red" 'for f in a b
do
    git commit -m "$f"
done')"
assert "a commit inside a multiline if/then is still gated" 2 \
    "$(gate_rc "$red" 'if true
then
    git commit -m x
fi')"
# The escape hatch must reach across lines too, or the fix above would take it away from exactly
# the shapes it just started gating.
assert "--no-verify on the second line is still a bypass" 0 \
    "$(gate_rc "$red" 'git add -A
git commit --no-verify -m wip')"
# One commit's extent ends at the newline exactly as it does at `;` - or the second line's flag
# would have been read as the first commit's, and the first would land ungated.
assert "a later line's --no-verify does not exempt line one" 2 \
    "$(gate_rc "$red" 'git commit -m first
git commit --no-verify -m second')"
assert "a multiline non-commit command is not gated" 0 \
    "$(gate_rc "$red" 'git add -A
git status')"

# The `function` keyword spelling. `foo() { git commit; }` was already caught because the bare
# `()` are operator tokens that reopen command position; `function foo { git commit; }` has no
# operator before the brace, so the name consumed the position and the body went unseen.
assert "a commit inside a function-keyword body is still gated" 2 \
    "$(gate_rc "$red" 'function deploy { git commit -m x; }; deploy')"

# --- WHICH WORKING TREE IT GATES -----------------------------------------------------------------
#
# The gate script and its working directory both came from `$CLAUDE_PROJECT_DIR`, which names the
# SESSION's project root. A SUBAGENT has its own working directory while that variable still points
# at the session's most recent worktree - and a subagent's `git commit ...` is bare, so it matches
# the registration and arrives here. Observed 2026-08-31: a subagent committing in
# `.claude/worktrees/proxy-server-shell` was gated against `.claude/worktrees/bench-harness`, failing
# check-file-refs.sh on citations to files that do not exist on its branch while its own tree ran the
# same gate at exit 0, and five commits went through with --no-verify.
#
# THE WORSE HALF LEAVES NO TRACE, and gets its own case below: the misresolution can fail OPEN, a red
# tree passing because the session's tree is green. Nobody investigates a commit that was allowed.
#
# REAL GIT REPOS, unlike `make_project` above, because the resolution now climbs to the repository
# root - and the assertions are on the gate's own LABEL rather than on paths, since `mktemp -d` under
# `/var` and `git rev-parse --show-toplevel` under `/private/var` name the same directory differently
# on macOS.
make_worktree() { # <label> <gate-exit-code> -> a git repo whose stub gate announces itself
    local dir="$TMP/wt-$1-$RANDOM$RANDOM"
    mkdir -p "$dir/.githooks" "$dir/nested/deeper"
    ( cd "$dir" && git init -q . )
    printf '#!/bin/sh\necho "GATE OF %s SPOKE in $(basename "$PWD")"\nexit %s\n' "$1" "$2" > "$dir/.githooks/pre-commit"
    chmod +x "$dir/.githooks/pre-commit"
    echo "$dir"
}
wt_fire() { # <CLAUDE_PROJECT_DIR> <payload-cwd|-> <command> -> "<exit>|<stderr>"
    local payload out rc=0
    payload=$(python3 -c '
import json, sys
d = {"tool_name": "Bash", "tool_input": {"command": sys.argv[1]}}
if sys.argv[2] != "-":
    d["cwd"] = sys.argv[2]
print(json.dumps(d))' "$3" "$2")
    out=$(printf '%s' "$payload" | CLAUDE_PROJECT_DIR="$1" "$HOOKS/pre-commit-gate.sh" 2>&1 >/dev/null) || rc=$?
    printf '%s|%s' "$rc" "$(printf '%s' "$out" | tr '\n' ' ')"
}

session_green=$(make_worktree session-green 0)
commit_red=$(make_worktree commit-red 1)
session_red=$(make_worktree session-red 1)
commit_green=$(make_worktree commit-green 0)

# 1. THE INCIDENT. The commit runs in a worktree whose gate is red; the session's is green.
wt_out=$(wt_fire "$session_green" "$commit_red" 'git commit -m "in the other worktree"')
assert "a commit is gated by the tree it runs in, not the session's" 2 "${wt_out%%|*}"
case "$wt_out" in
    *"GATE OF commit-red"*) got=ran_the_commits_gate ;;
    *"GATE OF session-green"*) got=ran_the_sessions_gate ;;
    *) got="neither: ${wt_out#*|}" ;;
esac
assert "...and it is the commit's OWN gate script that ran" ran_the_commits_gate "$got"
case "${wt_out#*|}" in *"in wt-commit-red"*) got=ran_there ;; *) got="wrong cwd: ${wt_out#*|}" ;; esac
assert "...run WITH that tree as its working directory" ran_there "$got"

# 2. THE SILENT HALF, which is the one nobody would have found: a red tree allowed because the
# SESSION's tree is green. Exit 0 here is the fix; the pre-fix hook blocked on the session's gate.
wt_out=$(wt_fire "$session_red" "$commit_green" 'git commit -m "clean tree"')
assert "a clean tree is not blocked by the session's red one" 0 "${wt_out%%|*}"
case "${wt_out#*|}" in *"GATE OF session-red"*) got=ran_the_sessions_gate ;; *) got=left_it_alone ;; esac
assert "...and the session's gate was not consulted at all" left_it_alone "$got"

# 3. `git -C <path> commit` names its own repository, and is the strongest signal there is.
wt_out=$(wt_fire "$session_green" - "git -C $commit_red commit -m x")
assert "git -C names the tree to gate" 2 "${wt_out%%|*}"
case "$wt_out" in *"GATE OF commit-red"*) got=followed_dash_c ;; *) got="ignored it: ${wt_out#*|}" ;; esac
assert "...even with no cwd in the payload" followed_dash_c "$got"

# 3b. THE LEADING-CD TIER, previously untested end to end (review round on
# astubbs/parallel-consumer#382): a leading `cd <path> &&` is the command saying where it runs, and
# outranks the payload cwd.
wt_out=$(wt_fire "$session_green" "$commit_green" "cd $commit_red && git commit -m x")
assert "a leading cd names the tree to gate" 2 "${wt_out%%|*}"
case "$wt_out" in *"GATE OF commit-red"*) got=followed_the_cd ;; *) got="ignored it: ${wt_out#*|}" ;; esac
assert "...over the payload cwd" followed_the_cd "$got"

# 3c. TWO command-position cds are AMBIGUOUS - the commit may run in either - so the gate must fall
# back to the payload cwd rather than trusting the FIRST cd. Pre-fix, this gated wt-commit-green
# (the first cd) and let the red tree pass.
wt_out=$(wt_fire "$session_green" "$commit_red" "cd $commit_green && echo x && cd $commit_red && git commit -m x")
assert "two cds fall back to the payload cwd" 2 "${wt_out%%|*}"
case "$wt_out" in *"GATE OF commit-red"*) got=gated_the_payload_cwd ;; *) got="gated the first cd: ${wt_out#*|}" ;; esac
assert "...which is the red tree the commit actually runs in" gated_the_payload_cwd "$got"

# 3d. A RELATIVE cd or -C is relative to the PAYLOAD cwd, never to the hook process - same-named
# subdirectories exist in every worktree, so the wrong resolution succeeds on the wrong tree.
mkdir -p "$commit_green/redsub/.githooks"
( cd "$commit_green/redsub" && git init -q . )
printf '#!/bin/sh\necho "GATE OF redsub SPOKE"\nexit 1\n' > "$commit_green/redsub/.githooks/pre-commit"
chmod +x "$commit_green/redsub/.githooks/pre-commit"
wt_out=$(wt_fire "$session_green" "$commit_green" 'cd redsub && git commit -m x')
assert "a relative cd resolves against the payload cwd" 2 "${wt_out%%|*}"
case "$wt_out" in *"GATE OF redsub"*) got=resolved_relative ;; *) got="missed it: ${wt_out#*|}" ;; esac
assert "...and runs the resolved tree's own gate" resolved_relative "$got"
wt_out=$(wt_fire "$session_green" "$commit_green" 'git -C redsub commit -m x')
assert "a relative git -C resolves against the payload cwd" 2 "${wt_out%%|*}"

# 3e. REPEATED -C values COMPOSE: `git -C sub -C .. commit` runs in the ORIGINAL repository, so the
# red gate must fire. Keeping only the last token resolved `..` against the payload cwd - the
# parent, which has no gate - and the commit passed unchecked (Codex review on
# astubbs/parallel-consumer#382).
mkdir -p "$commit_red/sub"
wt_out=$(wt_fire "$session_green" "$commit_red" 'git -C sub -C .. commit -m x')
assert "repeated -C paths compose instead of last-wins" 2 "${wt_out%%|*}"
case "$wt_out" in *"GATE OF commit-red"*) got=composed_the_chain ;; *) got="escaped to the parent: ${wt_out#*|}" ;; esac
assert "...and the composed chain lands back in the red tree" composed_the_chain "$got"

# 3f. `cd /x & git commit` backgrounds the cd - the commit stays in the payload cwd, so trusting
# the prefix would run the green tree's gate over the red tree the commit actually lands in.
wt_out=$(wt_fire "$session_green" "$commit_red" "cd $commit_green & git commit -m x")
assert "a backgrounded cd does not relocate the gate" 2 "${wt_out%%|*}"
case "$wt_out" in *"GATE OF commit-red"*) got=gated_the_real_tree ;; *) got="trusted the subshell cd: ${wt_out#*|}" ;; esac
assert "...and the red tree's own gate is the one that ran" gated_the_real_tree "$got"

# 4. A COMMIT FROM A SUBDIRECTORY has to climb: the gate lives at the checkout root, and stopping at
# the literal directory would find no gate and fail open - a silent skip, not a visible error.
wt_out=$(wt_fire "$session_green" "$commit_red/nested/deeper" 'git commit -m "from a subdir"')
assert "a commit from a subdirectory climbs to the repository root" 2 "${wt_out%%|*}"

# 5. THE FALLBACK IS A DECISION, not an accident: with nothing in the payload saying where the
# command runs, `$CLAUDE_PROJECT_DIR` is still the best available answer and is still used. All the
# cases above this section rely on it, so this pins it explicitly alongside its replacements.
wt_out=$(wt_fire "$commit_red" - 'git commit -m "no cwd in the payload"')
assert "with nothing else to go on it falls back to the project dir" 2 "${wt_out%%|*}"
case "$wt_out" in *"GATE OF commit-red"*) got=used_the_fallback ;; *) got="${wt_out#*|}" ;; esac
assert "...and says so rather than skipping" used_the_fallback "$got"
case "$wt_out" in *"did not say where it runs"*) got=labelled ;; *) got=unlabelled ;; esac
assert "...labelled as the session's root in the refusal" labelled "$got"

# 6. A BYPASS IS STILL A BYPASS wherever the commit runs - the escape hatch must not have been
# narrowed to the session's tree by any of the above.
wt_out=$(wt_fire "$session_green" "$commit_red" 'git commit --no-verify -m "I have a reason"')
assert "--no-verify bypasses the OTHER tree's red gate too" 0 "${wt_out%%|*}"

rm -rf "$session_green" "$commit_red" "$session_red" "$commit_green"

# ---------------------------------------------------------------------------------------------
# inject-merge-checklist.sh
# ---------------------------------------------------------------------------------------------

echo
echo "--- inject-merge-checklist.sh ---"

# The prompt reaches the JSON builder on STDIN, never as argv - the harness has the same ~128 KiB
# MAX_ARG_STRLEN ceiling as the hook it is testing, and a test that dies building its own fixture
# would report the hook broken (or fixed) for reasons that have nothing to do with the hook.
injected() { # <prompt> -> YES | NO
    local out
    out=$(printf '%s' "$1" \
        | python3 -c 'import json,sys; print(json.dumps({"prompt":sys.stdin.read()}))' \
        | CLAUDE_PROJECT_DIR="$REPO_ROOT" "$HOOKS/inject-merge-checklist.sh" 2>/dev/null)
    [ -n "$out" ] && echo YES || echo NO
}

assert "merge-prep prompt: 'ready to merge?'"          YES "$(injected 'is this ready to merge?')"
assert "merge-prep prompt: 'squash it'"                YES "$(injected 'squash it and land it')"
assert "merge-prep prompt: 'tidy up the commits'"      YES "$(injected 'can you tidy up the commits')"
assert "merge-prep prompt: 'rebase onto master'"       YES "$(injected 'rebase onto master please')"
assert "unrelated prompt: a test failure"              NO  "$(injected 'why is ShardTest failing under load')"
assert "unrelated prompt: reading code"                NO  "$(injected 'explain how offset encoding works')"
assert "unrelated prompt: emerge is not merge"         NO  "$(injected 'the pattern should emerge from the data')"

# NEUTRALITY, ENFORCED. docs/merge-checklist.md is the single source; the hook is a delivery
# mechanism. An earlier version also prepended its own summary of the checklist's two standing
# asks - a second copy, in the one place nobody would think to look for drift. This asserts the
# injected text is the file's own bytes plus a pointer, and nothing else.
body=$(python3 -c 'import json,sys; print(json.dumps({"prompt":"ready to merge?"}))' \
    | CLAUDE_PROJECT_DIR="$REPO_ROOT" "$HOOKS/inject-merge-checklist.sh" 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["hookSpecificOutput"]["additionalContext"])')
checklist_head=$(head -1 "$REPO_ROOT/docs/merge-checklist.md" 2>/dev/null)
case "$body" in
    *"$checklist_head"*) got=verbatim ;;
    *)                   got=missing ;;
esac
assert "the checklist is injected verbatim from its own file" verbatim "$got"

preamble=${body%%$'\n'*}
if [ "${#preamble}" -le 200 ]; then got=pointer_only; else got="carries its own advice (${#preamble} chars)"; fi
# A hook payload carries the whole prompt. Passed as argv it hit Linux's ~128 KiB MAX_ARG_STRLEN
# and died with "Argument list too long" BEFORE python started - so on exactly the long, pasted-in
# prompts a human is most likely to be mid-decision on, the checklist silently did not appear.
big_prompt="ready to merge $(head -c 150000 /dev/zero | tr '\0' 'x')"
assert "a 150 KB merge-prep prompt is still injected" YES "$(injected "$big_prompt")"

assert "the preamble points at the doc rather than restating it" pointer_only "$got"


# ---------------------------------------------------------------------------
# after-push-check-ci.sh - fires after a push that moved a ref, silent otherwise.
#
# The negative controls matter more than the positive one here: this hook runs on EVERY Bash call
# (no `if:` matcher, because prefix matching would miss `cd worktree && git push`), so a leak means
# every unrelated command carries a CI lecture and the reader learns to skim past it.
# ---------------------------------------------------------------------------
push_hook() { # <json payload> -> prints injected context, or nothing
    printf '%s' "$1" | "$HOOKS/after-push-check-ci.sh" 2>/dev/null | tr -d '\n'
}
fired() { [ -n "$(push_hook "$1")" ] && echo fired || echo silent; }

assert "a real push injects the CI reminder" fired \
    "$(fired '{"tool_input":{"command":"git push -q origin b"},"tool_response":{"stderr":"To github.com:a/b.git\n   a1..b2  b -> b"}}')"

assert "a push behind a cd still fires (the prefix trap this hook avoids)" fired \
    "$(fired '{"tool_input":{"command":"cd /w/t && git push -q origin b"},"tool_response":{"stderr":"   a1..b2  b -> b"}}')"

assert "a non-push Bash call is silent" silent \
    "$(fired '{"tool_input":{"command":"git status"},"tool_response":{"stdout":"clean"}}')"

assert "a push that moved nothing is silent" silent \
    "$(fired '{"tool_input":{"command":"git push"},"tool_response":{"stderr":"Everything up-to-date"}}')"

assert "a dry-run push is silent" silent \
    "$(fired '{"tool_input":{"command":"git push --dry-run origin b"},"tool_response":{"stderr":"To github.com"}}')"

assert "a REJECTED push is silent - no CI started" silent \
    "$(fired '{"tool_input":{"command":"git push origin b"},"tool_response":{"stderr":" ! [rejected]  b -> b (fetch first)"}}')"

assert "a malformed payload never breaks the tool call" silent \
    "$(fired 'not json at all')"

# ---------------------------------------------------------------------------
# after-pr-create-refresh-cache.sh - folds a newly created PR into the in-flight tool's PR cache.
#
# The negative controls carry this one too. It runs on EVERY Bash call, and it SHELLS OUT on a
# match - so a leak is not just noise, it is a `gh pr view` per unrelated command. The three ways it
# must stay silent are: not a pr-create at all; a create that failed, where gh printed no PR URL and
# there is nothing to fold in; and a --dry-run, which creates nothing.
#
# `PC_INFLIGHT_HOOK_TOOL` is not set here on purpose: with no CLAUDE_PROJECT_DIR pointing at a tree
# that has bin/inflight.mjs, the hook exits before running anything, so these cases assert the
# DECISION rather than the refresh. The refresh itself is covered by bin/test-inflight.mjs.
# ---------------------------------------------------------------------------
prcache_hook() { # <json payload> -> prints injected context, or nothing
    printf '%s' "$1" | CLAUDE_PROJECT_DIR=/nonexistent-on-purpose \
        "$HOOKS/after-pr-create-refresh-cache.sh" 2>/dev/null | tr -d '\n'
}
prcache_fired() { [ -n "$(prcache_hook "$1")" ] && echo fired || echo silent; }

assert "a non-create Bash call is silent" silent \
    "$(prcache_fired '{"tool_input":{"command":"git status"},"tool_response":{"stdout":"clean"}}')"

assert "a create behind a cd is still recognised, not prefix-matched away" silent \
    "$(prcache_fired '{"tool_input":{"command":"cd /w && gh pr create --title x"},"tool_response":{"stdout":"https://github.com/a/b/pull/7"}}')"

assert "a create that printed no PR url is silent - nothing was created" silent \
    "$(prcache_fired '{"tool_input":{"command":"gh pr create"},"tool_response":{"stderr":"a pull request for branch already exists"}}')"

assert "a dry-run create is silent" silent \
    "$(prcache_fired '{"tool_input":{"command":"gh pr create --dry-run"},"tool_response":{"stdout":"https://github.com/a/b/pull/7"}}')"

assert "a malformed payload never breaks the tool call" silent \
    "$(prcache_fired 'not json at all')"
# ---------------------------------------------------------------------------------------------
# warn-low-disk.sh
#
# This hook has an unusual failure mode: its correct behaviour on a healthy machine is to print
# NOTHING, which is byte-identical to it being broken, misconfigured, or not running at all. So
# almost every case here forces it to speak, and the one case that asserts silence pairs with a
# case proving the same invocation can be made to talk.
#
# Every case pins PC_DISK_STATE_DIR into this test's own mktemp directory. Without that, the
# throttle state is shared with the live session running the test, and cases would pass or fail
# depending on whether a real warning had fired in the last ten minutes.
#
# The Docker Desktop cases pin PC_DISK_UNAME and a fake disk image, so they exercise the sparse-file
# branch on a Linux CI runner too. They deliberately do NOT fake `stat`: the hook resolves stat
# syntax from the real uname precisely because a forced platform would otherwise read filesystem
# blocks instead of file blocks and get a wrong number rather than an error.
# ---------------------------------------------------------------------------------------------

DISK_HOOK="$HOOKS/warn-low-disk.sh"

# EVERY case pins ALL FOUR thresholds, and each then raises exactly the one it is about. An earlier
# version left the host thresholds at their defaults, so the cases silently depended on how much free
# space the machine running them happened to have - and three of them flipped to failing mid-session
# when this host dropped below the 25 GiB default warn line. A self-test for a disk warner must not
# be a function of the disk. `env` applies assignments in order, so a caller's override wins.
DISK_ALL_QUIET="PC_DISK_HOST_WARN_GIB=0 PC_DISK_HOST_CRIT_GIB=0 PC_DISK_VM_WARN_GIB=0 PC_DISK_VM_CRIT_GIB=0"

disk_band() { # <VAR=value>... -> SILENT | WARN | CRITICAL | MALFORMED | BLOCKED:<rc>
    local out rc state
    state="$(mktemp -d "$TMP/disk.XXXXXX")"
    out="$(echo '{"tool_input":{"command":"echo hi"}}' |
        env PC_DISK_STATE_DIR="$state" $DISK_ALL_QUIET "$@" "$DISK_HOOK" 2>/dev/null)"
    rc=$?
    # A PreToolUse hook exiting non-zero can take the tool call away. This one never may.
    [ "$rc" -ne 0 ] && { echo "BLOCKED:$rc"; return; }
    [ -z "$out" ] && { echo SILENT; return; }
    printf '%s' "$out" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)["hookSpecificOutput"]
except Exception:
    print("MALFORMED"); raise SystemExit
if d.get("permissionDecision") != "allow":
    print("NOT_ALLOW:" + str(d.get("permissionDecision"))); raise SystemExit
ctx = d.get("additionalContext", "")
if ctx.startswith("DISK CRITICAL."):
    print("CRITICAL")
elif ctx.startswith("Disk running low."):
    print("WARN")
else:
    print("UNRECOGNISED:" + ctx[:40])
'
}

disk_context() { # <VAR=value>... -> the additionalContext string, or empty
    local state
    state="$(mktemp -d "$TMP/disk.XXXXXX")"
    echo '{"tool_input":{"command":"echo hi"}}' |
        env PC_DISK_STATE_DIR="$state" $DISK_ALL_QUIET "$@" "$DISK_HOOK" 2>/dev/null |
        python3 -c 'import json,sys
try: print(json.load(sys.stdin)["hookSpecificOutput"]["additionalContext"])
except Exception: pass'
}

# A fake Docker Desktop: an empty disk image (so allocated rounds to 0 GiB) plus a settings file
# naming the ceiling, which makes free-space arithmetic exact regardless of the host's real disk.
make_fake_docker() { # <ceiling-MiB> -> directory
    local dir; dir="$(mktemp -d "$TMP/dockerfake.XXXXXX")"
    : >"$dir/Docker.raw"
    printf '{"DiskSizeMiB": %s}\n' "$1" >"$dir/settings.json"
    echo "$dir"
}

# The default state of a working machine. Paired with the forced cases below, which prove the same
# call path can be made to speak - silence here is therefore a verdict, not an absence.
assert "a disk above every threshold says nothing at all" SILENT "$(disk_band)"

# Negative controls: the guard must be able to fire, in both bands.
assert "host below the warn threshold warns" \
    WARN "$(disk_band PC_DISK_HOST_WARN_GIB=99999999)"
assert "host below the critical threshold escalates" \
    CRITICAL "$(disk_band PC_DISK_HOST_CRIT_GIB=99999999)"

# The single most important property: a disk warner that blocked Bash would remove the commands
# needed to clear the disk. Asserted at the worst band, where the temptation to block would be
# greatest.
assert "even at critical it allows the call" \
    CRITICAL "$(disk_band PC_DISK_HOST_CRIT_GIB=99999999)"

# A bogus CLAUDE_PROJECT_DIR falls back to the working directory and still measures something real.
# Asserted so the fallback is a documented decision rather than an accident nobody noticed.
assert "a bogus project dir falls back to the working directory" \
    WARN "$(disk_band CLAUDE_PROJECT_DIR=/nonexistent/path/that/cannot/exist PC_DISK_HOST_WARN_GIB=99999999)"

# ...but when the disk CANNOT be read, silence is the only honest answer. The failure this rules
# out is the dangerous one: a hook that fails to measure and therefore says nothing is byte-identical
# to a hook reporting a healthy disk, so the guard must be the ABSENCE of a reading, never a default
# of "fine". Shadowing `df` on PATH asks exactly that question - an earlier version of this case
# emptied PATH entirely, which stopped `/usr/bin/env bash` finding an interpreter and tested only
# that a script which never ran printed nothing.
shadow_df() { # <body> -> a PATH prefix whose `df` behaves as given
    local dir; dir="$(mktemp -d "$TMP/shadow.XXXXXX")"
    { echo '#!/bin/sh'; echo "$1"; } >"$dir/df"
    chmod +x "$dir/df"
    echo "$dir"
}

for scenario in "exit 1::a df that fails" "echo garbage::a df that answers unparseably" "printf ''::a df that answers nothing"; do
    body="${scenario%%::*}"; label="${scenario##*::}"
    dir="$(shadow_df "$body")"
    out="$(echo '{}' | env PATH="$dir:$PATH" \
        PC_DISK_STATE_DIR="$(mktemp -d "$TMP/disk.XXXXXX")" $DISK_ALL_QUIET \
        PC_DISK_HOST_WARN_GIB=99999999 "$DISK_HOOK" 2>/dev/null; echo "rc=$?")"
    case "$out" in
        "rc=0") got=silent_and_clean ;;
        rc=*)   got="non-zero exit: $out" ;;
        *)      got="claimed a reading it never took: $out" ;;
    esac
    assert "$label leaves it silent, exiting 0" silent_and_clean "$got"
done

# The positive control for the three above: the SAME stub mechanism with a plausible answer must
# produce a warning. Without this, all three could be passing because the stub broke the invocation.
dir_ok="$(shadow_df 'echo "Filesystem 1024-blocks Used Available Capacity Mounted"; echo "/dev/fake 100000000 99000000 1048576 99% /"')"
out_ok="$(echo '{}' | env PATH="$dir_ok:$PATH" \
    PC_DISK_STATE_DIR="$(mktemp -d "$TMP/disk.XXXXXX")" $DISK_ALL_QUIET \
    PC_DISK_HOST_WARN_GIB=25 "$DISK_HOOK" 2>/dev/null)"
case "$out_ok" in *'Host volume: 1 GiB free'*) got=read_the_stub ;; *) got="did not read it: $out_ok" ;; esac
assert "a stubbed df reporting 1 GiB free does warn" read_the_stub "$got"

# THE HIGH-WATER-MARK CORRECTION. Docker Desktop's disk image never shrinks, so after a prune it
# still claims the space. Without this correction the hook nags for days about space that is free.
# The image claims 18 of a 20 GiB ceiling, leaving 2 GiB - comfortably under a realistic 12 GiB
# threshold, so the cheap signal trips. Docker then reports it is really holding 2 GiB, so there are
# 18 GiB actually free and the alarm is stale.
fake="$(make_fake_docker 20480)"                     # a 20 GiB ceiling
state_pruned="$(mktemp -d "$TMP/disk.XXXXXX")"
echo "2GB" >"$state_pruned/docker-df"                # ...of which docker really holds 2 GiB
out_pruned="$(echo '{}' | env \
    PC_DISK_STATE_DIR="$state_pruned" $DISK_ALL_QUIET PC_DISK_UNAME=Darwin \
    PC_DISK_DESKTOP_RAW="$fake/Docker.raw" PC_DISK_DESKTOP_SETTINGS="$fake/settings.json" \
    PC_DISK_VM_ALLOC_GIB=18 PC_DISK_VM_WARN_GIB=12 PC_DISK_VM_CRIT_GIB=5 "$DISK_HOOK" 2>/dev/null)"
[ -z "$out_pruned" ] && got=suppressed || got="warned anyway"
assert "a VM alarm that a prune already cleared is suppressed" suppressed "$got"

# ...and the other half, which is what stops the correction from becoming a blanket mute.
state_full="$(mktemp -d "$TMP/disk.XXXXXX")"
echo "19GB" >"$state_full/docker-df"                 # docker really is holding 19 of 20 GiB
out_full="$(echo '{}' | env \
    PC_DISK_STATE_DIR="$state_full" $DISK_ALL_QUIET PC_DISK_UNAME=Darwin \
    PC_DISK_DESKTOP_RAW="$fake/Docker.raw" PC_DISK_DESKTOP_SETTINGS="$fake/settings.json" \
    PC_DISK_VM_ALLOC_GIB=18 PC_DISK_VM_WARN_GIB=12 PC_DISK_VM_CRIT_GIB=0 "$DISK_HOOK" 2>/dev/null)"
case "$out_full" in *'~1 GiB headroom of 20 GiB'*) got=reported ;; *) got="not reported: $out_full" ;; esac
assert "a VM genuinely near full still warns, with the corrected figure" reported "$got"

# THE CEILING PARSE, against the shape the real file has. `settings-store.json` is a large object
# with many keys, not the single-key fixture above, and the parse this replaced leaned on
# `grep -o '[0-9]*$'` - a pattern that can also match the empty string at end of line, which is
# implementation-defined territory. GNU grep 3.11 emits one match; ugrep 7.5.0 emits two, and the
# trailing `awk` then ran its `printf` twice and concatenated "20" and "0" into "200". A ceiling ten
# times too large is not a visible error: free space is DERIVED from it, so the hook simply stops
# warning about the Docker disk. Nothing in the fixtures could see that, because the fixture had one
# key and the box had GNU grep.
fake_multi="$(mktemp -d "$TMP/dockerfake.XXXXXX")"
: >"$fake_multi/Docker.raw"
printf '%s\n' '{"AutoStart":false,"DiskSizeMiB":20480,"MemoryMiB":8192}' >"$fake_multi/settings.json"
ctx_multi="$(disk_context PC_DISK_UNAME=Darwin \
    PC_DISK_DESKTOP_RAW="$fake_multi/Docker.raw" PC_DISK_DESKTOP_SETTINGS="$fake_multi/settings.json" \
    PC_DISK_VM_ALLOC_GIB=19 PC_DISK_VM_WARN_GIB=12 PC_DISK_VM_CRIT_GIB=0 PC_DISK_HOST_WARN_GIB=99999999)"
case "$ctx_multi" in
    *'~1 GiB headroom of 20 GiB'*) got=ceiling_20 ;;
    *)                             got="wrong ceiling: $ctx_multi" ;;
esac
assert "the ceiling is read from a realistic multi-key settings line" ceiling_20 "$got"

# `docker system df` reports logical sizes that double-count shared layers, so corrected usage can
# exceed the ceiling. The user must never be shown a negative headroom.
state_over="$(mktemp -d "$TMP/disk.XXXXXX")"
echo "500GB" >"$state_over/docker-df"
out_over="$(echo '{}' | env \
    PC_DISK_STATE_DIR="$state_over" $DISK_ALL_QUIET PC_DISK_UNAME=Darwin \
    PC_DISK_DESKTOP_RAW="$fake/Docker.raw" PC_DISK_DESKTOP_SETTINGS="$fake/settings.json" \
    PC_DISK_VM_ALLOC_GIB=18 PC_DISK_VM_WARN_GIB=12 PC_DISK_VM_CRIT_GIB=5 "$DISK_HOOK" 2>/dev/null)"
case "$out_over" in *-[0-9]*GiB*) got="negative headroom shown" ;; *) got=clamped ;; esac
assert "over-reported docker usage cannot print negative headroom" clamped "$got"

# THROTTLE. Firing on every Bash call is what makes the warning cheap to ignore.
state_throttle="$(mktemp -d "$TMP/disk.XXXXXX")"
first="$(echo '{}' | env PC_DISK_STATE_DIR="$state_throttle" $DISK_ALL_QUIET PC_DISK_HOST_WARN_GIB=99999999 "$DISK_HOOK" 2>/dev/null)"
second="$(echo '{}' | env PC_DISK_STATE_DIR="$state_throttle" $DISK_ALL_QUIET PC_DISK_HOST_WARN_GIB=99999999 "$DISK_HOOK" 2>/dev/null)"
[ -n "$first" ] && [ -z "$second" ] && got=throttled || got="first=${#first} second=${#second}"
assert "the same band does not repeat on the next call" throttled "$got"

# ...but getting worse must beat the throttle, or the escalation nobody wants to miss is the one
# guaranteed to be swallowed.
third="$(echo '{}' | env PC_DISK_STATE_DIR="$state_throttle" $DISK_ALL_QUIET PC_DISK_HOST_CRIT_GIB=99999999 "$DISK_HOOK" 2>/dev/null)"
case "$third" in *'DISK CRITICAL.'*) got=escalated ;; *) got="swallowed" ;; esac
assert "worsening from warn to critical speaks through the throttle" escalated "$got"

# ...and never the other way round. After a critical warning, a reading that has merely eased back to
# `warn` must stay quiet until the timer expires - re-announcing the same disk in gentler language
# reads as "it got better", which is the one thing a figure this coarse must not be allowed to say.
# Added because a mutant deleting exactly that arm of the throttle condition survived the whole
# suite: every other case here drives the band UPWARD, so the downgrade rule was asserted nowhere.
# `$state_throttle` now records `critical` from the case above, which is the state this needs.
fourth="$(echo '{}' | env PC_DISK_STATE_DIR="$state_throttle" $DISK_ALL_QUIET PC_DISK_HOST_WARN_GIB=99999999 "$DISK_HOOK" 2>/dev/null)"
[ -z "$fourth" ] && got=stayed_quiet || got="downgraded aloud: $fourth"
assert "a downgrade from critical to warn stays quiet inside the window" stayed_quiet "$got"

# PER-SESSION THROTTLE. The stamp was one file per UID, so every concurrent session shared it and the
# first agent to notice silenced all the others for the whole window. In the incident this hook was
# written for - eleven agents taking the host volume from ample to 8.8 GiB in about an hour - ten of
# them could not have been told, while they were the ten still filling the disk. Two reviewers
# flagged it independently, and the keying was settled as a product call: warn every session, and let
# the OPERATOR be the gate against duplicate cleanups rather than the throttle. That only holds
# because the message tells each agent to report and suggest - asserted below, because a regression
# there is silent: the hook would still warn, and eleven agents would each start pruning.
#
# The session-less fallback is not re-asserted here; the cases above pipe `{}` and prove it, since an
# absent `session_id` is what makes them share one stamp at all.
state_sessions="$(mktemp -d "$TMP/disk.XXXXXX")"
disk_session() { # <session-id> [VAR=value...] -> one invocation sharing $state_sessions
    local sid="$1"; shift
    printf '{"session_id":"%s","tool_input":{"command":"echo hi"}}' "$sid" |
        env PC_DISK_STATE_DIR="$state_sessions" $DISK_ALL_QUIET "$@" "$DISK_HOOK" 2>/dev/null
}
a_first="$(disk_session sess-aaa PC_DISK_HOST_WARN_GIB=99999999)"
b_first="$(disk_session sess-bbb PC_DISK_HOST_WARN_GIB=99999999)"
a_again="$(disk_session sess-aaa PC_DISK_HOST_WARN_GIB=99999999)"
[ -n "$a_first" ] && [ -n "$b_first" ] && [ -z "$a_again" ] &&
    got=per-session || got="a1=${#a_first} b1=${#b_first} a2=${#a_again}"
assert "a second session is warned while the first stays throttled" per-session "$got"

# The id reaches a FILE PATH, so it is FILTERED rather than escaped or rejected - `tr -cd` deletes
# the traversal and leaves a usable key. So this asserts both halves: the hook still speaks, and
# nothing landed outside the state directory it has established it owns.
esc_dir="$(mktemp -d "$TMP/disk.XXXXXX")"
esc_out="$(printf '{"session_id":"../../pwned-%s","tool_input":{"command":"echo hi"}}' "$$" |
    env PC_DISK_STATE_DIR="$esc_dir" $DISK_ALL_QUIET PC_DISK_HOST_WARN_GIB=99999999 "$DISK_HOOK" 2>/dev/null)"
stray="$(find "$esc_dir/.." -maxdepth 1 -name 'pwned-*' 2>/dev/null | head -1)"
[ -n "$esc_out" ] && [ -z "$stray" ] && got=contained || got="spoke=${#esc_out} stray=$stray"
assert "a traversal in session_id cannot write outside the state dir" contained "$got"

disk_ctx() { # <hook output> -> the additionalContext string
    printf '%s' "$1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["hookSpecificOutput"]["additionalContext"])'
}
case "$(disk_ctx "$(disk_session sess-msg PC_DISK_HOST_WARN_GIB=99999999)")" in
    *"Do NOT reclaim anything yourself"*"report and suggest, never act"*) got=defers_to_operator ;;
    *) got="did not defer" ;;
esac
assert "the warn message tells the agent to report, not reclaim" defers_to_operator "$got"

case "$(disk_ctx "$(disk_session sess-crit PC_DISK_HOST_CRIT_GIB=99999999)")" in
    *"Do NOT run any reclaim command yourself"*"report and suggest, never act"*) got=defers_to_operator ;;
    *) got="did not defer" ;;
esac
assert "the critical message tells the agent to report, not reclaim" defers_to_operator "$got"

# SPEAK, THEN STAMP. Stamping first means a kill between the write and the message swallows that
# warning for the whole window - likeliest exactly when the disk is full, which is this hook's entire
# subject. No test can kill the hook mid-flight, so this asserts the ORDER IN THE SOURCE, which is
# the thing that was actually wrong. Structural, like the settings.json cases further down, and for
# the same reason: what it protects has no observable difference until the moment it matters.
emit_line="$(grep -n '^printf' "$DISK_HOOK" | tail -1 | cut -d: -f1)"
stamp_line="$(grep -n '^echo "\$now \$band"' "$DISK_HOOK" | tail -1 | cut -d: -f1)"
[ -n "$emit_line" ] && [ -n "$stamp_line" ] && [ "$stamp_line" -gt "$emit_line" ] &&
    got=stamps_after_speaking || got="emit=${emit_line:-none} stamp=${stamp_line:-none}"
assert "the throttle stamp is written after the message, not before" stamps_after_speaking "$got"

# CROSS-PLATFORM. A platform whose Docker layout is unknown still reports the host volume, and must
# not invent or imply a Docker figure it never read.
ctx_unknown="$(disk_context PC_DISK_UNAME=Plan9 PC_DISK_HOST_WARN_GIB=99999999)"
case "$ctx_unknown" in
    *'Host volume:'*'Docker'*) got="claims a docker reading" ;;
    *'Host volume:'*)          got=host_only ;;
    *)                         got="no host reading: $ctx_unknown" ;;
esac
assert "an unknown platform reports the host volume and no Docker figure" host_only "$got"

# On Linux the engine writes into the host filesystem, so when that is the SAME mount the host
# check already covers it and a second figure would be noise.
#
# Both paths are pinned INSIDE this test's own directory, as two DIFFERENT paths, so they share a
# filesystem by construction on any host while still forcing the hook to compare devices rather than
# strings. The first version left the project dir at the real one and pointed only the docker root at
# `$TMP`, assuming `mktemp` lands on the project's filesystem. It does not wherever `/tmp` is a tmpfs
# - this box included - so the case drove the SEPARATE-mount branch and asserted the opposite of its
# own name. Right about what to check, wrong about its scope, with every sibling case green
# throughout: the failure docs/agent-harness.md rule 3 was extended to ask about.
same_mount="$(mktemp -d "$TMP/samemount.XXXXXX")"
mkdir -p "$same_mount/project" "$same_mount/docker"
ctx_linux_same="$(disk_context PC_DISK_UNAME=Linux CLAUDE_PROJECT_DIR="$same_mount/project" \
    PC_DISK_DOCKER_ROOT="$same_mount/docker" PC_DISK_HOST_WARN_GIB=99999999)"
case "$ctx_linux_same" in
    *'Docker data filesystem'*) got="reported twice" ;;
    *'Host volume:'*)           got=not_duplicated ;;
    *)                          got="no host reading: $ctx_linux_same" ;;
esac
assert "Linux docker root on the project's own mount is not reported twice" not_duplicated "$got"

# The GREEN half of that pair, and the reason the case above cannot stand alone: delete the Linux
# branch from the hook entirely and "not reported twice" still passes. Which device a path sits on
# cannot be arranged portably - `/tmp` is a tmpfs here and part of `/` elsewhere - so `df`'s device
# column is stubbed rather than hunted for, and the stub answers per path.
two_dev="$(shadow_df 'for p in "$@"; do last="$p"; done
case "$last" in
    *dockerroot*) dev=/dev/docker ;;
    *)            dev=/dev/project ;;
esac
echo "Filesystem 1024-blocks Used Available Capacity Mounted on"
echo "$dev 100000000 99000000 2097152 99% /"')"
mkdir -p "$TMP/dockerroot"
ctx_linux_split="$(disk_context PATH="$two_dev:$PATH" PC_DISK_UNAME=Linux \
    PC_DISK_DOCKER_ROOT="$TMP/dockerroot" PC_DISK_HOST_WARN_GIB=99999999)"
case "$ctx_linux_split" in
    *'Host volume: 2 GiB free.'*'Docker data filesystem: 2 GiB free.'*) got=both_reported ;;
    *) got="not reported separately: $ctx_linux_split" ;;
esac
assert "Linux docker root on a SEPARATE mount is reported alongside the host" both_reported "$got"

# THE STATE FILE IS INPUT, NOT OUR OWN DATA. It sits at a predictable path in a shared /tmp and is
# read back on a LATER run, so its content is untrusted in exactly the way a `df` answer is. Every
# case above pins a fresh `PC_DISK_STATE_DIR`, so the stamp was always absent or hook-written and
# this read was never exercised against anything else - which is why a green suite sat on top of an
# arbitrary-command-execution bug and a broken never-exit-non-zero invariant.
#
# `last_at` reaches `$(( ))`, and bash arithmetic resolves a non-numeric operand as a variable NAME,
# recursively - so a command substitution inside an array subscript RUNS. The shapes below are the
# ones that mattered: hostile, merely garbled, from the future, and genuinely fine.
stamp_case() { # <stamp-content> -> SILENT | WARNED | BLOCKED:<rc>
    local dir out rc
    dir="$(mktemp -d "$TMP/disk.XXXXXX")"
    printf '%s\n' "$1" >"$dir/last-warning"
    out="$(echo '{}' | env PC_DISK_STATE_DIR="$dir" $DISK_ALL_QUIET \
        PC_DISK_HOST_WARN_GIB=99999999 "$DISK_HOOK" 2>/dev/null)"
    rc=$?
    [ "$rc" -ne 0 ] && { echo "BLOCKED:$rc"; return; }
    [ -n "$out" ] && echo WARNED || echo SILENT
}

# THE PAYLOAD MUST BE SPACE-FREE, and that is the whole trick. `read -r last_at last_band` splits on
# whitespace, so `band[$(touch /path)] warn` lands in `last_at` as `band[$(touch` - a truncated
# subscript that is a syntax error rather than a command substitution, and it cannot execute whatever
# the guard does. The first version of this case used exactly that and was therefore decorative: it
# passed with both guards deleted. A redirect needs no space, so `id>FILE` is the shape that really
# reaches the evaluator - it is the payload that executed against the unguarded hook.
pwn_dir="$(mktemp -d "$TMP/disk.XXXXXX")"
pwn_marker="$pwn_dir/EXECUTED"
printf 'band[$(id>%s)] warn\n' "$pwn_marker" >"$pwn_dir/last-warning"
echo '{}' | env PC_DISK_STATE_DIR="$pwn_dir" $DISK_ALL_QUIET PC_DISK_HOST_WARN_GIB=99999999 \
    "$DISK_HOOK" >/dev/null 2>&1
[ -e "$pwn_marker" ] && got="EXECUTED IT" || got=inert
assert "a command substitution in the throttle stamp is not executed" inert "$got"

# ...and the same payload must not take the Bash call away either. A `set -u` abort is a NON-ZERO
# exit, the one thing this hook must never do - so this asserts the invariant against hostile STATE,
# where every other never-block case asserts it only against readings the hook took itself. The
# garbled variants need no attacker at all: a torn write is likeliest when the disk is full, which
# is the condition this hook exists for.
assert "a hostile throttle stamp still warns, exiting 0"   WARNED "$(stamp_case 'band[$(id)] warn')"
assert "a garbled throttle stamp still warns, exiting 0"   WARNED "$(stamp_case 'garbage warn')"
assert "a one-field throttle stamp still warns, exiting 0" WARNED "$(stamp_case 'warn')"
# A stamp from the future - a clock step, an NTP correction, a resumed VM - makes the age negative,
# which is always inside the window, holding the warner shut until wall-clock time catches up.
assert "a future-dated throttle stamp is treated as stale" WARNED "$(stamp_case '9999999999 warn')"
# The control that stops the four above from passing on a hook that ignores the stamp entirely.
assert "a fresh same-band stamp still throttles"           SILENT "$(stamp_case "$(date +%s) warn")"

# `mkdir -p` SUCCEEDS against a directory another user already owns, so it is not the check it looks
# like - and everything in that directory is then read back by this hook. `/` stands in for a
# pre-created state dir: it exists, `mkdir -p` returns 0 on it, and it is root-owned on every machine
# this runs on. Skipped rather than silently inverted where that last assumption does not hold.
if [ -O / ]; then
    echo "skip: foreign state-dir case - this process owns /, so no foreign directory is available"
else
    out_foreign="$(echo '{}' | env PC_DISK_STATE_DIR=/ $DISK_ALL_QUIET \
        PC_DISK_HOST_WARN_GIB=99999999 "$DISK_HOOK" 2>/dev/null; echo "rc=$?")"
    case "$out_foreign" in
        "rc=0") got=refused_silently ;;
        rc=*)   got="non-zero exit: $out_foreign" ;;
        *)      got="used a foreign state dir: $out_foreign" ;;
    esac
    assert "a state directory owned by another user is refused, exiting 0" refused_silently "$got"
fi

# `$HOME` is expanded on the Docker Desktop branch, and an unset one aborts under `set -u` - a
# non-zero exit, which takes the Bash call away. `env -u` is the only way to reach it, so this case
# cannot go through disk_context.
out_nohome="$(echo '{}' | env -u HOME PC_DISK_STATE_DIR="$(mktemp -d "$TMP/disk.XXXXXX")" \
    $DISK_ALL_QUIET PC_DISK_UNAME=Darwin PC_DISK_HOST_WARN_GIB=99999999 "$DISK_HOOK" 2>/dev/null)"
rc_nohome=$?
if [ "$rc_nohome" -eq 0 ] && [ -n "$out_nohome" ]; then got=warned_and_clean
else got="rc=$rc_nohome bytes=${#out_nohome}"; fi
assert "an unset HOME on the Docker Desktop branch still warns, exiting 0" warned_and_clean "$got"

# THE COLUMN PARSE. `df -P` puts Available immediately before Capacity, but nothing guarantees the
# DEVICE column is one word - macOS autofs reports `map auto_home`, and a CIFS share can carry a
# space too. Counting fields from the left then read the USED column as free space: a confident
# wrong number in the dangerous direction, since Used is largest exactly when free is smallest.
dir_spaced="$(shadow_df 'echo "Filesystem 1024-blocks Used Available Capacity Mounted on"; echo "map auto_home 100000000 99000000 1048576 99% /home"')"
ctx_spaced="$(disk_context PATH="$dir_spaced:$PATH" PC_DISK_HOST_WARN_GIB=99999999)"
case "$ctx_spaced" in
    *'Host volume: 1 GiB free'*)  got=read_available ;;
    *'Host volume: 94 GiB free'*) got="read the USED column" ;;
    *)                            got="unexpected: $ctx_spaced" ;;
esac
assert "a df device name containing a space does not shift the free-space column" read_available "$got"

# ...and a well-formed line whose Available column is not a number must be silence, not zero. The
# three `df`-unreadable cases above never emit a SECOND line, so this gate was never reached.
dir_nan="$(shadow_df 'echo "Filesystem 1024-blocks Used Available Capacity Mounted on"; echo "/dev/fake 100000000 99000000 N/A 99% /"')"
out_nan="$(echo '{}' | env PATH="$dir_nan:$PATH" \
    PC_DISK_STATE_DIR="$(mktemp -d "$TMP/disk.XXXXXX")" $DISK_ALL_QUIET \
    PC_DISK_HOST_WARN_GIB=99999999 "$DISK_HOOK" 2>/dev/null; echo "rc=$?")"
case "$out_nan" in
    "rc=0") got=silent_and_clean ;;
    rc=*)   got="non-zero exit: $out_nan" ;;
    *)      got="claimed a reading from a non-numeric column: $out_nan" ;;
esac
assert "a df whose Available column is not a number leaves it silent" silent_and_clean "$got"

# THE CORRECTION MUST NOT TOUCH A LINUX READING. `vm_is_high_water` is set only on the Docker Desktop
# sparse-image branch; Linux's figure is LIVE and needs no confirming. Nothing exercised that gate on
# the branch it protects - delete it and only the Desktop cases notice. Asserted two ways: the figure
# reported is the live one, and no `docker system df` cache is created at all.
live_dev="$(shadow_df 'for p in "$@"; do last="$p"; done
case "$last" in
    *dockerroot*) dev=/dev/docker;  avail=1048576 ;;
    *)            dev=/dev/project; avail=104857600 ;;
esac
echo "Filesystem 1024-blocks Used Available Capacity Mounted on"
echo "$dev 209715200 104857600 $avail 50% /"')"
state_live="$(mktemp -d "$TMP/disk.XXXXXX")"
ctx_live="$(echo '{}' | env PATH="$live_dev:$PATH" PC_DISK_STATE_DIR="$state_live" \
    $DISK_ALL_QUIET PC_DISK_UNAME=Linux PC_DISK_DOCKER_ROOT="$TMP/dockerroot" \
    PC_DISK_VM_WARN_GIB=12 "$DISK_HOOK" 2>/dev/null |
    python3 -c 'import json,sys
try: print(json.load(sys.stdin)["hookSpecificOutput"]["additionalContext"])
except Exception: pass')"
case "$ctx_live" in
    *'Docker data filesystem: 1 GiB free'*) got=live_reading ;;
    *)                                      got="not the live reading: $ctx_live" ;;
esac
assert "a Linux docker filesystem is reported from its live df reading" live_reading "$got"
[ -e "$state_live/docker-df" ] && got="consulted docker" || got=no_docker_call
assert "the high-water correction never runs for a Linux live reading" no_docker_call "$got"

# THE REFRESH HALF OF THE CORRECTION. Every case above pre-seeds `docker-df`, so `command -v docker`,
# the `docker system df` call and the cache write were all dead code as far as the suite knew. This
# drives them: no cache, a stubbed `docker`, and the suppression can only happen if the stub ran.
docker_stub="$(mktemp -d "$TMP/shadow.XXXXXX")"
{ echo '#!/bin/sh'; echo 'echo 2GB'; } >"$docker_stub/docker"
chmod +x "$docker_stub/docker"
state_refresh="$(mktemp -d "$TMP/disk.XXXXXX")"
out_refresh="$(echo '{}' | env PATH="$docker_stub:$PATH" PC_DISK_STATE_DIR="$state_refresh" \
    $DISK_ALL_QUIET PC_DISK_UNAME=Darwin \
    PC_DISK_DESKTOP_RAW="$fake/Docker.raw" PC_DISK_DESKTOP_SETTINGS="$fake/settings.json" \
    PC_DISK_VM_ALLOC_GIB=18 PC_DISK_VM_WARN_GIB=12 "$DISK_HOOK" 2>/dev/null)"
if [ -z "$out_refresh" ] && [ -s "$state_refresh/docker-df" ]; then got=refreshed_and_suppressed
else got="cache=$([ -s "$state_refresh/docker-df" ] && echo written || echo missing) bytes=${#out_refresh}"; fi
assert "an absent docker-df cache is refreshed from docker itself" refreshed_and_suppressed "$got"

# A FULLY PRUNED DOCKER IS A READING, NOT AN ABSENCE. The correction used to fire only when the total
# was ABOVE zero, which conflated "docker holds nothing" with "we could not read docker" - so the one
# case where the sparse file is most wrong, an image emptied completely, was the one case the
# correction refused to clear. It nagged until the file itself shrank, which it never does.
state_pruned_zero="$(mktemp -d "$TMP/disk.XXXXXX")"
printf '0B\n0B\n0B\n' >"$state_pruned_zero/docker-df"
out_zero="$(echo '{}' | env PC_DISK_STATE_DIR="$state_pruned_zero" $DISK_ALL_QUIET \
    PC_DISK_UNAME=Darwin PC_DISK_DESKTOP_RAW="$fake/Docker.raw" \
    PC_DISK_DESKTOP_SETTINGS="$fake/settings.json" \
    PC_DISK_VM_ALLOC_GIB=18 PC_DISK_VM_WARN_GIB=12 "$DISK_HOOK" 2>/dev/null)"
[ -z "$out_zero" ] && got=suppressed || got="nagged about an empty image: $out_zero"
assert "a fully pruned Docker clears the stale VM alarm" suppressed "$got"

# ...and a reading we could NOT parse must not be treated as "docker holds nothing", which would
# suppress a real warning. An unrecognised unit disqualifies the whole reading.
state_unknown="$(mktemp -d "$TMP/disk.XXXXXX")"
printf '19GB\nnonsense\n' >"$state_unknown/docker-df"
out_unknown="$(echo '{}' | env PC_DISK_STATE_DIR="$state_unknown" $DISK_ALL_QUIET \
    PC_DISK_UNAME=Darwin PC_DISK_DESKTOP_RAW="$fake/Docker.raw" \
    PC_DISK_DESKTOP_SETTINGS="$fake/settings.json" \
    PC_DISK_VM_ALLOC_GIB=18 PC_DISK_VM_WARN_GIB=12 "$DISK_HOOK" 2>/dev/null)"
[ -n "$out_unknown" ] && got=warned_from_high_water || got="suppressed on an unreadable reading"
assert "an unparseable docker-df row does not suppress the warning" warned_from_high_water "$got"

# REGISTRATION. docs/agent-harness.md names "unregistered" as one of the three states byte-identical
# to a healthy hook, and a self-test genuinely cannot prove the harness INVOKES one - but that the
# tracked settings.json NAMES it is mechanically checkable, and nothing checked it. This asserts both
# directions, so a hook added to either side without the other goes red, and it pins the disk hook's
# two registration properties: present as a PreToolUse hook, and deliberately unfiltered.
# NO HEREDOC INSIDE A COMMAND SUBSTITUTION - this ran as `$(python3 - <<REGCHECK ... )` and broke
# bash 3.2, which macOS ships and which the shell: macos lane in .github/workflows/repo-hygiene.yml
# pins on purpose. That parser scans a substitution for its closing paren treating the heredoc body
# as SHELL TEXT, and it does not recognise `#` comments while doing it - so an ordinary apostrophe in
# a python comment (a possessive, in a sentence about an earlier count) was the fifth and unbalancing
# single quote in the body, and 3.2 then read to the end of the file looking for the close. It landed
# as an unexpected-EOF error against the last line of the file and exit 2, after some sixty cases had
# already run, so the lane said the suite could not run rather than naming a case - while bash 5
# parses the same text correctly and every ubuntu lane and every local run stayed green.
#
# Three other python heredocs inside `$( ... )` survive on master - check-cve-exclusions.sh,
# check-ossindex-audit.sh, check-shell-hazards.sh - which is not a counter-example, just bodies whose
# quotes happen to balance when read as shell. Heredoc at top level into a file, then run the file:
# the body is never scanned as shell at all, so no later edit to the python can bring this back. The
# python itself is unchanged. bin/check-shell-hazards.sh is the right long-term home for the class;
# a row there would report those three, so it is queued in the inflight note rather than done here.
cat >"$TMP/regcheck.py" <<'REGCHECK'
import json, re, sys, pathlib
root = pathlib.Path(sys.argv[1])
cfg = json.loads((root / ".claude/settings.json").read_text())
registered = {h["command"].rsplit("/", 1)[-1].rstrip('"')
              for groups in cfg["hooks"].values() for g in groups for h in g["hooks"]}
# TWO SELF-TEST SHAPES COUNT, because the repo has two. Most hooks get a section in THIS suite,
# referenced as `$HOOKS/<name>`. A hook may instead get its own `bin/test-<name>.sh` - the shape
# master introduced with check-shallow-history.sh - and that is equally real coverage, because
# `bin/check-all.sh --with-tests` globs `bin/test-*.sh`, so such a file runs in CI with no wiring.
# Recognising only the first shape reported a genuinely self-tested hook as untested, which would
# have pushed the next author to satisfy the assertion rather than the property.
# A LEADING-COMMENT MENTION DOES NOT COUNT: `# Self-test for .claude/hooks/foo.sh` is prose, and
# accepting it would let a hook buy coverage with a sentence. The path must appear in code.
covered = set(re.findall(r"\$HOOKS/([a-z0-9-]+\.sh)",
                         (root / "bin/test-check-agent-hooks.sh").read_text()))
for selftest in sorted((root / "bin").glob("test-*.sh")):
    code = "\n".join(l for l in selftest.read_text().splitlines()
                     if not l.lstrip().startswith("#"))
    covered |= set(re.findall(r"\.claude/hooks/([a-z0-9-]+\.sh)", code))
disk = [h for g in cfg["hooks"].get("PreToolUse", []) for h in g["hooks"]
        if h["command"].rstrip('"').endswith("warn-low-disk.sh")]
problems = []
registrations = sum(len(g["hooks"]) for groups in cfg["hooks"].values() for g in groups)
# The doc states these in prose, and they have now drifted THREE times: it said "five" against seven
# registered; this branch's own first pass incremented it to "six" against eight; and then master
# split the sentence into SCRIPTS and REGISTRATIONS while this check still pinned the older wording,
# so merging the two went red on the doc rather than on either change - which is the check working,
# not failing. A number nobody verifies is a number that rots, so both are verified here rather than
# trusted to the next editor, and both are needed because one script can be registered against
# several events.
# Runs to twenty because a table that stops at the current count turns the next hook into a
# self-test failure whose message reads like the doc is wrong - which is how this list ended one
# short of the number the doc had to state. The literal symptom is worth knowing, because it does
# not look like a missing entry: "says thirteen registrations; settings.json has 13".
WORDS = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7,
         "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13,
         "fourteen": 14, "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18,
         "nineteen": 19, "twenty": 20}
doc = (root / "docs/agent-harness.md").read_text()
m = re.search(r"`\.claude/settings\.json`\*\* - ([a-z]+) hook scripts across ([a-z]+) registrations", doc)
if not m:
    problems.append("docs/agent-harness.md no longer states the settings.json script and registration counts")
else:
    if WORDS.get(m.group(1)) != len(registered):
        problems.append("docs/agent-harness.md says %s hook scripts; settings.json registers %d distinct"
                        % (m.group(1), len(registered)))
    if WORDS.get(m.group(2)) != registrations:
        problems.append("docs/agent-harness.md says %s registrations; settings.json has %d"
                        % (m.group(2), registrations))
if registered - covered:
    problems.append("registered but not self-tested: %s" % sorted(registered - covered))
if covered - registered:
    problems.append("self-tested but not registered: %s" % sorted(covered - registered))
if not disk:
    problems.append("warn-low-disk.sh is not registered as a PreToolUse hook")
elif any("if" in h for h in disk):
    problems.append("warn-low-disk.sh grew an `if` filter")
print("; ".join(problems) if problems else "in_sync")
REGCHECK
registration="$(python3 "$TMP/regcheck.py" "$REPO_ROOT")"
assert "every registered hook is self-tested, and the disk hook is registered unfiltered" in_sync "$registration"

# ---------------------------------------------------------------------------------------------
# inject-recorded-knowledge.sh
#
# It runs at session start with no input to parse, so the risks are different from the other three:
# it must never break a session, and it must actually name the documents. A reminder that silently
# emits nothing is worse than none - it looks installed.
# ---------------------------------------------------------------------------------------------

echo
echo "--- inject-recorded-knowledge.sh ---"

knowledge_out=$(CLAUDE_PROJECT_DIR="$REPO_ROOT" "$HOOKS/inject-recorded-knowledge.sh" 2>/dev/null)
knowledge_rc=$?

assert "exits 0" 0 "$knowledge_rc"

# The point of the hook: a path you can grep for, not a vague nudge to go and look.
case "$knowledge_out" in
    *"docs/solutions/"*) got=names_paths ;;
    *)                   got=no_paths ;;
esac
assert "names actual document paths" names_paths "$got"

# One line per document, or it is not an index. Compare against the real corpus rather than a
# hardcoded number, so adding a solution does not fail this test.
want_count=$(find "$REPO_ROOT/docs/solutions" -name '*.md' -type f 2>/dev/null | wc -l | tr -d ' ')
got_count=$(printf '%s\n' "$knowledge_out" | grep -c 'docs/solutions/.*\.md')
assert "lists every solution document" "$want_count" "$got_count"

# Titles, not slugs: the filename is already in the path, and the title is what makes an agent
# recognise the document as relevant to the thing in front of it.
case "$knowledge_out" in
    *"duplication scanners do not look where agents actually duplicate"*) got=titles ;;
    *) got=slugs_only ;;
esac
assert "uses frontmatter titles, not just filenames" titles "$got"

# A YAML-quoted title must render WITHOUT its quotes. The title-check above happens to use an
# unquoted one, so it passed while quoted titles rendered with literal quote marks - found in review
# on astubbs/parallel-consumer#320. Asserted against whichever document actually needs quoting, so
# the case survives that document being renamed.
quoted=$(grep -rl '^title:[[:space:]]*"' "$REPO_ROOT/docs/solutions" 2>/dev/null | head -1)
if [ -n "$quoted" ]; then
    bare=$(sed -n 's/^title:[[:space:]]*"//p' "$quoted" | head -1 | sed 's/"$//')
    case "$knowledge_out" in
        *"\"${bare}\""*) got=quotes_leaked ;;
        *"$bare"*)         got=quotes_stripped ;;
        *)                 got=title_missing ;;
    esac
    assert "strips YAML quoting from a quoted title" quotes_stripped "$got"
fi

# Open work must be grouped by CONSEQUENCE across every type, in the order docs/inflight/AGENTS.md defines - and
# signal-integrity classes must come FIRST: you cannot judge the code through instruments that lie,
# so `misdirection` before any product defect. Asserted against the FIXTURE corpus built below, not
# the real one - the real-corpus assertion required a live bug/misdirection note to exist on master
# forever, so the PR deleting the last one (deletion is the directory's prescribed lifecycle) would
# have turned Repo Hygiene red for doing its job (astubbs#324 review). The fixture carries one
# well-tagged bug and one feature, so grouping and ordering are asserted on controlled input.

# A note with no class must still appear. A marker someone forgot to add must be VISIBLE, never a
# way for a note to drop silently out of the index - that failure mode is the one the whole hook
# exists to prevent.
class_tmp=$(mktemp -d)
mkdir -p "$class_tmp/docs/inflight" "$class_tmp/docs/solutions/x"
printf -- '---\ntitle: "s"\n---\n' > "$class_tmp/docs/solutions/x/s.md"
printf '# An unclassified note\n' > "$class_tmp/docs/inflight/bug-no-class.md"
printf '# A closed note\n\n<!-- inflight-type: task -->\n<!-- inflight-state: closed - will not do -->\n' > "$class_tmp/docs/inflight/task-closed.md"
# Open + mistagged + prose that MENTIONS `inflight-state:` without carrying the marker. The safety
# net must judge open/closed by the whole `-->` marker exactly as the groups do: its first version
# used a bare substring grep, so this exact shape vanished - not grouped, not unmatched, not counted.
printf '# A prose mention of the marker\n\nThe gate greps for inflight-state: markers.\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: misdirekshun -->\n' > "$class_tmp/docs/inflight/bug-prose-mention.md"
printf '# A well-tagged bug\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: misdirection -->\n' > "$class_tmp/docs/inflight/bug-well-tagged.md"
printf '# A proposed feature\n\n<!-- inflight-type: feature -->\n' > "$class_tmp/docs/inflight/feature-idea.md"
unclassified_out=$(CLAUDE_PROJECT_DIR="$class_tmp" "$HOOKS/inject-recorded-knowledge.sh" 2>/dev/null)

# Grouped by IMPACT across every type, and rendered as REAL markdown headings. Both changed together:
# a feature that exists to prevent a crash has to sort beside the crashes rather than under "proposed
# work", and an agent has to be able to filter the index structurally instead of reading it whole.
# Asserted as ORDER and STRUCTURE, never as one literal heading string - the previous version matched
# `**bug / misdirection**` exactly and so went red the moment either changed, which is what a
# string-matching test does in place of catching a regression.
mis_at=$(grep -n '^## misdirection$' <<<"$unclassified_out" | head -1 | cut -d: -f1)
feat_at=$(grep -n '^## feature - proposed' <<<"$unclassified_out" | head -1 | cut -d: -f1)
if [ -n "$mis_at" ] && [ -n "$feat_at" ] && [ "$mis_at" -lt "$feat_at" ]; then
    got=signal_integrity_first
elif [ -z "$mis_at" ] || [ -z "$feat_at" ]; then
    got=group_missing
else
    got=proposed_work_first
fi
assert "groups open work by impact, signal integrity first" signal_integrity_first "$got"

case "$unclassified_out" in
    *$'\n## '*) got=real_headings ;;
    *)          got=bold_pseudo_headings ;;
esac
assert "groups are markdown headings, not bold text" real_headings "$got"

case "$unclassified_out" in
    *"An unclassified note"*) got=listed ;;
    *)                        got=dropped ;;
esac
assert "a note whose tags match no group is still listed" listed "$got"
case "$unclassified_out" in
    *"A prose mention of the marker"*) got=listed ;;
    *)                                 got=dropped ;;
esac
assert "an open note mentioning inflight-state: in prose is still listed" listed "$got"

# THE WORD ANYWHERE IN THE MARKER, not anchored to the front. `parked - deferred` is as deferred as
# `deferred - parked`; position carries no meaning, and requiring it invented a rule nobody agreed to.
pos_tmp=$(mktemp -d); mkdir -p "$pos_tmp/docs/inflight" "$pos_tmp/docs/solutions/x"
printf -- '---\ntitle: "s"\n---\n' > "$pos_tmp/docs/solutions/x/s.md"
printf '# Deferred with the word at the FRONT\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\n<!-- inflight-state: deferred - parked -->\n' > "$pos_tmp/docs/inflight/ci-front.md"
printf '# Deferred with the word in the MIDDLE\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\n<!-- inflight-state: parked - deferred, gated on a user base -->\n' > "$pos_tmp/docs/inflight/ci-middle.md"
pos_out=$(CLAUDE_PROJECT_DIR="$pos_tmp" "$HOOKS/inject-recorded-knowledge.sh" 2>/dev/null)
def_block=$(sed -n '/^# Deferred/,/^# /p' <<<"$pos_out")
case "$def_block" in *"word at the FRONT"*) got=found ;; *) got=missing ;; esac
assert "a state beginning 'deferred' is deferred" found "$got"
case "$def_block" in *"word in the MIDDLE"*) got=found ;; *) got=missing ;; esac
assert "a state with 'deferred' later is equally deferred" found "$got"

# PARKED IS DEFERRED - the two words name one disposition, so a note using either must land in the
# same section. Before this, `parked` alone matched neither is_open nor is_deferred and fell in with
# closed and blocked under "not shown", which is how notes went missing on astubbs#323.
printf '# Parked with no other keyword\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\n<!-- inflight-state: parked - nobody has argued for it -->\n' > "$pos_tmp/docs/inflight/ci-parked.md"
park_out=$(CLAUDE_PROJECT_DIR="$pos_tmp" "$HOOKS/inject-recorded-knowledge.sh" 2>/dev/null)
park_def=$(awk '/^# Deferred/{f=1;next} f&&/^# /{exit} f' <<<"$park_out")
case "$park_def" in *"Parked with no other keyword"*) got=deferred ;; *) got=stranded ;; esac
assert "a bare 'parked' state is deferred" deferred "$got"

# ...and its reason must survive. Anchoring the extractor to the word `deferred` printed an EMPTY
# reason for every note that said `parked` instead, which is a note in the right section saying nothing.
case "$park_def" in *"nobody has argued for it"*) got=reason_shown ;; *) got=reason_lost ;; esac
assert "a parked note still shows its reason" reason_shown "$got"
rm -rf "$pos_tmp"

# A stated note leaves OPEN WORK but must still be NAMED. Counting was the previous contract and it
# hid two notes tagged `parked - deferred`, which matched neither filter and lost their titles to a
# bare number - the filter making exactly the omission this index claims it cannot make.
open_block=$(sed -n '/^# Open work/,/^# /p' <<<"$unclassified_out")
case "$open_block" in
    *"A closed note"*) got=leaked ;;
    *)                 got=excluded ;;
esac
assert "a closed note is kept out of open work" excluded "$got"

case "$unclassified_out" in
    *"A closed note"*) got=named ;;
    *)                 got=hidden_silently ;;
esac
assert "an excluded note is still named, not just counted" named "$got"
rm -rf "$class_tmp"

# A session must survive a repo that does not look like this one. Two shapes: no docs at all, and
# a docs/ with no solutions - both are "say nothing, exit 0", never a stack trace into the session.
empty_dir=$(mktemp -d)
out_empty=$(CLAUDE_PROJECT_DIR="$empty_dir" "$HOOKS/inject-recorded-knowledge.sh" 2>/dev/null)
assert "a tree with no docs/ exits 0" 0 "$?"
assert "a tree with no docs/ emits nothing" "" "$out_empty"
mkdir -p "$empty_dir/docs"
out_nosol=$(CLAUDE_PROJECT_DIR="$empty_dir" "$HOOKS/inject-recorded-knowledge.sh" 2>/dev/null)
assert "a docs/ with no solutions exits 0" 0 "$?"
assert "a docs/ with no solutions emits nothing" "" "$out_nosol"
rm -rf "$empty_dir"

# check-merge-outstanding-work.sh
#
# The negative controls matter more than the positive one here. The guard's whole risk is that it
# either fires on ordinary commands (and gets routed around) or fails to fire when it counts. The
# substring-vs-token case below is not hypothetical: the first draft grepped for "gh pr merge" and
# blocked `gh pr comment --body "run gh pr merge later"`.
# ---------------------------------------------------------------------------------------------

echo
echo "--- check-merge-outstanding-work.sh ---"

HOOK_UNDER_TEST="$HOOKS/check-merge-outstanding-work.sh"

# Isolate every case from the caller's shell: an exported override there must not flip a DENY case.
unset MERGE_DESPITE_OUTSTANDING_WORK

# A session dir holding a task file written just now == background work in flight.
ow_session="ow-selftest-$$"
ow_tasks="/tmp/claude-$(id -u)/selftest/$ow_session/tasks"
mkdir -p "$ow_tasks"
printf 'still writing\n' > "$ow_tasks/agent-live.output"
export CLAUDE_CODE_SESSION_ID="$ow_session"

expect DENY  "a merge is refused while a task is still writing"            'gh pr merge 31 -R astubbs/parallel-consumer --rebase'
expect DENY  "a merge later in a compound command is still seen"           'echo hi && gh pr merge 31 --squash'
expect DENY  "a full-path gh is still a merge"                             '/usr/local/bin/gh pr merge 31 --rebase'
expect ALLOW "a binary merely ENDING in gh is not gh"                      '/usr/local/bin/sleigh pr merge 31'

# GLOBAL FLAGS BEFORE THE SUBCOMMAND. gh takes --repo/-R either side of `pr`, and only the trailing
# form keeps `gh pr merge` adjacent - so a three-token check passes the leading form straight
# through. It is the shape house style produces: AGENTS.md writes every prior-art command as
# `gh issue list -R astubbs/parallel-consumer`, and this repo's `gh` resolves to the WRONG repo
# without one, so an agent is actively encouraged to type it. Found babysitting astubbs#324; all
# four spellings were silently allowed before the fix.
expect DENY  "a -R before the subcommand is still a merge"                 'gh -R astubbs/parallel-consumer pr merge 31 --squash'
expect DENY  "a --repo before the subcommand is still a merge"             'gh --repo astubbs/parallel-consumer pr merge 31'
expect DENY  "an attached --repo=VALUE is still a merge"                   'gh --repo=astubbs/parallel-consumer pr merge 31'
expect DENY  "an attached -RVALUE is still a merge"                        'gh -Rastubbs/parallel-consumer pr merge 31'
expect ALLOW "a leading -R on a NON-merge subcommand still passes"         'gh -R astubbs/parallel-consumer pr view 31'
expect ALLOW "a leading -R on an unrelated subcommand still passes"        'gh -R astubbs/parallel-consumer issue list'
expect DENY  "a mid-position -R (between pr and merge) is still a merge"   'gh pr -R astubbs/parallel-consumer merge 31 --squash'
expect ALLOW "a mid-position -R on a non-merge subcommand still passes"    'gh pr -R astubbs/parallel-consumer view 31'
expect ALLOW "the words gh pr merge inside --body are not a merge"         'gh pr comment 5 --body "remember to run gh pr merge later"'
# QUOTE-SPLIT SPELLINGS. shlex joins mer""ge / mer'ge' / mer\ge back into the token `merge`, so
# the token scan sees a merge - but the cheap pre-filter used to test the RAW payload for the
# substring `merge` and exited first, making the pre-filter the decider its own comment says it
# must never be. Found by the astubbs#324 review; verified red before the quote-strip fix.
expect DENY  "a quote-split merge (mer\"\"ge) is still a merge"            'gh pr mer""ge 31 --squash'
expect DENY  "a single-quote-split merge is still a merge"                 "gh pr me'rge' 31 --squash"
expect DENY  "a backslash-split merge is still a merge"                    'gh pr mer\ge 31 --squash'
expect ALLOW "a non-merge gh command passes"                               'gh pr view 31'
expect ALLOW "an unrelated command passes"                                 'git status'

# THE DOCUMENTED OVERRIDE, delivered the only way an agent can deliver it: as an env-prefix on the
# merge command, which reaches the hook as command TOKENS - a hook only ever sees the HARNESS's
# process env. The first version of this suite asserted only the process-env form (kept at the
# bottom as the human-wrapping-the-harness path), which hid that the in-session route was dead.
expect ALLOW "env-prefix override on the command releases the guard"       'MERGE_DESPITE_OUTSTANDING_WORK=1 gh pr merge 31 --rebase'
expect DENY  "an env-prefix set to 0 is not an override"                   'MERGE_DESPITE_OUTSTANDING_WORK=0 gh pr merge 31 --rebase'
expect DENY  "the override token AFTER the command is not a prefix"        'gh pr merge 31 --body "MERGE_DESPITE_OUTSTANDING_WORK=1"'

# THE PR'S OWN INFLIGHT NOTE, surfaced at merge. A note recording what is still open is written so
# the items are not forgotten and is then read by nobody at the moment it could still change the
# outcome. These cases prove the arm fires, quotes the right part, and stays out of the way otherwise.
touch -t 200109090146 "$ow_tasks/agent-live.output"   # nothing in flight, so only the note arm can fire

# IN A SCRATCH REPOSITORY, NOT THE REAL WORKING TREE. The guard resolves the note from
# `git rev-parse --show-toplevel`, so this arm used to write `docs/inflight/pr-90001-selftest.md`
# into whatever checkout the suite was run from and delete it afterwards. Two things were wrong with
# that, and the second is why it is fixed here rather than noted: it MUTATES a tree the suite does
# not own, and the fixed path is shared, so two concurrent runs - ordinary on a box running several
# agent sessions - race between one run's write and the other's cleanup. Measured: five concurrent
# runs, two of them red on this arm, deterministically green once isolated.
ow_repo="$(mktemp -d)"
(
  cd "$ow_repo" || exit 1
  git init -q .
  git checkout -q -b selftest/outstanding-fixture 2>/dev/null || git branch -q -m selftest/outstanding-fixture
  : > .keep
  git add .keep
  git -c user.email=selftest@example.invalid -c user.name=selftest commit -qm "fixture"
)
ow_return="$PWD"
cd "$ow_repo" || exit 1
mkdir -p docs/inflight
cat > docs/inflight/pr-90001-selftest.md <<'NOTE'
# astubbs#90001 - self-test fixture
<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
## Open
- SELFTEST_OPEN_ITEM
## Already fixed
- SELFTEST_RESOLVED_ITEM
NOTE
expect DENY  "a PR with an open inflight note is stopped at merge"          'gh pr merge 90001 --squash'
expect ALLOW "a PR with no inflight note is not stopped"                    'gh pr merge 90002 --squash'
expect ALLOW "the documented override releases the note arm too"            'MERGE_DESPITE_OUTSTANDING_WORK=1 gh pr merge 90001 --squash'

# The note arm must quote the OPEN section and stop at "Already fixed" - a note whose resolved
# section has grown must not bury the two lines that still matter.
note_out="$(printf '{"tool_name":"Bash","tool_input":{"command":"gh pr merge 90001 --squash"}}' \
    | CLAUDE_CODE_SESSION_ID="$ow_session" bash "$HOOK_UNDER_TEST" 2>/dev/null)"
case "$note_out" in *SELFTEST_OPEN_ITEM*) got=quoted ;; *) got=missing ;; esac
assert "the open item is quoted into the deny reason" quoted "$got"
case "$note_out" in *SELFTEST_RESOLVED_ITEM*) got=leaked ;; *) got=stopped ;; esac
assert "the Already-fixed section is NOT quoted" stopped "$got"
rm -f docs/inflight/pr-90001-selftest.md
cd "$ow_return" || exit 1
rm -rf "$ow_repo"

# Stale task file == nothing in flight. Proves the window is load-bearing rather than "any file".
touch -t 200109090146 "$ow_tasks/agent-live.output"
expect ALLOW "a task that stopped writing long ago does not block"         'gh pr merge 31 --rebase'

# BOTH ARMS OF THE MTIME READ, plus the two that cannot be reached with a working stat.
#
# CI is Linux, so the BSD arm shipped unexecuted, and the fail-closed arm is unreachable on any
# platform with a functioning stat - a file `find` has just listed can always be dated. A stub `stat`
# earlier on PATH forces each in turn: same file, same command throughout, only stat changing.
stub_bin="$TMP/stub-stat"; mkdir -p "$stub_bin"
saved_path="$PATH"
stub_stat() { cat > "$stub_bin/stat"; chmod +x "$stub_bin/stat"; PATH="$stub_bin:$saved_path"; }

# A BSD stat: rejects -c, answers `-f %m <file>`. MODELLED, not borrowed - the mtime read goes
# through python3 (which this suite already requires for every payload) rather than the host's stat,
# because on macOS the host stat IS BSD stat and would reject the `-c` a delegating stub hands it.
# That is how this case reddened on the very platform the branch exists for.
stub_stat <<'STUB'
#!/usr/bin/env bash
case "$1" in
    -c) exit 1 ;;                     # BSD stat has no -c
    -f) exec python3 -c 'import os,sys; print(int(os.stat(sys.argv[1]).st_mtime))' "$3" ;;
esac
exit 1
STUB
expect ALLOW "BSD stat: a long-stale task still does not block"            'gh pr merge 31 --rebase'
printf 'still writing\n' > "$ow_tasks/agent-live.output"
expect DENY  "BSD stat: a task still writing is still caught"              'gh pr merge 31 --rebase'

# A stat that answers nothing. The file matched the session's tasks glob, so something is there;
# being unable to date it is not evidence that nothing is running. Red against the pre-fix code,
# which skipped the file and allowed the merge - the macOS behaviour this branch exists to fix.
stub_stat <<'STUB'
#!/usr/bin/env bash
exit 1
STUB
touch -t 200109090146 "$ow_tasks/agent-live.output"
expect DENY  "an undateable task file is assumed LIVE, not stale"          'gh pr merge 31 --rebase'

# A stat that FAILS AND STILL PRINTS - which is what real GNU coreutils does when handed `-f %m
# FILE`: exit 1, six lines of filesystem prose on stdout (measured on 9.7). So the value reaching
# the guard is not absent, it is garbage, and a fail-closed arm testing only `-z` waves it through
# to `$(( now - mtime ))`, where the arithmetic evaluates `File` as a variable name and `set -u`
# aborts the hook - non-zero with no verdict, which PreToolUse ALLOWS. Hence the arm tests the
# value's SHAPE. Keep the task file LIVE so nothing but that arm can produce the refusal.
stub_stat <<'STUB'
#!/usr/bin/env bash
case "$1" in -c) exit 1 ;; esac
printf '  File: "x"\n    ID: 7f048f3f Namelen: 255     Type: tmpfs\nBlock size: 4096\n'
exit 1
STUB
printf 'still writing\n' > "$ow_tasks/agent-live.output"
expect DENY  "a stat that fails but still PRINTS cannot defeat the guard"  'gh pr merge 31 --rebase'

PATH="$saved_path"
rm -f "$stub_bin/stat"
touch -t 200109090146 "$ow_tasks/agent-live.output"
expect ALLOW "the same stale file, with stat working, does not block"      'gh pr merge 31 --rebase'

# Fail-open paths. A guard that blocks on its own bug jams the tool call shut.
printf 'still writing\n' > "$ow_tasks/agent-live.output"
export CLAUDE_CODE_SESSION_ID=""
expect ALLOW "no session id fails OPEN"                                    'gh pr merge 31 --rebase'
export CLAUDE_CODE_SESSION_ID="$ow_session"
# The payload must CONTAIN "merge", or the cheap pre-filter exits before python3 ever runs and
# this case tests the pre-filter instead of the parser's except branch it names (astubbs#324
# review: the original 'not json' payload never reached json.load).
got=$(printf 'not json but merge' | "$HOOK_UNDER_TEST" 2>/dev/null); \
    case "$got" in *'"deny"'*) got=DENY ;; *) got=ALLOW ;; esac
assert "unparseable payload fails OPEN" ALLOW "$got"

# The process-env form of the override is kept for a human whose own shell wraps the harness.
got=$(printf '%s' '{"tool_input":{"command":"gh pr merge 31 --rebase"}}' \
    | MERGE_DESPITE_OUTSTANDING_WORK=1 "$HOOK_UNDER_TEST" 2>/dev/null); \
    case "$got" in *'"deny"'*) got=DENY ;; *) got=ALLOW ;; esac
assert "the process-env override releases the guard" ALLOW "$got"

rm -rf "/tmp/claude-$(id -u)/selftest/$ow_session"

echo
# The squash-subject section counts into `fails` (its harness predates `assert`); fold it in so a
# failure there fails the script rather than printing FAIL and exiting 0.
failures=$((failures + fails))
# DEFERRED IS NOT CLOSED. A state beginning with `deferred` means decided-but-not-now: kept out of
# open work, listed in its own section at the BOTTOM with its reason, and NOT rolled into the
# "not shown" count - counting it there would tell you to delete work that was deliberately scheduled.
def_tmp=$(mktemp -d)
mkdir -p "$def_tmp/docs/inflight" "$def_tmp/docs/solutions/x"
printf -- '---\ntitle: "s"\n---\n' > "$def_tmp/docs/solutions/x/s.md"
printf '# An open bug\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: stall -->\n' > "$def_tmp/docs/inflight/bug-open.md"
printf '# A deferred bug\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: misdirection -->\n<!-- inflight-state: deferred - after v6 -->\n' > "$def_tmp/docs/inflight/bug-deferred.md"
printf '# A closed note\n\n<!-- inflight-type: task -->\n<!-- inflight-state: closed - will not do -->\n' > "$def_tmp/docs/inflight/task-closed.md"
def_out=$(CLAUDE_PROJECT_DIR="$def_tmp" "$HOOKS/inject-recorded-knowledge.sh" 2>/dev/null)

case "$(sed -n '/^# Open work/,/^# Deferred/p' <<<"$def_out")" in
    *"A deferred bug"*) got=leaked ;; *) got=excluded ;;
esac
assert "a deferred note is kept out of open work" excluded "$got"
case "$def_out" in *"# Deferred - decided, not now"*) got=has_section ;; *) got=no_section ;; esac
assert "deferred work gets its own section" has_section "$got"
case "$def_out" in *"_deferred - after v6_"*) got=reason_shown ;; *) got=reason_lost ;; esac
assert "the deferral reason is shown, greppable by version" reason_shown "$got"
# The not-shown section takes the CLOSED note and leaves the deferred one alone. This was a count
# ("1 note(s) not shown") until astubbs#323's review found the count hiding two notes that matched
# neither filter; the intent is unchanged - a deferred note must never be swept in with the abandoned
# ones - but it is now checked by name, which is what makes a miscategorised note visible at all.
# Bounded to its OWN section: "Not shown above" is emitted BEFORE "Deferred", so an open-ended range
# swallows the deferred section and every deferred note reads as swept in.
not_shown=$(awk '/^# Not shown above/{f=1;next} f&&/^# /{exit} f' <<<"$def_out")
case "$not_shown" in *"A closed note"*) got=listed ;; *) got=missing ;; esac
assert "the closed note is named in the not-shown section" listed "$got"
case "$not_shown" in *"A deferred bug"*) got=swept_in ;; *) got=left_alone ;; esac
assert "a deferred note is not swept into the not-shown section" left_alone "$got"
d_at=$(grep -n '^# Deferred' <<<"$def_out" | head -1 | cut -d: -f1)
o_at=$(grep -n '^# Open work' <<<"$def_out" | head -1 | cut -d: -f1)
{ [ -n "$d_at" ] && [ -n "$o_at" ] && [ "$o_at" -lt "$d_at" ]; } && got=below || got=above
assert "deferred is listed below open work" below "$got"
rm -rf "$def_tmp"

# INVOKED BY A RELATIVE PATH - the exact command the index prints for an agent to re-run. The script
# cd's to the project root, so anything resolved from BASH_SOURCE afterwards silently resolves to
# nothing and the whole index comes back empty.
rel_out=$( cd "$REPO_ROOT" && bash .claude/hooks/inject-recorded-knowledge.sh 2>/dev/null )
case "$rel_out" in *"# Open work"*) got=works ;; *) got=silently_empty ;; esac
assert "the hook works when invoked by a relative path" works "$got"

echo
echo "--- remind-inflight-on-push.sh ---"

# The PUSH-time complement to the merge guard. Informational (`additionalContext`), never a deny - a
# hook that blocked pushes would be routed around within a day. `gh` is stubbed so the case does not
# depend on a live PR, and the stub is first on PATH rather than mocked inside the hook.
PUSH_HOOK="$HOOKS/remind-inflight-on-push.sh"

# ITS OWN REPO, ON A REAL BRANCH. Both hooks resolve the PR from `git rev-parse --abbrev-ref HEAD`
# and exit silently when that returns "HEAD" - correct behaviour, since a detached checkout has no
# branch to look a PR up by. GitHub Actions checks PRs out DETACHED, so these cases passed on every
# developer machine and failed only in CI, which is the worst possible place to learn it. Running
# them inside a scratch repo on a named branch makes the ambient checkout irrelevant.
push_repo="$(mktemp -d)"
(
  cd "$push_repo" || exit 1
  git init -q .
  git checkout -q -b selftest/push-fixture 2>/dev/null || git branch -q -m selftest/push-fixture
  # AND A HOSTED `origin`, which is now load-bearing rather than cosmetic: both hooks derive the
  # repository to ask gh about from `origin` and refuse to fall back to an unqualified lookup, so a
  # fixture without one exercises the could-not-derive path instead of the case under test. A repo
  # made by `git init` has no remote at all, which is exactly the gap that made this line necessary.
  git remote add origin https://github.com/astubbs/parallel-consumer.git
  # An UNBORN branch reads as empty from `rev-parse --abbrev-ref HEAD`, which trips the same silent
  # exit as a detached one - so the fixture needs a commit, not just a branch name.
  : > .keep
  git add .keep
  git -c user.email=selftest@example.invalid -c user.name=selftest commit -qm "fixture"
)
cd "$push_repo" || exit 1
push_stub="$(mktemp -d)"
printf '#!/usr/bin/env bash\necho 90003\n' > "$push_stub/gh"
chmod +x "$push_stub/gh"
mkdir -p docs/inflight
cat > docs/inflight/pr-90003-selftest.md <<'NOTE'
# astubbs#90003 - self-test fixture
<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
## Open
- PUSH_OPEN_ITEM
## Already fixed
- PUSH_RESOLVED_ITEM
NOTE

# PRIVATE TMPDIR, for the reason the branch-context section states at length: the reminder's throttle
# stamp is named after the BRANCH, this fixture always uses the same branch name, and the stamp lives
# in shared /tmp - so a second concurrent run of this suite clears and rewrites the stamp between this
# function's `rm` and its fire, and the reminder that should have appeared is throttled away. Measured
# on a pristine master worktree: one run in three failed, on a different case each time, which is
# exactly how a shared-state flake presents - and seen twice as "an immediate second push is
# throttled" reporting `repeated`, when a concurrent run removed the stamp the first invocation
# had just written before the second could read it. Test-infra contention, not a hook bug: the
# hook needs no seam for this, since it already honours TMPDIR.
PUSH_TMPDIR="$(mktemp -d)"

push_fire() { # <command> -> stdout of the hook
    rm -f "$PUSH_TMPDIR"/pc-push-reminder-* 2>/dev/null
    printf '{"tool_name":"Bash","tool_input":{"command":"%s"}}' "$1" \
        | PATH="$push_stub:$PATH" TMPDIR="$PUSH_TMPDIR" bash "$PUSH_HOOK" 2>/dev/null
}

out="$(push_fire 'git push')"
case "$out" in *PUSH_OPEN_ITEM*) got=reminded ;; *) got=silent ;; esac
assert "a push on a PR with an open note reminds" reminded "$got"

# GLOBAL FLAGS THAT TAKE A VALUE. Dropping the flag but not its value left the value where the
# subcommand should be, so `git -C <path> push` matched nothing and the hook was silently dead for
# the form an agent uses most. Review of astubbs#324 found it; the session that wrote the hook had
# been pushing with `git -C` all along and never saw a reminder.
for form in 'git -C /some/path push' 'git -c user.name=x push' 'git --git-dir=/d push' 'git -C /p -c a=b push'; do
    out="$(push_fire "$form")"
    case "$out" in *PUSH_OPEN_ITEM*) got=reminded ;; *) got=silent ;; esac
    assert "reminds on: $form" reminded "$got"
done

# A GIT CALL BEFORE THE PUSH. `git add -A && git commit -m x && git push` is how an agent actually
# pushes, and the shared helper's first version answered with the FIRST git invocation's subcommand -
# so it returned `add`, and this hook silently did nothing on a real push. Found by review on
# astubbs#357, reproduced before it was believed, and pinned here for both hooks because a helper
# that reads one subcommand cannot see a chained push.
for form in 'git add -A && git commit -m x && git push' 'git commit --amend && git push' 'git -C /p status && git push' 'git fetch origin && git push -u origin HEAD'; do
    out="$(push_fire "$form")"
    case "$out" in *PUSH_OPEN_ITEM*) got=reminded ;; *) got=silent ;; esac
    assert "reminds on a push CHAINED after another git call: $form" reminded "$got"
done

# UNSPACED AND SEMICOLON OPERATORS, which is the same silent-miss one level down. `shlex.split` splits
# on whitespace and quotes only, so `&&`/`;` stay fused to whatever touches them: `-A&&git` is not
# `git`, and `push;` is not `push`. The semicolon case is the one that matters most - `git push; echo
# done` is SPACED and still missed, so this was never only about writing operators tightly. Found by
# review on astubbs#357 and confirmed by running it, which also found the spaced-semicolon case the
# report did not have.
for form in 'git add -A&&git commit -m x&&git push' 'git push;echo done' 'git push; echo done' 'git commit -m x&&git push'; do
    out="$(push_fire "$form")"
    case "$out" in *PUSH_OPEN_ITEM*) got=reminded ;; *) got=silent ;; esac
    assert "reminds with unspaced/semicolon operators: $form" reminded "$got"
done

# A COMMAND AFTER THE PUSH. `git push && git status` tokenises to the invocation lines
# `push`,`status`, and the first single-tokeniser-spawn refactor matched a bare `push` line only at
# the start or end of the whole list - so the commonest compound push of all went silent. Found
# three times over by review on astubbs/parallel-consumer#382; these pin the per-line match, and
# the `\n` form pins the lexer treating a line break as a command boundary.
for form in 'git push && git status' 'git commit -m x && git push && git tag y' 'git push\ngit status'; do
    out="$(push_fire "$form")"
    case "$out" in *PUSH_OPEN_ITEM*) got=reminded ;; *) got=silent ;; esac
    assert "reminds on a push FOLLOWED by another command: $form" reminded "$got"
done

# ...and the matching negative control the review asked for: a CHAIN of git calls with no push in it
# must stay silent. The positive chained cases above cannot show that on their own.
for form in 'git add -A && git commit -m x' 'git fetch origin&&git status'; do
    out="$(push_fire "$form")"
    case "$out" in *PUSH_OPEN_ITEM*) got=fired ;; *) got=silent ;; esac
    assert "a chain of git calls with NO push stays silent: $form" silent "$got"
done

# The negative controls matter as much: a hook that fires on any mention of the word is noise, and
# noise is how a reminder gets ignored.
out="$(push_fire 'git commit -m "push later"')"
case "$out" in *PUSH_OPEN_ITEM*) got=fired ;; *) got=silent ;; esac
assert "a commit MESSAGE mentioning push does not fire" silent "$got"
case "$out" in *PUSH_RESOLVED_ITEM*) got=leaked ;; *) got=stopped ;; esac
assert "the Already-fixed section is not quoted" stopped "$got"
case "$out" in *'"deny"'*) got=blocked ;; *) got=advisory ;; esac
assert "the push reminder never denies" advisory "$got"

# TOKENS, NOT SUBSTRINGS - the rule the other two hooks state. A commit message mentioning a push,
# and a non-git binary, must both stay silent.
out="$(push_fire 'git commit -m "ready to push"')"
[ -z "$out" ] && got=silent || got=fired
assert "a commit message mentioning push does not fire" silent "$got"
out="$(push_fire 'npm push')"
[ -z "$out" ] && got=silent || got=fired
assert "a non-git binary does not fire" silent "$got"

# WHICH BRANCH IT IS REMINDING ABOUT. Same defect as the history-rewrite guard's, in the hook whose
# entire output is a claim about a named PR: `git push origin other-branch` from this directory used
# to look up THIS branch's PR and quote its inflight note, opening with "You are pushing to
# astubbs/parallel-consumer#N" - a flat statement about a branch the command does not touch. The stub
# above answers 90003 whatever it is asked, so the observable is the argv, as it is for the sibling.
push_log_stub="$(mktemp -d)"
cat > "$push_log_stub/gh" <<GH
#!/usr/bin/env bash
printf '%s\n' "\$*" >> "$push_log_stub/argv.log"
echo 90003
GH
chmod +x "$push_log_stub/gh"
push_fire_logged() { # <command> -> stdout of the hook
    rm -f "$PUSH_TMPDIR"/pc-push-reminder-* 2>/dev/null
    : > "$push_log_stub/argv.log"
    printf '{"tool_name":"Bash","tool_input":{"command":"%s"}}' "$1" \
        | PATH="$push_log_stub:$PATH" TMPDIR="$PUSH_TMPDIR" bash "$PUSH_HOOK" 2>/dev/null
}
push_head() { stub_head_arg "$push_log_stub/argv.log"; }

push_fire_logged 'git push origin feats/somewhere-else' >/dev/null
assert "the reminder looks up the branch the push names" feats/somewhere-else "$(push_head)"
[ "$(push_head)" = "selftest/push-fixture" ] && got=the_pre_fix_answer || got=not_the_cwd_branch
assert "and not the branch this directory is on" not_the_cwd_branch "$got"

# A bare push has no refspec, so the directory is all there is - and the reminder must say the branch
# was inferred rather than asserting it.
out="$(push_fire_logged 'git push')"
assert "a bare push falls back to this directory's branch" selftest/push-fixture "$(push_head)"
case "$out" in *"names no branch"*) got=says_it_inferred ;; *) got=presented_as_fact ;; esac
assert "and the reminder says the branch was inferred" says_it_inferred "$got"
case "$out" in *'"deny"'*) got=blocked ;; *) got=advisory ;; esac
assert "the caveat did not turn the reminder into a deny" advisory "$got"

# VALUE-TAKING PUSH OPTIONS. Dropping only the flag leaves its value where the repository should
# be, shifting every positional: `-o ci.skip` would read `ci.skip` as the repo and `origin` as the
# refspec. `--recurse-submodules` was the one missing from the skip list (cross-model review,
# astubbs/parallel-consumer#382).
push_fire_logged 'git push -o ci.skip origin feats/somewhere-else' >/dev/null
assert "a value-taking push option does not shift the refspec" feats/somewhere-else "$(push_head)"
push_fire_logged 'git push --recurse-submodules on-demand origin feats/somewhere-else' >/dev/null
assert "--recurse-submodules value is not read as the repository" feats/somewhere-else "$(push_head)"

# AN UNEXPANDED SHELL VARIABLE is source text, not a branch: the shell would expand it before git
# ever saw it, so asserting anything about the literal is a confident wrong answer. Fall back to
# the directory and say the branch was inferred.
out="$(push_fire_logged 'git push origin $SOMEBRANCH')"
assert "an unexpanded \$VAR refspec falls back to the directory branch" selftest/push-fixture "$(push_head)"
case "$out" in *"names no branch"*) got=says_it_inferred ;; *) got=presented_as_fact ;; esac
assert "and the \$VAR fallback says the branch was inferred" says_it_inferred "$got"
rm -rf "$push_log_stub"

# THROTTLED, or a push loop repeats the whole note and teaches the reader to skip it.
printf '{"tool_name":"Bash","tool_input":{"command":"git push"}}' | PATH="$push_stub:$PATH" TMPDIR="$PUSH_TMPDIR" bash "$PUSH_HOOK" >/dev/null 2>&1
out="$(printf '{"tool_name":"Bash","tool_input":{"command":"git push"}}' | PATH="$push_stub:$PATH" TMPDIR="$PUSH_TMPDIR" bash "$PUSH_HOOK" 2>/dev/null)"
[ -z "$out" ] && got=throttled || got=repeated
assert "an immediate second push is throttled" throttled "$got"

rm -f docs/inflight/pr-90003-selftest.md
rm -rf "$push_stub" "$PUSH_TMPDIR"

echo
# ---------------------------------------------------------------------------------------------
# remind-master-drift-on-push.sh
#
# ADDED BY astubbs#339, not by the PR that wrote the hook. The registration check further down
# reports every hook `settings.json` registers that nothing under `bin/` references, and this hook
# was the first thing it caught. astubbs#357 tested the SHARED push detection thoroughly - chained
# git calls, unspaced operators, and a negative control - through the sibling reminder's fixture, but
# nothing exercised THIS hook's own contract, and master's copy of this suite already cites "the
# drift section" that was never added. So these cases are the hook's own behaviour: what it reports,
# when it stays quiet, and the throttle that decides between them.
#
# OFFLINE BY CONSTRUCTION. The hook only fetches when `MASTER_DRIFT_REF` contains a slash, so a local
# branch standing in for `origin/master` skips that branch entirely and no case here touches the
# network. Every case also pins a private TMPDIR and `MASTER_DRIFT_FETCH_FLOOR_SECONDS=0`: the stamp
# path is derived from the branch NAME, so concurrent copies of this suite would collide on it -
# the same shared-state flake the push-reminder section above documents - and the five-minute fetch
# floor would otherwise silence every case after the first.
# ---------------------------------------------------------------------------------------------

echo
echo "--- remind-master-drift-on-push.sh ---"

# THE CWD IS INHERITED BY THE SECTION AFTER THIS ONE, so this section restores whatever it found.
# `check-history-rewrite.sh` below documents that it runs from the push fixture's scratch repo,
# reached only by inheriting the cwd, and that its enriched-refusal cases fail from the real tree
# because the CI checkout is detached. A `cd "$REPO_ROOT"` here therefore turned that section red
# on every runner while passing locally, where the checkout is on a branch - caught by CI, not by
# this suite, which is the reason it is written down rather than just fixed.
drift_prev_cwd="$PWD"
DRIFT_HOOK="$HOOKS/remind-master-drift-on-push.sh"
drift_repo="$(mktemp -d)"
DRIFT_TMPDIR="$(mktemp -d)"
# Identity on the FIXTURE REPO rather than per commit: `git merge` below also writes a commit,
# and a `-c`-per-commit helper silently does not cover it.
drift_commit() { git commit -qm "$1"; }
(
    cd "$drift_repo" || exit 1
    git init -q .
    git symbolic-ref HEAD refs/heads/basefix
    git config user.email selftest@example.invalid
    git config user.name selftest
    printf 'base\n' >shared.txt
    printf 'x\n' >untouched.txt
    git add . && drift_commit "fixture root"
    # The branch touches shared.txt and mine.txt; the stand-in master touches shared.txt and
    # theirs.txt. So exactly ONE file is changed on both sides, which is what makes the overlap
    # cases below assertions rather than coincidences.
    git checkout -q -b feature
    printf 'mine\n' >>shared.txt
    printf 'm\n' >mine.txt
    git add . && drift_commit "branch work"
    git checkout -q basefix
    printf 'theirs\n' >>shared.txt
    printf 't\n' >theirs.txt
    git add . && drift_commit "MASTER-SUBJECT-ONE"
    git checkout -q feature
)
cd "$drift_repo" || exit 1

drift_fire() { # [VAR=value...] -> stdout of the hook
    printf '{"tool_name":"Bash","tool_input":{"command":"git push"}}' |
        env TMPDIR="$DRIFT_TMPDIR" MASTER_DRIFT_REF=basefix MASTER_DRIFT_FETCH_FLOOR_SECONDS=0 \
            "$@" bash "$DRIFT_HOOK" 2>/dev/null
}
drift_ctx() { # <hook output> -> the additionalContext string, or empty
    [ -n "$1" ] || return 0
    printf '%s' "$1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["hookSpecificOutput"]["additionalContext"])'
}

# WHAT IT IS FOR: the subjects. A divergence count is what `git rev-list --left-right --count`
# already answers; the reason this hook exists is that nobody reads the commit bodies, so naming the
# commit is the assertion that matters most here.
drift_first="$(drift_fire)"
drift_report="$(drift_ctx "$drift_first")"
case "$drift_report" in *MASTER-SUBJECT-ONE*) got=named_the_commit ;; *) got="did not name it" ;; esac
assert "reports the subject of a commit the base ref gained" named_the_commit "$got"

# OVERLAP, and its negative half in the same case. `shared.txt` is changed on both sides and must be
# named; `theirs.txt` is changed only on the base side, so reporting it would turn "master has moved
# under work this branch is doing" into a list of everything master did.
case "$drift_report" in
    *"changed on BOTH sides"*shared.txt*) got=named_overlap ;;
    *) got="no overlap section" ;;
esac
assert "names the file changed on both sides" named_overlap "$got"
case "${drift_report#*BOTH sides}" in *theirs.txt*) got="claimed a base-only file" ;; *) got=overlap_only ;; esac
assert "does not report a base-only file as overlap" overlap_only "$got"

# THROTTLED ON THE BASE SHA, not a clock: the same tip must be reported once however often you push.
drift_second="$(drift_fire)"
[ -z "$drift_second" ] && got=throttled || got="repeated"
assert "the same base tip is not reported twice" throttled "$got"

# ...and a base that MOVES reports again immediately, which is the half a clock-based throttle gets
# wrong. Same push, same branch, one new commit on the stand-in master.
( cd "$drift_repo" && git checkout -q basefix && printf 'more\n' >>theirs.txt && git add . && drift_commit "MASTER-SUBJECT-TWO" && git checkout -q feature )
drift_moved="$(drift_ctx "$(drift_fire)")"
case "$drift_moved" in *MASTER-SUBJECT-TWO*) got=reported_again ;; *) got="stayed quiet" ;; esac
assert "a base ref that moved is reported again at once" reported_again "$got"

# THE PUSH REFSPEC NAMES THE BRANCH, and the measurement must describe the SAME branch as the name
# (astubbs/parallel-consumer#382). Two halves: pushing the base branch itself by refspec must be
# silent even from a drifted worktree - the pre-fix hook read HEAD and would have reported this
# worktree's drift against a push that never touched it - and pushing a named side branch must be
# measured by that branch's own ref, so the session worktree's overlap cannot leak into its report.
( cd "$drift_repo" && git branch -q sidework "$(git merge-base basefix feature)" )
drift_refspec_tmp="$(mktemp -d)"
out="$(printf '{"tool_name":"Bash","tool_input":{"command":"git push origin basefix"}}' |
    env TMPDIR="$drift_refspec_tmp" MASTER_DRIFT_REF=basefix MASTER_DRIFT_FETCH_FLOOR_SECONDS=0 bash "$DRIFT_HOOK" 2>/dev/null)"
[ -z "$out" ] && got=silent || got="reported the session's drift"
assert "pushing the base branch BY REFSPEC is silent even from a drifted worktree" silent "$got"
drift_side_tmp="$(mktemp -d)"
out="$(printf '{"tool_name":"Bash","tool_input":{"command":"git push origin sidework"}}' |
    env TMPDIR="$drift_side_tmp" MASTER_DRIFT_REF=basefix MASTER_DRIFT_FETCH_FLOOR_SECONDS=0 bash "$DRIFT_HOOK" 2>/dev/null)"
drift_side_report="$(drift_ctx "$out")"
case "$drift_side_report" in *MASTER-SUBJECT-ONE*) got=measured_sidework ;; *) got="no report" ;; esac
assert "a refspec-named branch is measured by its own local ref" measured_sidework "$got"
case "$drift_side_report" in *"BOTH sides"*) got="leaked the worktree's overlap" ;; *) got=no_false_overlap ;; esac
assert "and the session worktree's overlap is not attributed to it" no_false_overlap "$got"

# `git push origin src:dst` PUBLISHES src - dst is only the remote label. Measuring dst read a
# same-named local branch that was not being pushed, or went silent when none existed (Codex
# review, astubbs/parallel-consumer#382). sidework:renamed must measure sidework's own drift.
drift_srcdst_tmp="$(mktemp -d)"
out="$(printf '{"tool_name":"Bash","tool_input":{"command":"git push origin sidework:renamed"}}' |
    env TMPDIR="$drift_srcdst_tmp" MASTER_DRIFT_REF=basefix MASTER_DRIFT_FETCH_FLOOR_SECONDS=0 bash "$DRIFT_HOOK" 2>/dev/null)"
drift_srcdst_report="$(drift_ctx "$out")"
case "$drift_srcdst_report" in *MASTER-SUBJECT-ONE*) got=measured_the_src ;; *) got="no report" ;; esac
assert "a src:dst push is measured on its SOURCE branch" measured_the_src "$got"

# THE REPOSITORY COMES FROM THE PAYLOAD CWD: a hook process running somewhere else entirely (a
# subagent) must still measure the repository the command runs in. Pre-fix, rev-parse in the
# hook's own directory found no repo and the reminder silently vanished.
drift_cwd_tmp="$(mktemp -d)"
out="$( (cd "$drift_cwd_tmp" && printf '{"tool_name":"Bash","cwd":"%s","tool_input":{"command":"git push"}}' "$drift_repo" |
    env TMPDIR="$drift_cwd_tmp" MASTER_DRIFT_REF=basefix MASTER_DRIFT_FETCH_FLOOR_SECONDS=0 bash "$DRIFT_HOOK" 2>/dev/null) )"
drift_cwd_report="$(drift_ctx "$out")"
case "$drift_cwd_report" in *MASTER-SUBJECT-*) got=found_the_repo ;; *) got="stayed silent" ;; esac
assert "the drift repo is derived from the payload cwd, not the hook's" found_the_repo "$got"
rm -rf "$drift_refspec_tmp" "$drift_side_tmp" "$drift_srcdst_tmp" "$drift_cwd_tmp"

# UNCOMMITTED WORK COUNTS AS THIS BRANCH'S, which the hook states as a deliberate choice: a file you
# are editing right now is the one you most want to hear about. `untouched.txt` is in neither side's
# history, so only a working-tree read can put it in the overlap.
( cd "$drift_repo" && git checkout -q basefix && printf 'base edit\n' >>untouched.txt && git add . && drift_commit "MASTER-TOUCHES-UNTOUCHED" && git checkout -q feature )
printf 'local edit\n' >>untouched.txt
drift_dirty="$(drift_ctx "$(drift_fire)")"
case "$drift_dirty" in *"BOTH sides"*untouched.txt*) got=counted_worktree ;; *) got="ignored the uncommitted edit" ;; esac
assert "an uncommitted edit counts as this branch's side of the overlap" counted_worktree "$got"
git checkout -q -- untouched.txt 2>/dev/null || true

# SILENT WHEN THERE IS NOTHING TO INHERIT. Three separate exits, each of which would be a false
# report: level with the base, pushing the base branch itself, and a command that is not a push.
#
# EACH ONE GETS A FRESH TMPDIR, and that is the whole point of these three cases rather than a
# detail. The stamp already holds the current base SHA by now, so the SHA throttle alone would make
# the hook silent - and a silence case that passes whether or not the exit it names exists asserts
# nothing. A fresh stamp directory removes the throttle as an explanation and leaves only the exit
# under test. Caught by this suite: the first version of the level-with-base case passed while the
# repo sat in a broken merge state, which is exactly the false pass the fresh stamp rules out.
#
# `reset --hard` rather than a merge, because both sides appended to `shared.txt` and the catch-up
# merge conflicted - leaving a repo whose index could not be read, which is what produced that pass.
( cd "$drift_repo" && git reset --hard -q basefix )
[ -z "$(drift_fire TMPDIR="$(mktemp -d)")" ] && got=silent || got=spoke
assert "a branch level with the base ref stays silent" silent "$got"

# NO OVERLAP takes the other branch of the report, and must not imply a merge is required. It has to
# come AFTER the catch-up above: while the branch still carried its own edit to `shared.txt`, that
# file was on both sides by construction, so this arm was unreachable and the case would have been
# asserting the overlap arm under the wrong name.
( cd "$drift_repo" && git checkout -q basefix && printf 'unrelated\n' >unrelated.txt && git add . && drift_commit "MASTER-UNRELATED" && git checkout -q feature )
drift_none="$(drift_ctx "$(drift_fire)")"
case "$drift_none" in
    *"None of them touch a file this branch changes"*) got=said_nothing_forces_it ;;
    *) got="wrong branch of the report" ;;
esac
assert "with no overlap it says nothing forces a merge today" said_nothing_forces_it "$got"
( cd "$drift_repo" && git checkout -q basefix )
[ -z "$(drift_fire TMPDIR="$(mktemp -d)")" ] && got=silent || got=spoke
assert "pushing the base branch itself stays silent" silent "$got"
( cd "$drift_repo" && git checkout -q feature )
drift_nonpush="$(printf '{"tool_name":"Bash","tool_input":{"command":"git status"}}' |
    env TMPDIR="$(mktemp -d)" MASTER_DRIFT_REF=basefix MASTER_DRIFT_FETCH_FLOOR_SECONDS=0 bash "$DRIFT_HOOK" 2>/dev/null)"
[ -z "$drift_nonpush" ] && got=silent || got=spoke
assert "a non-push git command stays silent" silent "$got"

# IT MUST NEVER TAKE THE CALL AWAY. It emits no `permissionDecision` at all, so the assertion is on
# the two things that could still do it: a non-zero exit, and a report on a repo it cannot read.
( cd "$drift_repo" && git checkout -q basefix && printf 'z\n' >>unrelated.txt && git add . && drift_commit "MASTER-EXIT-CHECK" && git checkout -q feature )
drift_fire >/dev/null 2>&1
assert "reporting exits 0, so the Bash call survives" 0 "$?"
drift_broken="$(cd "$DRIFT_TMPDIR" && printf '{"tool_name":"Bash","tool_input":{"command":"git push"}}' |
    env TMPDIR="$DRIFT_TMPDIR" MASTER_DRIFT_REF=basefix MASTER_DRIFT_FETCH_FLOOR_SECONDS=0 bash "$DRIFT_HOOK" 2>/dev/null; echo "rc=$?")"
case "$drift_broken" in *rc=0*) got=exited_zero ;; *) got="$drift_broken" ;; esac
assert "outside a git repo it exits 0 rather than failing the call" exited_zero "$got"

rm -rf "$drift_repo" "$DRIFT_TMPDIR"
cd "$drift_prev_cwd" || exit 1

echo "--- check-history-rewrite.sh ---"
#
# RUNS FROM THE PUSH FIXTURE'S SCRATCH REPO, inherited via the cwd - see the note at the top of the
# drift section. The enriched refusal only renders when a PR is found, and finding one needs a named
# branch; the CI checkout is detached, so running these from the real tree fails only there.

# A force-push re-anchors every inline review comment and destroys the incremental diff a reviewer
# works from. This guard exists because docs/merge-checklist.md already said "re-cut at the end" and
# that did not prevent it happening twice in one session - once costing a reviewer their diff, once
# starting a re-cut with three reviews mid-flight.
HIST_HOOK="$HOOKS/check-history-rewrite.sh"
# STUBBED `gh`, because the enriched refusal only renders when a PR is found - and "found" depends on
# a branch, a PR and a network. Unstubbed, CI took the generic-refusal path and the quiet-PR case
# below read as reads_as_safe while every developer machine passed. The stub answers a PR with zero
# threads and zero running jobs, which is precisely the quiet PR that case exists to test.
hist_stub="$(mktemp -d)"
cat > "$hist_stub/gh" <<'GH'
#!/usr/bin/env bash
case "$*" in
    *"pr list"*)  echo 90003 ;;
    *"/comments"*) echo 0 ;;
    *"run list"*) echo 0 ;;
    *)            echo "" ;;
esac
GH
chmod +x "$hist_stub/gh"
hist() { printf '{"tool_name":"Bash","tool_input":{"command":"%s"}}' "$1" | PATH="$hist_stub:$PATH" bash "$HIST_HOOK" 2>/dev/null; }
hist_expect() { # <DENY|ALLOW> <desc> <command>
    local out got
    out="$(hist "$3")"
    [ -n "$out" ] && got=DENY || got=ALLOW
    assert "$2" "$1" "$got"
}

hist_expect DENY  "a force-push is refused"                      'git push --force origin main'
hist_expect DENY  "short -f at end of command is refused"        'git push -f'
hist_expect DENY  "--force-with-lease is still a rewrite"        'git push --force-with-lease origin main'
hist_expect DENY  "a rebase is refused"                          'git rebase --onto master abc branchname'
hist_expect DENY  "an amend is refused"                          'git commit --amend -m x'
hist_expect DENY  "filter-branch is refused"                     'git filter-branch --tree-filter x'

# Finishing a rebase already in progress is not starting one - blocking it would strand the tree
# mid-operation, which is worse than the rewrite.
hist_expect ALLOW "rebase --abort is not a rewrite"              'git rebase --abort'
hist_expect ALLOW "rebase --continue is not a rewrite"           'git rebase --continue'
hist_expect ALLOW "an ordinary push is untouched"                'git push origin main'
hist_expect ALLOW "reset --hard to a remote ref is untouched"    'git reset --hard origin/main'

# TOKENS, NOT SUBSTRINGS - prose about force-pushing must not fire, or the guard gets routed around.
hist_expect ALLOW "a commit message mentioning rebase"           'git commit -m "notes on rebase and force-push"'
hist_expect ALLOW "a gh comment mentioning force-push"           'gh pr comment 1 --body "we should force-push"'
hist_expect ALLOW "a non-git -f flag"                            'grep -f patterns.txt file'

# The override is delivered as an env PREFIX, which reaches a hook as command tokens - a hook only
# ever sees the harness process env.
hist_expect ALLOW "the documented override releases it"          'REWRITE_HISTORY_CONFIRMED=1 git push --force'
hist_expect DENY  "an override set to 0 is not an override"      'REWRITE_HISTORY_CONFIRMED=0 git push --force'

# EVERY OTHER WAY TO MOVE A REF. The first version caught four shapes and let seven through - a
# guard that reaches only what you thought of is a documented bypass, so each is pinned here.
hist_expect DENY  "reset backwards drops commits"                'git reset --hard HEAD~3'
hist_expect DENY  "reset to a bare SHA drops commits"            'git reset --hard 1a2b3c4d'
hist_expect DENY  "branch -f moves a ref"                        'git branch -f main abc1234'
hist_expect DENY  "checkout -B with a start point moves a ref"   'git checkout -B main abc1234'
hist_expect DENY  "switch -C with a start point moves a ref"     'git switch -C main abc1234'
hist_expect DENY  "update-ref writes a branch directly"          'git update-ref refs/heads/main abc1234'
hist_expect DENY  "deleting a remote branch"                     'git push origin --delete topic'
hist_expect DENY  "the colon form of remote deletion"            'git push origin :topic'

# Forward sync and ordinary branch creation are routine and must stay silent, or the guard becomes
# noise and gets waved through.
hist_expect ALLOW "reset --hard to a remote ref is a sync"       'git reset --hard origin/main'
hist_expect ALLOW "reset --hard HEAD discards local edits only"  'git reset --hard HEAD'
hist_expect ALLOW "checkout -B with no start point"              'git checkout -B tmp'
hist_expect ALLOW "creating a branch"                            'git branch newbranch'

# A QUIET PR MUST NOT READ AS SAFE. Zero threads and zero running jobs is what a reviewer part-way
# through the diff looks like - the case that loses most from a rewrite - so the refusal has to say
# that absence was measured rather than implying nothing is at risk.
quiet_out="$(hist 'git push --force origin main')"
case "$quiet_out" in
    *"NOT evidence that a rewrite is safe"*|*"inline review comment"*|*"IN PROGRESS"*) got=states_the_risk ;;
    *) got=reads_as_safe ;;
esac
assert "a PR with nothing outstanding still states the risk" states_the_risk "$got"

# The deny must NAME what would be lost, not just refuse.
hist_out="$(hist 'git push --force origin main')"
case "$hist_out" in *"LAST step before a merge"*) got=explains ;; *) got=bare_refusal ;; esac
assert "the refusal says when a rewrite IS allowed" explains "$got"

# --- WHICH BRANCH THE REFUSAL IS ABOUT -------------------------------------------------------
#
# A hook does NOT run in the directory its guarded command runs in, and this repository keeps many
# worktrees checked out at once - so `git rev-parse --abbrev-ref HEAD` in the hook process answers
# about whichever branch the SESSION sits on. Twice on 2026-08-31 that made this hook's most
# confident sentence describe an unrelated branch: a force-push of `feats/proxy-verdict-free-return`
# (open PR astubbs/parallel-consumer#295, with review history) and a `git commit --amend` inside the
# `feats/ks-streams-fork-machinery` worktree were both reported against
# `docs/god-branch-decomposition-plan`, the plan worktree that session occupied.
#
# THE OBSERVABLE IS THE ARGV gh WAS HANDED, not the message - a lookup for the wrong branch succeeds
# and reads exactly like a correct one, which is what let this run for as long as it did. Same
# technique as case 3 of the lookup section below, for the same reason.
#
# EACH CASE ASSERTS BOTH HALVES: the branch asked about is the one the command names, AND it is not
# the branch the working directory is on. The second half is the negative control - it is precisely
# the pre-fix answer, so a fixture that stopped reaching the defect (both names collapsing to one)
# fails rather than passing vacuously.
hb_prev_cwd="$PWD"
hb_cwd_branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
hb_stub="$(mktemp -d)"
cat > "$hb_stub/gh" <<GH
#!/usr/bin/env bash
printf '%s\n' "\$*" >> "$hb_stub/argv.log"
case "\$*" in
    *"pr list"*)   echo 90007 ;;
    *"/comments"*) echo 0 ;;
    *"run list"*)  echo 0 ;;
esac
GH
chmod +x "$hb_stub/gh"

# A SECOND WORKTREE, standing in for the one the session is not sitting in. Hosted `origin` because
# both the slug derivation and the refusal depend on it - see the push fixture's own note.
hb_other="$(mktemp -d)"
(
  cd "$hb_other" || exit 1
  git init -q .
  git checkout -q -b selftest/other-worktree 2>/dev/null || git branch -q -m selftest/other-worktree
  git remote add origin https://github.com/astubbs/parallel-consumer.git
  : > .keep
  git add .keep
  git -c user.email=selftest@example.invalid -c user.name=selftest commit -qm "fixture"
)

hb_fire() { # <command> [payload-cwd | "-" to omit the cwd field] -> stdout of the hook
    : > "$hb_stub/argv.log"
    if [ "${2:-}" = "-" ]; then
        printf '{"tool_name":"Bash","tool_input":{"command":"%s"}}' "$1" \
            | PATH="$hb_stub:$PATH" bash "$HIST_HOOK" 2>/dev/null
    else
        printf '{"tool_name":"Bash","cwd":"%s","tool_input":{"command":"%s"}}' "${2:-$PWD}" "$1" \
            | PATH="$hb_stub:$PATH" bash "$HIST_HOOK" 2>/dev/null
    fi
}
hb_head() { stub_head_arg "$hb_stub/argv.log"; }

hb_out="$(hb_fire 'git push --force origin feats/elsewhere')"
assert "a force-push names the branch its refspec names" feats/elsewhere "$(hb_head)"
[ "$(hb_head)" = "$hb_cwd_branch" ] && got=the_pre_fix_answer || got=not_the_cwd_branch
assert "and not the branch this directory happens to be on" not_the_cwd_branch "$got"
[ -n "$hb_out" ] && got=DENY || got=ALLOW
assert "naming the branch from the command still REFUSES" DENY "$got"

# THE PR HEAD IS THE DESTINATION of a `src:dst` refspec, not the source - `dst` is what a pull
# request has as its head branch.
hb_fire 'git push --force origin HEAD:feats/destination' >/dev/null
assert "a src:dst refspec is read at its destination" feats/destination "$(hb_head)"
hb_fire 'git push origin --delete doomed-branch' >/dev/null
assert "a remote branch deletion names the branch being deleted" doomed-branch "$(hb_head)"

# NO REFSPEC IS A REAL ANSWER: the working directory is all there is, and the refusal has to say so
# rather than presenting a guess as a measurement.
hb_out="$(hb_fire 'git push -f')"
assert "a push with no refspec falls back to this directory" "$hb_cwd_branch" "$(hb_head)"
case "$hb_out" in *"DOES NOT NAME A BRANCH"*) got=says_it_guessed ;; *) got=presented_as_fact ;; esac
assert "and the refusal says the branch was not named by the command" says_it_guessed "$got"

# THE AMEND CASE, which is the second half of the 2026-08-31 incident: a non-push rewrite has no
# refspec to read, so the only improvement available is to look in the right DIRECTORY and to say
# which one. A leading `cd <path> &&` is the command saying where it runs, and outranks everything.
hb_fire "cd $hb_other && git commit --amend --no-edit" >/dev/null
assert "an amend behind a cd prefix is looked up in THAT worktree" selftest/other-worktree "$(hb_head)"
[ "$(hb_head)" = "$hb_cwd_branch" ] && got=the_pre_fix_answer || got=not_the_cwd_branch
assert "and not in the directory the hook itself runs in" not_the_cwd_branch "$got"

# ...and with no `cd`, the tool call's own `cwd` from the payload, which is the field the pre-fix
# hook ignored entirely in favour of its own process directory.
hb_out="$(hb_fire 'git commit --amend --no-edit' "$hb_other")"
assert "an amend is looked up in the tool call's own directory" selftest/other-worktree "$(hb_head)"
case "$hb_out" in *"$hb_other"*) got=names_the_directory ;; *) got=unattributed ;; esac
assert "the refusal names the directory it derived the branch from" names_the_directory "$got"

# THE REVIEW ROUND ON astubbs/parallel-consumer#382, pinned. Each of these was a way for this
# refusing guard to answer about the wrong thing - or not answer at all - found by fresh reviewers
# and an independent cross-model pass after the first fix landed.

# `git -C <path>` used to put the path where the subcommand should be, so the guard matched NOTHING
# and a force-push sailed through in silence - a total bypass of the thing this hook refuses.
hb_out="$(hb_fire "git -C $hb_other push --force origin feats/elsewhere")"
[ -n "$hb_out" ] && got=DENY || got=ALLOW
assert "git -C <path> push --force is not a silent bypass" DENY "$got"
assert "and its refspec still names the branch" feats/elsewhere "$(hb_head)"
hb_out="$(hb_fire "git -C $hb_other commit --amend --no-edit")"
assert "git -C relocates the amend lookup to THAT worktree" selftest/other-worktree "$(hb_head)"

# A NEWLINE is a command boundary, not whitespace: `git push -f<newline>git log -1` used to swallow
# the next command as push arguments and name `log` as the branch - a confident wrong answer where
# the honest one is the labelled fallback.
hb_out="$(hb_fire 'git push -f\ngit log -1')"
assert "a newline-separated payload does not donate a fake branch" "$hb_cwd_branch" "$(hb_head)"
case "$hb_out" in *"DOES NOT NAME A BRANCH"*) got=says_it_guessed ;; *) got=presented_as_fact ;; esac
assert "and that fallback says the branch was not named" says_it_guessed "$got"
hb_fire 'git push --force origin feats/real\ngit log' >/dev/null
assert "a real refspec before a newline still wins" feats/real "$(hb_head)"

# UNSPACED OPERATORS stay operators: `feature&&git` is not a branch name.
hb_fire 'git push --force origin feats/spliced&&git status' >/dev/null
assert "an unspaced && does not fuse into the branch name" feats/spliced "$(hb_head)"

# TWO command-position `cd`s are AMBIGUOUS - the amend may run in either - so the guard must fall
# back to the payload cwd, whose label already says it is a guess, rather than presenting the
# FIRST cd as the directory the command changes into.
hb_dead="$(mktemp -d)"
hb_fire "cd $hb_dead && echo x && cd $hb_other && git commit --amend --no-edit" "$hb_other" >/dev/null
assert "two cds fall back to the payload cwd, not the first cd" selftest/other-worktree "$(hb_head)"
rm -rf "$hb_dead"

# A RELATIVE `cd` is relative to where the COMMAND runs. The hook process sits elsewhere, so
# resolving it from the hook's own directory would test the wrong tree - and succeed, because
# same-named subdirectories exist in every worktree of this repository.
( cd "$hb_other" && mkdir -p relsub && cd relsub && : > .keep )
hb_fire 'cd relsub && git commit --amend --no-edit' "$hb_other" >/dev/null
assert "a relative cd resolves against the payload cwd" selftest/other-worktree "$(hb_head)"

# THE LAST-RESORT TIER: no refspec, no cd, no payload cwd leaves only the hook process's own
# directory, and the refusal must SAY that is what it used - the least trustworthy answer in the
# derivation order, which is exactly why its label has to survive.
hb_out="$(hb_fire 'git commit --amend --no-edit' -)"
case "$hb_out" in *"this hook process's directory"*) got=labelled_last_resort ;; *) got=unlabelled ;; esac
assert "with no cwd at all, the hook-directory fallback is labelled" labelled_last_resort "$got"

# THE CODEX ROUND (astubbs/parallel-consumer#382), pinned - four more ways to steer the guard.

# A -C recorded while scanning an EARLIER invocation must not survive into the one that carries
# the verdict: `git -C <dir> status && git commit --amend` runs the amend in the payload cwd.
# Pre-fix this either answered about <dir> or, when <dir> was not a repository, went completely
# SILENT - a bypass of the refusal itself.
hb_bleed="$(mktemp -d)"
hb_out="$(hb_fire "git -C $hb_bleed status && git commit --amend --no-edit" "$hb_other")"
[ -n "$hb_out" ] && got=DENY || got=ALLOW
assert "a -C on an EARLIER command does not bleed into the amend" DENY "$got"
assert "...and the amend is answered from the payload cwd" selftest/other-worktree "$(hb_head)"
rm -rf "$hb_bleed"

# --recurse-submodules takes a separate value; the python copy of the parser must skip it too
# (change one, change the other - and the first round changed only the bash one).
hb_fire 'git push --force --recurse-submodules on-demand origin feats/subm' >/dev/null
assert "the python parser skips --recurse-submodules and its value" feats/subm "$(hb_head)"

# `cd /x & git commit` BACKGROUNDS the cd into a subshell - the amend stays where the payload says,
# so the prefix must not be trusted across a cwd-losing operator.
hb_fire "cd $hb_other & git commit --amend --no-edit" >/dev/null
assert "a backgrounded cd does not relocate the amend" "$hb_cwd_branch" "$(hb_head)"

# AN UNEXPANDED $VAR is source text, not a branch - fall back with the label, never assert the
# literal.
hb_out="$(hb_fire 'git push --force origin $TARGET_BRANCH')"
assert "an unexpanded \$VAR refspec falls back to this directory" "$hb_cwd_branch" "$(hb_head)"
case "$hb_out" in *"DOES NOT NAME A BRANCH"*) got=says_it_guessed ;; *) got=presented_as_fact ;; esac
assert "and the \$VAR fallback is labelled as a guess" says_it_guessed "$got"

rm -rf "$hb_stub" "$hb_other"
cd "$hb_prev_cwd" || exit 1
echo
echo "--- the PR lookup, in the three hooks that make one ---"

# ONE LOOKUP, THREE ANSWERS. `gh pr list --head "$branch" 2>/dev/null || true` produced a single
# outcome for three unrelated situations, and every hook that used it told the operator the same
# thing in all three: the branch has no PR; the lookup FAILED (gh missing, unauthenticated,
# rate-limited, offline); or the lookup answered for the WRONG REPOSITORY, because gh prefers
# `upstream` in this fork and `remote.origin.gh-resolved` is local and uncommitted. Both of the
# first two were hit live on astubbs/parallel-consumer#356 in one day, each time reported as "No PR
# was found for this branch".
#
# These arms run in the fixture repo the push section created, whose `origin` is what the hooks now
# derive the slug from. Each swaps in its own `gh`, first on PATH, so the answer under test is
# chosen rather than ambient - the property the section is about is what the hook SAYS, and the
# refusal it must still emit while saying it.
lk_stub="$(mktemp -d)"
lk_tmp="$(mktemp -d)"
lk_gh() { cat > "$lk_stub/gh"; chmod +x "$lk_stub/gh"; }
lk_fire() { # <hook-path> <command> -> stdout of the hook
    printf '{"tool_name":"Bash","tool_input":{"command":"%s"}}' "$2" \
        | PATH="$lk_stub:$PATH" TMPDIR="$lk_tmp" CLAUDE_CODE_SESSION_ID=selftest-lookup \
          bash "$1" 2>/dev/null
}
lk_rewrite() { lk_fire "$HIST_HOOK" 'git push --force origin HEAD'; }

# 1. THE LOOKUP FAILED. Red against the pre-fix hook, which printed "No PR was found for this
# branch" here - an assertion of absence built on an answer nobody received.
lk_gh <<'GH'
#!/usr/bin/env bash
echo "gh: To get started with GitHub CLI, please run: gh auth login" >&2
exit 4
GH
lk_out="$(lk_rewrite)"
case "$lk_out" in *"lookup FAILED"*) got=names_the_failure ;; *) got=silent_about_it ;; esac
assert "a failed lookup is reported as a failure" names_the_failure "$got"
case "$lk_out" in *"No PR was found"*|*"came back empty"*) got=claims_absence ;; *) got=claims_nothing ;; esac
assert "a failed lookup never claims the branch has no PR" claims_nothing "$got"
case "$lk_out" in *"auth login"*) got=quotes_gh ;; *) got=discards_the_reason ;; esac
assert "the refusal repeats what gh actually said" quotes_gh "$got"
[ -n "$lk_out" ] && got=DENY || got=ALLOW
assert "a failed lookup still REFUSES the rewrite" DENY "$got"

# 2. THE NEAR MISS: gh answers, and the answer is that there is no PR. gh exits 0 printing nothing
# for a head branch with no open PR, so this - and only this - is a measured absence. The message
# must still name the repository it asked, or the reader cannot tell it from the wrong-repo answer.
lk_gh <<'GH'
#!/usr/bin/env bash
exit 0
GH
lk_out="$(lk_rewrite)"
case "$lk_out" in *"came back empty"*) got=measured_absence ;; *) got=undifferentiated ;; esac
assert "an empty answer is reported as a measured absence" measured_absence "$got"
case "$lk_out" in *"lookup FAILED"*) got=cries_wolf ;; *) got=accurate ;; esac
assert "an empty answer is not reported as a failure" accurate "$got"
case "$lk_out" in *astubbs/parallel-consumer*) got=names_the_repo ;; *) got=unattributed ;; esac
assert "the no-PR message names the repository it asked" names_the_repo "$got"
[ -n "$lk_out" ] && got=DENY || got=ALLOW
assert "no PR still REFUSES the rewrite" DENY "$got"

# 3. EVERY CALL NAMES THE REPOSITORY. The wrong-repo case cannot be observed from the message - a
# lookup that answers for confluentinc/parallel-consumer succeeds and reads exactly like a correct
# one - so it is pinned at the only place it is visible: the argv gh was handed. Red against the
# pre-fix hook, whose three calls carried no `-R` and whose `gh api` used the `{owner}/{repo}`
# placeholder, which resolves the same wrong way.
cat > "$lk_stub/gh" <<GH
#!/usr/bin/env bash
printf '%s\n' "\$*" >> "$lk_stub/argv.log"
case "\$*" in
    *"pr list"*)   echo 90003 ;;
    *"/comments"*) echo 0 ;;
    *"run list"*)  echo 0 ;;
esac
GH
chmod +x "$lk_stub/gh"
: > "$lk_stub/argv.log"
lk_rewrite > /dev/null
lk_calls=0; lk_unqualified=0
while IFS= read -r line; do
    [ -n "$line" ] || continue
    lk_calls=$((lk_calls + 1))
    case "$line" in
        *"-R astubbs/parallel-consumer"*|*"repos/astubbs/parallel-consumer/"*) ;;
        *) lk_unqualified=$((lk_unqualified + 1)) ;;
    esac
done < "$lk_stub/argv.log"
assert "all three gh calls were made" 3 "$lk_calls"
assert "no gh call is left to resolve the repository itself" 0 "$lk_unqualified"

# 4. AND NO UNQUALIFIED FALLBACK when the slug cannot be derived. A local-path `origin` has no
# hosting slug to read, and asking gh anyway is the wrong-repo bug arriving by another door - so the
# lookup is not attempted at all, and the refusal says so.
git remote set-url origin /a/local/path/parallel-consumer
: > "$lk_stub/argv.log"
lk_out="$(lk_rewrite)"
git remote set-url origin https://github.com/astubbs/parallel-consumer.git
case "$lk_out" in *"DID NOT RUN"*) got=says_it_skipped ;; *) got=quiet ;; esac
assert "an underivable repository is reported, not guessed at" says_it_skipped "$got"
[ -s "$lk_stub/argv.log" ] && got=called_gh_anyway || got=called_nothing
assert "gh is not called unqualified when the slug is unknown" called_nothing "$got"
[ -n "$lk_out" ] && got=DENY || got=ALLOW
assert "an underivable repository still REFUSES the rewrite" DENY "$got"

# 5. THE PUSH REMINDER, whose whole output is a reminder - so a lookup that failed produced silence
# there, indistinguishable from a branch with no PR and from a branch whose note says nothing.
# Advisory in both directions: it must say the lookup failed, and must still never deny.
lk_gh <<'GH'
#!/usr/bin/env bash
echo "gh: could not connect to api.github.com" >&2
exit 1
GH
rm -f "$lk_tmp"/pc-push-reminder-*
lk_out="$(lk_fire "$PUSH_HOOK" 'git push')"
case "$lk_out" in *"could not find out"*) got=says_so ;; *) got=silent ;; esac
assert "the push reminder says when the lookup failed" says_so "$got"
case "$lk_out" in *'"deny"'*) got=blocked ;; *) got=advisory ;; esac
assert "the push reminder still never denies" advisory "$got"
lk_gh <<'GH'
#!/usr/bin/env bash
exit 0
GH
rm -f "$lk_tmp"/pc-push-reminder-*
lk_out="$(lk_fire "$PUSH_HOOK" 'git push')"
[ -z "$lk_out" ] && got=silent || got=fired
assert "a branch with no PR stays silent on push" silent "$got"

# 6. THE MERGE GUARD's second arm reads the PR's inflight note, and a failed lookup switched that
# arm off without a word - the merge then proceeded with half the guard having measured nothing.
# It stays fail-open by design (its header owns that decision), so the fix is that it says so.
lk_gh <<'GH'
#!/usr/bin/env bash
echo "gh: HTTP 403: API rate limit exceeded" >&2
exit 1
GH
lk_out="$(lk_fire "$HOOKS/check-merge-outstanding-work.sh" 'gh pr merge --squash')"
case "$lk_out" in *"did NOT check"*) got=says_so ;; *) got=silent ;; esac
assert "the merge guard says when it could not identify the PR" says_so "$got"
case "$lk_out" in *'"deny"'*) got=blocked ;; *) got=advisory ;; esac
assert "the merge guard stays fail-open on its own inability" advisory "$got"

# 7. AND A FOURTH ANSWER NOBODY DESIGNED: none of the three. The lookup prints `found`, `failed` or
# `none` on every path it can reach, so an EMPTY answer means the interpreter died before printing -
# killed for memory, or a BaseException that its `except Exception` cannot catch. Neither status
# dispatch had a fallback arm, so empty fell through the `found`/`failed` tests and landed exactly
# where `none` lands: silence. That is this branch's own defect one level down - an absence of
# measurement rendered as a measurement of absence - and `check-history-rewrite.sh` already carried
# the backstop (`detail` empty -> "produced no answer at all") that these two lacked.
#
# The lookup is the only python3 call in either hook whose first argument is `-`, so a stub that
# kills just that invocation leaves the token scan and the JSON emitter on the real interpreter -
# the hook still decides that the command is in scope, and still emits, which is what makes the
# silence attributable to the dispatch rather than to a hook that never ran.
lk_real_py3="$(command -v python3)"
cat > "$lk_stub/python3" <<PY3
#!/usr/bin/env bash
if [ "\${1:-}" = "-" ]; then cat > /dev/null; exit 137; fi
exec "$lk_real_py3" "\$@"
PY3
chmod +x "$lk_stub/python3"
rm -f "$lk_tmp"/pc-push-reminder-*
lk_out="$(lk_fire "$PUSH_HOOK" 'git push')"
case "$lk_out" in *"no recognizable answer"*) got=says_so ;; *) got=silent ;; esac
assert "the push reminder speaks up when the lookup answered nothing at all" says_so "$got"
case "$lk_out" in *'"deny"'*) got=blocked ;; *) got=advisory ;; esac
assert "an unrecognizable answer still never denies the push" advisory "$got"
lk_out="$(lk_fire "$HOOKS/check-merge-outstanding-work.sh" 'gh pr merge --squash')"
case "$lk_out" in *"no recognizable answer"*) got=says_so ;; *) got=silent ;; esac
assert "the merge guard speaks up when the lookup answered nothing at all" says_so "$got"
case "$lk_out" in *'"deny"'*) got=blocked ;; *) got=advisory ;; esac
assert "an unrecognizable answer leaves the merge guard fail-open" advisory "$got"
rm -f "$lk_stub/python3"

rm -rf "$lk_stub" "$lk_tmp"
echo
echo "--- inject-branch-context.sh ---"

# WHY THESE CASES LOOK THE WAY THEY DO. An injection hook's CORRECT output on a boring branch is
# silence, and silence is byte-identical to being broken, unregistered or absent - so most cases
# below force the hook to speak, and every asserted-silent case is paired with a forced one proving
# the same call path can still talk. The lesson is astubbs/parallel-consumer#339's, applied here.
#
# EVERY CASE PINS ITS OWN FIXTURE. A self-test for a branch-context hook must not be a function of
# which branch it happens to be run on: the CI checkout is detached (which this hook deliberately
# stays silent for), and a developer's worktree has whatever commits it has. Each case builds a
# scratch repository with the commits, notes, marker and `gh` answers it wants to assert about.
BCTX_HOOK="$HOOKS/inject-branch-context.sh"

bctx_payload() { # <cwd> <event> [tool_name] [prompt] [agent_type] [agent_id] -> hook payload JSON
    # BUILT IN PYTHON, NOT BY printf. `%r` is a Python conversion and shell printf rejects it, which
    # produced an empty payload and turned every case here into a silent-looking pass-through -
    # exactly the "silence is indistinguishable from broken" failure these cases exist to catch,
    # committed inside the cases themselves.
    python3 -c '
import json, sys
cwd, event = sys.argv[1], sys.argv[2]
d = {"cwd": cwd, "hook_event_name": event}
if event == "SessionStart":
    d["source"] = "startup"
argv = sys.argv + [""] * 8
if argv[3]:
    d["tool_name"] = argv[3]
if argv[4]:
    d["tool_input"] = {"prompt": argv[4]}
if argv[5]:
    d["agent_type"] = argv[5]
if argv[6]:
    d["agent_id"] = argv[6]
print(json.dumps(d))
' "$@"
}

# A PRIVATE TMPDIR PER SUITE RUN, and it is not tidiness. The hook keys its throttle stamps and its
# PR cache off ${TMPDIR:-/tmp}, and these fixtures use FIXED names (`bctxfixture01`) - so two
# concurrent runs of this suite, which is ordinary on a box with several agent sessions, clear and
# recreate each other's stamps and the throttle assertions fail at random. Diagnosed from exactly
# that failure in the push-reminder cases below, which shared /tmp the same way.
BCTX_TMPDIR="$(mktemp -d)"

bctx_fire() { # <payload-json> [PATH-override] -> the hook's stdout
    # Templated, not bare: it costs nothing and gives the file a recognisable name. No portability
    # on the platform its portability cases are about.
    local payload="$1" pathover="${2:-$PATH}" tmp out
    tmp=$(mktemp "$BCTX_TMPDIR/payload.XXXXXX")
    printf '%s' "$payload" > "$tmp"
    out=$(PATH="$pathover" TMPDIR="$BCTX_TMPDIR" bash "$BCTX_HOOK" < "$tmp" 2>/dev/null)
    rm -f "$tmp"
    printf '%s' "$out"
}

bctx_context() { # <hook stdout> -> the additionalContext string, or the raw text for SessionStart
    printf '%s' "$1" | python3 -c '
import json,sys
raw=sys.stdin.read()
try:
    print(json.loads(raw)["hookSpecificOutput"]["additionalContext"])
except Exception:
    print(raw)
'
}

bctx_clean_stamps() { rm -f "$BCTX_TMPDIR"/pc-branch-context-* 2>/dev/null; return 0; }

bctx_repo() { # <dir> - master, then a feature branch with a bodied commit, a bodyless one, a note
    local d="$1"
    mkdir -p "$d/docs/inflight" "$d/docs/plans"
    (
      cd "$d" || exit 1
      git init -q .
      git checkout -q -b master 2>/dev/null || git branch -q -m master
      echo base > base.txt
      git add base.txt
      git -c user.email=selftest@example.invalid -c user.name=selftest commit -qm "base"
      git checkout -q -b feats/bctx-fixture
      echo one > one.txt
      git add one.txt
      git -c user.email=selftest@example.invalid -c user.name=selftest commit -qF - <<'MSG'
feat(fixture) astubbs#999: the decision this branch made on purpose

BCTX_BODY_ONLY_STRING. Three non-empty body lines, so the hook must report 3.

It argues by name against the most extractable thing in the diff.

Third line.
MSG
      printf '# handoff\nBCTX_NOTE_INNARDS\n' > docs/inflight/pr-999-handoff.md
      git add docs/inflight/pr-999-handoff.md
      git -c user.email=selftest@example.invalid -c user.name=selftest commit -qm "docs(inflight): the handoff note"
      printf 'owner: selftest\nstatus: BCTX_MARKER_LINE\n' > .worktree-owner
    )
}

bctx_gh_stub() { # <dir> <mode> - a `gh` first on PATH answering in one of five shapes
    cat > "$1/gh" <<STUB
#!/usr/bin/env bash
case "\$*" in
  *"pr view"*)
    case "$2" in
      pr)      cat <<'JSON'
{"number":9412,"title":"BCTX_PR_TITLE","body":"line1\nline2\nline3","url":"https://example.invalid/9412","state":"OPEN","isDraft":false,
 "comments":[{"author":{"login":"github-actions"}},{"author":{"login":"github-actions"}},{"author":{"login":"a-real-human"}}],
 "reviews":[{"author":{"login":"claude"},"state":"COMMENTED"},{"author":{"login":"claude"},"state":"COMMENTED"}]}
JSON
               ;;
      nopr)    echo "no pull requests found for branch \"whatever\"" >&2; exit 1 ;;
      broken)  echo "error connecting to github.com" >&2; exit 1 ;;
      slow)    sleep 30 ;;
      garbage) echo "not json at all" ;;
    esac
    ;;
esac
exit 0
STUB
    chmod +x "$1/gh"
}

bctx_tmp="$(mktemp -d)"
bctx_repo "$bctx_tmp/wt"
# `origin` exists so the repo slug resolves; nothing ever contacts it, because `gh` is stubbed.
git -C "$bctx_tmp/wt" remote add origin https://github.com/selftest-owner/selftest-repo.git
# The DISPATCHER sits in its own working tree on master - the coordinator's real position, and
# the one that makes "describe the tree you dispatched TO, not the one you are in" a real assertion.
bctx_dispatcher="$bctx_tmp/dispatcher"
bctx_repo "$bctx_dispatcher"
git -C "$bctx_dispatcher" checkout -q master
stub_pr="$(mktemp -d)";      bctx_gh_stub "$stub_pr" pr
stub_nopr="$(mktemp -d)";    bctx_gh_stub "$stub_nopr" nopr
stub_broken="$(mktemp -d)";  bctx_gh_stub "$stub_broken" broken
stub_slow="$(mktemp -d)";    bctx_gh_stub "$stub_slow" slow

sess_payload="$(bctx_payload "$bctx_tmp/wt" SessionStart)"

# --- SessionStart, the forced-to-speak baseline every silent case below is measured against ------
bctx_clean_stamps
out="$(bctx_fire "$sess_payload" "$stub_pr:$PATH")"
ctx="$(bctx_context "$out")"
case "$ctx" in *'Branch context: `feats/bctx-fixture`'*) got=named ;; *) got=silent_or_wrong ;; esac
assert "SessionStart on a feature branch names the branch" named "$got"
# SessionStart injects RAW STDOUT; only PreToolUse discards it and needs the JSON envelope. Emitting
# the envelope here would put a wall of JSON in front of the agent instead of the report.
case "$out" in *'hookSpecificOutput'*) got=envelope ;; *) got=plain ;; esac
assert "SessionStart emits plain text, not the PreToolUse envelope" plain "$got"

# --- the commits, which are the point of the hook ------------------------------------------------
case "$ctx" in *'the decision this branch made on purpose'*) got=listed ;; *) got=missing ;; esac
assert "the branch's commit subjects are listed" listed "$got"
case "$ctx" in *'[  3]'*) got=counted ;; *) got=uncounted ;; esac
assert "a commit body is reported by its non-empty line count" counted "$got"
case "$ctx" in *'[  -]'*) got=marked ;; *) got=unmarked ;; esac
assert "a bodyless commit is marked as having none" marked "$got"
# CHEAP: NAMES AND POINTERS, NEVER BODIES. The contract inject-recorded-knowledge.sh's header states
# and this hook inherits - the failure being fixed is not knowing the record EXISTS. If body text
# ever starts being inlined, this block's size stops being bounded by the number of commits.
case "$ctx" in *BCTX_BODY_ONLY_STRING*) got=inlined ;; *) got=pointer_only ;; esac
assert "commit BODIES are not inlined, only counted" pointer_only "$got"
case "$ctx" in *BCTX_NOTE_INNARDS*) got=inlined ;; *) got=pointer_only ;; esac
assert "note CONTENTS are not inlined, only named" pointer_only "$got"

# --- the handoff note and the marker --------------------------------------------------------------
case "$ctx" in *'docs/inflight/pr-999-handoff.md'*) got=listed ;; *) got=missing ;; esac
assert "a branch-only docs/inflight note is listed" listed "$got"
case "$ctx" in *BCTX_MARKER_LINE*) got=quoted ;; *) got=missing ;; esac
assert "the .worktree-owner marker is quoted" quoted "$got"

# --- the PR, including the comment authors that were the 2026-08-24 miss ---------------------------
case "$ctx" in *'selftest-owner/selftest-repo#9412'*) got=named ;; *) got=missing ;; esac
assert "the PR is named with the slug derived from origin" named "$got"
case "$ctx" in *'BCTX_PR_TITLE'*) got=titled ;; *) got=missing ;; esac
assert "the PR title is reported" titled "$got"
case "$ctx" in *'body: 3 lines'*) got=measured ;; *) got=missing ;; esac
assert "the PR body is measured, not fetched into context" measured "$got"
# The miss this hook was written for was a PR COMMENT, not the body - so a count that buries the one
# human comment under five bot ones is the same miss with extra steps.
case "$ctx" in *'a-real-human x1'*) got=attributed ;; *) got=anonymous ;; esac
assert "PR comments are counted per author" attributed "$got"
case "$ctx" in *'Read the ones from a-real-human'*) got=singled_out ;; *) got=buried ;; esac
assert "the non-bot commenter is singled out from the bots" singled_out "$got"
case "$ctx" in *'claude COMMENTED x2'*) got=aggregated ;; *) got=repeated ;; esac
assert "repeat reviews by one author are aggregated, not repeated" aggregated "$got"

# --- DEGRADED READS ARE LOUD, NEVER SHORT ---------------------------------------------------------
# Measured incident, not a hypothesis: inject-recorded-knowledge.sh's GNU-only `xargs -r` silently
# shortens its own index under a BSD xargs, and a truncated-but-plausible block is worse than none.
bctx_clean_stamps
ctx="$(bctx_context "$(bctx_fire "$sess_payload" "$stub_broken:$PATH")")"
case "$ctx" in *'Open PR - UNKNOWN'*) got=loud ;; *) got=quiet ;; esac
assert "a failing gh makes the PR section LOUDLY unknown" loud "$got"
case "$ctx" in *'Not "no PR", UNKNOWN'*) got=says_so ;; *) got=ambiguous ;; esac
assert "the unknown PR section refuses to be read as 'no PR'" says_so "$got"
# ...and the branch half must survive the PR half failing, or one dead network call costs the commits.
case "$ctx" in *'the decision this branch made on purpose'*) got=kept ;; *) got=lost ;; esac
assert "the commits still appear when gh fails" kept "$got"

# A CONFIRMED absence is a different answer from a failure, and must not read as an alarm - every
# fresh branch would otherwise print one, and an alarm that is always on gets scrolled past.
bctx_clean_stamps
ctx="$(bctx_context "$(bctx_fire "$sess_payload" "$stub_nopr:$PATH")")"
case "$ctx" in *'PR: none open for this branch'*) got=measured ;; *) got=alarmed ;; esac
assert "a confirmed absent PR is reported as a fact, not an alarm" measured "$got"
case "$ctx" in *'UNKNOWN'*) got=alarmed ;; *) got=measured ;; esac
assert "a confirmed absent PR does not also raise UNKNOWN" measured "$got"

# THE NETWORK CALL IS BOUNDED. `gh` sits on a path that fires per dispatch and per subagent, so a
# hung link must cost the timeout and not the session. GNU `timeout(1)` is deliberately NOT used -
# macOS does not ship it - so this case is what proves the python-side bound actually bounds.
bctx_clean_stamps
bctx_start=$(date +%s)
ctx="$(bctx_context "$(bctx_fire "$sess_payload" "$stub_slow:$PATH")")"
bctx_elapsed=$(( $(date +%s) - bctx_start ))
[ "$bctx_elapsed" -lt 20 ] && got=bounded || got=hung
assert "a hanging gh is bounded well under its 30s sleep (took ${bctx_elapsed}s)" bounded "$got"
case "$ctx" in *'UNKNOWN'*) got=loud ;; *) got=quiet ;; esac
assert "a timed-out gh is reported, not silently dropped" loud "$got"

# --- SILENCE, each paired with the forced case above proving the path can still talk ---------------
bctx_clean_stamps
mp="$sess_payload"
git -C "$bctx_tmp/wt" checkout -q master
out="$(bctx_fire "$mp" "$stub_pr:$PATH")"
[ -z "$out" ] && got=silent || got=spoke
assert "on master there is no inherited branch to describe, so it is silent" silent "$got"
git -C "$bctx_tmp/wt" checkout -q feats/bctx-fixture
bctx_clean_stamps
out="$(bctx_fire "$mp" "$stub_pr:$PATH")"
[ -n "$out" ] && got=spoke || got=silent
assert "...and the same payload speaks again once off master (the pairing)" spoke "$got"

# DETACHED HEAD. GitHub Actions checks PRs out detached, so a case that only passes on a developer
# machine is how remind-inflight-on-push.sh's fixtures went red in CI and nowhere else.
bctx_clean_stamps
git -C "$bctx_tmp/wt" checkout -q --detach HEAD
out="$(bctx_fire "$mp" "$stub_pr:$PATH")"
[ -z "$out" ] && got=silent || got=spoke
assert "a detached checkout has no branch to describe, so it is silent" silent "$got"
git -C "$bctx_tmp/wt" checkout -q feats/bctx-fixture

bctx_clean_stamps
nogit="$(mktemp -d)"
np="$(bctx_payload "$nogit" SessionStart)"
out="$(bctx_fire "$np" "$stub_pr:$PATH")"
[ -z "$out" ] && got=silent || got=spoke
assert "a directory outside any git repo is silent" silent "$got"
rmdir "$nogit" 2>/dev/null

# FAILING OPEN. Every error path prints nothing and exits 0 - a broken reminder must not be a broken
# session, which is the contract every hook in this directory shares.
bctx_clean_stamps
out="$(bctx_fire 'not json at all' "$stub_pr:$PATH")"
[ -z "$out" ] && got=silent || got=spoke
assert "an unparseable payload fails OPEN" silent "$got"
printf '{"tool_name":"Read","tool_input":{"file_path":"/etc/hosts"}}' > "$bctx_tmp/read.json"
out=$(PATH="$stub_pr:$PATH" bash "$BCTX_HOOK" < "$bctx_tmp/read.json" 2>/dev/null); rc=$?
assert "an ordinary tool call the hook has no business with exits 0" 0 "$rc"
[ -z "$out" ] && got=silent || got=spoke
assert "...and says nothing" silent "$got"

# A LOCAL-PATH ORIGIN IS NOT A REPOSITORY. `git clone /path/to/repo` - how a scratch or baseline
# checkout is made - would otherwise yield a slug from the last two path segments and send `gh` after
# a repo that does not exist, naming a plausible wrong one while failing. Paired with the PR case
# above, which proves the same path does resolve a real slug.
bctx_clean_stamps
local_origin="$(mktemp -d)"
bctx_repo "$local_origin/wt"
git -C "$local_origin/wt" remote add origin "$local_origin/some/local/path"
lp="$(bctx_payload "$local_origin/wt" SessionStart)"
ctx="$(bctx_context "$(bctx_fire "$lp" "$stub_pr:$PATH")")"
case "$ctx" in *'is not a remote URL'*) got=accurate ;; *) got=plausible_wrong_repo ;; esac
assert "a local-path origin is reported as having no repository to ask" accurate "$got"
case "$ctx" in *'/some/local/path'*|*'#9412'*) got=invented ;; *) got=clean ;; esac
assert "...and no slug is invented from the path segments" clean "$got"

# A `file://` ORIGIN IS THE SAME LOCAL CLONE, WEARING A SCHEME. The first version of the guard above
# asked only whether the URL contained `://`, so `git clone file:///path/to/repo` - the documented
# way to force a real transport against a local repository - walked through it and produced the very
# `git/parallel-consumer` slug the guard exists to prevent. Found in review of
# astubbs/parallel-consumer#350; the bare-path case above cannot catch it, which is why it is its own
# case rather than another spelling of the same one.
git -C "$local_origin/wt" remote set-url origin "file://$local_origin/some/local/path"
ctx="$(bctx_context "$(bctx_fire "$lp" "$stub_pr:$PATH")")"
case "$ctx" in *'is not a remote URL'*) got=accurate ;; *) got=plausible_wrong_repo ;; esac
assert "a file:// origin is a local clone, not a repository to ask about" accurate "$got"
case "$ctx" in *'/some/local/path'*|*'#9412'*) got=invented ;; *) got=clean ;; esac
assert "...and no slug is invented from its path segments either" clean "$got"

# EVERY SPELLING THE GUARD CLAIMS TO HANDLE, DRIVEN THROUGH `git remote` RATHER THAN ASSERTED IN
# PROSE. Review of astubbs/parallel-consumer#350 noted that the claim "verified against N forms" rested
# on ad-hoc checks: only the default `https://` origin and the `file://` case above actually reached
# the code, so an edit to the scheme regex could regress credentialed, trailing-slash, scp-style,
# `ssh://`-with-a-port or `git://` remotes and nothing would go red. Each row sets the SAME
# repository, so the expected slug is a constant and any row that resolves differently is the bug.
bctx_accepts() { # <label> <url>
    bctx_clean_stamps
    git -C "$bctx_tmp/wt" remote set-url origin "$2"
    case "$(bctx_context "$(bctx_fire "$sess_payload" "$stub_pr:$PATH")")" in
        *'selftest-owner/selftest-repo#9412'*) got=resolved ;;
        *) got=lost ;;
    esac
    assert "a $1 origin still resolves to the repository" resolved "$got"
}
bctx_accepts "credentialed https"   'https://x-access-token:TOKEN@github.com/selftest-owner/selftest-repo.git'
bctx_accepts "trailing-slash https" 'https://github.com/selftest-owner/selftest-repo/'
bctx_accepts "scp-style"            'git@github.com:selftest-owner/selftest-repo.git'
bctx_accepts "ssh://"               'ssh://git@github.com/selftest-owner/selftest-repo.git'
bctx_accepts "ssh:// on a nonstandard port" 'ssh://git@ghe.internal:2222/selftest-owner/selftest-repo.git'
bctx_accepts "git://"               'git://github.com/selftest-owner/selftest-repo.git'

# A scheme that is not a git transport is the same class as `file://`, and the reject arm needs a row
# too or the allowlist could quietly become "any scheme" again without a case noticing.
bctx_clean_stamps
git -C "$bctx_tmp/wt" remote set-url origin 'ftp://example.com/selftest-owner/selftest-repo'
case "$(bctx_context "$(bctx_fire "$sess_payload" "$stub_pr:$PATH")")" in
    *'is not a remote URL'*) got=rejected ;;
    *) got=trusted ;;
esac
assert "an ftp:// origin is not a transport git clones from, and is rejected" rejected "$got"
git -C "$bctx_tmp/wt" remote set-url origin https://github.com/selftest-owner/selftest-repo.git
rm -rf "$local_origin"

# NO BSD `mktemp` ARM HERE, DELIBERATELY. A previous revision stubbed `mktemp` to reject a bare
# invocation, modelling BSD, and asserted the hook still spoke under it. The premise was false:
# running the sweep on a real Mac showed bare `mktemp` works
# (docs/solutions/workflow-issues/gnu-only-constructs-fail-silently-on-bsd-2026-08-25.md records it
# as one of two carefully-argued findings that were both wrong, alongside `xargs -r`). The cases
# passed, against a fiction. Removed rather than left green, because a test that models a platform
# incorrectly asserts that fiction indefinitely and will be read later as evidence about macOS.
# The hook still passes a template to `mktemp` - that costs nothing and names the file recognisably -
# but no claim rests on it.

# --- DISPATCH MODE: the shape of the 2026-08-24 incident -------------------------------------------
# The dispatcher's own cwd is the WRONG branch to describe when it is handing work to another
# worktree, and describing it would be noise - which is how a reminder gets scrolled past.
bctx_clean_stamps
dp="$(bctx_payload "$bctx_dispatcher" PreToolUse Agent "Run a simplify-then-review pass in $bctx_tmp/wt. Changed files: one.txt")"
out="$(bctx_fire "$dp" "$stub_pr:$PATH")"
ctx="$(bctx_context "$out")"
case "$ctx" in *'feats/bctx-fixture'*) got=dispatched_to ;; *) got=wrong_target ;; esac
assert "a dispatch describes the worktree named in its PROMPT" dispatched_to "$got"
case "$ctx" in *'named in the dispatch prompt'*) got=says_which ;; *) got=ambiguous ;; esac
assert "...and says which directory it is describing, so the two cannot be confused" says_which "$got"
# PreToolUse discards raw stdout; only the JSON envelope reaches the model.
case "$out" in *'"additionalContext"'*) got=enveloped ;; *) got=raw ;; esac
assert "a PreToolUse emission is wrapped in the JSON envelope" enveloped "$got"
case "$out" in *'"deny"'*) got=blocked ;; *) got=advisory ;; esac
assert "the branch-context hook never denies a tool call" advisory "$got"
# A PreToolUse hook cannot alter the call it fires on - the tool_use block was composed before the
# hook ran (measured against 2.1.231). Saying so is the difference between a useful late signal and
# a false promise that the dispatch was vetted.
case "$ctx" in *'not before it'*) got=honest ;; *) got=overclaims ;; esac
assert "the dispatch block states that it arrived AFTER the dispatch" honest "$got"

# A PATH AT THE END OF A SENTENCE. A worktree name may contain `.` and `-`, so the match swallows the
# full stop and the directory test then fails - which is the shape a dispatch prompt actually takes,
# and the first thing this resolver got wrong.
bctx_clean_stamps
dp2="$(bctx_payload "$bctx_dispatcher" PreToolUse Agent "Simplify pass in $bctx_tmp/wt.")"
ctx="$(bctx_context "$(bctx_fire "$dp2" "$stub_pr:$PATH")")"
case "$ctx" in *'feats/bctx-fixture'*) got=resolved ;; *) got=lost ;; esac
assert "a worktree path ending a sentence still resolves" resolved "$got"

# The real tool_name is `Agent`; `Task` is the name the documentation and every dispatcher uses.
# Both were measured to reach a matcher, and both must reach this hook.
bctx_clean_stamps
dp3="$(bctx_payload "$bctx_dispatcher" PreToolUse Task "Simplify pass in $bctx_tmp/wt.")"
ctx="$(bctx_context "$(bctx_fire "$dp3" "$stub_pr:$PATH")")"
case "$ctx" in *'feats/bctx-fixture'*) got=handled ;; *) got=missed ;; esac
assert "tool_name 'Task' is handled as well as 'Agent'" handled "$got"

# --- SUBAGENT MODE: the registration that actually closes the incident ------------------------------
# SessionStart does NOT fire for an agent spawned via the Task tool (measured against 2.1.231: the
# subagent shares the dispatcher's session_id and gets no session of its own), so without this the
# subagent could never receive branch context by any route.
bctx_clean_stamps
sub="$(bctx_payload "$bctx_tmp/wt" PreToolUse Bash "" general-purpose bctxfixture01)"
ctx="$(bctx_context "$(bctx_fire "$sub" "$stub_pr:$PATH")")"
case "$ctx" in *'feats/bctx-fixture'*) got=reached ;; *) got=missed ;; esac
assert "a subagent's own tool call carries it the branch context" reached "$got"
# THROTTLED PER agent_id, or it repeats on every Read the subagent makes for the rest of its life.
out="$(bctx_fire "$sub" "$stub_pr:$PATH")"
[ -z "$out" ] && got=throttled || got=repeated
assert "a second tool call from the same agent is throttled" throttled "$got"
# ...and a DIFFERENT agent is not - the pairing that stops the throttle from silently swallowing
# every subagent after the first.
sub2="$(bctx_payload "$bctx_tmp/wt" PreToolUse Bash "" general-purpose bctxfixture02)"
out="$(bctx_fire "$sub2" "$stub_pr:$PATH")"
[ -n "$out" ] && got=spoke || got=swallowed
assert "...but a different agent_id still gets told" spoke "$got"

# --- NO SILENT CAPS. A list that quietly stops is the same failure as a section that quietly vanishes.
bctx_clean_stamps
(
  cd "$bctx_tmp/wt" || exit 1
  i=0
  while [ "$i" -lt 45 ]; do
      echo "$i" > "bulk$i.txt"
      git add "bulk$i.txt"
      git -c user.email=selftest@example.invalid -c user.name=selftest commit -qm "chore(fixture): bulk commit $i"
      i=$((i + 1))
  done
)
ctx="$(bctx_context "$(bctx_fire "$sess_payload" "$stub_pr:$PATH")")"
case "$ctx" in *'NOT LISTED (capped at'*) got=announced ;; *) got=silent_cap ;; esac
assert "a capped commit list announces what it left out" announced "$got"
case "$ctx" in *'Commits on this branch (47)'*) got=true_total ;; *) got=cap_as_total ;; esac
assert "...and the heading counts every commit, not just the listed ones" true_total "$got"

bctx_clean_stamps
rm -rf "$bctx_tmp" "$stub_pr" "$stub_nopr" "$stub_broken" "$stub_slow" "$BCTX_TMPDIR"


# ---------------------------------------------------------------------------------------------
# check-branch-behind-its-own-remote.sh
#
# The deny arm needs real git state, not just a parsed command line, so this builds a throwaway
# origin plus a clone and drives the hook from inside it, via the shared verdict() harness with
# VERDICT_CWD pointed at the fixture.
#
# THE NEGATIVE CONTROLS MATTER MORE THAN USUAL. A hook that exits 0 on every payload passes an
# ALLOW-only suite perfectly, and that is exactly the shape this hook would fail into - every guard
# in it exits silent when it cannot answer. So every ALLOW case below is paired with a DENY that
# proves the same code path can still fire.
#
# The four cases marked "regression" each fail against the FIRST version of this hook, which review
# caught: it denied its own remedy (including `git merge origin/master` on master, the commonest
# command in the repo), it denied `--continue`/`--abort` and trapped a conflicted rebase, and its
# override was a raw-payload substring that any prose mentioning the variable could satisfy.
#
# Reported through `assert`, not `expect`: `expect` counts into `fails`, which is folded into
# `failures` far above this point, so a failure recorded here would be silently dropped.
# ---------------------------------------------------------------------------------------------

echo
echo "--- check-branch-behind-its-own-remote.sh ---"

bbr_tmp="$(mktemp -d)"
bbr_git() { git -c user.email=selftest@example.invalid -c user.name=selftest "$@"; }

# `outside a git repo` below is only a real case while $bbr_tmp is not itself inside one; a CI that
# points TMPDIR into the workspace would silently turn it into something else.
if git -C "$bbr_tmp" rev-parse --show-toplevel >/dev/null 2>&1; then
    echo "FAIL: mktemp -d landed inside a git repo, so the outside-a-repo case tests nothing"
    failures=$((failures + 1))
fi

(
    set -e
    bbr_git init -q --bare "$bbr_tmp/origin.git"
    bbr_git clone -q "$bbr_tmp/origin.git" "$bbr_tmp/seed"
    cd "$bbr_tmp/seed"
    echo one > f.txt && bbr_git add f.txt && bbr_git commit -qm "chore(fixture): first"
    bbr_git branch -M feat/thing
    bbr_git push -q origin feat/thing
    # A second branch, so the master case below is not the same ref under another name.
    bbr_git branch -q master && bbr_git push -q origin master
) >/dev/null 2>&1

bbr_git clone -q "$bbr_tmp/origin.git" "$bbr_tmp/work" >/dev/null 2>&1
(cd "$bbr_tmp/work" && bbr_git checkout -q feat/thing) >/dev/null 2>&1

bbr_publish() { # <branch> - push a commit the clone has never seen
    (
        set -e
        cd "$bbr_tmp/seed"
        bbr_git checkout -q "$1"
        echo "$1-$(wc -c < f.txt)" >> f.txt
        bbr_git add f.txt
        bbr_git commit -qm "chore(fixture): pushed to $1 by somebody else"
        bbr_git push -q origin "$1"
    ) >/dev/null 2>&1
}

bbr_expect() { # <expected> <name> <cwd> <command>
    local got
    got=$(HOOK_UNDER_TEST="$HOOKS/check-branch-behind-its-own-remote.sh" VERDICT_CWD="$3" verdict "$4")
    assert "$2" "$1" "$got"
}

# UP TO DATE: nothing to say, whatever the command.
bbr_expect ALLOW "up to date, merge allowed"        "$bbr_tmp/work" 'git merge origin/master'
bbr_expect ALLOW "up to date, rebase allowed"       "$bbr_tmp/work" 'git rebase origin/master'

bbr_publish feat/thing

# THE INCIDENT'S SHAPE: merging something ELSE into a branch behind its own tip.
bbr_expect DENY  "behind its own remote blocks merge"   "$bbr_tmp/work" 'git merge origin/master'
bbr_expect DENY  "...and blocks rebase"                 "$bbr_tmp/work" 'git rebase origin/master'
bbr_expect DENY  "...through a compound command"        "$bbr_tmp/work" 'git fetch origin master && git merge origin/master'
bbr_expect DENY  "...and with a -C repo flag"           "$bbr_tmp/work" 'git -C . merge origin/master'

# REGRESSION: the remedy the deny message prescribes must not itself be denied.
bbr_expect ALLOW "merging its OWN published tip is the remedy, not a violation" \
    "$bbr_tmp/work" 'git merge origin/feat/thing'
bbr_expect ALLOW "...and rebasing onto it"              "$bbr_tmp/work" 'git rebase origin/feat/thing'
bbr_expect ALLOW "...and @{upstream}"                   "$bbr_tmp/work" 'git merge @{upstream}'
bbr_expect ALLOW "...and FETCH_HEAD"                    "$bbr_tmp/work" 'git merge FETCH_HEAD'

# REGRESSION: finishing or abandoning an in-progress operation must never be blocked, or a
# conflicted rebase becomes a trap with no exit.
bbr_expect ALLOW "rebase --continue is never blocked"   "$bbr_tmp/work" 'git rebase --continue'
bbr_expect ALLOW "rebase --abort is never blocked"      "$bbr_tmp/work" 'git rebase --abort'
bbr_expect ALLOW "rebase --skip is never blocked"       "$bbr_tmp/work" 'git rebase --skip'
bbr_expect ALLOW "merge --abort is never blocked"       "$bbr_tmp/work" 'git merge --abort'

# THE FLAG WORD IS NOT A SUBCOMMAND, and `push` is deliberately not an arm.
bbr_expect ALLOW "the word merge in a commit message"   "$bbr_tmp/work" 'git commit -m "explain the merge"'
bbr_expect ALLOW "git push alone is not an arm"         "$bbr_tmp/work" 'git push origin feat/thing'
# ...but a merge CHAINED with a push still denies, so the case above is not passing on the
# pre-filter alone.
bbr_expect DENY  "a merge chained with a push still denies" \
    "$bbr_tmp/work" 'git merge origin/master && git push origin feat/thing'

# A REDIRECTION IS AN OPERATOR, NOT AN ARGUMENT. `punctuation_chars=True` emits `>` as its own
# token, and the arg walk once stopped only at command separators - so a redirect target could sit
# in the argument list and spoof an exemption. Both cases below name a redirect target that is
# EXACTLY a control flag / a remedy ref, which is the only way the confusion is observable.
bbr_expect DENY  "a redirect target cannot spoof a control flag" \
    "$bbr_tmp/work" 'git merge origin/master > --abort'
bbr_expect DENY  "a redirect target cannot spoof the remedy ref" \
    "$bbr_tmp/work" 'git merge origin/master > origin/feat/thing'

# THE OVERRIDE IS A TOKEN. The first version matched the raw payload, so any prose mentioning the
# variable let a merge straight through - and the deny message teaches the agent that exact string.
bbr_expect ALLOW "override token lets a deliberate merge through" \
    "$bbr_tmp/work" 'BRANCH_FRESHNESS_OVERRIDE=1 git merge origin/master'
bbr_expect DENY  "REGRESSION: the override named in prose is NOT an override" \
    "$bbr_tmp/work" 'git commit -m "note BRANCH_FRESHNESS_OVERRIDE=1" && git merge origin/master'

# REGRESSION: on master, origin/<branch> IS origin/master, so the commonest command in the repo was
# denied every time master advanced.
(cd "$bbr_tmp/work" && bbr_git checkout -q master && bbr_git branch -q --set-upstream-to=origin/master) >/dev/null 2>&1
bbr_publish master
bbr_expect ALLOW "on master, merging origin/master is the remedy" \
    "$bbr_tmp/work" 'git merge origin/master'
bbr_expect DENY  "...but a stale master merging something else still denies" \
    "$bbr_tmp/work" 'git merge origin/feat/thing'
(cd "$bbr_tmp/work" && bbr_git checkout -q feat/thing) >/dev/null 2>&1

# FAIL-OPEN PATHS, each documented in the hook's header and none previously tested.
(cd "$bbr_tmp/work" && bbr_git checkout -q --detach) >/dev/null 2>&1
bbr_expect ALLOW "detached HEAD, silent"                "$bbr_tmp/work" 'git merge origin/master'
(cd "$bbr_tmp/work" && bbr_git checkout -q feat/thing) >/dev/null 2>&1

(cd "$bbr_tmp/work" && bbr_git checkout -q -b local-only) >/dev/null 2>&1
bbr_expect ALLOW "a branch with no origin/<branch>, silent" "$bbr_tmp/work" 'git merge origin/master'
(cd "$bbr_tmp/work" && bbr_git checkout -q feat/thing) >/dev/null 2>&1

bbr_expect ALLOW "outside a git repo, silent"           "$bbr_tmp" 'git merge origin/master'

# A BROKEN GUARD MUST SAY SO. Without python3 the hook can answer nothing; the failure it must NOT
# have is the silent one, which is byte-identical to a healthy quiet hook.
bbr_nopy="$bbr_tmp/nopy"
mkdir -p "$bbr_nopy"
printf '#!/bin/sh\nexit 127\n' > "$bbr_nopy/python3"
chmod +x "$bbr_nopy/python3"
bbr_payload="$bbr_tmp/payload.json"
printf '%s' 'git merge origin/master' | python3 -c 'import json,sys; print(json.dumps({"tool_name":"Bash","hook_event_name":"PreToolUse","tool_input":{"command":sys.stdin.read()}}))' > "$bbr_payload"
bbr_stderr="$(cd "$bbr_tmp/work" && PATH="$bbr_nopy:$PATH" "$HOOKS/check-branch-behind-its-own-remote.sh" < "$bbr_payload" 2>&1 >/dev/null)"
case "$bbr_stderr" in *"CANNOT RUN"*) got=loud ;; *) got=silent ;; esac
assert "a broken python3 says the guard cannot run, rather than going quiet" loud "$got"

# ---- the SessionStart arm, which produces no output ever and is therefore the half most able to
# stop working invisibly. Driven by event, and asserted on the tracking ref actually moving.
bbr_session() { # <fetch-floor-seconds>
    printf '%s' 'startup' | python3 -c 'import json,sys; print(json.dumps({"hook_event_name":"SessionStart","source":sys.stdin.read()}))' > "$bbr_tmp/session.json"
    (cd "$bbr_tmp/work" && BRANCH_FRESHNESS_FETCH_FLOOR="$1" "$HOOKS/check-branch-behind-its-own-remote.sh" < "$bbr_tmp/session.json" >/dev/null 2>&1)
}
bbr_behind() { (cd "$bbr_tmp/work" && git rev-list --count HEAD..origin/feat/thing 2>/dev/null || echo unknown); }

bbr_publish feat/thing
bbr_before="$(bbr_behind)"
bbr_session 0
bbr_after="$(bbr_behind)"
[ "$bbr_after" -gt "$bbr_before" ] 2>/dev/null && got=fetched || got=did-not-fetch
assert "SessionStart fetches, so the tracking ref actually moves" fetched "$got"

# THROTTLED: with a floor far in the future the second session start must NOT fetch, or the
# stamp is doing nothing and a resumed session becomes a fetch loop.
bbr_publish feat/thing
bbr_before="$(bbr_behind)"
bbr_session 86400
bbr_after="$(bbr_behind)"
[ "$bbr_after" = "$bbr_before" ] && got=throttled || got=fetched-anyway
assert "...and a second start inside the floor is throttled" throttled "$got"

rm -rf "$bbr_tmp"

if [ "$failures" -eq 0 ]; then
    echo "All .claude/hooks self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
