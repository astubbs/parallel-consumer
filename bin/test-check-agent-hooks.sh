#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for the three hooks in `.claude/hooks/`. Feeds each one a crafted hook payload on stdin
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

# ---------------------------------------------------------------------------------------------
# check-squash-subject.sh
#
# The subject the hook must judge is the one `gh` will actually use: the LAST `--subject`, inside
# the `gh pr merge` command, cross-checked against the PR being merged. Cases 1-6 are the six
# defects found in review; the rest is behaviour that already worked and must not regress.
#
# The fixture numbers are four digits on purpose. bin/check-issue-refs.sh flags an unqualified
# `#NNN` below 1000 on any added line, and it is right to - these literals are indistinguishable
# from a real citation. The cases do not depend on the values, so they sit above the threshold.
# ---------------------------------------------------------------------------------------------

verdict() { # <bash-command> -> ALLOW | DENY
    local payload out
    payload=$(python3 -c 'import json,sys; print(json.dumps({"tool_name":"Bash","tool_input":{"command":sys.argv[1]}}))' "$1")
    out=$(printf '%s' "$payload" | "$HOOKS/check-squash-subject.sh" 2>/dev/null)
    case "$out" in
        *'"deny"'*) echo DENY ;;
        *)          echo ALLOW ;;
    esac
}

echo "--- check-squash-subject.sh ---"

# 1. `gh` honours the LAST --subject. Reading the first one allowed the bad subject to land while
#    reporting the good one as proof it was fine.
assert "two --subject flags, the LAST one is bad" DENY \
    "$(verdict 'gh pr merge 1299 --squash --subject "ok (#1299)" --subject "bad no number"')"

# 2. A `--subject` elsewhere on the command line is not part of the merge. A regex over the whole
#    line matched the decoy and never looked at the real one.
assert "a decoy --subject before the merge command" DENY \
    "$(verdict 'echo --subject "x (#1001)" ; gh pr merge 1299 --squash --subject "bad no number"')"

# 3. Any `(#N)` used to satisfy the check, so the astubbs#206 shape - a number pointing at a
#    DIFFERENT PR - passed while the right number was sitting in the same command.
assert "a (#N) naming the wrong PR" DENY \
    "$(verdict 'gh pr merge 1299 --squash --subject "tooling: thing (#1206)"')"

# 4. Same defect, the commoner spelling of it: an issue reference read as a PR number.
assert "a (#N) that is an issue reference, not the PR" DENY \
    "$(verdict 'gh pr merge 1299 --squash --subject "fixes the thing (#1012)"')"

# 5. FALSE POSITIVE. An escaped apostrophe ends the shell string, so a regex capture stopped before
#    the suffix and denied a correct merge. A PreToolUse deny is hard - the agent cannot argue.
assert "escaped apostrophe inside the subject" ALLOW \
    "$(verdict "gh pr merge 1299 --squash --subject 'don'\''t drop it (#1299)'")"

# 6. FALSE POSITIVE. The words "--subject" inside --body text are body content, not a flag.
assert "the word --subject appearing inside --body" ALLOW \
    "$(verdict 'gh pr merge 1299 --squash --body "we discussed --subject and why" --subject "tidy: thing (#1299)"')"

assert "plain correct subject" ALLOW \
    "$(verdict 'gh pr merge 1299 --squash --subject "tooling: thing (#1299)"')"
assert "plain subject with no (#N) at all" DENY \
    "$(verdict 'gh pr merge 1299 --squash --subject "tooling: thing"')"
assert "no --subject: GitHub appends the number itself" ALLOW \
    "$(verdict 'gh pr merge 1299 --squash --body-file /tmp/body.md')"
assert "not a merge command at all" ALLOW \
    "$(verdict 'git commit -m "hello"')"

# Boundaries, pinned so a later tightening is a deliberate act rather than a surprise.
assert "--subject=VALUE form is read too" DENY \
    "$(verdict 'gh pr merge 1299 --squash --subject="tooling: thing"')"
assert "no PR argument, so no cross-check is possible" ALLOW \
    "$(verdict 'gh pr merge --squash --subject "tooling: thing (#1206)"')"
# The convention puts (#N) in the TRAILING slot, and this hook deliberately does not require it -
# raised in review, kept on purpose. What is unfixable after a merge is a number that is MISSING or
# points at the WRONG PR, and both of those are denied above. A number that is present and correct
# but sits mid-subject is a visible, cosmetic deviation that review catches and that still links
# correctly; a hard PreToolUse deny for it would block "port (#N) to master" and every other
# unusual-but-fine subject, with no way for the agent to argue. Pinned so tightening it later is a
# deliberate act - see the BOUNDARY note in .claude/hooks/check-squash-subject.sh.
assert "a correct (#N) outside the trailing slot is allowed (stated boundary)" ALLOW \
    "$(verdict 'gh pr merge 1299 --squash --subject "fix(core) (#1299): detail"')"
assert "...but the same shape with the WRONG number is still denied" DENY \
    "$(verdict 'gh pr merge 1299 --squash --subject "fix(core) (#1206): detail"')"
assert "two merges chained, the second is bad" DENY \
    "$(verdict 'gh pr merge 1299 --squash --subject "a (#1299)" && gh pr merge 1300 --squash --subject "b"')"
assert "unbalanced quotes fail OPEN, never on our own parse bug" ALLOW \
    "$(verdict 'gh pr merge 1299 --squash --subject "unterminated')"
# Round three. All three of these were live holes, reproduced before being fixed - `-t` and the
# URL selector let the mistake through, the unexpanded subject blocked a legitimate merge.
#
# 7. `-t` is gh's documented short form of --subject. Reading only the long spelling meant the
#    parser saw NO override, took the "GitHub appends the number itself" branch, and allowed a
#    subject that lands verbatim with no number - the astubbs#206 shape via another spelling.
assert "-t is --subject: no number is still denied" DENY \
    "$(verdict 'gh pr merge 1299 --squash -t "tooling: thing with no number"')"
assert "-tVALUE attached form" DENY \
    "$(verdict 'gh pr merge 1299 --squash -t"tooling: thing with no number"')"
assert "-t=VALUE form" DENY \
    "$(verdict 'gh pr merge 1299 --squash -t="tooling: thing with no number"')"
assert "-t inside a combined shorthand group" DENY \
    "$(verdict 'gh pr merge 1299 --squash -st "tooling: thing with no number"')"
assert "-t carrying the right number is allowed" ALLOW \
    "$(verdict 'gh pr merge 1299 --squash -t "tooling: thing (#1299)"')"
assert "-t naming the WRONG PR" DENY \
    "$(verdict 'gh pr merge 1299 --squash -t "tooling: thing (#1206)"')"
# `t` here is part of -b's VALUE, not a flag. Reading it as a subject would deny a merge whose
# real subject is fine - so the shorthand scan stops at the first value-taking letter.
assert "a 't' inside another short flag's value is not a subject" ALLOW \
    "$(verdict 'gh pr merge 1299 --squash -bt --subject "tooling: thing (#1299)"')"

# 8. FALSE POSITIVE. shlex does not expand shell variables, so the hook was judging a string that
#    is not the one gh will send - and denying it. Every other unresolvable case fails open; this
#    one hard-blocked a legitimate merge, which the header calls the more expensive direction.
assert "an unexpanded \$VAR subject fails OPEN" ALLOW \
    "$(verdict 'gh pr merge 1299 --squash --subject "$SUBJECT"')"
assert "an unexpanded command substitution fails OPEN" ALLOW \
    "$(verdict 'gh pr merge 1299 --squash --subject "$(cat msg.txt)"')"

# 9. The PR selector may be a number, a URL or a branch. Only `isdigit()` was read, so a URL left
#    the cross-check switched off entirely and ANY number satisfied it - including a wrong one.
assert "a URL selector still cross-checks the number" DENY \
    "$(verdict 'gh pr merge https://github.com/astubbs/parallel-consumer/pull/1299 --squash --subject "thing (#1206)"')"
assert "a URL selector with the right number is allowed" ALLOW \
    "$(verdict 'gh pr merge https://github.com/astubbs/parallel-consumer/pull/1299 --squash --subject "thing (#1299)"')"
# A branch name carries no number and resolving it needs the network, so any (#N) is accepted -
# the same stated boundary as a merge with no selector at all. Pinned, not incidental.
assert "a branch-name selector cannot be cross-checked (stated boundary)" ALLOW \
    "$(verdict 'gh pr merge some-branch --squash --subject "thing (#1206)"')"
assert "a branch-name selector with NO number is still denied" DENY \
    "$(verdict 'gh pr merge some-branch --squash --subject "thing with no number"')"
# A flag VALUE that looks like a PR number must not be read as the selector. Discriminating on
# purpose: read 1206 as the PR and the correct (#1299) becomes a mismatch, so the old parser
# hard-denied a perfectly good merge.
assert "a flag value is not mistaken for the PR selector" ALLOW \
    "$(verdict 'gh pr merge --body 1206 --squash 1299 --subject "thing (#1299)"')"

# Round four. Segmenting the command with a regex over the RAW line found `gh pr merge` inside
# quoted body text, cut the line into two slices that each had unbalanced quotes, and fail-opened
# both - so the real --subject was never judged. Body text could switch the hook off. Segmentation
# now runs over shlex TOKENS, where a quoted body is a single token and cannot look like a command.
assert "the phrase 'gh pr merge' inside --body does not disable the guard" DENY \
    "$(verdict 'gh pr merge 1299 --body "text gh pr merge here" --subject "bad no number"')"
assert "...and the same body with a correct subject still passes" ALLOW \
    "$(verdict 'gh pr merge 1299 --body "text gh pr merge here" --subject "tooling: thing (#1299)"')"
assert "the phrase inside the SUBJECT does not disable the guard either" DENY \
    "$(verdict 'gh pr merge 1299 --squash --subject "how to gh pr merge safely"')"
# An absolute path to gh is still gh; a word merely ending in those letters is not a command.
assert "an absolute path to gh is still matched" DENY \
    "$(verdict '/usr/local/bin/gh pr merge 1299 --squash --subject "tooling: thing"')"

# Round five. A slice ran to the next `gh pr merge`, so it swallowed everything after `&&`/`;`/`|`
# and an unrelated command's --subject became "the last one" - in BOTH directions.
assert "a decoy --subject after && cannot vouch for a bad merge" DENY \
    "$(verdict 'gh pr merge 1299 --squash --subject "bad" && echo --subject "decoy (#1299)"')"
assert "a later --subject after && cannot condemn a good merge" ALLOW \
    "$(verdict 'gh pr merge 1299 --squash --subject "good (#1299)" && echo --subject "no number"')"
assert "a pipe also ends the merge slice" ALLOW \
    "$(verdict 'gh pr merge 1299 --squash --subject "good (#1299)" | tee --subject "x"')"

# `fix/pull/1299` is a legal branch name (`git check-ref-format --branch` accepts it). Reading it
# as a PR URL cross-checked the merge against 1299 and DENIED a correct subject - a false positive.
assert "a branch named like a URL is not read as one" ALLOW \
    "$(verdict 'gh pr merge fix/pull/1299 --squash --subject "tooling: thing (#1300)"')"
assert "a real URL selector still cross-checks" DENY \
    "$(verdict 'gh pr merge https://github.com/astubbs/parallel-consumer/pull/1299 --squash --subject "w (#1206)"')"

# Round six. Registering the guard for EVERY Bash call (round five) widened what it must get right,
# and review found seven more. Each was reproduced first; all seven go red against the prior parser.
assert "a newline ends the merge slice, like any other boundary" DENY \
    "$(verdict 'gh pr merge 1299 --subject "bad"
echo --subject "decoy (#1299)"')"
assert "a decoy after # is a comment, not a subject" DENY \
    "$(verdict 'gh pr merge 1299 --subject "bad" # decoy --subject "x (#1299)"')"
# FALSE POSITIVE, and one the hook created for itself by dropping its `if`: printing a command is
# not running it, and hard-denying a diagnostic echo is exactly what an agent cannot argue with.
assert "echo'ing the command is text, not a merge" ALLOW \
    "$(verdict 'echo gh pr merge 1299 --subject bad')"
assert "gh in command position after && is still a merge" DENY \
    "$(verdict 'echo ready && gh pr merge 1299 --subject "bad no number"')"
# `gh -R owner/repo pr merge ...` is valid and the local CLI accepts it; requiring `gh pr merge`
# to be adjacent silently missed it.
assert "a global flag before the subcommand is still a merge" DENY \
    "$(verdict 'gh -R astubbs/parallel-consumer pr merge 1299 --subject "bad"')"
assert "...and the same shape with a correct subject passes" ALLOW \
    "$(verdict 'gh -R astubbs/parallel-consumer pr merge 1299 --subject "ok (#1299)"')"
# AGENTS.md reserves the trailing parenthesised slot for the PR number ALONE - two bare numbers
# recreate the issue-vs-PR ambiguity the convention exists to remove, whichever order they are in.
assert "an extra (#N) alongside the right one is rejected" DENY \
    "$(verdict 'gh pr merge 1299 --squash --subject "fix (#1206) (#1299)"')"
assert "...in either order" DENY \
    "$(verdict 'gh pr merge 1299 --squash --subject "fix (#1299) (#1206)"')"
# shlex strips quoting, so a literal dollar looked exactly like an unresolved expansion and the
# subject failed open with no number at all. Only real expansion syntax fails open now.
assert "a literal \$5 is judged, not treated as an expansion" DENY \
    "$(verdict "gh pr merge 1299 --squash --subject 'reduce cost to \$5'")"
assert "a real \$VAR expansion still fails OPEN" ALLOW \
    "$(verdict 'gh pr merge 1299 --squash --subject "$SUBJECT"')"

# Round seven. Both are consequences of running on every Bash call: the parser now sees commands
# it never used to, so what it treats as a command boundary and as an executable both matter more.
#
# FALSE POSITIVE. posix lexing strips quoting, so the `(` in an ordinary string argument looked
# exactly like a subshell, reset command position, and hard-denied a harmless command.
assert "a QUOTED paren is an argument, not a subshell" ALLOW \
    "$(verdict 'echo "(" gh pr merge 1299 --subject bad')"
assert "...while a real subshell still is one" DENY \
    "$(verdict '( gh pr merge 1299 --subject "bad no number" )')"
# `command gh ...` runs gh - Bash's own `help command` says so - and cleared command position.
assert "a merge run through the 'command' builtin is still a merge" DENY \
    "$(verdict 'command gh pr merge 1299 --subject "bad no number"')"
assert "...and passes with the right number" ALLOW \
    "$(verdict 'command gh pr merge 1299 --subject "tooling: thing (#1299)"')"
assert "other execution wrappers count too" DENY \
    "$(verdict 'sudo gh pr merge 1299 --subject "bad no number"')"

assert "a non-Bash tool is ignored" ALLOW \
    "$(printf '%s' '{"tool_name":"Read","tool_input":{"command":"gh pr merge 1299 --subject \"x\""}}' \
        | "$HOOKS/check-squash-subject.sh" 2>/dev/null | grep -c '"deny"' | sed 's/^0$/ALLOW/;s/^[1-9].*/DENY/')"

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

echo
if [ "$failures" -eq 0 ]; then
    echo "All .claude/hooks self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
