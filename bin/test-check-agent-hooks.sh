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

HOOKS="$(cd "$(dirname "$0")/.." && pwd)/.claude/hooks"
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

# Fail open: no gate script in the project at all must not block every commit.
empty="$TMP/empty$RANDOM"; mkdir -p "$empty"
assert "absent gate script fails OPEN" 0 \
    "$(gate_rc "$empty" 'git commit -m "ordinary"')"

# ---------------------------------------------------------------------------------------------
# inject-merge-checklist.sh
# ---------------------------------------------------------------------------------------------

echo
echo "--- inject-merge-checklist.sh ---"

injected() { # <prompt> -> YES | NO
    local out
    out=$(python3 -c 'import json,sys; print(json.dumps({"prompt":sys.argv[1]}))' "$1" \
        | "$HOOKS/inject-merge-checklist.sh" 2>/dev/null)
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
    | "$HOOKS/inject-merge-checklist.sh" 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["hookSpecificOutput"]["additionalContext"])')
checklist_head=$(head -1 "$(dirname "$HOOKS")/../docs/merge-checklist.md" 2>/dev/null)
case "$body" in
    *"$checklist_head"*) got=verbatim ;;
    *)                   got=missing ;;
esac
assert "the checklist is injected verbatim from its own file" verbatim "$got"

preamble=${body%%$'\n'*}
if [ "${#preamble}" -le 200 ]; then got=pointer_only; else got="carries its own advice (${#preamble} chars)"; fi
assert "the preamble points at the doc rather than restating it" pointer_only "$got"

echo
if [ "$failures" -eq 0 ]; then
    echo "All .claude/hooks self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
