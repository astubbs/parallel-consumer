#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-file-refs.sh - its EXIT CODE CONTRACT, which the gate module's own unit
# tests (file-ref-gate.test.js) cannot reach: they test the rule, this tests the shell that decides
# what the rule's answer, or its absence, means to a caller.
#
# WHY THAT CONTRACT NEEDS A TEST AT ALL. .githooks/pre-commit classifies exit 2 from this script as
# "could not run" and WARNS, while 1 blocks the commit. The two codes are therefore load-bearing in
# both directions: a "could not run" reported as 1 blocks a commit over a violation nobody committed,
# and a real finding reported as 2 is waved through with a warning. That first direction is not
# hypothetical - a stale `--require` in NODE_OPTIONS kills node during preload, node exits 1 for it,
# and this script used to hand that 1 straight on as "dangling references found".
#
# No network and no `gh`: this gate reads only git and the filesystem, so the fixture repo is the
# whole world it sees.

set -uo pipefail

REPO_ROOT="$(cd "${BASH_SOURCE[0]%/*}/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# --- fixture repo, with the script under test in its real layout ---------------------------------
mkdir -p "$TMP/repo/bin/lib" "$TMP/repo/.github/scripts" "$TMP/repo/docs"
cp "$REPO_ROOT/bin/check-file-refs.sh" "$TMP/repo/bin/"
cp "$REPO_ROOT/bin/lib/node-gate.sh" "$TMP/repo/bin/lib/"
cp "$REPO_ROOT/.github/scripts/file-ref-gate.js" "$TMP/repo/.github/scripts/"

cd "$TMP/repo"
git init -q -b master
git config user.email test@example.invalid
git config user.name test
# The base tree the gate ratchets against must be clean, so a finding added below is NEW rather than
# inherited - that is the distinction newFindings() draws and this fixture has to respect it.
echo "A citation of bin/check-file-refs.sh, which exists." > docs/note.md
git add -A
git commit -qm base

fails=0

run() { # <env-prefix...> -- sets $out and $got
    out="$("$@" 2>&1)"
    got=$?
}

check() { # <name> <expected-exit> <must-contain>
    local name=$1 want=$2 must=$3
    if [ "$got" != "$want" ]; then
        echo "FAIL: $name (expected exit $want, got $got)"
        echo "$out" | sed 's/^/      /'
        fails=$((fails + 1))
        return
    fi
    case "$out" in
        *"$must"*) ;;
        *) echo "FAIL: $name (output lacks: $must)"; echo "$out" | sed 's/^/      /'; fails=$((fails + 1)); return ;;
    esac
    echo "ok:   $name"
}

# --- 0: node ran and found nothing ----------------------------------------------------------------
run env -u NODE_OPTIONS bash bin/check-file-refs.sh
check "a tree whose citations all resolve exits 0" 0 "No new dangling file references"

# --- 1: node ran and found something --------------------------------------------------------------
echo "See bin/check-nothing-of-the-sort.sh for the details." >> docs/note.md
git add docs/note.md
run env -u NODE_OPTIONS bash bin/check-file-refs.sh
check "a citation of a path that is not in the tree exits 1" 1 "check-nothing-of-the-sort.sh"

# --- 2: node never ran, so there is no answer to report -------------------------------------------
# `command -v node` proved node was installed and nothing more. A `--require` naming a file that has
# since been deleted - an agent harness writes a preload into a temp directory, the temp directory is
# later cleaned up - kills node during preload, before one line of the gate executes, and node's exit
# status for that is 1: this script's code for "dangling references found".
#
# BOTH arms must be 2. The dirty one because a gate that did not run may not be believed even when it
# would have been right; the clean one because there the 1 is a pure fabrication, and that is the
# form the real incident took.
broken_preload="$TMP/nodepreflight-deleted-by-the-harness.cjs"   # deliberately never created

run env NODE_OPTIONS="--require=$broken_preload" bash bin/check-file-refs.sh
check "dirty tree: a node that cannot start is 'cannot run', not a finding" 2 "This is NOT a finding"
check "and the message names NODE_OPTIONS, without which the failure is baffling" 2 "NODE_OPTIONS is set"

git checkout -q docs/note.md
run env NODE_OPTIONS="--require=$broken_preload" bash bin/check-file-refs.sh
check "clean tree: the same, so a gate that checked nothing cannot accuse anyone" 2 "did not run"

# --- 2 is also the code for a bad argument, and must stay that way ---------------------------------
run env -u NODE_OPTIONS bash bin/check-file-refs.sh --nonsense
check "an unexpected argument is still 'cannot run', not a finding" 2 "unexpected argument"

if [ "$fails" -gt 0 ]; then
    echo "$fails case(s) failed"
    exit 1
fi
echo "all cases passed"
