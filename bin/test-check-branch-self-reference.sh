#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-branch-self-reference.sh.
#
# Carries the ADJACENT cases as well as the red ones, per docs/agent-harness.md rule 3: a red control
# proves the gate can fire, never that it is looking at the right thing. Every gate that misbehaved in
# this repo failed on SCOPE while passing its red control - so the near-misses that must stay GREEN
# (a different branch, a longer PR number sharing the prefix, a marked line) are the half that matters.
#
# THIS SUITE HAS BEEN WRONG THREE TIMES, ALL THE SAME SHAPE, so read the shape before adding a case.
# Each time the gate stopped looking at the right thing and every case still passed:
#   1. `checked-end` contains `checked`, so a block's END marker cleared the line beneath it.
#   2. The fixtures inherited GITHUB_HEAD_REF from CI, so every branch-name case tested a name that
#      was not in the fixture at all.
#   3. Unsetting those variables (the fix for 2) deleted all coverage of the branch resolution CI
#      actually uses - deleting the GITHUB_HEAD_REF preference from the gate passed 17/17, while on a
#      detached HEAD the mutant exits 0 forever, checking nothing.
# The root cause of all three is that a case asserted only an EXIT CODE. A gate that crashed on every
# input scored four `ok`s. So `assert` now requires a red case to actually SAY what it caught, and the
# CI branch-resolution path has its own cases below.
#
# THE BAR IS A MUTATION MATRIX, NOT A GREEN RUN. Green proves nothing here - it was green all three
# times. Before changing the gate, break it on purpose and confirm this suite goes red. The thirteen
# mutants it is known to kill: dropping the GITHUB_HEAD_REF preference; the branch match reverting to
# an unanchored `grep -F`; each of the four marker tests (file, block, line, line-above) reading the
# raw file instead of the code-span-stripped view; dropping the untracked-file arm; dropping the
# `/pull/NNN` arm; the gate crashing outright; removing the unknown-marker diagnosis; widening the
# marker-above window to two lines; widening the AGENTS.md exclusion to every file; and widening the
# scope past docs/inflight/. A new case earns its place by killing a mutant none of the others do.

set -uo pipefail

pass=0; fail=0
GATE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/bin/check-branch-self-reference.sh"

# Builds a fixture repo on branch feat/my-branch, runs the gate, and checks BOTH the exit code and
# that a failure named the file it was supposed to catch.
#   <name> <expected: pass|fail|pass_elsewhere|pass_rules|fail_untracked> <file-body>
assert() {
    local name="$1" expected="$2" body="$3" want="${4:-}" tmp rc got target out stage=1
    target="docs/inflight/note.md"
    case "$expected" in
        pass_elsewhere) target="docs/solutions/note.md"; expected=pass ;;
        pass_rules)     target="docs/inflight/AGENTS.md"; expected=pass ;;
        fail_untracked) stage=0; expected=fail ;;
    esac
    tmp="$(mktemp -d)"
    out="$(
        cd "$tmp" || exit 1
        git init -q .
        git checkout -q -b feat/my-branch 2>/dev/null || git branch -q -m feat/my-branch
        mkdir -p "$(dirname "$target")" docs/inflight
        # A scope control that lands outside docs/inflight/ must still find the directory populated,
        # or it passes through the `no documents to check` early exit and tests nothing at all.
        printf 'unrelated placeholder\n' > docs/inflight/other.md
        # The untracked case must exist ONLY after the commit: git ls-files reads the index, and a
        # note you have written but not staged is the commonest way to trip this gate locally.
        # Writing it on both sides of the commit would leave it tracked, testing nothing.
        [ "$stage" = 1 ] && printf '%b' "$body" > "$target"
        git add -A
        git -c user.email=t@e.invalid -c user.name=t commit -qm x
        [ "$stage" = 0 ] && printf '%b' "$body" > "$target"
        # UNSET the CI variables, or the fixture is not isolated: on a real PR, Actions sets
        # GITHUB_HEAD_REF to the actual branch, the gate prefers it over the fixture's checkout, and
        # every branch-name case silently tests the wrong name. Green on a developer machine where
        # the variable is absent, red only in CI - which is the worst place to learn it.
        # assert_ci below covers the path this unset removes.
        unset GITHUB_HEAD_REF GITHUB_REF GITHUB_REF_NAME
        PR_NUMBER=326 bash "$GATE" 2>&1
    )"
    rc=$?
    [ "$rc" -eq 0 ] && got=pass || got=fail
    # A non-zero exit is not evidence the gate FIRED - a crash is also non-zero. Require the report.
    if [ "$got" = fail ] && ! grep -q 'SELF-REF:' <<<"$out"; then
        printf 'FAIL: %s (exited non-zero without reporting a self-reference: %s)\n' "$name" "${out%%$'\n'*}"
        fail=$((fail + 1)); rm -rf "$tmp"; return
    fi
    # Some cases are about WHAT the gate said, not merely that it said something. Without this, a
    # mutation that deletes a specific diagnosis still passes on the back of an unrelated mention in
    # the same fixture.
    if [ -n "$want" ] && ! grep -q "$want" <<<"$out"; then
        printf 'FAIL: %s (output did not contain %s)\n' "$name" "$want"; fail=$((fail + 1)); rm -rf "$tmp"; return
    fi
    if [ "$got" = "$expected" ]; then
        printf 'ok:   %s\n' "$name"; pass=$((pass + 1))
    else
        printf 'FAIL: %s (expected %s, got %s) %s\n' "$name" "$expected" "$got" "${out%%$'\n'*}"; fail=$((fail + 1))
    fi
    rm -rf "$tmp"
}

# The branch resolution CI actually uses: a DETACHED HEAD plus GITHUB_HEAD_REF, which is what
# actions/checkout produces on a pull_request event. Without these two cases, deleting the
# GITHUB_HEAD_REF preference from the gate leaves every other case green while the gate exits 0 on
# every PR forever.
#   <name> <expected: pass|fail> <head-ref> <file-body>
assert_ci() {
    local name="$1" expected="$2" head_ref="$3" body="$4" tmp rc got out
    tmp="$(mktemp -d)"
    out="$(
        cd "$tmp" || exit 1
        git init -q .
        git checkout -q -b some-other-local-name 2>/dev/null || git branch -q -m some-other-local-name
        mkdir -p docs/inflight
        printf '%b' "$body" > docs/inflight/note.md
        git add -A
        git -c user.email=t@e.invalid -c user.name=t commit -qm x
        git checkout -q --detach
        unset GITHUB_REF GITHUB_REF_NAME
        GITHUB_HEAD_REF="$head_ref" PR_NUMBER=326 bash "$GATE" 2>&1
    )"
    rc=$?
    [ "$rc" -eq 0 ] && got=pass || got=fail
    if [ "$got" = fail ] && ! grep -q 'SELF-REF:' <<<"$out"; then
        printf 'FAIL: %s (exited non-zero without reporting a self-reference)\n' "$name"
        fail=$((fail + 1)); rm -rf "$tmp"; return
    fi
    if [ "$got" = "$expected" ]; then
        printf 'ok:   %s\n' "$name"; pass=$((pass + 1))
    else
        printf 'FAIL: %s (expected %s, got %s) %s\n' "$name" "$expected" "$got" "${out%%$'\n'*}"; fail=$((fail + 1))
    fi
    rm -rf "$tmp"
}

# --- RED: the gate must fire ---
assert "an unmarked branch-name mention fails"  fail 'work continues on feat/my-branch for now\n'
assert "an unmarked PR-number mention fails"    fail 'see astubbs#326 for the fix\n'
assert "the fully qualified PR form fails too"  fail 'see astubbs/parallel-consumer#326\n'
# A BARE `#NNN` counts. It was invisible until review: the regex demanded the `astubbs` prefix, and
# the hole was masked only by the issue-refs gate's QUALIFY_BELOW=1000 forcing qualification today.
# The bare number IS the fixture here; qualifying it would test the opposite thing, so the line
# carries the `issue-refs` line-scope opt-out. Note the two gates meeting: check-issue-refs.sh blocked
# this line on first commit, which is the QUALIFY_BELOW coupling the gate's own comment describes,
# demonstrated rather than argued. (Marker names appear in backticks throughout this file on purpose -
# both gates strip code spans before reading markers, so writing ABOUT one does not invoke it.)
# issue-refs: exempt-begin
assert "a bare #NNN mention fails"              fail 'see #326 for the fix\n'
# issue-refs: exempt-end
# The URL form is what `gh` prints and what a paste carries, and it is already live in this directory.
assert "a PR cited by URL fails"                fail 'tracked in https://github.com/astubbs/parallel-consumer/pull/326\n'
# git ls-files reads the INDEX, so an unstaged note was invisible - the commonest local case.
assert "an untracked note is still checked"     fail_untracked 'work continues on feat/my-branch\n'
# MENTION IS NOT USE. A note explaining the convention must not exempt itself by naming the marker.
assert "a quoted exempt-file does not exempt"   fail 'write `post-merge: exempt-file` to skip a file\nastubbs#326 is unmarked here\n'
assert "a quoted checked-begin opens nothing"   fail 'use `post-merge: checked-begin` to open a block\nastubbs#326 is unmarked here\n'
# A quoted PAIR is the one that matters: unclosed, awk emits no range and the mutant survives. Closed,
# reading the raw file wraps the mention in a real block and silences it.
assert "a quoted begin/end pair covers nothing" fail 'write `post-merge: checked-begin` first\nastubbs#326 is unmarked here\nthen `post-merge: checked-end`\n'
# The LINE and ABOVE marker tests need the same guard as the file and block ones - a sentence that
# tells the reader which marker to type must not thereby type it.
assert "a quoted marker on the line clears nothing"  fail 'on feat/my-branch, write `post-merge: checked` to clear it\n'
assert "a quoted marker above clears nothing"        fail 'write `post-merge: checked` above the line\non feat/my-branch\n'
# An unrecognised marker is not a no-op - the author believes the line is handled.
assert "an unknown exempt marker is an error"   fail 'post-merge: exempt\nastubbs#326 all over\n' 'unrecognised'

# --- GREEN: near-misses, each one character from something it must catch ---
assert "a DIFFERENT branch name is ignored"     pass 'work continues on feat/other-branch\n'
assert "a DIFFERENT PR number is ignored"       pass 'see astubbs#32 for that\n'
assert "a longer number sharing the prefix"     pass 'see astubbs#3266 for that\n'
# Scope controls for the bare form: a longer number, and a digit-adjacent one, must NOT match.
assert "a bare longer number is ignored"        pass 'see #3266 for that\n'
assert "a number embedded in a word is ignored" pass 'ref abc#326x and v1#3260\n'
assert "a longer PR URL number is ignored"      pass 'see .../parallel-consumer/pull/3266 there\n'
# Branch names NEST by convention here (bugs/857-..., fix/909-...), so a substring match on the name
# blocks unrelated PRs. These are the two cases that actually reproduced.
assert "a longer branch sharing the prefix"     pass 'follow-up lives on feat/my-branch-followup\n'
assert "a deeper path under the branch name"    pass 'log at builds/feat/my-branch2/output.log\n'
# The directory's RULES docs are not notes - they cite PRs permanently and correctly.
assert "AGENTS.md in the directory is ignored"  pass_rules 'the tag gate shipped in astubbs#326\n'
assert "an unrelated document is ignored"       pass 'nothing self-referential here at all\n'
# Scope control: the SAME sentence outside docs/inflight/ is history, not a live claim, and must pass.
assert "a mention outside docs/inflight is fine" pass_elsewhere 'landed via feat/my-branch, see astubbs#326\n'
assert "a marker on the same line clears it"    pass 'on feat/my-branch <!-- post-merge: checked -->\n'
assert "a marker on the line above clears it"   pass '<!-- post-merge: checked -->\non feat/my-branch\n'
assert "a marker two lines above does NOT"      fail '<!-- post-merge: checked -->\nfiller line\non feat/my-branch\n'
assert "a checked block clears its range"       pass '<!-- post-merge: checked-begin -->\nastubbs#326 did the thing\nand feat/my-branch too\n<!-- post-merge: checked-end -->\n'
assert "exempt-file clears the whole file"      pass 'post-merge: exempt-file\nastubbs#326 all over\nfeat/my-branch again\n'
# A block that CLOSED before the mention must not cover it - otherwise one block anywhere in a file
# silences everything after it, which is the failure mode this gate is itself about.
assert "a closed block does not cover later"    fail '<!-- post-merge: checked-begin -->\nx\n<!-- post-merge: checked-end -->\nastubbs#326 unmarked here\n'

# --- The CI branch-resolution path: detached HEAD, name from the event ---
assert_ci "the event branch name is preferred"  fail feat/my-branch 'work continues on feat/my-branch\n'
assert_ci "the local detached name is not used" pass feat/my-branch 'work continues on some-other-local-name\n'

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ] || exit 1
echo "All check-branch-self-reference self-tests passed"
