#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Self-test for bin/check-shell-lint.sh.
#
# A green lint lane proves nothing on its own: it looks identical whether the linter examined 59
# scripts or zero of them. `docs/agent-harness.md` requires a red control that shows the gate can
# fire, AND - since 18a61321b - a green near-miss arm, so the control proves the gate is looking at
# the right thing rather than merely capable of failing.
#
# Every case runs against a THROWAWAY copy of the tree in a temp directory. Nothing here edits a
# tracked file, so an interrupted run cannot leave a mutant behind.

set -euo pipefail

pass=0
fail=0

if ! command -v shellcheck > /dev/null 2>&1; then
    echo "SKIP: shellcheck not installed - cannot self-test the lint gate" >&2
    exit 2
fi

repo_root="$(git rev-parse --show-toplevel)"

# Runs the gate over a scratch repo containing one script, and asserts the outcome.
# $1 name, $2 expected (pass|fail), $3 script body, $4 optional substring the output must contain
assert_case() {
    local name="$1" expected="$2" body="$3" want="${4:-}"
    local tmp out got
    tmp="$(mktemp -d)"
    (
        cd "$tmp" || exit 1
        git init -q .
        mkdir -p bin
        cp "$repo_root/bin/check-shell-lint.sh" bin/
        printf '%s\n' "$body" > bin/subject.sh
        git add -A
        git -c user.email=t@t -c user.name=t commit -q -m fixture
    ) > /dev/null 2>&1

    if out="$(cd "$tmp" && bin/check-shell-lint.sh 2>&1)"; then got=pass; else got=fail; fi

    if [ "$got" != "$expected" ]; then
        printf 'FAIL: %s (expected %s, got %s)\n%s\n' "$name" "$expected" "$got" "$out"
        fail=$((fail + 1))
    elif [ -n "$want" ] && ! grep -qF "$want" <<< "$out"; then
        # A red case must SAY what it caught. A gate that crashes on every input scores a pass on
        # exit code alone, which is how three earlier gates in this repo were found to be vacuous.
        printf 'FAIL: %s (output did not contain %s)\n%s\n' "$name" "$want" "$fail"
        fail=$((fail + 1))
    else
        printf 'ok:   %s\n' "$name"
        pass=$((pass + 1))
    fi
    rm -rf "$tmp"
}

# --- RED CONTROL: the array-expansion ambiguity, a real finding from the tree this landed on -----
# SC1087: `"$var[..."` reads as an array expansion. In bin/check-quarantine-registry.sh it was a
# regex character class and happened to work, but the two are indistinguishable to a reader.
assert_case "red: ambiguous array expansion is caught" fail \
    '#!/usr/bin/env bash
method=foo
grep -qE "[[:space:]]$method[[:space:]]*\\(" /dev/null' \
    "SC1087"

# --- WHAT THIS LANE CANNOT DO, asserted so nobody assumes otherwise ------------------------------
# ShellCheck has NO bash-version awareness: --shell=bash means "bash, any version", so a bash-4
# builtin on a bash 3.2 platform is invisible to it. `mapfile` - the headline example of this
# repo's August portability class, which died with exit 127 on macOS - passes clean here. It is
# flagged only under --shell=sh/dash, as POSIX portability, which these bash scripts are not.
# The detector for THAT class is running the scripts on macOS, which is the `shell: macos` lane in
# astubbs/parallel-consumer#355, not this one. This case is a green assertion on purpose: if a
# future ShellCheck gains version awareness it will go red, and that is the signal to widen this
# lane and narrow astubbs#355's claim.
assert_case "green (documented gap): bash-4 mapfile is INVISIBLE to shellcheck" pass \
    '#!/usr/bin/env bash
mapfile -t xs < <(printf "a\nb\n")
echo "${xs[0]}"'

# --- RED CONTROL: the directive that silently disabled the linter --------------------------------
# A prose comment whose first word is `shellcheck` parses as a DIRECTIVE and aborts analysis of the
# file. This is the real finding from the tree this gate landed on, kept as a permanent case.
assert_case "red: prose comment parsed as a directive is caught" fail \
    '#!/usr/bin/env bash
# shellcheck does not catch this, so we grep instead
echo hi' \
    "shellcheck"

# --- GREEN NEAR-MISS: one character away from the case above -------------------------------------
# Same sentence, prefixed so it is prose rather than a directive. If this went red the gate would be
# matching on the word `shellcheck` rather than on directive syntax, and the red case above would be
# proving nothing.
assert_case "green near-miss: the same comment, prefixed, is fine" pass \
    '#!/usr/bin/env bash
# NOTE: shellcheck does not catch this, so we grep instead
echo hi'

# --- GREEN NEAR-MISS: a warning-severity finding must NOT fail an error-severity gate ------------
# SC2164 (cd without ||) is a warning. It has to pass here, or the severity floor is not the floor.
assert_case "green near-miss: a warning does not trip the error floor" pass \
    '#!/usr/bin/env bash
cd /tmp
echo hi'

# --- The severity floor is real, not decorative --------------------------------------------------
severity_case() {
    local tmp out
    tmp="$(mktemp -d)"
    (
        cd "$tmp" || exit 1
        git init -q .
        mkdir -p bin
        cp "$repo_root/bin/check-shell-lint.sh" bin/
        printf '%s\n' '#!/usr/bin/env bash
cd /tmp
echo hi' > bin/subject.sh
        git add -A
        git -c user.email=t@t -c user.name=t commit -q -m fixture
    ) > /dev/null 2>&1
    if out="$(cd "$tmp" && SHELL_LINT_SEVERITY=warning bin/check-shell-lint.sh 2>&1)"; then
        printf 'FAIL: severity floor is decorative - SC2164 passed at severity=warning\n'
        fail=$((fail + 1))
    elif ! grep -qF 'SC2164' <<< "$out"; then
        # A nonzero exit is not proof the FLOOR moved. Without this, the arm passes when the script
        # dies early for any unrelated reason - the same "red for the wrong reason" defect this whole
        # branch is about, and one that contaminated a forbidden-apis control arm earlier in it.
        printf 'FAIL: exited nonzero at severity=warning but never mentioned SC2164 - red for the wrong reason\n%s\n' "$out"
        fail=$((fail + 1))
    else
        printf 'ok:   the severity floor lowers (SC2164 caught at warning)\n'
        pass=$((pass + 1))
    fi
    rm -rf "$tmp"
}
severity_case

# --- Refuses to report success over an empty set -------------------------------------------------
empty_case() {
    local tmp rc
    tmp="$(mktemp -d)"
    (
        cd "$tmp" || exit 1
        git init -q .
        mkdir -p bin
        cp "$repo_root/bin/check-shell-lint.sh" bin/check-shell-lint.sh
        git add -A
        git -c user.email=t@t -c user.name=t commit -q -m fixture
        git rm -q --cached bin/check-shell-lint.sh
    ) > /dev/null 2>&1
    ( cd "$tmp" && bin/check-shell-lint.sh ) > /dev/null 2>&1 && rc=0 || rc=$?
    if [ "$rc" = "2" ]; then
        printf 'ok:   an empty target set exits 2, not 0\n'
        pass=$((pass + 1))
    else
        printf 'FAIL: empty target set exited %s, expected 2 (a vacuous run must not read as clean)\n' "$rc"
        fail=$((fail + 1))
    fi
    rm -rf "$tmp"
}
empty_case

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
