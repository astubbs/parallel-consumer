#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Self-test for bin/check-all.sh.
#
# The runner has one job that matters and one that is easy to get subtly wrong. The job: never let a
# gate that could not run be counted as a gate that passed. The subtlety: it maintains a small
# exception list of scripts that are not tree gates, and a hand-maintained list inside a script whose
# whole purpose is to abolish hand-maintained lists will rot the moment somebody renames a file - so
# the assertion that catches that rot is itself under test here.
#
# Fixtures are a throwaway bin/ containing a copy of the subject plus stub gates, so the real
# repository's gate set cannot make these arms pass or fail by accident.

set -uo pipefail

subject="$(cd "$(dirname "$0")" && pwd)/check-all.sh"
pass=0
fail=0

# $1 fixture root. Creates the five scripts the subject's exception list asserts must exist, so that
# assertion is satisfied unless an arm is deliberately testing it.
stub_exceptions() {
    # The PR-scoped four must carry a `gh ` call and the needs-args one a `usage:` line, because the
    # subject now corroborates each recorded reason rather than merely checking the name resolves.
    # Stubs that skipped that would be testing a laxer script than the one that ships.
    for n in check-pr-ready.sh check-human-lgtm.sh check-review-posted.sh \
             check-pr-analysis-surfaces.sh; do
        printf '#!/usr/bin/env bash\n# gh pr view - stub\nexit 0\n' > "$1/bin/$n"
        chmod +x "$1/bin/$n"
    done
    printf '#!/usr/bin/env bash\n# usage: check-ossindex-audit.sh <log>\nexit 0\n' \
        > "$1/bin/check-ossindex-audit.sh"
    chmod +x "$1/bin/check-ossindex-audit.sh"
}

# $1 name, $2 exit code the fixture gate returns
make_fixture() {
    local dir; dir="$(mktemp -d)"
    mkdir -p "$dir/bin"
    cp "$subject" "$dir/bin/check-all.sh"
    stub_exceptions "$dir"
    printf '#!/usr/bin/env bash\necho "fixture gate speaking"\nexit %s\n' "$2" > "$dir/bin/check-$1.sh"
    chmod +x "$dir/bin/check-$1.sh"
    echo "$dir"
}

assert() {
    local name="$1" want_rc="$2" want="$3" out="$4" rc="$5"
    if [ "$rc" = "$want_rc" ] && grep -qF "$want" <<< "$out"; then
        printf 'ok:   %s\n' "$name"; pass=$((pass + 1))
    else
        printf 'FAIL: %s (exit %s, wanted %s, looking for "%s")\n%s\n' "$name" "$rc" "$want_rc" "$want" "$out"
        fail=$((fail + 1))
    fi
}

run() { local d="$1"; shift; ( cd "$d" && bash bin/check-all.sh --gates-only "$@" 2>&1 ); }

echo "=== a failing gate must fail the sweep, and be named ==="
d="$(make_fixture failing 1)"; out="$(run "$d")"; rc=$?
assert "red: a gate exiting 1 fails the sweep" 1 "FAILED: check-failing.sh" "$out" "$rc"
rm -rf "$d"

# GREEN NEAR-MISS: identical fixture, gate returns 0. If this went red too, the runner would be
# failing on the fixture's existence rather than on its verdict.
d="$(make_fixture failing 0)"; out="$(run "$d")"; rc=$?
assert "green near-miss: the same gate exiting 0 passes" 0 "no gate failed" "$out" "$rc"
rm -rf "$d"

echo
echo "=== a skip is not a pass, which is the whole point ==="

# A gate that CANNOT RUN must never be counted among the passes. This is the defect the runner
# exists to avoid importing from the gates it wraps.
d="$(make_fixture cannot 2)"; out="$(run "$d")"; rc=$?
assert "exit 2 is reported as CANNOT RUN, not as a pass" 0 "COULD NOT RUN: check-cannot.sh" "$out" "$rc"
assert "...and the pass count excludes it" 0 "0 passed" "$out" "$rc"
rm -rf "$d"

d="$(make_fixture nothing 3)"; out="$(run "$d")"; rc=$?
assert "exit 3 is nothing-in-scope, also not a pass" 0 "1 nothing-in-scope" "$out" "$rc"
rm -rf "$d"

echo
echo "=== the exception list must not rot silently ==="

# Rename one of the excepted scripts and the runner must refuse, rather than quietly sweeping one
# gate fewer than it claims. A list nobody validates is how this class comes back.
d="$(make_fixture ok 0)"; rm -f "$d/bin/check-review-posted.sh"
out="$(run "$d")"; rc=$?
assert "red: an exception naming a missing script exits 2" 2 "do not exist" "$out" "$rc"
assert "...and says which one" 2 "check-review-posted.sh" "$out" "$rc"
rm -rf "$d"

echo
echo "=== nothing to run is not success ==="
d="$(mktemp -d)"; mkdir -p "$d/bin"; cp "$subject" "$d/bin/check-all.sh"; stub_exceptions "$d"
# Only the excepted stubs exist, and every one of them is skipped, so nothing actually runs.
out="$(run "$d")"; rc=$?
assert "red: a sweep where nothing ran exits 2" 2 "NOTHING RAN" "$out" "$rc"
rm -rf "$d"

echo
echo "=== --pr opts the PR-state reporters back in ==="
d="$(make_fixture ok 0)"; out="$(run "$d" --pr)"; rc=$?
assert "green: --pr runs the PR-scoped gates instead of skipping them" 0 "5 passed" "$out" "$rc"
# The needs-an-argument gate stays skipped even under --pr, because --pr changes SCOPE and not the
# fact that the script cannot be invoked bare. Pinned so nobody "fixes" that into a usage error.
assert "...but the needs-an-argument gate is still skipped" 0 "1 skipped" "$out" "$rc"
rm -rf "$d"

echo
echo "=== a gate bash cannot parse is BROKEN, not skipped ==="

# `bash a-script-with-a-syntax-error` exits 2, the same code the repo uses for "cannot run". Without
# a parse pre-check the two are indistinguishable and a broken gate lands in the skip bucket. The
# sweep normally still goes red via ShellCheck - measured - but not on a machine without ShellCheck,
# where the linter itself exits 2 and both skips agree on a false green.
d="$(mktemp -d)"; mkdir -p "$d/bin"; cp "$subject" "$d/bin/check-all.sh"; stub_exceptions "$d"
printf '#!/usr/bin/env bash\nif true\necho unreachable\n' > "$d/bin/check-malformed.sh"
chmod +x "$d/bin/check-malformed.sh"
out="$(run "$d")"; rc=$?
assert "red: an unparseable gate FAILS the sweep" 1 "FAILED: check-malformed.sh" "$out" "$rc"
assert "...and is not laundered into the cannot-run bucket" 1 "does not parse" "$out" "$rc"
rm -rf "$d"

echo
echo "=== an exception whose stated reason expired must not stay silently skipped ==="

# Existence catches a rename. It cannot catch a script that stopped being what the list says it is,
# which is the same under-checking arriving by staleness - so each reason is corroborated.
d="$(make_fixture ok 0)"
printf '#!/usr/bin/env bash\nexit 0\n' > "$d/bin/check-review-posted.sh"   # no gh call any more
out="$(run "$d")"; rc=$?
assert "red: a PR-scoped exception with no gh call exits 2" 2 "no longer corroborated" "$out" "$rc"
assert "...and names the script" 2 "check-review-posted.sh" "$out" "$rc"
rm -rf "$d"

echo
echo "=== the self-tests loop actually runs, and runs FIRST ==="

# Every other arm passes --gates-only, so the MODE=all branch - the one CI and the documented
# pre-push command use - had no coverage at all. The ordering is a claim in both the header and
# bin/AGENTS.md: a gate's self-test runs before the gate it protects.
d="$(make_fixture ok 0)"
printf '#!/usr/bin/env bash\nexit 0\n' > "$d/bin/test-fixture.sh"; chmod +x "$d/bin/test-fixture.sh"
out="$( cd "$d" && bash bin/check-all.sh 2>&1 )"; rc=$?
assert "green: the default mode runs the self-tests too" 0 "test-fixture.sh" "$out" "$rc"
tests_at="$(grep -n '=== self-tests ===' <<< "$out" | head -1 | cut -d: -f1)"
gates_at="$(grep -n '=== gates ===' <<< "$out" | head -1 | cut -d: -f1)"
if [ -n "$tests_at" ] && [ -n "$gates_at" ] && [ "$tests_at" -lt "$gates_at" ]; then
    printf 'ok:   self-tests are reported BEFORE gates, as bin/AGENTS.md claims\n'; pass=$((pass + 1))
else
    printf 'FAIL: self-tests/gates ordering (tests at %s, gates at %s)\n%s\n' "$tests_at" "$gates_at" "$out"
    fail=$((fail + 1))
fi
rm -rf "$d"

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
