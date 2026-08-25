#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/ci-mutation-test.sh - specifically for its VERDICT, not for PIT.
#
# WHY THIS EXISTS. Measured 2026-08-25 over the last 40 `maven.yml` pull_request runs: the
# "Mutation Tests (PIT, PR-scoped)" check reported success on every one of them and scored ZERO
# mutants on every one of them. Nothing was broken - each skip was individually correct - but the
# lane had no way to say so in the one place anybody looks, so a green tick meant "did nothing"
# 40 times out of 40 and would have meant exactly the same thing had the scope regex been stale.
# AGENTS.md names that shape ("exits 0 printing nothing to mutate, skipping ... indistinguishable
# from a pass in the job summary"); this file is what makes it distinguishable and keeps it so.
#
# So every case below asserts an EXIT CODE, because the exit code is now the verdict:
#
#   0  scored at least PIT_MIN_MUTANTS mutants
#   2  CANNOT RUN - the scope matches nothing in the tree, or PIT scored nothing when asked to
#   3  nothing in scope - legitimate, and deliberately not 0
#
# A lane that cannot demonstrate a red is not evidence of anything, so the red arms come first and
# each has a green near-miss one character away from it - the shape bin/test-check-shell-lint.sh
# established on this branch.
#
# NO MAVEN RUNS HERE. Every case either exits before the build (the scoping arms) or feeds a
# captured PIT log through PIT_DRY_RUN_LOG (the verdict arms). The whole file is seconds.

set -euo pipefail

pass=0
fail=0

repo_root="$(git rev-parse --show-toplevel)"
subject="$repo_root/bin/ci-mutation-test.sh"

if [ ! -x "$subject" ]; then
    echo "test-ci-mutation-test: $subject is missing or not executable - CANNOT RUN" >&2
    exit 2
fi

# Builds a throwaway repo that looks enough like this one for the scoping logic: a core module with
# two main-source packages, one commit as the base, and a `refs/remotes/origin/master` pointing at
# it. Nothing here touches the real tree.
#
# $1 receives the temp dir path via stdout.
make_fixture() {
    local tmp
    tmp="$(mktemp -d)"
    (
        cd "$tmp" || exit 1
        git init -q .
        mkdir -p parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets
        mkdir -p parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state
        mkdir -p bin
        cp "$subject" bin/ci-mutation-test.sh
        printf 'package bz.stub.parallelconsumer.offsets;\nclass RunLengthEncoder {}\n' \
            > parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/RunLengthEncoder.java
        printf 'package bz.stub.parallelconsumer.state;\nclass ShardManager {}\n' \
            > parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/ShardManager.java
        git add -A
        git -c user.email=t@t -c user.name=t commit -q -m base
        git update-ref refs/remotes/origin/master HEAD
    ) > /dev/null 2>&1
    printf '%s' "$tmp"
}

# Runs the subject inside a fixture and asserts the exit code.
# $1 name, $2 expected exit code, $3 path a modification is written to (empty = no change),
# $4 extra env assignments as a string, $5 optional substring the output must contain.

# The verdict for one case, shared by all three fixture helpers below. It was written out three
# times, and the three copies had already drifted: two said "output never said why" and one said
# "but the output never said why". Nothing depended on the difference, which is how it survived.
# `pass` and `fail` are script-global.
record_result() {
    local name="$1" want_rc="$2" rc="$3" out="$4" want="${5:-}"
    if [ "$rc" != "$want_rc" ]; then
        printf 'FAIL: %s (expected exit %s, got %s)\n%s\n' "$name" "$want_rc" "$rc" "$out"
        fail=$((fail + 1))
    elif [ -n "$want" ] && ! grep -qF "$want" <<< "$out"; then
        # An exit code alone is not proof: a script that dies early scores the "cannot run" code for
        # the wrong reason. Each red arm must also SAY what it caught.
        printf 'FAIL: %s (exit %s was right, but the output never said why - wanted %s)\n%s\n' \
            "$name" "$rc" "$want" "$out"
        fail=$((fail + 1))
    else
        printf 'ok:   %s\n' "$name"
        pass=$((pass + 1))
    fi
}

assert_exit() {
    local name="$1" want_rc="$2" touch_path="$3" envs="$4" want="${5:-}"
    local tmp out rc
    tmp="$(make_fixture)"
    if [ -n "$touch_path" ]; then
        (
            cd "$tmp" || exit 1
            printf '// edited\n' >> "$touch_path"
            git add -A
            git -c user.email=t@t -c user.name=t commit -q -m change
        ) > /dev/null 2>&1
    fi
    # `env` rather than an exported assignment: each case must start from a clean environment, or a
    # variable set by an earlier case silently changes a later one's scope.
    set +e
    # shellcheck disable=SC2086  # $envs is a deliberate word-split list of KEY=VALUE assignments
    out="$(cd "$tmp" && env PIT_BASE_REF=master $envs bin/ci-mutation-test.sh 2>&1)"
    rc=$?
    set -e
    rm -rf "$tmp"

    record_result "$name" "$want_rc" "$rc" "$out" "$want"
}

# Writes a fake PIT log to $1 with the given statistics block. An empty $2 means the run died before
# reaching the statistics stage, which is what "never completes" actually looks like in the log.
write_pit_log() {
    local path="$1" generated="${2:-}"
    {
        printf '9:00:00 pm PIT >> INFO : Sending 3 test classes to minion\n'
        printf '9:00:01 pm PIT >> INFO : Calculated coverage in 3 seconds.\n'
        if [ -n "$generated" ]; then
            printf -- '- Statistics\n'
            printf '>> Line Coverage (for mutated classes only): 100/120 (83%%)\n'
            printf '>> Generated %s mutations Killed %s (100%%)\n' "$generated" "$generated"
            printf '>> Mutations with no coverage 0. Test strength 100%%\n'
            printf '>> Ran 40 tests (1.00 tests per mutation)\n'
        fi
    } > "$path"
}

# Writes a fake pitest mutations.xml under $1 (a fixture root) with $2 mutants all carrying status
# $3. The log and the report are SEPARATE fixtures on purpose: a log claiming mutants alongside a
# report where none were evaluated is precisely the state this lane must fail on, and a helper that
# derived one from the other could not express it.
write_pit_report() {
    local root="$1" count="$2" status="${3:-KILLED}" i
    mkdir -p "$root/parallel-consumer-core/target/pit-reports"
    {
        printf '<?xml version="1.0" encoding="UTF-8"?>\n<mutations>\n'
        for ((i = 1; i <= count; i++)); do
            printf "<mutation detected='true' status='%s' numberOfTestsRun='2'><sourceFile>RunLengthEncoder.java</sourceFile><lineNumber>%s</lineNumber><description>replaced return with 0</description></mutation>\n" \
                "$status" "$((100 + i))"
        done
        printf '</mutations>\n'
    } > "$root/parallel-consumer-core/target/pit-reports/mutations.xml"
}

echo "=== Scoping arms: a skip must never be reported as a pass ==="

# --- RED CONTROL: the stale-scope class, exactly as AGENTS.md describes it -----------------------
# This is the io.confluent -> bz.stub rename simulated. Before this guard existed the run below
# printed "nothing to mutate, skipping" and exited 0 - a green tick over a scope that can never
# match anything again.
assert_exit "red: a decidable scope matching nothing in the tree exits 2" 2 \
    "" "PIT_DECIDABLE_PACKAGES=^io\.confluent\.csid\.utils\.offsets\." \
    "matches NOTHING"

# --- GREEN NEAR-MISS: the same guard, one package name away --------------------------------------
# If this went red too, the guard would be rejecting every regex rather than checking it against the
# tree, and the control above would be proving nothing. It reaches the scoping decision and finds
# nothing changed, so 3 - not 0, and not 2.
assert_exit "green near-miss: a live scope with no changed classes exits 3" 3 \
    "" "PIT_DECIDABLE_PACKAGES=^bz\.stub\.parallelconsumer\.offsets\." \
    "nothing to mutate"

# --- RED CONTROL: no core main-source classes at all ---------------------------------------------
# A wrong checkout, a moved module, a `git ls-files` that returned nothing. Refusing to report
# success over an empty target set is the contract bin/check-shell-lint.sh already holds.
empty_tree_case() {
    local tmp out rc
    tmp="$(mktemp -d)"
    (
        cd "$tmp" || exit 1
        git init -q .
        mkdir -p bin
        cp "$subject" bin/ci-mutation-test.sh
        git add -A
        git -c user.email=t@t -c user.name=t commit -q -m base
        git update-ref refs/remotes/origin/master HEAD
    ) > /dev/null 2>&1
    set +e
    out="$(cd "$tmp" && env PIT_BASE_REF=master bin/ci-mutation-test.sh 2>&1)"
    rc=$?
    set -e
    rm -rf "$tmp"
    if [ "$rc" = "2" ] && grep -qF "no core main-source classes found" <<< "$out"; then
        printf 'ok:   red: an empty main-source tree exits 2, not 0\n'
        pass=$((pass + 1))
    else
        printf 'FAIL: empty main-source tree exited %s (wanted 2)\n%s\n' "$rc" "$out"
        fail=$((fail + 1))
    fi
}
empty_tree_case

# --- The two legitimate skips are 3, and they are told apart from each other ---------------------
assert_exit "green: changed classes all outside the decidable scope exits 3" 3 \
    "parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/ShardManager.java" \
    "PIT_DECIDABLE_PACKAGES=^bz\.stub\.parallelconsumer\.offsets\." \
    "outside the decidable packages"

# --- GREEN NEAR-MISS: widening the scope turns that same skip into a real run --------------------
# Same fixture, same changed file, one extra package in the regex. It now reaches the build, which
# in a fixture with no pom means a non-zero maven status - what matters is that it is neither 3
# (skipped) nor 2 (vacuous), i.e. the scope decision flipped. PIT_DRY_RUN_LOG keeps maven out of it.
widened_scope_case() {
    local tmp out rc log
    tmp="$(make_fixture)"
    log="$tmp/pit.log"
    write_pit_log "$log" 7
    write_pit_report "$tmp" 7
    (
        cd "$tmp" || exit 1
        printf '// edited\n' >> parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/ShardManager.java
        git add -A
        git -c user.email=t@t -c user.name=t commit -q -m change
    ) > /dev/null 2>&1
    set +e
    out="$(cd "$tmp" && env PIT_BASE_REF=master \
        PIT_DECIDABLE_PACKAGES='^bz\.stub\.parallelconsumer\.(offsets|state)\.' \
        PIT_DRY_RUN_LOG="$log" bin/ci-mutation-test.sh 2>&1)"
    rc=$?
    set -e
    rm -rf "$tmp"
    if [ "$rc" = "0" ] && grep -qF "PR-scoped to CHANGED decidable classes" <<< "$out"; then
        printf 'ok:   green near-miss: widening the scope makes the same change decidable\n'
        pass=$((pass + 1))
    else
        printf 'FAIL: widened scope exited %s (wanted 0 and a scoped target)\n%s\n' "$rc" "$out"
        fail=$((fail + 1))
    fi
}
widened_scope_case

echo
echo "=== Full-sweep arms: the same question for a PIT glob rather than a regex ==="

# Runs the subject in full-sweep mode (no base ref) with the given target glob.
# $1 name, $2 expected exit, $3 target glob, $4 optional substring.
assert_sweep() {
    local name="$1" want_rc="$2" target="$3" want="${4:-}"
    local tmp out rc log
    tmp="$(make_fixture)"
    log="$tmp/pit.log"
    write_pit_log "$log" 5
    write_pit_report "$tmp" 5
    set +e
    out="$(cd "$tmp" && env PIT_FULL_SWEEP=true PIT_TARGET_CLASSES="$target" \
        PIT_DRY_RUN_LOG="$log" bin/ci-mutation-test.sh 2>&1)"
    rc=$?
    set -e
    rm -rf "$tmp"
    record_result "$name" "$want_rc" "$rc" "$out" "$want"
}

# --- RED CONTROL: a sweep target naming a package that no longer exists --------------------------
assert_sweep "red: a full-sweep target matching nothing exits 2" 2 \
    'io.confluent.csid.utils.offsets.*' "matches NOTHING"

# --- GREEN NEAR-MISS: the same glob against the package that does exist --------------------------
assert_sweep "green near-miss: a live full-sweep target runs" 0 \
    'bz.stub.parallelconsumer.offsets.*'

# --- GREEN: a COMMA-SEPARATED target, which is what the PR path builds ---------------------------
# The first version of the guard read the whole list as one string and rejected `Foo,Foo$*` - a live
# target - so the lane exited 2 on a run that had previously scored 27 mutants. Found by running it,
# not by this file, which is exactly why the case is here now.
assert_sweep "green: a Foo,Foo\$* comma list is one target, not two failures" 0 \
    'bz.stub.parallelconsumer.offsets.RunLengthEncoder,bz.stub.parallelconsumer.offsets.RunLengthEncoder$*'

# --- GREEN: a bare star means everything, and must not be read as an empty prefix ----------------
assert_sweep "green: a bare * target is not treated as matching nothing" 0 '*'

echo
echo "=== Verdict arms: PIT ran, so did it actually score anything? ==="

# Runs the subject over a fixture whose changed class IS decidable, with a canned PIT log.
# $1 name, $2 expected exit, $3 generated-mutant count ("" = no statistics block at all),
# $4 optional substring, $5 optional extra env.
assert_verdict() {
    local name="$1" want_rc="$2" generated="$3" want="${4:-}" envs="${5:-}" status="${6:-KILLED}"
    local tmp out rc log
    tmp="$(make_fixture)"
    log="$tmp/pit.log"
    write_pit_log "$log" "$generated"
    # $status of "none" writes no report at all - PIT always emits one when it runs, so its absence
    # is itself a broken lane rather than a case to wave through.
    if [ -n "$generated" ] && [ "$status" != "none" ]; then
        write_pit_report "$tmp" "$generated" "$status"
    fi
    (
        cd "$tmp" || exit 1
        printf '// edited\n' >> parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/RunLengthEncoder.java
        git add -A
        git -c user.email=t@t -c user.name=t commit -q -m change
    ) > /dev/null 2>&1
    set +e
    # shellcheck disable=SC2086  # $envs is a deliberate word-split list of KEY=VALUE assignments
    out="$(cd "$tmp" && env PIT_BASE_REF=master PIT_DRY_RUN_LOG="$log" $envs bin/ci-mutation-test.sh 2>&1)"
    rc=$?
    set -e
    rm -rf "$tmp"
    record_result "$name" "$want_rc" "$rc" "$out" "$want"
}

# --- RED CONTROL: PIT was asked to mutate something and produced no statistics -------------------
# This is the astubbs#57 sighting recorded in docs/inflight/ci-mutation-testing.md
# ("PIT did not produce a report"). It used to exit with PIT's own status, which the workflow then
# swallowed with continue-on-error.
assert_verdict "red: PIT reached no statistics stage exits 2" 2 "" "scored NOTHING"

# --- RED CONTROL: statistics present, zero mutants generated -------------------------------------
# The subtler half, and the one a human reading the summary would skim past: a complete, tidy
# statistics block over an empty mutant set. Verified by COUNT, never by the presence of the block.
assert_verdict "red: a statistics block reporting 0 mutations exits 2" 2 "0" "scored NOTHING"

# --- GREEN NEAR-MISS: one mutant is enough ------------------------------------------------------
# One character away from the case above. If this failed too, the guard would be rejecting the log
# format rather than reading the count out of it.
assert_verdict "green near-miss: 1 generated mutation is a real run" 0 "1"

assert_verdict "green: a normal run with 42 mutations passes" 0 "42"

# --- The floor is a number and it moves ----------------------------------------------------------
# A threshold nobody can raise is decoration. Same log, same fixture, a floor above the count.
assert_verdict "red: PIT_MIN_MUTANTS is a real floor, not decoration" 2 "3" \
    "scored NOTHING" "PIT_MIN_MUTANTS=10"

assert_verdict "green near-miss: exactly meeting the floor passes" 0 "10" "" "PIT_MIN_MUTANTS=10"

echo
echo "=== Evaluated arms: GENERATING a mutant is not SCORING it ==="

# The verdict used to turn on the generated count, which is the number of mutants PIT CREATED. A run
# whose minions all die creates plenty and scores none, and against a generated-count floor it exited
# 0 - a green tick over a lane that measured nothing. Not hypothetical: the plan that scoped this lane
# recorded `state.*` hanging with minions dying, which is exactly this shape. The three arms below
# hold the log identical at 42 generated and vary only the report, so the count cannot be what flips
# them.

# --- RED CONTROL: every mutant came back MEMORY_ERROR --------------------------------------------
assert_verdict "red: 42 generated but every mutant MEMORY_ERROR exits 2" 2 "42" \
    "0 EVALUATED" "" "MEMORY_ERROR"

# --- RED CONTROL: the same shape via RUN_ERROR, so it is the CLASS not the one status -------------
assert_verdict "red: every mutant RUN_ERROR is equally unscored" 2 "42" \
    "minions died" "" "RUN_ERROR"

# --- RED CONTROL: PIT reported statistics but wrote no report at all ------------------------------
# PIT emits mutations.xml whenever it runs, so its absence alongside a statistics block means the
# lane cannot show its working. Nothing to count is not the same as nothing to find.
assert_verdict "red: a statistics block with no mutations.xml exits 2" 2 "42" \
    "scored NOTHING" "" "none"

# --- GREEN NEAR-MISS: identical log, identical count, mutants actually evaluated ------------------
# One field of the report away from the first arm. If this went red too, the guard would be rejecting
# the report format rather than reading statuses out of it, and the controls above would prove
# nothing.
assert_verdict "green near-miss: the same 42, this time KILLED, passes" 0 "42" \
    "42 evaluated mutant(s)" "" "KILLED"

# --- GREEN: NO_COVERAGE is an evaluated outcome, not an infrastructure failure --------------------
# A mutant no test reaches is the lane's most valuable finding. Counting it as unevaluated would make
# the lane fail hardest exactly when it has most to say. Exit 0 rather than 1 because this lane
# REPORTS findings without blocking on them - the same deliberate staging spotbugs:spotbugs runs
# under - so the survivor table is the product and the exit code only answers "did it measure".
assert_verdict "green: NO_COVERAGE counts as evaluated - it is a finding, not a failure" 0 "42" \
    "42 evaluated" "" "NO_COVERAGE"

printf '\n%s passed, %s failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
