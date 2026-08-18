#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-api-breaking.sh - proves the gate can still SAY NO, and still refuses to
# run when it cannot actually compare. Modelled on bin/test-check-proto-breaking.sh, and here for a
# sharper reason than symmetry: while building the gate, THREE different mistakes each produced a
# confident green "No changes." while comparing nothing at all. Those three are cases 3-5 below.
#
# Each case mutates one thing, runs the gate, asserts the exit code, and restores the tree. CI runs
# this BEFORE the gate itself, so the tree the real check sees is untouched.
#
# Usage: bash bin/test-check-api-breaking.sh
# Exit codes: 0 = the gate behaves; 1 = the gate has regressed.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

MODULE=parallel-consumer-core
SRC=$MODULE/src/main/java/bz/stub/parallelconsumer/PollContext.java
WORK=$(mktemp -d)
failures=0

cleanup() {
    [ -f "$WORK/PollContext.orig" ] && cp "$WORK/PollContext.orig" "$SRC"
    rm -rf "$WORK"
}
trap cleanup EXIT

cp "$SRC" "$WORK/PollContext.orig"

# The exact artifact, not a glob: `$MODULE-*.jar` also matches the -sources and -javadoc jars.
VERSION=$(./mvnw -q help:evaluate -Dexpression=project.version -DforceStdout 2>/dev/null | tail -1)
JAR=$MODULE/target/$MODULE-$VERSION.jar

build() {
    ./mvnw --batch-mode -Papi-compat -pl "$MODULE" -am -Dmaven.test.skip=true -Dcopyright.skip=true \
        -Dmaven.javadoc.skip=true package >"$WORK/build.log" 2>&1 \
        || { echo "    build failed - see $WORK/build.log"; return 1; }
}

run_gate() {
    set +e
    JAPICMP_MODULES=$MODULE JAPICMP_BASELINE_JAR="$1" bash bin/check-api-breaking.sh >"$WORK/gate.log" 2>&1
    local rc=$?
    set -e
    echo "$rc"
}

expect() {
    local name=$1 want=$2 got=$3
    if [ "$got" = "$want" ]; then
        echo "  PASS  $name (exit $got)"
    else
        echo "  FAIL  $name - expected exit $want, got $got"
        sed 's/^/        /' "$WORK/gate.log" | tail -15
        failures=$((failures + 1))
    fi
}

echo "Building the pristine jar to use as this test's baseline..."
build || { echo "cannot self-test: the pristine build does not compile"; exit 1; }
cp "$JAR" "$WORK/baseline.jar"

echo
echo "Case 1: an unchanged tree is compatible"
build && expect "unchanged tree passes" 0 "$(run_gate "$WORK/baseline.jar")"

echo
echo "Case 2: a renamed public method is a break"
sed -i 's/public V value() {/public V valueRenamed() {/' "$SRC"
build && expect "renamed public method is caught" 1 "$(run_gate "$WORK/baseline.jar")"
cp "$WORK/PollContext.orig" "$SRC"

echo
echo "Case 3: a removed public method is a break"
python3 - "$SRC" <<'PY'
import re, sys
p = sys.argv[1]
s = open(p, encoding='utf-8').read()
# drop the whole `public K key() { ... }` accessor
s = re.sub(r'\n    public K key\(\) \{.*?\n    \}\n', '\n', s, count=1, flags=re.S)
open(p, 'w', encoding='utf-8').write(s)
PY
build && expect "removed public method is caught" 1 "$(run_gate "$WORK/baseline.jar")"
cp "$WORK/PollContext.orig" "$SRC"

echo
echo "Case 4: comparing an artifact against ITSELF must not pass"
# The original bug: baseline and new resolve to the same file, japicmp says "No changes." green.
build
expect "self-comparison refuses to run" 2 "$(run_gate "$(realpath "$JAR")")"

echo
echo "Case 5: a stale jar (a rebuild that never happened) must not pass"
# Touch a source file so it is newer than the jar, WITHOUT rebuilding - exactly the state a failed
# rebuild leaves behind, which twice produced a green gate over an unchanged artifact.
touch "$SRC"
expect "stale jar refuses to run" 2 "$(run_gate "$WORK/baseline.jar")"

echo
if [ "$failures" -eq 0 ]; then
    echo "All cases passed - the gate can still say no, and still refuses to run when blind."
    exit 0
fi
echo "$failures case(s) FAILED - the gate has regressed."
exit 1
