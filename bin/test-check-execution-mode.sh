#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-execution-mode.sh.
#
# Each case is a gap the guard exists to catch, pinned so a regression is visible. The guard's whole
# job is to refuse a green verdict when a mode was not exercised, so the cases that matter are the
# ones where surefire itself is perfectly happy: a suite that ran, passed, and skipped the only tests
# that could have proven anything.
#
# Run it from anywhere; it works in a temp tree and never touches the repo's own reports.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GUARD="$HERE/check-execution-mode.sh"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

passes=0
failures=0

# Writes a surefire report. Naming matters: the guard identifies the mode-proving suite by the class
# name in the file name, exactly as surefire writes it.
report() {
  local dir="$1" class="$2" tests="$3" fails="$4" errors="$5" skipped="$6"
  mkdir -p "$dir"
  cat > "$dir/TEST-$class.xml" <<XML
<?xml version="1.0" encoding="UTF-8"?>
<testsuite xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" name="$class" time="1.0" tests="$tests" errors="$errors" skipped="$skipped" failures="$fails">
  <properties/>
</testsuite>
XML
}

expect_exit() {
  local label="$1" expected="$2"
  shift 2
  local actual=0
  "$@" > "$WORK/out.md" 2> "$WORK/out.err" || actual=$?
  if [ "$actual" -eq "$expected" ]; then
    echo "  ok   $label (exit $actual)"
    passes=$((passes + 1))
  else
    echo "  FAIL $label: expected exit $expected, got $actual"
    sed 's/^/       /' "$WORK/out.err" || true
    failures=$((failures + 1))
  fi
}

expect_stdout_contains() {
  local label="$1" needle="$2"
  if grep -F -- "$needle" "$WORK/out.md" >/dev/null; then
    echo "  ok   $label"
    passes=$((passes + 1))
  else
    echo "  FAIL $label: report did not contain '$needle'"
    sed 's/^/       /' "$WORK/out.md" || true
    failures=$((failures + 1))
  fi
}

echo "check-execution-mode self-test"

# ---------------------------------------------------------------------------------------------
# THE CASE THE GUARD EXISTS FOR. Every other test passed, the mode's own tests all skipped. Surefire
# is green. This must be exit 1 - the LANE is broken - and never exit 0.
# ---------------------------------------------------------------------------------------------
D="$WORK/all-skipped/target/surefire-reports"
report "$D" "bz.stub.parallelconsumer.SomeOtherTest" 40 0 0 0
report "$D" "bz.stub.parallelconsumer.internal.VirtualThreadExecutionModeTest" 7 0 0 7
expect_exit "all mode tests skipped is a broken lane, not a pass" 1 "$GUARD" virtual-threads "$D"
expect_stdout_contains "...and says so in the summary" "verified nothing"

# ---------------------------------------------------------------------------------------------
# The mode-proving suite is absent entirely - a test-selection filter, a rename, a module that did
# not build. Indistinguishable from success in the surefire totals.
# ---------------------------------------------------------------------------------------------
D="$WORK/marker-missing/target/surefire-reports"
report "$D" "bz.stub.parallelconsumer.SomeOtherTest" 40 0 0 0
expect_exit "mode-proving suite missing is a broken lane" 1 "$GUARD" virtual-threads "$D"
expect_stdout_contains "...and names the suite it wanted" "VirtualThreadExecutionModeTest"

# ---------------------------------------------------------------------------------------------
# No reports at all.
# ---------------------------------------------------------------------------------------------
D="$WORK/empty/target/surefire-reports"
mkdir -p "$D"
expect_exit "no reports is a broken lane" 1 "$GUARD" virtual-threads "$D"

# ---------------------------------------------------------------------------------------------
# The happy path: the mode ran and everything agreed.
# ---------------------------------------------------------------------------------------------
D="$WORK/green/target/surefire-reports"
report "$D" "bz.stub.parallelconsumer.SomeOtherTest" 380 0 0 8
report "$D" "bz.stub.parallelconsumer.internal.VirtualThreadExecutionModeTest" 7 0 0 1
expect_exit "exercised and agreeing is a pass" 0 "$GUARD" virtual-threads "$D"
expect_stdout_contains "...and reports the skip count rather than hiding it" "Skipped"

# ---------------------------------------------------------------------------------------------
# The mode ran and the tree disagreed - exit 2, the useful red.
# ---------------------------------------------------------------------------------------------
D="$WORK/findings/target/surefire-reports"
report "$D" "bz.stub.parallelconsumer.SomeOtherTest" 380 2 1 8
report "$D" "bz.stub.parallelconsumer.internal.VirtualThreadExecutionModeTest" 7 0 0 1
expect_exit "exercised with disagreements is exit 2, not exit 1" 2 "$GUARD" virtual-threads "$D"
expect_stdout_contains "...and says not to silence them" "Do not silence"

# ---------------------------------------------------------------------------------------------
# BOTH reds at once: the mode never ran AND other tests failed. Exit 1 must win - failures from a run
# that cannot be shown to have exercised the mode are not evidence in either direction.
# ---------------------------------------------------------------------------------------------
D="$WORK/both/target/surefire-reports"
report "$D" "bz.stub.parallelconsumer.SomeOtherTest" 380 3 0 8
report "$D" "bz.stub.parallelconsumer.internal.VirtualThreadExecutionModeTest" 7 0 0 7
expect_exit "broken lane beats real findings" 1 "$GUARD" virtual-threads "$D"

# ---------------------------------------------------------------------------------------------
# The default mode has no marker suite - the whole run is its evidence - but an empty run is still a
# broken lane.
# ---------------------------------------------------------------------------------------------
D="$WORK/default-green/target/surefire-reports"
report "$D" "bz.stub.parallelconsumer.SomeOtherTest" 380 0 0 8
expect_exit "default mode needs no marker suite" 0 "$GUARD" default "$D"

# ---------------------------------------------------------------------------------------------
# An unknown mode is a typo in the workflow, and must not silently pass.
# ---------------------------------------------------------------------------------------------
expect_exit "an unknown mode is rejected" 1 "$GUARD" no-such-mode "$WORK/green/target/surefire-reports"

echo
echo "$passes passed, $failures failed"
[ "$failures" -eq 0 ]
