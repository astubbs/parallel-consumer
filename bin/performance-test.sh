#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run only the performance test suite (tests tagged @Tag("performance")).
# These are excluded from the regular CI build because they take a long time
# and need substantial hardware. Called by the "Performance Tests" leg of the
# `test` matrix in .github/workflows/maven.yml (a required check on every PR),
# Run by maven.yml as the required "Performance Tests" check.
#
# Usage: bin/performance-test.sh [extra-maven-args...]

set -euo pipefail

# WHERE THE THROUGHPUT FIGURES GO.
#
# This lane gates on wall-clock deadlines, and a deadline cannot say how MUCH slower a tree got - it
# says only "slower than the bound, on this runner, today". That is why a four-to-tenfold regression
# sat in this required check for weeks reading as flakiness: red-timing-lane and busy-runner are the
# same signal. The tests now emit one `PC-THROUGHPUT` line each, on pass and on fail; this collects
# them so the answer is in the job log rather than in whoever thinks to grep for it.
#
# COLLECTING IS NOT GATING, DELIBERATELY. Nothing here fails on a slow number. A threshold picked
# before anyone has seen the real spread would be a guess, and a flapping perf gate gets disabled
# within a week - which costs more than having none. The gate is tracked separately, in
# docs/inflight/perf-throughput-regression-gate.md, and wants a few runs of data first.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG="$(mktemp -t pc-performance-XXXXXX)"
SUMMARY="$ROOT/target/performance-throughput.txt"

# Do not let a test failure skip the summary - a failing run is exactly when the number is wanted,
# and `set -e` would otherwise exit before it is printed. The maven status is preserved and
# re-raised at the end, so the lane still fails when the suite fails.
rc=0
# -DreuseForks=false gives every performance class its OWN JVM.
#
# WHY: failsafe declares no reuseForks, so it silently took the default of TRUE - every class in this
# lane shared one VM. Nobody chose that for a lane whose whole output is a throughput number. When
# MultiInstanceRebalanceTest's capacity profiles joined the lane, the throughput test that runs after
# them fell from ~71,000 records/second to 39,684-44,992 on CI across three runs, and it read as a 45%
# product regression on the branch that added them. It is not one: the same test on the same tree,
# alone, returns 73,722.
#
# THE FLAG NAME IS THE TRAP. The user property is `reuseForks`, NOT `failsafe.reuseForks` - checked
# with `mvnw help:describe -Dplugin=...maven-failsafe-plugin:<v> -Dgoal=integration-test -Ddetail`,
# which prints "User property: reuseForks". The qualified guess is accepted and silently does
# nothing, the same shape as -DforkCount vs -Dsurefire.forkCount that this repo's pom already warns
# about. It binds to failsafe precisely because failsafe has no explicit <reuseForks>; surefire's
# explicit true in the root pom still wins for unit tests, which this lane skips anyway.
#
# NOT VERIFIABLE LOCALLY. The full lane on a development machine already passes at 72,498 - there is
# enough headroom to absorb the carryover a GitHub runner cannot. So this lane is the instrument, and
# CI is the only place the change can be judged.
./mvnw --batch-mode \
  -Pci \
  clean verify \
  -DskipUTs=true \
  -DreuseForks=false \
  -Dincluded.groups=performance \
  -Dexcluded.groups= \
  "$@" 2>&1 | tee "$LOG" || rc=${PIPESTATUS[0]}

mkdir -p "$(dirname "$SUMMARY")"
grep -o 'PC-THROUGHPUT .*' "$LOG" > "$SUMMARY" || true

# WHICH tests are represented, not just how many lines came back.
#
# The lane selects every @Tag("performance") class and only some of them call ThroughputReport, so a
# nonempty summary carrying one figure reads as "the lane measured" when most of the lane in fact
# contributed nothing - which leaves a regression in any of the others exactly as opaque as it was
# before this summary existed. Naming the silent ones is the difference between a partial measurement
# and one that looks complete.
#
# Both lists are DERIVED, never hand-maintained here. `ran` comes from the failsafe reports this run
# just wrote (the invocation above is `clean verify`, so nothing stale survives) and `reported` from
# the summary's own test= field. A hardcoded roster of the performance classes would be the copy that
# goes stale the day somebody adds a fifth, and it would go stale silently - the same failure this
# whole block exists to remove.
# `|| true` on each: this block is diagnostic, and `set -e` must not let a summary that could not be
# computed swallow the maven exit code re-raised at the end of the script.
ran=$(find . -type f -path '*/target/failsafe-reports/TEST-*.xml' 2>/dev/null \
        | sed -e 's#.*/TEST-##' -e 's#\.xml$##' -e 's#.*\.##' | sed '/^$/d' | sort -u) || ran=""
reported=$(sed -n 's/.*[[:space:]]test=\([A-Za-z0-9_$]*\).*/\1/p' "$SUMMARY" 2>/dev/null \
        | sed '/^$/d' | sort -u) || reported=""
silent=$(comm -23 <(printf '%s' "$ran") <(printf '%s' "$reported")) || silent=""

echo
echo "=== PC-THROUGHPUT (records/second per performance test) ==="
if [ -s "$SUMMARY" ]; then
  cat "$SUMMARY"
  echo
  echo "Saved to $SUMMARY"
  echo "Compare against the same test on the merge-base before reading any number as a regression:"
  echo "machine-to-machine spread is large, so only a like-for-like pair carries information."
else
  # An empty summary is a real finding, not a quiet nothing: either no performance test ran, or the
  # emitter stopped being called, and both look identical to a green lane otherwise.
  echo "NONE FOUND - either no performance test ran, or ThroughputReport is no longer reached."
  echo "Check the failsafe summary above before assuming this lane measured anything."
fi

if [ -n "$silent" ]; then
  echo
  echo "NOT MEASURED - these ran in this lane and emitted no PC-THROUGHPUT line:"
  printf '%s\n' "$silent" | sed 's/^/  /'
  echo "A figure for one test is not a figure for the lane. Add a ThroughputReport.report call to"
  echo "each of them, or accept that a regression there stays invisible to this summary."
fi
rm -f "$LOG"

exit "$rc"
