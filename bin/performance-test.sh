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
./mvnw --batch-mode \
  -Pci \
  clean verify \
  -DskipUTs=true \
  -Dincluded.groups=performance \
  -Dexcluded.groups= \
  "$@" 2>&1 | tee "$LOG" || rc=${PIPESTATUS[0]}

mkdir -p "$(dirname "$SUMMARY")"
# WHAT MACHINE PRODUCED THESE NUMBERS. One comment line, ahead of the data, because a rate without
# the box it came from cannot be compared with anything later - hosted runners are not identical, and
# the same code has been observed at a 1.54x spread across them.
#
# It is METADATA, not an input to any verdict. bin/check-throughput-regression.sh cancels machine
# speed by comparing against the other classes in the SAME run, which is strictly better than bucketing
# by hardware: it needs no model list, and it keeps working on a runner nobody has seen before. This
# line exists so somebody can later CHECK that normalisation is doing its job - if the normalised ratio
# turns out to correlate with the CPU model, the normalisation is failing and this is how you find out.
{
  printf '# machine cpu=%s cores=%s memkb=%s\n' \
    "$(sed -n 's/^model name[[:space:]]*: //p' /proc/cpuinfo 2>/dev/null | head -1 | tr ' ' '_')" \
    "$(nproc 2>/dev/null || echo unknown)" \
    "$(sed -n 's/^MemTotal:[[:space:]]*\([0-9]*\).*/\1/p' /proc/meminfo 2>/dev/null || echo unknown)"
} > "$SUMMARY" || true
grep -o 'PC-THROUGHPUT .*' "$LOG" >> "$SUMMARY" || true

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
