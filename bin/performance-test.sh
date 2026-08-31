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
grep -o 'PC-THROUGHPUT .*' "$LOG" > "$SUMMARY" || true

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
rm -f "$LOG"

exit "$rc"
