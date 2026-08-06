#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Soak a single test to resurface an intermittent failure, and report the failure RATE - because
# "it passed" tells you nothing about a test that fails 1 run in 6.
#
# WHY LOAD, NOT JUST REPETITION. The failures this exists to reproduce are load-dependent: they need
# the box contended enough that a poll or an await misses its deadline. A fast idle machine can pass
# the same test a hundred times and prove nothing - the CI runner that fails it has TWO cores and runs
# the suite forked. So this leaves roughly SOAK_FREE_CORES cores' worth of headroom and burns the rest,
# which is the cheapest honest way to make a 12-core laptop behave like a 2-core runner.
#
# Repetition is serial on purpose. Running concurrent Maven copies in one worktree looks like the
# obvious way to add load, but they share `target/` - `${project.build.directory}` comes from the POM
# and cannot be redirected per-run with -D - so they race on failsafe reports and build state, and you
# end up debugging the harness instead of the test.
#
# Usage:
#   bin/soak-test.sh <ITTestClass#method> [runs] [extra-maven-args...]
#
#   SOAK_FREE_CORES=N  cores to leave usable (default 2, matching the hosted runner). 0 means zero
#                      cores left free, i.e. MAXIMUM contention - for an unloaded baseline set it to
#                      the core count or higher.
#   SOAK_DIR=path      where per-run logs go (default a mktemp dir, printed at the end)
#
# Examples:
#   bin/soak-test.sh 'PartitionStateCommittedOffsetIT#committedOffsetRemoved' 20
#   SOAK_FREE_CORES=99 bin/soak-test.sh 'PartitionStateCommittedOffsetIT#committedOffsetRemoved' 5   # unloaded baseline
#
# Failing runs keep their whole log: an intermittent failure you did not capture has to be reproduced
# all over again.

set -euo pipefail

TEST="${1:?usage: bin/soak-test.sh <ITTestClass#method> [runs] [extra-maven-args...]}"
RUNS="${2:-10}"
if [ "$#" -gt 1 ]; then shift 2; else shift 1; fi

if command -v nproc >/dev/null 2>&1; then CORES=$(nproc); else CORES=$(sysctl -n hw.ncpu 2>/dev/null || echo 4); fi
FREE_CORES="${SOAK_FREE_CORES:-2}"
LOAD=$(( CORES - FREE_CORES ))
[ "$LOAD" -lt 0 ] && LOAD=0
SOAK_DIR="${SOAK_DIR:-$(mktemp -d -t soak.XXXXXX)}"
mkdir -p "$SOAK_DIR"

echo "SOAK: ${TEST}"
echo "SOAK: ${RUNS} run(s); ${CORES} cores, burning ${LOAD} to leave ~${FREE_CORES}; logs in ${SOAK_DIR}"

LOAD_PIDS=()
start_load() {
  for _ in $(seq 1 "$LOAD"); do
    ( while :; do :; done ) & LOAD_PIDS+=("$!")
  done
}
stop_load() {
  for p in "${LOAD_PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
  LOAD_PIDS=()
}
# Never leave burners running if the soak is interrupted - they have no natural exit.
trap 'stop_load' EXIT INT TERM

# Build once, unloaded: a slow build under contention is not the signal we are looking for, and it
# would dominate the wall clock of every run.
echo "SOAK: building first (unloaded)"
./mvnw -q -pl parallel-consumer-core -am test-compile \
  -Dlicense.skip -Dcopyright.skip=true -Djacoco.skip=true >"${SOAK_DIR}/build.log" 2>&1 \
  || { echo "SOAK: build FAILED - see ${SOAK_DIR}/build.log"; exit 1; }

[ "$LOAD" -gt 0 ] && start_load

PASS=0; FAIL=0; FAILED_RUNS=()
for n in $(seq 1 "$RUNS"); do
  LOG="${SOAK_DIR}/run-${n}.log"
  # -am is required, not optional: without it the enforcer's ReactorModuleConvergence rule fails
  # with "Module parents have been found which could not be found in the reactor" before any test runs.
  if ./mvnw -o -pl parallel-consumer-core -am verify \
      -Dit.test="$TEST" -Dtest=SKIPNONE \
      -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false \
      -Dlicense.skip -Dcopyright.skip=true -Djacoco.skip=true \
      "$@" >"$LOG" 2>&1; then
    PASS=$(( PASS + 1 )); echo "SOAK: run ${n}/${RUNS} PASS  (${PASS} pass / ${FAIL} fail)"
  else
    FAIL=$(( FAIL + 1 )); FAILED_RUNS+=("$n"); echo "SOAK: run ${n}/${RUNS} FAIL  (${PASS} pass / ${FAIL} fail)"
  fi
done

stop_load

echo
echo "SOAK RESULT: ${FAIL}/${RUNS} failed (${LOAD} load threads, ~${FREE_CORES} cores left free)"
if [ "$FAIL" -gt 0 ]; then
  echo "SOAK: failing runs: ${FAILED_RUNS[*]}"
  echo "SOAK: logs: ${SOAK_DIR}/run-<n>.log"
  # Surface the autopsy immediately: per AGENTS.md it answers "contention artifact or genuine bug"
  # before any manual reading.
  for n in "${FAILED_RUNS[@]}"; do
    echo "--- run ${n} ---"
    if grep -q "AMBIENT PROBE AUTOPSY" "${SOAK_DIR}/run-${n}.log" 2>/dev/null; then
      grep -A8 "AMBIENT PROBE AUTOPSY" "${SOAK_DIR}/run-${n}.log" | head -10
    else
      grep -E "<<< FAILURE|expected *:|but was *:" "${SOAK_DIR}/run-${n}.log" | head -5
    fi
  done
  exit 1
fi
echo "SOAK: no failures - which is NOT proof the flake is gone. Raise the run count, lower"
echo "SOAK: SOAK_FREE_CORES, and remember the runner that reproduces it has two cores."
