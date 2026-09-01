#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-throughput-regression.sh, using REAL numbers from real CI runs.
#
# The cases are not invented. Each one is an observation recovered by bin/perf-backfill.sh from a run
# still in GitHub's log-retention window, so this file is simultaneously the check's test and the
# evidence its thresholds were derived from - a threshold whose justification lives only in a commit
# message is a threshold nobody can re-derive.
#
# Per-class seconds for the regressed case are apportioned from its recorded neighbour TOTAL in the
# baseline's proportions. The check sums matched classes, so the total is what it reads and the split
# is presentational; said here rather than left for someone to discover the numbers are not verbatim.

set -uo pipefail

CHECK="$(cd "$(dirname "$0")" && pwd)/check-throughput-regression.sh"
BASE="$(cd "$(dirname "$0")/.." && pwd)/docs/perf-baseline.tsv"
failures=0

run_case() { # description expected-exit rate very large load
    local desc="$1" expected="$2" rate="$3" very="$4" large="$5" load="$6"
    local dir out rc
    dir="$(mktemp -d)"
    mkdir -p "$dir/bin" "$dir/docs" "$dir/target" "$dir/parallel-consumer-core/target/failsafe-reports"
    cp "$CHECK" "$dir/bin/"; cp "$BASE" "$dir/docs/"
    # An EMPTY rate means "the summary exists but carries no usable figure for the subject" - the
    # emitter stopped being reached, or the test did not run. That is a different case from no summary
    # at all, and the two must not collapse: one is a broken lane, the other is a clean tree.
    if [ -n "$rate" ]; then
        printf 'PC-THROUGHPUT test=MultiInstanceHighVolumeTest processed=3000000 expected=3000000 elapsedMs=1 recordsPerSecond=%s outcome=X\n' \
            "$rate" > "$dir/target/performance-throughput.txt"
    else
        printf '# machine cpu=synthetic cores=2 memkb=1\n' > "$dir/target/performance-throughput.txt"
    fi
    for pair in "VeryLargeMessageVolumeTest:$very" "LargeVolumeInMemoryTests:$large" "LoadTest:$load"; do
        [ -n "${pair#*:}" ] || continue
        printf '<?xml version="1.0"?>\n<testsuite name="x.%s" time="%s" tests="1"/>\n' \
            "${pair%%:*}" "${pair#*:}" > "$dir/parallel-consumer-core/target/failsafe-reports/TEST-x.${pair%%:*}.xml"
    done
    out=$( cd "$dir" && bash bin/check-throughput-regression.sh 2>&1 ); rc=$?
    rm -rf "$dir"
    if [ "$rc" -eq "$expected" ]; then
        printf 'ok    %-52s exit %d  %s\n' "$desc" "$rc" "$(printf '%s' "$out" | sed -n 's/.*RATIO *\([0-9.]*\).*/ratio=\1/p')"
    else
        printf 'FAIL  %-52s expected %d, got %d\n%s\n' "$desc" "$expected" "$rc" "$out"
        failures=$((failures + 1))
    fi
}

# MUST FAIL - the regression this whole exercise is about. Run 33478449495 on
# astubbs/parallel-consumer#29: 43,552 rec/s against 134.65s of neighbours. Ratio ~0.578.
run_case "regressed: astubbs#29 at 43,552 (the real one)" 1 43552 53.47 40.18 40.99

# MUST FAIL - the worst observed, 29,372 rec/s. If the coarse end ever stops failing, the check is broken.
run_case "regressed: worst observed, 29,372" 1 29372 55.81 41.95 42.79

# MUST PASS - the same branch AFTER the one-line fix, run 33487673494: 76,950 against 131.88s. Ratio ~1.0.
run_case "healthy: astubbs#29 after the fix, 76,950" 0 76950 52.37 39.36 40.15

# MUST PASS - the slowest healthy run seen, 57,215 on docs/beads-evaluation. Ratio ~0.778, which WARNS
# but must not fail: a warning is "look at this", and this run was genuinely fine.
run_case "healthy but slow: 57,215 (warns, must not fail)" 0 57215 54.78 41.17 41.99

# MUST NOT PASS - a summary with no usable rate. Exit 2, never 0: a test that did not run, or an
# emitter that stopped being reached, looks identical to a clean lane otherwise.
run_case "summary exists but carries no rate" 2 "" 51.69 38.85 39.63

# MUST NOT PASS - no neighbour ran, so machine speed cannot be cancelled and a raw comparison across
# machines would be meaningless. Exit 2 rather than a verdict.
run_case "no neighbour class ran" 2 77960 "" "" ""

if [ "$failures" -eq 0 ]; then
    echo "All check-throughput-regression self-tests passed"
    exit 0
fi
printf '%d check-throughput-regression self-test(s) failed\n' "$failures"
exit 1
