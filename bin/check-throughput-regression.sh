#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Compares this run's throughput against docs/perf-baseline.tsv, AFTER cancelling machine speed using
# the other performance classes in the same run.
#
# WHY NORMALISE AT ALL, RATHER THAN COMPARE THE RAW NUMBER
#
# The same test, same code, same lane has been observed at 109,898, 82,505 and 71,387 records/second
# across three CI runs - a 1.54x spread on identical trees. A raw comparison against any stored
# number is therefore mostly measuring which runner turned up, and a threshold tight enough to catch a
# real regression would fire constantly on that spread. This is why
# docs/inflight/perf-throughput-regression-gate.md deferred a gate rather than guessing a bound.
#
# The way out is the one that actually settled the question by hand during the
# astubbs/parallel-consumer#29 investigation: compare the subject against ITS NEIGHBOURS IN THE SAME
# RUN. A uniformly slower machine slows everything proportionally, so the neighbours carry the
# machine's speed and dividing it out leaves the part that is about the code. In that investigation
# the neighbours came back +5% and +4% against baseline while the throughput test was -39%, and that
# asymmetry - not the -39% itself - is what ruled out "the runner was slow".
#
# WHY CLASS TIMES AND NOT THE NEIGHBOURS' OWN RATES
#
# Because they have none. `ThroughputReport.report` has exactly ONE call site
# (MultiInstanceHighVolumeTest); LoadTest, VeryLargeMessageVolumeTest and LargeVolumeInMemoryTests run
# in this lane and emit no PC-THROUGHPUT line - which bin/performance-test.sh already reports as "NOT
# MEASURED". So the machine-speed proxy is the failsafe class TIME, which every class has whether or
# not it reports a rate. If those tests ever gain rate reporting, prefer it: a rate is a measurement
# of the product, a class time is a measurement of the run.
#
# WHAT IT DOES AND DOES NOT CATCH - READ THIS BEFORE TRUSTING A GREEN
#
# It catches a regression that hits the throughput test HARDER than its neighbours. It is blind to one
# that slows everything equally, because that is indistinguishable from a slow runner using only
# within-run data. A blind spot stated is worth more than a bound that pretends not to have one.
#
# ON THE THRESHOLDS, AND THE CASE THAT MOTIVATED THIS
#
# Worked through on the astubbs/parallel-consumer#29 numbers: observed 43,552 against a 71,387
# baseline with neighbours ~4.5% slow gives a machine index of ~0.957, an expected ~68,300, and a
# ratio of ~0.64. So that regression WARNS here and does NOT fail. That is deliberate and it is the
# honest state of the art today: nobody has measured the spread of the NORMALISED ratio, so a fail
# bound tight enough to catch 0.64 would be exactly the guess this repo has twice refused to make. The
# coarse 2x fail bound catches the four-to-tenfold class of regression that sat unnoticed in this lane
# for weeks. Tighten it when the master runs from .github/workflows/perf-baseline.yml have produced
# enough normalised ratios to see their spread - and say in that commit what the spread was.
#
# EXIT CODES follow bin/check-all.sh's contract: 0 pass, 1 regression, 2 cannot run, 3 nothing in
# scope. A clean tree has no summary file, which is "nothing in scope", never a pass.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BASELINE="docs/perf-baseline.tsv"
SUMMARY="target/performance-throughput.txt"

# Ratio below which the run FAILS. Coarse on purpose - see "ON THE THRESHOLDS" above.
FAIL_BELOW=0.5
# Ratio below which the run WARNS loudly but passes.
WARN_BELOW=0.8

if [ ! -f "$BASELINE" ]; then
    echo "check-throughput-regression: $BASELINE is missing - cannot compare." >&2
    exit 2
fi

if [ ! -s "$SUMMARY" ]; then
    echo "check-throughput-regression: no $SUMMARY - the performance lane has not run here."
    exit 3
fi

# --- this run's rate -------------------------------------------------------------------------------
subject=$(awk -F'\t' '$1 == "rate" { print $2; exit }' "$BASELINE")
baseline_rate=$(awk -F'\t' '$1 == "rate" { print $3; exit }' "$BASELINE")
if [ -z "${subject:-}" ] || [ -z "${baseline_rate:-}" ]; then
    echo "check-throughput-regression: $BASELINE has no 'rate' row - nothing to compare against." >&2
    exit 2
fi

observed_rate=$(sed -n "s/.*[[:space:]]test=${subject}[[:space:]].*recordsPerSecond=\([0-9-]*\).*/\1/p" \
    "$SUMMARY" | tail -1)
if [ -z "${observed_rate:-}" ] || [ "$observed_rate" -le 0 ] 2>/dev/null; then
    # A missing or -1 rate is a real finding, not a quiet pass: either the test did not run, or
    # ThroughputReport stopped being reached, and both look identical to a clean lane otherwise.
    echo "check-throughput-regression: no usable recordsPerSecond for $subject in $SUMMARY." >&2
    echo "  Either it did not run, or ThroughputReport is no longer reached. Not treating this as a pass."
    exit 2
fi

# --- machine speed, from the neighbours ------------------------------------------------------------
# Sum baseline and observed seconds over the classes present in BOTH, so a class that did not run (or
# is not in the baseline yet) is skipped rather than silently counted as zero.
reports=$(find . -type f -path '*/target/failsafe-reports/TEST-*.xml' 2>/dev/null | sort)
observed_secs=0; baseline_secs=0; matched=0; skipped=""
while IFS=$'\t' read -r kind name value _unit; do
    [ "$kind" = "class-seconds" ] || continue
    obs=""
    for f in $reports; do
        case "$f" in *"$name"*) ;; *) continue ;; esac
        obs=$(sed -n 's/.*<testsuite[^>]* time="\([0-9.]*\)".*/\1/p' "$f" | head -1)
        [ -n "$obs" ] && break
    done
    if [ -z "$obs" ]; then
        skipped="$skipped $name"
        continue
    fi
    observed_secs=$(awk -v a="$observed_secs" -v b="$obs" 'BEGIN { printf "%.4f", a + b }')
    baseline_secs=$(awk -v a="$baseline_secs" -v b="$value" 'BEGIN { printf "%.4f", a + b }')
    matched=$((matched + 1))
done < <(grep -v '^#' "$BASELINE")

if [ "$matched" -eq 0 ]; then
    # Without a neighbour there is no machine-speed proxy, and a raw comparison against a 1.54x-spread
    # instrument is worse than none. Say so rather than emit a verdict that cannot mean anything.
    echo "check-throughput-regression: no baseline neighbour class ran, so machine speed cannot be" >&2
    echo "  cancelled. Refusing to compare raw numbers across machines - see this script's header." >&2
    exit 2
fi

# machine_index > 1 means this runner was FASTER than the baseline runner.
machine_index=$(awk -v b="$baseline_secs" -v o="$observed_secs" 'BEGIN { printf "%.4f", b / o }')
expected=$(awk -v r="$baseline_rate" -v m="$machine_index" 'BEGIN { printf "%.0f", r * m }')
ratio=$(awk -v o="$observed_rate" -v e="$expected" 'BEGIN { printf "%.3f", (e > 0) ? o / e : 0 }')

printf 'check-throughput-regression: %s\n' "$subject"
printf '  observed        %s records/second\n' "$observed_rate"
printf '  baseline        %s records/second\n' "$baseline_rate"
printf '  machine index   %s  (from %d neighbour class(es): %ss observed vs %ss baseline)\n' \
    "$machine_index" "$matched" "$observed_secs" "$baseline_secs"
printf '  expected here   %s records/second\n' "$expected"
printf '  RATIO           %s  (1.0 = exactly what the machine speed predicts)\n' "$ratio"
[ -n "$skipped" ] && printf '  skipped (did not run):%s\n' "$skipped"

below() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a < b) }'; }

if below "$ratio" "$FAIL_BELOW"; then
    printf '\nFAILED: ratio %s is below %s - this tree is more than 2x slower than the machine explains.\n' \
        "$ratio" "$FAIL_BELOW"
    printf 'That is the four-to-tenfold class of regression, not runner noise. Do not re-baseline to\n'
    printf 'clear it without establishing which of the two it is.\n'
    exit 1
fi

if below "$ratio" "$WARN_BELOW"; then
    printf '\nWARNING: ratio %s is below %s. Slower than the neighbours explain, but inside the band\n' \
        "$ratio" "$WARN_BELOW"
    printf 'where nobody has measured the normalised spread yet, so this does not fail the lane.\n'
    printf 'The astubbs/parallel-consumer#29 shortfall sat here, at ~0.64. Worth a second run before\n'
    printf 'dismissing: bin/performance-test.sh, or re-run the lane.\n'
    exit 0
fi

printf '\nOK: within what machine speed accounts for.\n'
