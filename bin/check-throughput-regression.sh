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
# It catches a regression that hits the throughput test HARDER than its neighbours. A regression that
# slows everything equally is invisible TO THIS CHECK, because within-run data alone cannot separate it
# from a slow runner.
#
# THAT IS A LIMIT OF THIS CHECK, NOT OF THE AVAILABLE DATA, and an earlier version of this comment
# wrongly implied the latter. Every performance run inside GitHub's log-retention window is queryable
# (bin/perf-backfill.sh), so a uniform slowdown IS detectable by comparing absolute rates ACROSS runs -
# the history exists, nothing here reads it yet. Doing so is tracked in
# docs/inflight/perf-a-queryable-history-instead-of-a-single-committed-baseline.md, and it is also what
# would let the baseline rise when the product gets FASTER, which a hand-updated file does not.
#
# THE THRESHOLDS ARE MEASURED, NOT GUESSED - AND HERE IS THE MEASUREMENT
#
# bin/perf-backfill.sh mined every performance run still inside GitHub's log-retention window and
# computed the ratio below for each. Twelve runs carried a rate. They separate:
#
#   0.407 0.475 0.476 0.507 0.578 0.580 0.591 0.605   <- every regressed run
#   0.778 0.922 1.000 1.000                           <- every healthy run
#
# The regressed group is astubbs/parallel-consumer#29 carrying an O(shards) accessor evaluated as a
# plain log argument on every control-loop pass; the healthy group includes that same branch after the
# one-line fix, at 1.000. There is a gap between 0.605 and 0.778 and no observation inside it.
#
# FAIL_BELOW sits at 0.70, near the middle of that gap, so every observed regression fails and every
# observed healthy run passes, with roughly 0.09 of margin below and 0.08 above. That is a bound
# derived from data, which is what docs/inflight/perf-throughput-regression-gate.md deferred a gate
# waiting for - and the waiting was never going to supply it, because nothing accumulated the spread.
#
# WARN_BELOW at 0.85 deliberately fires on the slowest healthy run seen (0.778). A warning means look
# at this, not this is broken, and the run in question was genuinely the slowest of the healthy set.
#
# 0.70 IS NOT TIMID, IT IS MEASURED. The healthy band's floor, 0.778, is a DOCS-ONLY branch - four
# markdown files, no main code - so identical code lost 22% after normalisation on that run. Anything
# tighter than about 0.72 therefore fails documentation PRs, and a gate that does that is switched off
# within a week. Tightening needs comparison against a DISTRIBUTION of master runs rather than a single
# baseline; see docs/inflight/perf-a-queryable-history-instead-of-a-single-committed-baseline.md.
#
# TWELVE OBSERVATIONS FROM ONE RETENTION WINDOW IS NOT A LAW. Eight of the twelve are the same branch
# and the same defect, so the regressed group is really one phenomenon sampled eight times, not eight
# independent regressions. Widen it when there is more history - bin/perf-backfill.sh is re-runnable
# and additive - and say in that commit what the new spread was.
#
# EXIT CODES follow bin/check-all.sh's contract: 0 pass, 1 regression, 2 cannot run, 3 nothing in
# scope. A clean tree has no summary file, which is "nothing in scope", never a pass.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BASELINE="docs/perf-baseline.tsv"
SUMMARY="target/performance-throughput.txt"

# Ratio below which the run FAILS. Coarse on purpose - see "ON THE THRESHOLDS" above.
FAIL_BELOW=0.70
# Ratio below which the run WARNS loudly but passes.
WARN_BELOW=0.85

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
    printf '\nFAILED: ratio %s is below %s.\n' "$ratio" "$FAIL_BELOW"
    printf 'Every regressed run ever measured here scored between 0.407 and 0.605; every healthy one\n'
    printf 'scored 0.778 or above. This is in the first band. Do not re-baseline to clear it without\n'
    printf 'establishing which of the two it is - bin/perf-backfill.sh shows you the history.\n'
    exit 1
fi

if below "$ratio" "$WARN_BELOW"; then
    printf '\nWARNING: ratio %s is below %s. Slower than the neighbours explain, but inside the band\n' \
        "$ratio" "$WARN_BELOW"
    printf 'where nobody has measured the normalised spread yet, so this does not fail the lane.\n'
    printf 'The slowest healthy run measured here scored 0.778, so this band is not by itself a fault -\n'
    printf 'but it is the band to look at twice. Re-run the lane before dismissing it.\n'
    exit 0
fi

# THE TRIGGER FOR RAISING THE BASELINE, because otherwise there is not one.
#
# --suggest-baseline exists but nobody is prompted to run it, and a capability nobody is reminded of is
# one that does not happen - the same failure that left a throughput gate deferred for a fortnight
# while the data to set it sat in CI logs. So the prompt goes where somebody is already looking: the
# output of the run that noticed.
#
# A SINGLE RUN CANNOT ESTABLISH THAT THE PRODUCT GOT FASTER, and this does not claim it does. Spread on
# effectively identical code has been measured at 0.778 to 1.000, so one high ratio is as likely to be
# a fast runner. What it says is "this happened, and if it keeps happening the baseline is stale",
# which is the honest content of one observation.
if [ "$(awk -v a="$ratio" 'BEGIN { exit !(a > 1.15) }'; echo $?)" = "0" ]; then
    printf '\nBASELINE MAY BE STALE: ratio %s - this tree beat the baseline by more than 15%% after\n' "$ratio"
    printf 'normalising. One run does not establish that, since the spread on identical code reaches 0.78\n'
    printf 'to 1.00. If master keeps landing here, the baseline is measuring a floor the code has left\n'
    printf 'behind, and every later regression is judged from too low a bar:\n'
    printf '  bin/perf-backfill.sh --suggest-baseline\n'
fi

printf '\nOK: within what machine speed accounts for.\n'
