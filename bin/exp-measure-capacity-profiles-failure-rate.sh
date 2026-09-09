#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# shell-justified: mirrors bin/exp-measure-large-instances-failure-rate.sh and every other
# experiment runner, all of which share bin/lib/chaos-experiment-common.sh's bash functions
# (pc_run_performance, pc_failsafe_stats, pc_classify_failsafe_stats) - splitting this one script
# into Node would mean reimplementing or shimming that shared library, not reusing it.
#
# Measure the failure RATE of MultiInstanceRebalanceTest's two capacity profiles that are NOT
# largeNumberOfInstances: cooperativeStickyRebalanceShouldNotStall and gentleChaosRebalance.
#
# WHY THIS SCRIPT EXISTS, SEPARATELY FROM exp-measure-large-instances-failure-rate.sh: 2026-09-07
# moved all three capacity profiles out of the required `Performance Tests` gate and onto
# `@Tag(CAPACITY_TAG)`, on the argument that they are measurements whose legitimate output is a
# rate, not a lane that can be red or green (docs/inflight/test-largenumberofinstances-cannot-gate-a-merge.md).
# Before that change, these two ran - and gated - on every PR via the required lane; afterward,
# `bin/performance-test.sh` excludes the tag and no scheduled or dispatchable runner selected them,
# so they ran NOWHERE. AGENTS.md ("A test that never runs is not a passing test, and nothing goes
# red to tell you") is exactly the trap: a lane that selects nothing passes silently. This script is
# the fix - it gives these two the same weekly cadence largeNumberOfInstances already has, on the
# same experiments.yml schedule, never gating.
#
# Both share MultiInstanceRebalanceTest's runScenario/ProgressTracker plumbing with
# largeNumberOfInstances, so the same failsafe-report classification and "No progress beyond N
# records" / "missing keys" extraction apply unchanged - only the test method and the output
# directory differ.
#
# `-e` is deliberately omitted - a failing iteration is the data. bin/lib/chaos-experiment-common.sh
# owns that reasoning, along with the maven invocation and the outcome classifier every runner here
# shares, including the discipline that a run which executed no test is NOT a data point.
set -u
# shellcheck source=bin/lib/chaos-experiment-common.sh
source "${BASH_SOURCE[0]%/*}/lib/chaos-experiment-common.sh"

D="$(git rev-parse --show-toplevel)"
REF="$(git -C "$D" rev-parse --short HEAD 2>/dev/null || echo unknown)"
ITERATIONS="${1:-10}"

# method:output-dir-name pairs - both capacity profiles that are not largeNumberOfInstances.
PROFILES=(
    "cooperativeStickyRebalanceShouldNotStall:cooperative-sticky-rebalance"
    "gentleChaosRebalance:gentle-chaos-rebalance"
)

for profile in "${PROFILES[@]}"; do
    method="${profile%%:*}"
    dirname="${profile##*:}"
    echo "=== $method: $ITERATIONS iteration(s) ==="
    pc_measure_profile_failure_rate "$D" "$method" "/tmp/$dirname" "$ITERATIONS" "$REF"
done
