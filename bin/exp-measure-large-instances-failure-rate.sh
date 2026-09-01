#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Measure the failure RATE of MultiInstanceRebalanceTest.largeNumberOfInstances, and the shape of
# the failure when it fails.
#
# WHY: amrynsky reported on confluentinc#857, 2026-01-11, that "every other run of this test is
# failing" with "No progress beyond N records after M rounds" - the closest thing to a repeatable
# in-repo instance of the reported paused-consumption symptom. It is @Tag("performance"), so it does
# not run in the default lanes, and nobody has measured it since. The fork's own note is titled for
# the gap: the claim that its residual failures are Kafka's HAS NEVER BEEN MEASURED.
#
# This answers three questions no judgement is needed for:
#   1. What IS the failure rate? "Every other run" is a claim, not a measurement.
#   2. Where does progress stop - the same record count each time, or scattered? A constant is a
#      structural boundary; scatter is load.
#   3. Which keys go missing - the same ones, or different? Same keys across runs would point at a
#      partition or shard, not at timing.
#
# Pure CPU. Run it and read the tally.
#
# `-e` is deliberately omitted - a failing iteration is the data. bin/lib/chaos-experiment-common.sh
# owns that reasoning, along with the maven invocation and the outcome classifier every runner here
# shares, including the discipline that a run which executed no test is NOT a data point: zero tests
# looks identical to a pass in a rate.
set -u
# shellcheck source=bin/lib/chaos-experiment-common.sh
source "${BASH_SOURCE[0]%/*}/lib/chaos-experiment-common.sh"

D="$(git rev-parse --show-toplevel)"
OUT=/tmp/large-instances; mkdir -p "$OUT"
for i in $(seq 1 "${1:-10}"); do
    log="$OUT/run-$i.log"
    pc_run_performance "$D" 'MultiInstanceRebalanceTest#largeNumberOfInstances' "$log" \
        -Dpc.log.dir="$OUT/pc-logs-$i"
    stats=$(pc_failsafe_stats "$D" MultiInstanceRebalance)
    r=$(pc_classify_failsafe_stats "$stats")
    if [ "$r" = DID-NOT-RUN ]; then
        printf '%s\trun=%s\tDID-NOT-RUN\t%s\n' "$(pc_now)" "$i" "${stats:-no-report}" >> "$OUT/tally.tsv"
        continue
    fi
    progress=$(grep -ohE 'No progress beyond [0-9]+ records after [0-9]+ rounds' "$log" | tail -1)
    keys=$(grep -ohE 'missing keys: \[[^]]{0,70}' "$log" | tail -1)
    printf '%s\trun=%s\t%s\t%s\t%s\n' "$(pc_now)" "$i" "$r" "${progress:-no-progress-line}" \
        "${keys:-no-keys-line}" >> "$OUT/tally.tsv"
done
