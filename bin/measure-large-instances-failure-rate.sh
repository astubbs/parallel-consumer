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
set -u
D="$(git rev-parse --show-toplevel)"
OUT=/tmp/large-instances; mkdir -p "$OUT"
for i in $(seq 1 "${1:-10}"); do
    log="$OUT/run-$i.log"
    JAVA_HOME="${JAVA_HOME:-/Users/astubbs/.sdkman/candidates/java/17.0.18-tem}" \
      "$D/mvnw" -f "$D/pom.xml" -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
        -Dincluded.groups=performance -Dexcluded.groups= \
        -Dit.test='MultiInstanceRebalanceTest#largeNumberOfInstances' \
        -Dpc.log.dir="$OUT/pc-logs-$i" \
        -Dfailsafe.failIfNoSpecifiedTests=false -Dcopyright.skip=true -Djacoco.skip=true \
        > "$log" 2>&1
    stats=$(grep -ohE 'tests="[0-9]+" errors="[0-9]+" skipped="[0-9]+" failures="[0-9]+"' \
              "$D/parallel-consumer-core/target/failsafe-reports"/TEST-*MultiInstanceRebalance*.xml \
              2>/dev/null | tail -1)
    # A run that did not execute the test is NOT a data point - the same discipline every other
    # experiment here needed. Zero tests looks identical to a pass in a rate.
    if ! printf '%s' "$stats" | grep -q 'tests="[1-9]'; then
        printf '%s\trun=%s\tDID-NOT-RUN\t%s\n' "$(date -u +%FT%TZ)" "$i" "${stats:-no-report}" >> "$OUT/tally.tsv"
        continue
    fi
    progress=$(grep -ohE 'No progress beyond [0-9]+ records after [0-9]+ rounds' "$log" | tail -1)
    keys=$(grep -ohE 'missing keys: \[[^]]{0,70}' "$log" | tail -1)
    if printf '%s' "$stats" | grep -qE 'errors="[1-9]|failures="[1-9]'; then r=FAILED; else r=passed; fi
    printf '%s\trun=%s\t%s\t%s\t%s\n' "$(date -u +%FT%TZ)" "$i" "$r" "${progress:-no-progress-line}" \
        "${keys:-no-keys-line}" >> "$OUT/tally.tsv"
done
