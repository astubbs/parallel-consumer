#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Answer ONE question about the confluentinc#857 async stall: when it fires, does the backlog DRAIN
# or stay FLAT? Draining means a timing proxy, as the CLASS2_STALL line turned out to be. Flat means
# a real wedge on master, reproducible on demand - the family's fourth mechanism.
#
# THE STOPPING CONDITION IS A VIOLATION, NEVER AN ENGAGEMENT. The diagnostic switching on proves the
# wiring and answers nothing; the seed reproduces most runs but not all, so a clean run is ordinary
# luck. An earlier attempt stopped on "diagnostic engaged" and reported no answer at all.
#
# Reads the siloed streams that docs/logging.md owns rather than the raw run log - probes.log is the
# detectors, so "did it fire" is a small file instead of a grep of tens of thousands of lines.
#
# `-e` is deliberately omitted - a failing iteration is the data. bin/lib/chaos-experiment-common.sh
# owns that reasoning, along with the maven invocation and the parsing every runner here shares.
set -u
# shellcheck source=bin/lib/chaos-experiment-common.sh
source "${BASH_SOURCE[0]%/*}/lib/chaos-experiment-common.sh"

SEED=9086872209853284830
D="$(git rev-parse --show-toplevel)"
OUT=/tmp/async-answer
mkdir -p "$OUT"
for i in $(seq 1 "${1:-10}"); do
    log="$OUT/run-$i.log"
    pc_run_chaos "$D" "$SEED" "$log" \
        -Dchaos.diagnoseStallRecovery=true -Dpc.log.dir="$OUT/pc-logs-run-$i"
    fired=$(pc_first_violation "$log")
    printf '%s\trun=%s\tfired=%s\tfinal=%s\n' "$(pc_now)" "$i" "${fired:-none}" \
        "$(grep -oE 'consumed=[0-9]+/[0-9]+' "$log" | tail -1)" >> "$OUT/tally.tsv"
    if [ -n "$fired" ]; then
        {   echo "FIRED on run $i. Consumed trajectory from the violation onward -"
            echo "climbing means it DRAINED (timing proxy); flat means a real WEDGE."
            pc_trajectory_after_violation "$log"
        } > "$OUT/ANSWER.txt"
        break
    fi
done
