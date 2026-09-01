#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Does the largeNumberOfInstances failure rate MOVE WITH SCALE?
#
# The experiment astubbs#a8b4e196e's own commit body proposes, and the one
# test-largenumberofinstances-residual-failures-unmeasured.md is about: is the residual failure the
# group coordinator failing to converge, or a PC defect?
#
#   rate RISES with scale  -> consistent with the coordinator struggling at size. Kafka's problem.
#   rate FLAT across scale -> hard to explain as "Kafka cannot converge at this size". Points at PC.
#   no failures at any scale -> the historical claim does not reproduce on this hardware at all,
#                               which is itself worth knowing before anyone chases it further.
#
# -Dperf.scale multiplies each capacity profile's own baseline, so proportions hold and one flag
# moves all three. Absent it is 1.0, which reproduces the historical numbers exactly - so scale 1 IS
# the configuration the January report was made against.
#
# Weaker than the bare-consumer control arm that note asks for, and does not replace it. It costs a
# flag instead of a harness.
#
# `-e` is deliberately omitted - a failing iteration is the data. bin/lib/chaos-experiment-common.sh
# owns that reasoning, along with the maven invocation and the outcome classifier every runner here
# shares.
set -u
# shellcheck source=bin/lib/chaos-experiment-common.sh
source "${BASH_SOURCE[0]%/*}/lib/chaos-experiment-common.sh"

D="$(git rev-parse --show-toplevel)"
OUT=/tmp/scale-sweep; mkdir -p "$OUT"
REPS="${1:-3}"
for scale in 1 2 4; do
  for i in $(seq 1 "$REPS"); do
    log="$OUT/s$scale-run-$i.log"
    pc_run_performance "$D" 'MultiInstanceRebalanceTest#largeNumberOfInstances' "$log" \
        -Dperf.scale="$scale" -Dpc.log.dir="$OUT/pc-logs-s$scale-$i"
    r=$(pc_failsafe_outcome "$D" MultiInstanceRebalance)
    if [ "$r" = DID-NOT-RUN ]; then
        printf '%s\tscale=%s\trun=%s\tDID-NOT-RUN\n' "$(pc_now)" "$scale" "$i" >> "$OUT/tally.tsv"
        continue
    fi
    printf '%s\tscale=%s\trun=%s\t%s\t%s\n' "$(pc_now)" "$scale" "$i" "$r" \
        "$(grep -ohE 'No progress beyond [0-9]+ records after [0-9]+ rounds' "$log" | tail -1)" >> "$OUT/tally.tsv"
  done
done
