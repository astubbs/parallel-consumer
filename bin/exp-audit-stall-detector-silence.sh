#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Does the NO_PROGRESS detector MISS real failures? Observed 2026-08-28: across the astubbs#344 arms,
# a third of the failing runs went red with NO_PROGRESS not firing at all. Either the seed produces a
# second signature, or the detector is missing occurrences it should catch.
#
# It matters because the 2026-08-25 Class 2 demotion left the gating liveness claim resting on this
# detector and INSTANCE_STALL. A detector that over-fires on slow runs AND stays quiet on real ones is
# worse than one that is absent, because the suite reports green on its say-so.
#
# METHOD: replay the reproducing seed N times and CLASSIFY every failing run by what actually caught
# it. A run that passes is not a data point here - only failures carry the question, and neither is a
# run that executed no test at all.
#
# These are the verdicts the classifier actually emits - grep pc_detector_verdict in
# bin/lib/chaos-experiment-common.sh if you are changing either side, because nothing checks that
# this table and that function agree:
#
#   no-progress          - the detector fired. Working as intended.
#   other-probe          - a different detector caught it. Not a miss; a different signature.
#   LEDGER-ONLY-MISS-CASE - the end-of-run correctness ledger failed with NO detector firing. THE
#                          MISS CASE: something went wrong and the liveness detectors said nothing.
#
# There is deliberately no `unclassified` verdict. This table used to list one and the classifier
# has never emitted it, which is worse than a missing row: a reader who never sees it concludes the
# corpus was clean rather than that the bucket does not exist. Every failing run lands in one of the
# three above - and the counts are printed beside the verdict so it can be re-checked by hand, which
# is the job the phantom bucket appeared to be doing.
#
# Reads the siloed probes.log (docs/logging.md) so "which detector fired" is a small file.
#
# `-e` is deliberately omitted - a failing iteration is the data. bin/lib/chaos-experiment-common.sh
# owns that reasoning, along with the maven invocation and the classifiers every runner here shares.
set -u
# shellcheck source=bin/lib/chaos-experiment-common.sh
source "${BASH_SOURCE[0]%/*}/lib/chaos-experiment-common.sh"

SEED=9086872209853284830
D="$(git rev-parse --show-toplevel)"
OUT=/tmp/detector-audit; mkdir -p "$OUT"
for i in $(seq 1 "${1:-8}"); do
    log="$OUT/run-$i.log"; pcl="$OUT/pc-logs-$i"
    pc_run_chaos "$D" "$SEED" "$log" -Dpc.log.dir="$pcl"
    outcome=$(pc_failsafe_outcome "$D" ChurnStorm)
    if [ "$outcome" != FAILED ]; then
        printf '%s\trun=%s\t%s - not a data point\n' "$(pc_now)" "$i" "$outcome" >> "$OUT/tally.tsv"
        continue
    fi
    pc_detector_verdict "$log"
    printf '%s\trun=%s\tFAILED\tverdict=%s\tno_progress=%s\tother_probe=%s\tprobes_log=%s\n' \
        "$(pc_now)" "$i" "$PC_VERDICT" "$PC_NO_PROGRESS" "$PC_OTHER_PROBE" "$pcl/probes.log" >> "$OUT/tally.tsv"
done
