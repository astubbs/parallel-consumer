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
# it. A run that passes is not a data point here - only failures carry the question.
#
#   no-progress   - the detector fired. Working as intended.
#   other-probe   - a different detector caught it. Not a miss; a different signature.
#   ledger-only   - the end-of-run correctness ledger failed with NO detector firing. THE MISS CASE:
#                   something went wrong and the liveness detectors said nothing.
#   unclassified  - read it by hand; the classifier is not the oracle.
#
# Reads the siloed probes.log (docs/logging.md) so "which detector fired" is a small file.
set -u
SEED=9086872209853284830
D="$(git rev-parse --show-toplevel)"
OUT=/tmp/detector-audit; mkdir -p "$OUT"
for i in $(seq 1 "${1:-8}"); do
    log="$OUT/run-$i.log"; pcl="$OUT/pc-logs-$i"
    JAVA_HOME="${JAVA_HOME:-/Users/astubbs/.sdkman/candidates/java/17.0.18-tem}" \
      "$D/mvnw" -f "$D/pom.xml" -Pci -pl parallel-consumer-core -am verify -DskipUTs=true \
        -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed="$SEED" \
        -Dit.test=ChaosChurnStormIT -Dpc.log.dir="$pcl" \
        -Dfailsafe.failIfNoSpecifiedTests=false -Dcopyright.skip=true -Djacoco.skip=true \
        > "$log" 2>&1
    rpt="$D/parallel-consumer-core/target/failsafe-reports"
    stats=$(grep -ohE 'errors="[0-9]+" skipped="[0-9]+" failures="[0-9]+"' \
              "$rpt"/TEST-*ChurnStorm*.xml 2>/dev/null | tail -1)
    if ! printf '%s' "$stats" | grep -qE 'errors="[1-9]|failures="[1-9]'; then
        printf '%s\trun=%s\tPASSED - not a data point\n' "$(date -u +%FT%TZ)" "$i" >> "$OUT/tally.tsv"
        continue
    fi
    np=$(grep -c 'NO_PROGRESS' "$log" 2>/dev/null || echo 0)
    other=$(grep -cE 'ZOMBIE_MEMBER|INSTANCE_STALL|LEDGER_KEY_ORDER' "$log" 2>/dev/null || echo 0)
    if   [ "$np"    -gt 0 ]; then verdict=no-progress
    elif [ "$other" -gt 0 ]; then verdict=other-probe
    else verdict=LEDGER-ONLY-MISS-CASE; fi
    printf '%s\trun=%s\tFAILED\tverdict=%s\tno_progress=%s\tother_probe=%s\tprobes_log=%s\n' \
        "$(date -u +%FT%TZ)" "$i" "$verdict" "$np" "$other" "$pcl/probes.log" >> "$OUT/tally.tsv"
done
