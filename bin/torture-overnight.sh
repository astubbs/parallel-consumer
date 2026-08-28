#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# OVERNIGHT TORTURE HARNESS - MVP spike. Runs for hours in short cycles, hunting the lock-ups and
# data-skips confluentinc#857's family has not yet accounted for, and packages everything an agent
# needs to review it in the morning without asking anyone a question.
#
# WHY CYCLES. A live-locked run tells you nothing while it hangs and everything the moment you look
# at it. Rather than teach the harness to detect every way PC can wedge, each cycle gets a hard
# wall-clock budget; when it expires the WATCHDOG TAKES A THREAD DUMP AND THEN KILLS IT. Restarting
# every 30 minutes costs almost nothing against an 8-hour run, and the dump is the artefact that
# turns a hang into a diagnosis - the six captures that identified the revoke deadlock exist only
# because something dumped the stack while it was still stuck.
#
# WHAT IT HUNTS, and why these first. The AB-BA revoke deadlock is fixed and verified. What remains
# unaccounted for in that family is:
#   * the UNBOUNDED REVOKE WAIT in transactional mode - the only issue upstream ever labelled a
#     verified bug (astubbs#44 / confluentinc#803). The chaos suite barely exercises
#     PERIODIC_TRANSACTIONAL_PRODUCER, so this rotation deliberately weights it.
#   * COMMIT-RESPONSE TIMEOUTS reported in the field twice and never reproduced (astubbs#175, astubbs#177).
#   * DATA SKIP - confluentinc#875 describes an offset silently never delivered, lag growing, and a
#     restart making it reappear. That is not a liveness failure and no liveness detector will see
#     it, so cycles assert delivery completeness, not just progress.
#
# NOT DOCKER YET. Deliberately: containers are the right long-term shape
# (docs/inflight/test-pc-soak-harness-architecture.md) but a script runs tonight.
#
# Usage:  bin/torture-overnight.sh [total-hours] [cycle-minutes]
#         bin/torture-overnight.sh 8 30
set -u
HOURS="${1:-8}"; CYCLE_MIN="${2:-30}"
D="$(git rev-parse --show-toplevel)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${TORTURE_OUT:-/tmp/torture-$STAMP}"
mkdir -p "$OUT"/{cycles,dumps}
TALLY="$OUT/tally.tsv"
J="${JAVA_HOME:-/Users/astubbs/.sdkman/candidates/java/17.0.18-tem}"
END=$(( $(date +%s) + HOURS*3600 ))
BUDGET=$(( CYCLE_MIN*60 ))

say() { printf '%s\t%s\n' "$(date -u +%FT%TZ)" "$*" >> "$TALLY"; }

# The rotation. Each entry is scenario + the commit mode it should exercise. Transactional is
# weighted because it holds the one open defect with a user report behind it.
ROTATION=(
  "ChaosChurnStormIT|PERIODIC_TRANSACTIONAL_PRODUCER"
  "ChaosRevokeUnderWorkIT|PERIODIC_TRANSACTIONAL_PRODUCER"
  "ChaosRevokeUnderWorkCooperativeIT|PERIODIC_CONSUMER_SYNC"
  "ChaosChurnStormIT|PERIODIC_CONSUMER_ASYNCHRONOUS"
  "ChaosRevokeUnderWorkDrainIT|PERIODIC_TRANSACTIONAL_PRODUCER"
  "ChaosKeyOrderIT|PERIODIC_CONSUMER_SYNC"
)

say "TORTURE START hours=$HOURS cycle=${CYCLE_MIN}m out=$OUT"
say "hunting: transactional revoke wait, commit-response timeouts, silent data skip"

cycle=0
while [ "$(date +%s)" -lt "$END" ]; do
    cycle=$((cycle+1))
    entry="${ROTATION[$(( (cycle-1) % ${#ROTATION[@]} ))]}"
    scenario="${entry%%|*}"; mode="${entry##*|}"
    seed=$(( (RANDOM<<15 | RANDOM) * 100003 + cycle ))
    cdir="$OUT/cycles/$cycle-$scenario-$mode"; mkdir -p "$cdir"
    log="$cdir/run.log"
    say "cycle=$cycle START scenario=$scenario mode=$mode seed=$seed"

    JAVA_HOME="$J" "$D/mvnw" -f "$D/pom.xml" -Pci -pl parallel-consumer-core -am verify \
        -DskipUTs=true -Dincluded.groups=chaos -Dexcluded.groups= \
        -Dchaos.seed="$seed" -Dit.test="$scenario" \
        -Dchaos.commitMode="$mode" \
        -Dpc.log.dir="$cdir/pc-logs" \
        -Dfailsafe.failIfNoSpecifiedTests=false -Dcopyright.skip=true -Djacoco.skip=true \
        > "$log" 2>&1 &
    mvn_pid=$!

    # WATCHDOG. Dump before killing - a hang with no stack is a rumour, a hang with a stack is a bug.
    waited=0; hung=no
    while kill -0 "$mvn_pid" 2>/dev/null; do
        sleep 15; waited=$((waited+15))
        if [ "$waited" -ge "$BUDGET" ]; then
            hung=yes
            say "cycle=$cycle BUDGET EXCEEDED - dumping every JVM before kill"
            for jp in $(pgrep -f 'surefire|failsafe|ChaosScenario' 2>/dev/null | head -12); do
                "$J/bin/jstack" -l "$jp" > "$OUT/dumps/cycle-$cycle-pid-$jp.txt" 2>/dev/null
            done
            pkill -P "$mvn_pid" 2>/dev/null; kill -9 "$mvn_pid" 2>/dev/null
            break
        fi
    done
    wait "$mvn_pid" 2>/dev/null; rc=$?

    rpt="$D/parallel-consumer-core/target/failsafe-reports"
    stats=$(grep -ohE 'tests="[0-9]+" errors="[0-9]+" skipped="[0-9]+" failures="[0-9]+"' \
              "$rpt"/TEST-*"${scenario%IT}"*.xml 2>/dev/null | tail -1)
    cp -r "$rpt" "$cdir/failsafe-reports" 2>/dev/null

    if [ "$hung" = yes ]; then verdict=HUNG-DUMP-CAPTURED
    elif ! printf '%s' "$stats" | grep -q 'tests="[1-9]'; then verdict=DID-NOT-RUN
    elif printf '%s' "$stats" | grep -qE 'errors="[1-9]|failures="[1-9]'; then verdict=FAILED
    else verdict=passed; fi

    # Signals worth surfacing in the tally itself, so the morning read does not need the logs.
    say "cycle=$cycle END $verdict rc=$rc seed=$seed$(printf '\t')$(
        printf 'timeouts=%s dupes=%s missing=%s deadlock=%s' \
          "$(grep -c 'Timeout waiting for commit response' "$log" 2>/dev/null || echo 0)" \
          "$(grep -c 'duplicate' "$log" 2>/dev/null || echo 0)" \
          "$(grep -c 'missing keys' "$log" 2>/dev/null || echo 0)" \
          "$(grep -cE 'BLOCKED|deadlock' "$log" 2>/dev/null || echo 0)")"
done

say "TORTURE COMPLETE cycles=$cycle"
{   echo "# Torture run $STAMP"
    echo "## Verdicts"; grep -oE 'END [A-Za-z-]+' "$TALLY" | sort | uniq -c
    echo "## Cycles with a thread dump (look here first)"; ls "$OUT/dumps" 2>/dev/null || echo none
    echo "## Full tally"; cat "$TALLY"
} > "$OUT/SUMMARY.md"
tar -czf "$OUT.tar.gz" -C "$(dirname "$OUT")" "$(basename "$OUT")" 2>/dev/null
say "packaged: $OUT.tar.gz"
