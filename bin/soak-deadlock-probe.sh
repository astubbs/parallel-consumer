#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# shell-justified: a loop around mvn invocations whose entire content is the exact flag set - no
# -Pci, no forking - that keeps the probe's window open. The value is in those flags being visible
# and copy-pasteable next to the note that explains them; wrapping them in Node would put a layer
# between the reader and the command line they need to reproduce by hand.
#
# Overnight A/B soak on the DETERMINISTIC deadlock probe (not chaos seeds - those never open the
# window). Each invocation is @RepeatedTest(20); one tally row per invocation per arm.
# docs/inflight/test-857-deadlock-ab-soak-harness.md owns the method and the results.
#
# TWO SETTINGS SILENTLY DESTROY THIS EXPERIMENT, which is why the mvn line is spelled out here
# rather than left to a wrapper:
#   * -Pci sets surefire.forkCount=1C, and forking removes the window the probe exists to open.
#   * A JDK other than 17 fails in a module this does not touch.
#
# THE WINDOW GATE IS PER ARM, and getting it uniform is the defect this script carried until
# 2026-09-02. "Zero declines means the window never opened" is right for the FIXED arm and exactly
# WRONG for the CONTROL arm: a blocking revoke never reaches the decline, it deadlocks, so on that
# arm zero declines with a FAILURE is the window opening, and zero declines with a PASS is the run
# that proves nothing. Scoring both arms by declines alone throws away every real control
# observation and keeps the empty ones - the inversion this whole probe exists to avoid.
#
# `set -e` is deliberately omitted: a failing iteration is the data.
set -u
# shellcheck source=bin/lib/chaos-experiment-common.sh
source "${BASH_SOURCE[0]%/*}/lib/chaos-experiment-common.sh"

usage() {
    cat >&2 <<USAGE
usage: ${0##*/} FIXED_TREE CONTROL_TREE [INVOCATIONS]

  FIXED_TREE     worktree carrying the fix
  CONTROL_TREE   worktree with the fix reverted - the arm that must deadlock
  INVOCATIONS    A/B pairs to run (default 12)

Both trees are required and are NOT defaulted: this script used to hardcode two absolute paths on
one machine, one of them a throwaway experiment worktree, so it silently measured nothing anywhere
else. JAVA_HOME must point at a JDK 17.
USAGE
    exit 2
}

[ $# -ge 2 ] || usage
FIXED=$1
CONTROL=$2
ROUNDS=${3:-12}
TALLY=${PC_SOAK_TALLY:-/tmp/probe-soak.tsv}

for d in "$FIXED" "$CONTROL"; do
    [ -x "$d/mvnw" ] || { echo "not a usable tree (no mvnw): $d" >&2; exit 2; }
done
[ -n "${JAVA_HOME:-}" ] || { echo "JAVA_HOME must be set to a JDK 17" >&2; exit 2; }

printf '# probe soak start %s  fixed=%s control=%s rounds=%s\n' \
    "$(pc_now)" "$FIXED" "$CONTROL" "$ROUNDS" >> "$TALLY"

for i in $(seq 1 "$ROUNDS"); do
    for arm in FIXED CONTROL; do
        [ "$arm" = FIXED ] && dir=$FIXED || dir=$CONTROL
        log=$(mktemp)
        "$dir/mvnw" -f "$dir/pom.xml" -q -pl parallel-consumer-core -am \
            -DskipUTs=true -Dit.test=Rebalance857CommitSyncDeadlockProbeIT \
            -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false verify > "$log" 2>&1
        rc=$?

        stats=$(pc_failsafe_stats "$dir" Rebalance857)
        outcome=$(pc_classify_failsafe_stats "$stats")
        declines=$(pc_count_matches 'Skipping offset commit during partition revocation' "$log")
        timeouts=$(pc_count_matches 'Timeout waiting for commit response' "$log")

        # Per-arm window gate - see the header. USABLE means the run is evidence either way; EMPTY
        # means the window never opened and the row must not be counted in a rate.
        if [ "$outcome" = DID-NOT-RUN ]; then
            window=DID-NOT-RUN
        elif [ "$arm" = FIXED ]; then
            [ "$declines" -gt 0 ] && window=USABLE || window=EMPTY
        else
            { [ "$outcome" = FAILED ] || [ "$timeouts" -gt 0 ]; } && window=USABLE || window=EMPTY
        fi

        printf '%s\tinv=%s\tarm=%-7s\trc=%s\t%s\toutcome=%s\twindow=%s\tdeclines=%s\ttimeouts=%s\n' \
            "$(pc_now)" "$i" "$arm" "$rc" "$stats" "$outcome" "$window" "$declines" "$timeouts" \
            >> "$TALLY"
        rm -f "$log"
    done
done
printf '# probe soak done %s\n' "$(pc_now)" >> "$TALLY"
