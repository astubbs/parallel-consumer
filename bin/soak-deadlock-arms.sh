#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Overnight A/B soak for the confluentinc#857 revoke-path deadlock, run UNATTENDED.
#
# WHY IT WRITES A TALLY AND NOTHING ELSE. The expensive thing about a soak driven from an agent
# session is not the compute, it is being watched: every progress check is a round trip, and a
# 12-hour soak polled every few minutes costs more than the experiment is worth. So this prints one
# line per rep to a tally file and is silent otherwise. Read the tally once, at the end.
#
# ARMS ARE INTERLEAVED, A,B,A,B, deliberately - the same discipline the 2026-08-18 60/60 soak used.
# Run all of A then all of B and the two arms sit in systematically different box conditions
# (thermal, page cache, whatever else is running), and the difference between them is no longer the
# one term under test.
#
#   FIXED   - the PR branch, revoke side declines with tryLock()
#   CONTROL - identical tree, one term changed: the revoke side BLOCKS on the same lock
#
# Usage:  bin/soak-deadlock-arms.sh <reps> <seed> <fixed-worktree> <control-worktree> <tally-file>
set -euo pipefail

REPS="${1:?reps}"; SEED="${2:?seed}"; FIXED="${3:?fixed worktree}"
CONTROL="${4:?control worktree}"; TALLY="${5:?tally file}"
JDK="${JAVA_HOME_OVERRIDE:-/Users/astubbs/.sdkman/candidates/java/17.0.18-tem}"

run_one() {
    local arm="$1" dir="$2" rep="$3" log verdict blocked observations
    log="$(mktemp)"
    if JAVA_HOME="$JDK" "$dir/mvnw" -f "$dir/pom.xml" -Pci -pl parallel-consumer-core -am verify \
         -DskipUTs=true -Dincluded.groups=chaos -Dexcluded.groups= \
         -Dchaos.seed="$SEED" -Dit.test=ChaosRevokeUnderWorkCooperativeIT \
         -Dfailsafe.failIfNoSpecifiedTests=false -Dcopyright.skip=true -Djacoco.skip=true \
         > "$log" 2>&1; then verdict=PASS; else verdict=FAIL; fi

    # The discriminator, not the exit code: the captured signature is the poll thread parked or
    # blocked inside the rebalance callback. Counted from the run's own log so a green that still
    # showed the signature cannot be read as a clean arm.
    # WINDOW FIRST, VERDICT SECOND. A rep where the revoke-with-pending-commit path never ran is
    # NOT a data point: both arms go green and the pass rate is manufactured. This was demonstrated
    # on 2026-08-27 - fixed and control arms both green on the same seed, with zero executions of
    # either branch of tryCommitOffsetsOnRevoke(). Count it, do not average it away.
    window=$(grep -cE 'Skipping offset commit during partition revocation|Acquired commitLock on revoke' "$log" || true)
    blocked=$(grep -cE 'BLOCKED|parked to wait for' "$log" || true)
    observations=$(grep -c 'OBSERVATION (does not fail the run)' "$log" || true)
    [ "$window" -eq 0 ] && verdict="NO-WINDOW"
    printf '%s\trep=%s\tarm=%-7s\tverdict=%-9s\twindow=%s\tblocked=%s\tclass2=%s\tlog=%s\n' \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$rep" "$arm" "$verdict" "$window" "$blocked" "$observations" "$log" >> "$TALLY"
}

printf '# soak start %s  seed=%s  reps=%s per arm, INTERLEAVED\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$SEED" "$REPS" >> "$TALLY"
for rep in $(seq 1 "$REPS"); do
    run_one FIXED   "$FIXED"   "$rep"
    run_one CONTROL "$CONTROL" "$rep"
done
printf '# soak done %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$TALLY"
