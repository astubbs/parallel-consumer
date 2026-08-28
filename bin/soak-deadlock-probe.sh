#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Overnight A/B soak on the DETERMINISTIC deadlock probe. docs/inflight/test-857-deadlock-ab-soak-harness.md
# owns the method, the two settings that silently destroy the experiment, and the results.
#
# KNOWN DEFECT: the declines==0 window gate is right for the FIXED arm and WRONG for the CONTROL arm,
# where a blocking revoke never declines - it deadlocks - so its window-evidence is failures and
# timeouts instead. Fix before reusing on a marginal result.
#
# Overnight A/B soak on the DETERMINISTIC deadlock probe (not chaos seeds - those never open the
# window). One line per invocation; each invocation is @RepeatedTest(20). No -Pci: that sets
# surefire.forkCount=1C and forking removes the window the probe exists to open.
set -u
FIXED=/Users/astubbs/github/parallel-consumer/.claude/worktrees/pr29
CONTROL=/Users/astubbs/github/parallel-consumer/.claude/worktrees/deadlock-control
TALLY=/tmp/probe-soak.tsv
JDK=/Users/astubbs/.sdkman/candidates/java/17.0.18-tem
printf '# probe soak start %s\n' "$(date -u +%FT%TZ)" >> "$TALLY"
for i in $(seq 1 12); do
  for arm in FIXED CONTROL; do
    [ "$arm" = FIXED ] && dir=$FIXED || dir=$CONTROL
    log=$(mktemp)
    JAVA_HOME="$JDK" "$dir/mvnw" -f "$dir/pom.xml" -q -pl parallel-consumer-core -am \
      -DskipUTs=true -Dit.test=Rebalance857CommitSyncDeadlockProbeIT \
      -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false verify > "$log" 2>&1
      rc=$?
    rpt="$dir/parallel-consumer-core/target/failsafe-reports"
    stats=$(grep -hoE 'tests="[0-9]+" errors="[0-9]+" skipped="[0-9]+" failures="[0-9]+"' \
              "$rpt"/TEST-*Rebalance857*.xml 2>/dev/null | tail -1)
    decl=$(grep -c 'Skipping offset commit during partition revocation' "$log" 2>/dev/null || echo 0)
    tmo=$(grep -c 'Timeout waiting for commit response' "$log" 2>/dev/null || echo 0)
    # A run with zero declines on the FIXED arm did not open the window and is NOT a data point.
    printf '%s\tinv=%s\tarm=%-7s\trc=%s\t%s\tdeclines=%s\ttimeouts=%s\n' \
      "$(date -u +%FT%TZ)" "$i" "$arm" "$rc" "$stats" "$decl" "$tmo" >> "$TALLY"
  done
done
printf '# probe soak done %s\n' "$(date -u +%FT%TZ)" >> "$TALLY"
