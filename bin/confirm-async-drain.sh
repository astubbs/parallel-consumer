#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Does the async stall ALWAYS drain, or only that once?
#
# The line was demoted to a timing proxy on a SINGLE firing. The Class 2 demotion used two seeds.
# An earlier attempt to firm this up hunted for a second SEED and found none in eight tries - that
# was the wrong instrument. What the question needs is more FIRINGS, and the known seed already
# fires most runs. Collect every firing's recovery trajectory instead of looking for new seeds.
#
# ONE TREE ONLY unless the diagnostic is backported. The pre-astubbs#344 worktree predates the lift
# of the recovery diagnostic into ChaosScenarioBase, so the flag is accepted and does nothing there:
# its runs FAIL (errors=1 in the failsafe report) but emit no telemetry, and a grep for violations
# reads that as 'did not fire'. Comparing trees needs the lift backported first.
#
#   consumed climbing after the violation -> DRAINED. Timing proxy, as Class 2 turned out to be.
#   consumed flat, inFlight stuck         -> WEDGE. A real defect, and the family's fourth mechanism.
#
# A run that does not fire is not a data point - the diagnostic only engages on a violation.
#
# `-e` is deliberately omitted - a failing iteration is the data. bin/lib/chaos-experiment-common.sh
# owns that reasoning, along with the maven invocation and the parsing every runner here shares -
# including this script's own hard-won lesson about reading the two trajectory ends separately.
set -u
# shellcheck source=bin/lib/chaos-experiment-common.sh
source "${BASH_SOURCE[0]%/*}/lib/chaos-experiment-common.sh"

W="$(pc_worktree_root)"
T=/tmp/drain-confirm; mkdir -p "$T"
SEED=9086872209853284830
for tree in pr29 pre-344; do
  d="$W/$tree"
  [ -d "$d" ] || { printf '%s\t%s\tMISSING TREE\n' "$(pc_now)" "$tree" >> "$T/tally.tsv"; continue; }
  for i in 1 2 3 4 5; do
    lg="$T/$tree-$i.log"
    pc_run_chaos "$d" "$SEED" "$lg" -Dchaos.diagnoseStallRecovery=true
    fired=$(pc_first_violation "$lg")
    if [ -z "$fired" ]; then
      printf '%s\t%s\trun=%s\tdid-not-fire - NOT a data point\n' "$(pc_now)" "$tree" "$i" >> "$T/tally.tsv"
      continue
    fi
    pc_consumed_bounds "$lg"
    traj="${PC_CONSUMED_FIRST:-?}->${PC_CONSUMED_LAST:-?}"
    if [ -n "$PC_CONSUMED_FIRST" ] && [ -n "$PC_CONSUMED_LAST" ] \
        && [ "$PC_CONSUMED_LAST" -gt "$PC_CONSUMED_FIRST" ] 2>/dev/null; then
      v=DRAINED
    else
      v=FLAT-OR-UNCLEAR
    fi
    printf '%s\t%s\trun=%s\tFIRED\t%s\ttrajectory=%s\n' "$(pc_now)" "$tree" "$i" "$v" "$traj" >> "$T/tally.tsv"
  done
done
printf '%s\tDRAIN CONFIRMATION COMPLETE\n' "$(pc_now)" >> "$T/tally.tsv"
