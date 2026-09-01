#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Does the async stall ALWAYS drain, or only in the firings collected so far?
#
# WHAT IS ALREADY KNOWN, so a result here can be recognised as new: the demotion to a timing proxy
# was made on a SINGLE firing, and ChaosChurnStormIT's "Calibration status" javadoc now records six
# firings that all drained. That javadoc is the standing answer; this script exists to widen it -
# more firings, and eventually a second tree - not to re-derive it. A run that drains reproduces a
# known result. A run that stays FLAT, or that advances and then stops short of the backlog, is the
# finding worth reporting.
#
# ONE TREE ONLY unless the diagnostic is backported. The pre-astubbs#344 worktree predates the lift
# of the recovery diagnostic into ChaosScenarioBase, so the flag is accepted and does nothing there:
# its runs FAIL (errors=1 in the failsafe report) but emit no telemetry, and a grep for violations
# reads that as 'did not fire'. Comparing trees needs the lift backported first.
#
#   the scenario's own done=true          -> DRAINED. Timing proxy, as Class 2 turned out to be.
#   consumed advanced, never completed    -> PARTIAL-THEN-STOPPED. A partial-progress wedge.
#   consumed flat, inFlight stuck         -> FLAT. A real defect, and the family's fourth mechanism.
#
# THE VERDICT IS THE COMPLETION FLAG, NOT THE DIRECTION. "Consumed went up after the violation" is
# also what a run that advanced once and then wedged short of the backlog looks like, and calling
# that DRAINED reports the defect this script hunts as absent. pc_consumed_bounds carries the
# denominator and the scenario's own done flag for exactly this.
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
TREES=(pr29 pre-344)
# Up front, before a single maven run, and FATAL rather than a tally row. This is a comparison, so a
# missing arm does not make it a smaller experiment - it makes it a different one, and the row this
# used to write ("MISSING TREE") went into an artifact alongside an exit 0 and a green job. Local
# only, deliberately: these sibling trees exist where somebody cut them, which is why the dispatch
# workflow no longer offers this runner.
for tree in "${TREES[@]}"; do pc_require_tree "$W/$tree" "$tree"; done
for tree in "${TREES[@]}"; do
  d="$W/$tree"
  for i in 1 2 3 4 5; do
    lg="$T/$tree-$i.log"
    pc_run_chaos "$d" "$SEED" "$lg" -Dchaos.diagnoseStallRecovery=true
    # THE TARGET SIGNAL, not "a violation". `fired` is the probe's aggregate count, so a run caught
    # by INSTANCE_STALL or DRAIN_OVERDUE makes it non-empty and its recovery trajectory would be
    # written up as the ASYNC stall's - a different mechanism, filed under this question's name. Both
    # are recorded; only NO_PROGRESS makes the run a data point here.
    fired=$(pc_first_violation "$lg")
    if ! pc_signal_fired NO_PROGRESS "$lg"; then
      printf '%s\t%s\trun=%s\tno NO_PROGRESS (other violations=%s) - NOT a data point\n' \
        "$(pc_now)" "$tree" "$i" "${fired:-none}" >> "$T/tally.tsv"
      continue
    fi
    pc_consumed_bounds "$lg"
    traj="${PC_CONSUMED_FIRST:-?}->${PC_CONSUMED_LAST:-?}/${PC_CONSUMED_EXPECTED:-?}"
    if [ "$PC_DIAGNOSTIC_DONE" = true ]; then
      v=DRAINED
    elif [ -n "$PC_CONSUMED_FIRST" ] && [ -n "$PC_CONSUMED_LAST" ] \
        && [ "$PC_CONSUMED_LAST" -gt "$PC_CONSUMED_FIRST" ] 2>/dev/null; then
      # advanced but never completed the backlog - the partial-progress wedge, reported as its own
      # verdict rather than folded into either of the two the question was framed around
      v=PARTIAL-THEN-STOPPED
    elif [ -n "$PC_CONSUMED_LAST" ]; then
      v=FLAT
    else
      v=UNCLEAR
    fi
    printf '%s\t%s\trun=%s\tFIRED\t%s\ttrajectory=%s\n' "$(pc_now)" "$tree" "$i" "$v" "$traj" >> "$T/tally.tsv"
  done
done
printf '%s\tDRAIN CONFIRMATION COMPLETE\n' "$(pc_now)" >> "$T/tally.tsv"
