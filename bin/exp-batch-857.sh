#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Every outstanding confluentinc#857 experiment, in one unattended run, appending to one tally.
# Batched deliberately: the expensive resource is the operator's attention, not CPU, so the machine
# does three experiments while a human reads one file.
#
#   A  The scale sweep, done VALIDLY. The 2026-08-28 attempt compared 1 against 4 on a desktop whose
#      documented baseline is 0.25 - sixteen times its intended load - so it measured overload, and
#      the higher scales timed out without ever tripping the progress assertion. Comparing 0.25
#      against 0.5 holds relative load sane and asks the real question: does the rate move with scale?
#      Rising suggests the group coordinator; flat points back at PC.
#
#   B  A SECOND seed for the async drain result. That line was demoted to a timing proxy on ONE
#      firing; the Class 2 demotion used two. Hunt random seeds until one fires, then replay it with
#      the recovery diagnostic and see whether it drains too.
#
#   C  The detector-silence audit ON THE TREE WHERE IT WAS SEEN. The audit that found nothing ran on
#      this branch; the original observation was on the pre-astubbs#344 tree. One seed on one tree
#      does not refute a claim made about another.
#
# Every phase records what actually RAN, not just its exit code - a run that executed no test is
# marked and not counted. That discipline has changed the answer three times this week.
#
# `-e` is deliberately omitted - a failing iteration is the data. bin/lib/chaos-experiment-common.sh
# owns that reasoning, along with the maven invocation and the classifiers this script shares with
# its five single-experiment siblings, which it used to carry a second copy of.
set -u
# shellcheck source=bin/lib/chaos-experiment-common.sh
source "${BASH_SOURCE[0]%/*}/lib/chaos-experiment-common.sh"

W="$(pc_worktree_root)"
D="$W/pr29"; PRE="$W/pre-344"
T=/tmp/batch857/tally.tsv; mkdir -p /tmp/batch857
say() { printf '%s\t%s\n' "$(pc_now)" "$*" >> "$T"; }

say "=== PHASE A: valid scale sweep, 0.25 vs 0.5 ==="
for sc in 0.25 0.5; do for i in 1 2 3; do
    lg=/tmp/batch857/A-s$sc-$i.log
    pc_run_performance "$D" 'MultiInstanceRebalanceTest#largeNumberOfInstances' "$lg" -Dperf.scale="$sc"
    say "A\tscale=$sc\trun=$i\t$(pc_failsafe_outcome "$D" MultiInstanceRebalance)\t$(grep -ohE 'No progress beyond [0-9]+ records after [0-9]+ rounds' "$lg" | tail -1)"
done; done

say "=== PHASE B: hunt a SECOND async seed, then discriminate it ==="
FOUND=
for i in $(seq 1 8); do
    s=$(( (RANDOM<<15 | RANDOM) * 100003 + i ))
    lg=/tmp/batch857/B-hunt-$i.log
    pc_run_chaos "$D" "$s" "$lg"
    o=$(pc_failsafe_outcome "$D" ChurnStorm)
    say "B-hunt\trun=$i\tseed=$s\t$o\tNO_PROGRESS=$(pc_count_matches NO_PROGRESS "$lg")"
    [ "$o" = FAILED ] && { FOUND=$s; break; }
done
if [ -n "$FOUND" ]; then
    for i in 1 2 3 4; do
        lg=/tmp/batch857/B-diag-$i.log
        pc_run_chaos "$D" "$FOUND" "$lg" -Dchaos.diagnoseStallRecovery=true
        fired=$(pc_first_violation "$lg")
        say "B-diag\trun=$i\tseed=$FOUND\tfired=${fired:-none}\tfinal=$(grep -oE 'consumed=[0-9]+/[0-9]+' "$lg" | tail -1)"
        if [ -n "$fired" ]; then
            pc_trajectory_after_violation "$lg" > /tmp/batch857/B-ANSWER.txt
            say "B-diag\tFIRED - trajectory in /tmp/batch857/B-ANSWER.txt (climbing=DRAINED, flat=WEDGE)"
            break
        fi
    done
else
    say "B\tno second seed found in 8 hunts - not a data point either way"
fi

say "=== PHASE C: detector-silence audit on the PRE-astubbs#344 tree ==="
for i in 1 2 3 4 5 6; do
    lg=/tmp/batch857/C-$i.log
    pc_run_chaos "$PRE" 9086872209853284830 "$lg"
    o=$(pc_failsafe_outcome "$PRE" ChurnStorm)
    if [ "$o" = FAILED ]; then
        pc_detector_verdict "$lg"
        say "C\trun=$i\tFAILED\tverdict=$PC_VERDICT\tno_progress=$PC_NO_PROGRESS\tother=$PC_OTHER_PROBE"
    else
        say "C\trun=$i\t$o - not a data point"
    fi
done
say "=== BATCH COMPLETE ==="
