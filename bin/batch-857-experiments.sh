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
set -u
W=/Users/astubbs/github/parallel-consumer/.claude/worktrees
D="$W/pr29"; PRE="$W/pre-344"
T=/tmp/batch857/tally.tsv; mkdir -p /tmp/batch857
J="${JAVA_HOME:-/Users/astubbs/.sdkman/candidates/java/17.0.18-tem}"
say() { printf '%s\t%s\n' "$(date -u +%FT%TZ)" "$*" >> "$T"; }

run_chaos() { # dir seed extra-args logfile
    JAVA_HOME="$J" "$1/mvnw" -f "$1/pom.xml" -Pci -pl parallel-consumer-core -am verify \
      -DskipUTs=true -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed="$2" \
      -Dit.test=ChaosChurnStormIT $3 -Dfailsafe.failIfNoSpecifiedTests=false \
      -Dcopyright.skip=true -Djacoco.skip=true > "$4" 2>&1
}
outcome() { # dir pattern -> passed|FAILED|DID-NOT-RUN
    local s; s=$(grep -ohE 'tests="[0-9]+" errors="[0-9]+" skipped="[0-9]+" failures="[0-9]+"' \
        "$1/parallel-consumer-core/target/failsafe-reports"/TEST-*"$2"*.xml 2>/dev/null | tail -1)
    printf '%s' "$s" | grep -q 'tests="[1-9]' || { echo DID-NOT-RUN; return; }
    printf '%s' "$s" | grep -qE 'errors="[1-9]|failures="[1-9]' && echo FAILED || echo passed
}

say "=== PHASE A: valid scale sweep, 0.25 vs 0.5 ==="
for sc in 0.25 0.5; do for i in 1 2 3; do
    lg=/tmp/batch857/A-s$sc-$i.log
    JAVA_HOME="$J" "$D/mvnw" -f "$D/pom.xml" -Pci -pl parallel-consumer-core -am verify \
      -DskipUTs=true -Dincluded.groups=performance -Dexcluded.groups= -Dperf.scale="$sc" \
      -Dit.test='MultiInstanceRebalanceTest#largeNumberOfInstances' \
      -Dfailsafe.failIfNoSpecifiedTests=false -Dcopyright.skip=true -Djacoco.skip=true > "$lg" 2>&1
    say "A\tscale=$sc\trun=$i\t$(outcome "$D" MultiInstanceRebalance)\t$(grep -ohE 'No progress beyond [0-9]+ records after [0-9]+ rounds' "$lg" | tail -1)"
done; done

say "=== PHASE B: hunt a SECOND async seed, then discriminate it ==="
FOUND=
for i in $(seq 1 8); do
    s=$(( (RANDOM<<15 | RANDOM) * 100003 + i ))
    lg=/tmp/batch857/B-hunt-$i.log
    run_chaos "$D" "$s" "" "$lg"
    o=$(outcome "$D" ChurnStorm)
    say "B-hunt\trun=$i\tseed=$s\t$o\tNO_PROGRESS=$(grep -c NO_PROGRESS "$lg" 2>/dev/null || echo 0)"
    [ "$o" = FAILED ] && { FOUND=$s; break; }
done
if [ -n "$FOUND" ]; then
    for i in 1 2 3 4; do
        lg=/tmp/batch857/B-diag-$i.log
        run_chaos "$D" "$FOUND" "-Dchaos.diagnoseStallRecovery=true" "$lg"
        fired=$(grep -oE 'violations=[1-9][0-9]*' "$lg" | head -1)
        say "B-diag\trun=$i\tseed=$FOUND\tfired=${fired:-none}\tfinal=$(grep -oE 'consumed=[0-9]+/[0-9]+' "$lg" | tail -1)"
        if [ -n "$fired" ]; then
            awk '/violations=[1-9]/{f=1} f' "$lg" | grep -oE 'consumed=[0-9]+/[0-9]+ started=[0-9]+ inFlight=[0-9]+' \
                > /tmp/batch857/B-ANSWER.txt
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
    run_chaos "$PRE" 9086872209853284830 "" "$lg"
    o=$(outcome "$PRE" ChurnStorm)
    if [ "$o" = FAILED ]; then
        np=$(grep -c NO_PROGRESS "$lg" 2>/dev/null || echo 0)
        oth=$(grep -cE 'ZOMBIE_MEMBER|INSTANCE_STALL|LEDGER_KEY_ORDER' "$lg" 2>/dev/null || echo 0)
        if   [ "$np"  -gt 0 ]; then v=no-progress
        elif [ "$oth" -gt 0 ]; then v=other-probe
        else v=LEDGER-ONLY-MISS-CASE; fi
        say "C\trun=$i\tFAILED\tverdict=$v\tno_progress=$np\tother=$oth"
    else
        say "C\trun=$i\t$o - not a data point"
    fi
done
say "=== BATCH COMPLETE ==="
