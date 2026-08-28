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
# Runs on both trees, because a drain on one says nothing about the other and the pre-astubbs#344
# tree is where five of six runs failed.
#
#   consumed climbing after the violation -> DRAINED. Timing proxy, as Class 2 turned out to be.
#   consumed flat, inFlight stuck         -> WEDGE. A real defect, and the family's fourth mechanism.
#
# A run that does not fire is not a data point - the diagnostic only engages on a violation.
set -u
W=/Users/astubbs/github/parallel-consumer/.claude/worktrees
T=/tmp/drain-confirm; mkdir -p "$T"
J="${JAVA_HOME:-/Users/astubbs/.sdkman/candidates/java/17.0.18-tem}"
SEED=9086872209853284830
for tree in pr29 pre-344; do
  d="$W/$tree"
  [ -d "$d" ] || { printf '%s\t%s\tMISSING TREE\n' "$(date -u +%FT%TZ)" "$tree" >> "$T/tally.tsv"; continue; }
  for i in 1 2 3 4 5; do
    lg="$T/$tree-$i.log"
    JAVA_HOME="$J" "$d/mvnw" -f "$d/pom.xml" -Pci -pl parallel-consumer-core -am verify \
      -DskipUTs=true -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed="$SEED" \
      -Dit.test=ChaosChurnStormIT -Dchaos.diagnoseStallRecovery=true \
      -Dfailsafe.failIfNoSpecifiedTests=false -Dcopyright.skip=true -Djacoco.skip=true > "$lg" 2>&1
    fired=$(grep -oE 'violations=[1-9][0-9]*' "$lg" | head -1)
    if [ -z "$fired" ]; then
      printf '%s\t%s\trun=%s\tdid-not-fire - NOT a data point\n' "$(date -u +%FT%TZ)" "$tree" "$i" >> "$T/tally.tsv"
      continue
    fi
    # First and last consumed reading after the violation. Climbing = drained.
    traj=$(awk '/violations=[1-9]/{f=1} f' "$lg" | grep -oE 'consumed=[0-9]+' | sed -n '1p;$p' | paste -sd'->' -)
    first=$(printf '%s' "$traj" | sed 's/->.*//;s/consumed=//')
    last=$(printf '%s'  "$traj" | sed 's/.*->//;s/consumed=//')
    if [ -n "$first" ] && [ -n "$last" ] && [ "$last" -gt "$first" ] 2>/dev/null; then v=DRAINED; else v=FLAT-OR-UNCLEAR; fi
    printf '%s\t%s\trun=%s\tFIRED\t%s\ttrajectory=%s\n' "$(date -u +%FT%TZ)" "$tree" "$i" "$v" "$traj" >> "$T/tally.tsv"
  done
done
printf '%s\tDRAIN CONFIRMATION COMPLETE\n' "$(date -u +%FT%TZ)" >> "$T/tally.tsv"
