#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Answer ONE question about the confluentinc#857 async stall: when it fires, does the backlog DRAIN
# or stay FLAT? Draining means a timing proxy, as the CLASS2_STALL line turned out to be. Flat means
# a real wedge on master, reproducible on demand - the family's fourth mechanism.
#
# THE STOPPING CONDITION IS THIS SIGNAL FIRING, NEVER AN ENGAGEMENT AND NEVER "SOMETHING FIRED".
# The diagnostic switching on proves the wiring and answers nothing; the seed reproduces most runs
# but not all, so a clean run is ordinary luck. An earlier attempt stopped on "diagnostic engaged"
# and reported no answer at all.
#
# And "something fired" is not this question either. `violations=` on the [diagnose] line is
# ProgressProbe's AGGREGATE count, so INSTANCE_STALL, ZOMBIE_MEMBER, DRAIN_OVERDUE or PROBE_DEGRADED
# each make it non-empty. Stopping on that writes ANSWER.txt from a trajectory that belongs to a
# different mechanism, under a heading that says it is the asynchronous stall - a wrong answer, filed
# confidently, in the one file anybody reads. So the run is accepted only when NO_PROGRESS itself
# fired; the aggregate reading is still recorded beside it, because a run where another detector
# fired first is worth seeing.
#
# Reads the siloed streams that docs/logging.md owns rather than the raw run log - probes.log is the
# detectors, so "did it fire" is a small file instead of a grep of tens of thousands of lines.
#
# `-e` is deliberately omitted - a failing iteration is the data. bin/lib/chaos-experiment-common.sh
# owns that reasoning, along with the maven invocation and the parsing every runner here shares.
set -u
# shellcheck source=bin/lib/chaos-experiment-common.sh
source "${BASH_SOURCE[0]%/*}/lib/chaos-experiment-common.sh"

SEED=9086872209853284830
D="$(git rev-parse --show-toplevel)"
OUT=/tmp/async-answer
mkdir -p "$OUT"
for i in $(seq 1 "${1:-10}"); do
    log="$OUT/run-$i.log"
    pc_run_chaos "$D" "$SEED" "$log" \
        -Dchaos.diagnoseStallRecovery=true -Dpc.log.dir="$OUT/pc-logs-run-$i"
    fired=$(pc_first_violation "$log")
    target=no
    pc_signal_fired NO_PROGRESS "$log" && target=yes
    printf '%s\trun=%s\tfired=%s\tno_progress=%s\tfinal=%s\n' "$(pc_now)" "$i" "${fired:-none}" \
        "$target" "$(grep -oE 'consumed=[0-9]+/[0-9]+' "$log" | tail -1)" >> "$OUT/tally.tsv"
    if [ "$target" = yes ]; then
        {   echo "NO_PROGRESS fired on run $i. Consumed trajectory from the first violation onward."
            echo "READ THE LAST ROW, NOT THE DIRECTION: done=true is DRAINED (a timing proxy);"
            echo "done=false with consumed short of its denominator is a WEDGE, whether or not the"
            echo "count climbed on the way there - a run that advanced once and then stopped is the"
            echo "finding, and 'it went up' cannot tell the two apart."
            pc_trajectory_after_violation "$log"
        } > "$OUT/ANSWER.txt"
        break
    fi
done
