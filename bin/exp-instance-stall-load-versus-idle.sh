#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# shell-justified: a loop around pc_run_chaos plus the burners from bin/lib/cpu-load.sh, both of
# which are shell libraries this repo's other 857 experiment runners already source. A Node port
# would have to re-implement the maven invocation, the failsafe classifier and the burner job
# control that bin/lib/chaos-experiment-common.sh and bin/lib/cpu-load.sh already own.
#
# THE QUESTION. Every INSTANCE_STALL and ZOMBIE_MEMBER sighting on ChaosChurnStormIT has replayed
# CLEAN on an idle box, which reads as starvation rather than a wedge - but "clean on idle" is the
# WEAK direction of the claim. A load-dependent stall by definition needs the load, so a green idle
# replay is consistent with both readings and settles neither. This runner supplies the missing arm:
# the same seed, the same tree, differing by exactly ONE term - whether CPU burners are running.
#
# THE STOPPING CONDITION, and it is not "a green run".
#
#   - starvation: the LOADED arm stretches the run's tail until the accused member's frozen stretch
#     outlasts the bound, and the detector reports it NON-GATING as INSTANCE_BUSY_IN_USER_CODE with
#     its workers in the scenario's heavy dwell. The idle arm stays clean.
#   - a wedge: some arm dumps an accused member holding work with NO worker in user code, and
#     INSTANCE_STALL/NO_WORK_COMPLETED fires. That is a claim about PC's control loop, in either arm.
#
# WHY THE DUMP THRESHOLD IS LOWERED ON EVERY RUN, and it is the whole reason this runner exists
# rather than a bare mvn line. InstanceStallDetector's dump defaults to the 150s bound, so a run
# whose freeze ends before the bound prints NOTHING and its green is VACUOUS - indistinguishable
# from a run where the window never opened. -Dchaos.instanceStallDumpAfterSeconds=20 makes the
# window's opening observable, so "the discriminator armed" is a fact in the log rather than an
# assumption. astubbs/parallel-consumer#485 is the worked incident: a clean replay banked as evidence
# when the mechanism had never executed.
#
# WHY maxInstanceStall READS 0ms ON A HEALTHY LOADED RUN, which looks like a broken instrument and
# is not. InstanceStallDetector only advances peakInstanceStallMs on the nobody-in-user-code branch;
# a busy member re-arms the clock every sample. So 0ms means "no member was ever frozen with all its
# workers idle", which IS the starvation signature rather than an absence of data. Read the dumps
# and the INSTANCE_BUSY_IN_USER_CODE observations, never the peak alone.
#
# AMBIENT LOAD IS NOT ZERO on a shared desktop - several agent sessions build against this box at
# once - so every row carries the loadavg read beside the run. The arm names below say what THIS
# script started, never what the machine was doing; only the reading says that.
#
# Usage:
#   bin/exp-instance-stall-load-versus-idle.sh <seed> [<seed>...]
#
#   EXP_FREE_CORES=N   cores left free in the LOADED arm (default 2, matching the hosted runner)
#   EXP_OUT=path       where per-run logs and the tally go (default a mktemp dir, printed at the end)
#
# Each seed gets both arms, idle first, so a machine that gets busier during the experiment biases
# AGAINST the finding rather than toward it.

set -u

# shellcheck source=bin/lib/chaos-experiment-common.sh
source "${BASH_SOURCE[0]%/*}/lib/chaos-experiment-common.sh"
# shellcheck source=bin/lib/cpu-load.sh
source "${BASH_SOURCE[0]%/*}/lib/cpu-load.sh"

# `-e` is deliberately omitted, as in every runner sourcing that lib: a fired probe violation is the
# data, not a script error, and each run is classified from the failsafe XML rather than maven's
# exit code. The burners still must never outlive the script.
trap 'pc_cpu_load_stop' EXIT INT TERM

[ "$#" -ge 1 ] || { echo "usage: $0 <seed> [<seed>...]" >&2; exit 2; }

TREE="$(cd "${BASH_SOURCE[0]%/*}/.." && pwd)"
FREE_CORES="${EXP_FREE_CORES:-2}"
OUT="${EXP_OUT:-$(mktemp -d -t instance-stall-load.XXXXXX)}"
mkdir -p "$OUT"
TALLY="$OUT/tally.tsv"

echo "EXP: tree ${TREE}"
echo "EXP: $(pc_cpu_count) cores; loaded arm leaves ~${FREE_CORES} free; logs in ${OUT}"
echo "EXP: ambient load before any burner: $(pc_cpu_loadavg)"

# Did the window open? The early dump is the only line that proves the detector looked at an accused
# member's threads, and its BUSY/STALL flavour is the discriminator itself.
exp_discriminator() { # run-log -> sets EXP_ARMED EXP_BUSY_DUMPS EXP_STALL_DUMPS EXP_BUSY_OBS EXP_STALL_VIOL
    EXP_BUSY_DUMPS=$(pc_count_matches 'INSTANCE_BUSY early dump' "$1")
    EXP_STALL_DUMPS=$(pc_count_matches 'INSTANCE_STALL early dump' "$1")
    EXP_BUSY_OBS=$(pc_count_matches 'INSTANCE_BUSY_IN_USER_CODE' "$1")
    EXP_STALL_VIOL=$(pc_count_matches "${PC_PROBE_ANNOUNCEMENT}INSTANCE_STALL" "$1")
    if [ "$(( EXP_BUSY_DUMPS + EXP_STALL_DUMPS ))" -gt 0 ]; then EXP_ARMED=yes; else EXP_ARMED=NO-VACUOUS; fi
}

# What the accused member's workers were doing, straight from the dump line - the signature the
# verdict rests on. "N worker(s) busy" is printed by the busy branch; the stall branch prints no
# such count precisely because its claim is that there are none.
exp_dump_signature() { # run-log
    local busy stall
    busy=$(grep -ohE 'INSTANCE_BUSY early dump \([0-9]+s in user code[^)]*\) for instance [0-9]+: [0-9]+ worker\(s\) busy' "$1" 2>/dev/null | tail -3 | tr '\n' ';')
    stall=$(grep -ohE 'INSTANCE_STALL early dump \([0-9]+s frozen[^)]*\) for instance [0-9]+' "$1" 2>/dev/null | tail -3 | tr '\n' ';')
    printf '%s%s' "${busy:-}" "${stall:-}"
}

exp_arm() { # seed arm-label free-cores
    local seed="$1" arm="$2" free="$3"
    local log="$OUT/seed-$seed-$arm.log"
    local before after outcome peaks sig burners

    pc_cpu_load_start "$free"
    burners="${PC_CPU_LOAD_BURNERS:-0}"
    # Let the burners actually reach the run queue before the reading and the run.
    sleep 5
    before="$(pc_cpu_loadavg)"
    echo "EXP: seed=$seed arm=$arm burners=$burners load-before=[$before]"

    pc_run_chaos "$TREE" "$seed" "$log" \
        -Dchaos.diagnoseStallRecovery=true \
        -Dchaos.instanceStallDumpAfterSeconds=20

    after="$(pc_cpu_loadavg)"
    pc_cpu_load_stop

    outcome=$(pc_failsafe_outcome "$TREE" ChurnStorm)
    exp_discriminator "$log"
    peaks=$(grep -ohE 'maxInstanceStall=[0-9]+ms' "$log" 2>/dev/null | tail -1)
    sig=$(exp_dump_signature "$log")
    pc_detector_verdict "$log"
    pc_consumed_bounds "$log"

    printf '%s\tseed=%s\tarm=%s\tburners=%s\tload_before=%s\tload_after=%s\toutcome=%s\tarmed=%s\tbusy_dumps=%s\tstall_dumps=%s\tbusy_obs=%s\tstall_viol=%s\t%s\tverdict=%s\tconsumed_last=%s/%s\tdone=%s\tsig=%s\n' \
        "$(pc_now)" "$seed" "$arm" "$burners" "$before" "$after" "$outcome" "$EXP_ARMED" \
        "$EXP_BUSY_DUMPS" "$EXP_STALL_DUMPS" "$EXP_BUSY_OBS" "$EXP_STALL_VIOL" \
        "${peaks:-no-peaks-line}" "$PC_VERDICT" "${PC_CONSUMED_LAST:-?}" "${PC_CONSUMED_EXPECTED:-?}" \
        "${PC_DIAGNOSTIC_DONE:-?}" "${sig:-none}" >> "$TALLY"

    echo "EXP:   -> outcome=$outcome armed=$EXP_ARMED busy_dumps=$EXP_BUSY_DUMPS stall_dumps=$EXP_STALL_DUMPS busy_obs=$EXP_BUSY_OBS stall_viol=$EXP_STALL_VIOL ${peaks:-}"
}

for seed in "$@"; do
    # Idle first, deliberately: see the header.
    exp_arm "$seed" idle "$(pc_cpu_count)"
    exp_arm "$seed" loaded "$FREE_CORES"
done

echo
echo "EXP: tally ${TALLY}"
column -t -s $'\t' "$TALLY" 2>/dev/null || cat "$TALLY"
echo
echo "EXP: a row with armed=NO-VACUOUS measured NOTHING - the window never opened and its outcome"
echo "EXP: carries no information either way. Do not count it as a green."
