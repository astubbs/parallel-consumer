#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# shell-justified: sourced by shell callers only, and it is a handful of lines wrapping job control -
# a Node implementation would have to spawn and reap the burners through a child-process API to do
# what `&` and `kill` already do here, and could not be sourced by bin/soak-test.sh at all.
#
# THE SINGLE HOME OF THE CPU-CONTENTION GENERATOR. Source this; do not copy from it - the same rule,
# and the same reason, as bin/lib/chaos-experiment-common.sh and bin/lib/quarantine-common.sh.
#
# WHY LOAD IS AN INSTRUMENT HERE, not an accident. The failures this repo's chaos and integration
# suites exist to catch are load-dependent: they need the box contended enough that a poll or an
# await misses its deadline. A fast idle machine can pass the same test a hundred times and prove
# nothing - the CI runner that fails it has TWO cores and runs the suite forked. Burning all but a
# couple of cores is the cheapest honest way to make a many-core desktop behave like that runner.
#
# It is also the CONTROL TERM of a load-versus-idle experiment: two arms, same seed, same tree,
# differing only by whether these burners are running. That is what
# bin/exp-instance-stall-load-versus-idle.sh uses it for, and why the generator had to leave
# bin/soak-test.sh - a second copy inside the experiment runner would have been a second thing to
# keep true.
#
# A BURNER HAS NO NATURAL EXIT, so every caller must stop them. Install the trap:
#
#     trap 'pc_cpu_load_stop' EXIT INT TERM
#
# Callers that already own EXIT must chain rather than replace it; a burner surviving the script
# silently taxes every later measurement on the machine, including another agent's.

# Cores on this box, however the platform spells it. Degrades LOUDLY per bin/AGENTS.md: an
# unrecognised platform says so rather than silently picking a number that makes the load wrong.
pc_cpu_count() {
    if command -v nproc >/dev/null 2>&1; then
        nproc
        return 0
    fi
    local bsd_cores
    if bsd_cores=$(sysctl -n hw.ncpu 2>/dev/null) && [ -n "$bsd_cores" ]; then
        printf '%s\n' "$bsd_cores"
        return 0
    fi
    printf 'cpu-load: cannot determine the core count on this platform; assuming 4.\n' >&2
    printf '  Set the caller free-core count explicitly if that is wrong.\n' >&2
    echo 4
}

PC_CPU_LOAD_PIDS=()

# pc_cpu_load_start <free-cores>
#
# Burns (cores - free) busy loops, leaving roughly <free-cores> usable. 0 means maximum contention;
# a number at or above the core count starts nothing, which is the unloaded baseline arm and is a
# legitimate call rather than a mistake - so it is silent about starting none.
#
# Sets PC_CPU_LOAD_BURNERS to the number actually started, because a caller reporting its arm must
# report what ran and not what it asked for.
pc_cpu_load_start() { # free-cores
    local free="${1:?pc_cpu_load_start needs a free-core count}"
    local cores
    cores=$(pc_cpu_count)
    local burn=$(( cores - free ))
    [ "$burn" -lt 0 ] && burn=0
    PC_CPU_LOAD_BURNERS="$burn"
    local ignored
    for ignored in $(seq 1 "$burn"); do
        ( while :; do :; done ) &
        PC_CPU_LOAD_PIDS+=("$!")
    done
    return 0
}

# Idempotent: safe to call from a trap that may also run after an explicit stop.
#
# THE `|| true` IS LOAD-BEARING and must not be tidied away. A burner PID can already be gone - reaped
# after an explicit stop, killed from outside, or dead of anything - and `kill` on a dead PID FAILS.
# `kill` is the last command of that `&&` list, so under `set -e` its failure is NOT exempt: it aborts
# the caller. bin/soak-test.sh sources this file under `set -euo pipefail` and calls this both bare and
# from an EXIT trap, so without the guard a stale PID skips the `SOAK RESULT` summary, and from the
# trap it silently turns a successful run's exit 0 into a 1. Measured both ways, one term varied.
pc_cpu_load_stop() {
    local p
    for p in "${PC_CPU_LOAD_PIDS[@]:-}"; do
        [ -n "$p" ] && kill "$p" 2>/dev/null || true
    done
    PC_CPU_LOAD_PIDS=()
    PC_CPU_LOAD_BURNERS=0
    return 0
}

# The load average as the kernel reports it, for the record a measurement must carry. AMBIENT LOAD IS
# NOT ZERO on a shared machine - several agent sessions build against this box at once - so an arm's
# burner count does not describe its contention and only the reading does. Every row an experiment
# writes should carry one of these taken beside the run, not a nominal setting.
pc_cpu_loadavg() {
    uptime | sed 's/.*load averages*: *//'
}
