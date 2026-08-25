#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run the Chaos Pain Suite - shared by the per-PR highcpu lane (pr-highcpu-fast-feedback.yml,
# check "highcpu / Chaos Pain Suite") and the on-demand chaos-pain.yml dispatch workflow.
#
# Env (data, not code - workflow inputs must pass through env, never ${{ }} into scripts):
#   CHAOS_SEED - optional seed (replays a schedule); empty = random, logged by the run
#   CHAOS_REPS - how many times to run the suite (default 1)
#
# @Quarantined chaos scenarios are EXCLUDED (the Quarantine Lane owns them): a known-RED detector
# must not drown the tripwire signal. If that leaves zero tests selected, the summary says so
# loudly instead of impersonating a real GREEN run (see docs/quarantined-tests.md).

set -euo pipefail

REPS="${CHAOS_REPS:-1}"
SEED_ARG=""
if [ -n "${CHAOS_SEED:-}" ]; then SEED_ARG="-Dchaos.seed=${CHAOS_SEED}"; fi

start=$(date +%s)

summary() {
    local total=$(( $(date +%s) - start ))
    echo "## Chaos suite timing"
    echo ""
    printf 'Total chaos wall-clock: **%dm %02ds** (build included)\n\n' $((total / 60)) $((total % 60))
    local tests
    tests=$(find . -path '*/failsafe-reports/TEST-*.xml' | wc -l | tr -d ' ')
    if [ "$tests" -eq 0 ]; then
        echo "### ZERO chaos tests selected - this run measured NOTHING"
        echo ""
        echo "All chaos-tagged scenarios are currently @Quarantined (excluded here; the"
        echo "Quarantine Lane runs them - see docs/quarantined-tests.md). Real coverage"
        echo "returns when the W4 variants or the quarantine owner fix (astubbs#80) land."
    else
        # Class 2 lag stagnation REPORTS rather than gates (see ProgressProbe.getObservations and
        # docs/inflight/test-class2-probe-asserts-timing-not-correctness.md), so a green run is the
        # only place its findings can ever appear. Printing them here is what stops "does not gate"
        # from meaning "nobody reads it" - the peak is the number a timing regression moves.
        # grep -m1 rather than `| head -1`: an early-exiting reader closes the pipe and pipefail
        # promotes the writer's EPIPE to a failure - see bin/AGENTS.md.
        echo "| Test class | Time | Lag stagnation peak | Class 2 observations |"
        echo "|---|---|---|---|"
        local any_observations=0
        find . -path '*/failsafe-reports/TEST-*.xml' -print0 | while IFS= read -r -d '' f; do
            local tag n t peak obs
            tag=$(head -3 "$f" | tr '\n' ' ')
            n=$(grep -o 'name="[^"]*"' <<< "$tag" | head -1 | cut -d'"' -f2)
            t=$(grep -o 'time="[^"]*"' <<< "$tag" | head -1 | cut -d'"' -f2)
            peak=$(grep -m1 -o 'maxLagStagnation=[0-9]*ms' "$f" | cut -d= -f2) || peak=""
            obs=$(grep -c 'OBSERVATION (does not fail the run)' "$f") || obs=0
            if [ -n "$n" ]; then echo "| $n | ${t}s | ${peak:-n/a} | ${obs} |"; fi
        done
        any_observations=$(find . -path '*/failsafe-reports/TEST-*.xml' -exec \
            grep -l 'OBSERVATION (does not fail the run)' {} + 2>/dev/null | wc -l | tr -d ' ')
        if [ "${any_observations:-0}" -gt 0 ]; then
            echo ""
            echo "### Class 2 observations fired in ${any_observations} scenario(s) - this did NOT fail the run"
            echo ""
            echo "\`CLASS2_STALL/LAG_STAGNATION\` measures how long a partition's committed offset stayed"
            echo "pinned. One incomplete record pins it legitimately, so a busy fleet and a wedged one look"
            echo "identical to it - three replays cross this bound and drain completely. Read the peak as a"
            echo "SPEED number: worth noticing if it moves, never a defect on its own. The liveness claim is"
            echo "\`INSTANCE_STALL\`, which gates; if that stayed silent, this run was slow, not stalled."
        fi
    fi
}

# Emit the summary on EVERY exit - a RED run's autopsy needs the timing/selection data most.
# MUST capture $? first and re-exit with it: an EXIT trap's own last command otherwise becomes
# the script's exit status, and everything below is made to succeed - which would report a real
# chaos RED as green (caught in PR astubbs#83 review round 6; repro: `set -e; trap true EXIT; false`
# exits 0).
emit_summaries() {
    local ec=$?
    summary || true
    if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then summary >> "$GITHUB_STEP_SUMMARY" || true; fi
    exit "$ec"
}
trap emit_summaries EXIT

for i in $(seq 1 "$REPS"); do
    echo "=== chaos rep $i/$REPS ==="
    time ./mvnw --batch-mode -Pci -pl parallel-consumer-core -am verify \
        -DskipUTs=true \
        -Dincluded.groups=chaos -Dexcluded.groups=quarantined \
        ${SEED_ARG:+"$SEED_ARG"}
done
