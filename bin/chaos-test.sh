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
# loudly instead of impersonating a real GREEN run (see docs/QUARANTINED_TESTS.md).

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
        echo "Quarantine Lane runs them - see docs/QUARANTINED_TESTS.md). Real coverage"
        echo "returns when the W4 variants or the quarantine owner fix (#80) land."
    else
        echo "| Test class | Time |"
        echo "|---|---|"
        find . -path '*/failsafe-reports/TEST-*.xml' -print0 | while IFS= read -r -d '' f; do
            local tag n t
            tag=$(head -3 "$f" | tr '\n' ' ')
            n=$(grep -o 'name="[^"]*"' <<< "$tag" | head -1 | cut -d'"' -f2)
            t=$(grep -o 'time="[^"]*"' <<< "$tag" | head -1 | cut -d'"' -f2)
            if [ -n "$n" ]; then echo "| $n | ${t}s |"; fi
        done
    fi
}

# Emit the summary on EVERY exit - a RED run's autopsy needs the timing/selection data most.
# MUST capture $? first and re-exit with it: an EXIT trap's own last command otherwise becomes
# the script's exit status, and everything below is made to succeed - which would report a real
# chaos RED as green (caught in PR #83 review round 6; repro: `set -e; trap true EXIT; false`
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
