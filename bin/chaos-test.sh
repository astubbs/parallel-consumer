#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run the Chaos Pain Suite - shared by the per-PR gate (maven.yml, check "Chaos Pain Suite") and
# the on-demand chaos-pain.yml dispatch workflow.
#
# Env (data, not code - workflow inputs must pass through env, never ${{ }} into scripts):
#   CHAOS_SEED - optional seed (replays a schedule); empty = random, logged by the run
#   CHAOS_REPS - how many times to run the suite (default 1)
#   CHAOS_SCENARIOS - optional comma-separated list of simple chaos test class names (e.g.
#     "ChaosKeyOrderIT,ChaosChurnStormIT"); empty = every chaos-tagged scenario, which is what
#     chaos-pain.yml and local replays keep. When set, this passes -Dit.test=<list> AND
#     -Dfailsafe.failIfNoSpecifiedTests=false to failsafe - the second flag is required because -am
#     builds the parent module first and it has no matching tests, so without it the build dies
#     before the requested scenarios run (docs/inflight/bug-857-family.md). The end-of-run summary
#     then verifies every requested scenario produced a failsafe report and fails the run if one did
#     not - a shard that silently ran fewer scenarios than it was assigned must not read as a pass.
#
# @Quarantined chaos scenarios are EXCLUDED (the Quarantine Lane owns them): a known-RED detector
# must not drown the tripwire signal. If that leaves zero tests selected, the summary says so
# loudly instead of impersonating a real GREEN run (see docs/quarantined-tests.md) - and when
# CHAOS_SCENARIOS is set, zero-selected is a hard failure rather than the advisory wording below.

set -euo pipefail

# Where the failsafe reports are and what to do about reps overwriting each other - shared with the
# experiment runners, which met the same hazard independently and answer it with the other policy.
# shellcheck source=bin/lib/chaos-reports-common.sh
. "${BASH_SOURCE[0]%/*}/lib/chaos-reports-common.sh"

# Must byte-match the WARN line ProgressProbe.observe() emits - grep it there before editing.
readonly OBSERVATION_MARKER="OBSERVATION (does not fail the run)"

REPS="${CHAOS_REPS:-1}"
SEED_ARG=""
if [ -n "${CHAOS_SEED:-}" ]; then SEED_ARG="-Dchaos.seed=${CHAOS_SEED}"; fi

# Note the `+` expansion: under `set -u`, bash 3.2 (which is what macOS ships) treats "${arr[@]}" on
# an EMPTY array as an unbound variable and aborts - same trap bin/lincheck-test.sh documents and
# guards against with the identical pattern.
TEST_ARG=()
if [ -n "${CHAOS_SCENARIOS:-}" ]; then
    TEST_ARG=(-Dit.test="${CHAOS_SCENARIOS}" -Dfailsafe.failIfNoSpecifiedTests=false)
fi

# Existence, not content, is the signal: summary() (run in the `emit_summaries` EXIT trap's command
# substitution, i.e. a subshell) writes this file when CHAOS_SCENARIOS requested a scenario that
# produced no failsafe report. A subshell can still write a real file, so this crosses back out
# where a plain variable assignment inside `$(summary)` could not. No mktemp: this needs no atomic
# creation, and mktemp's `-t` flag differs enough between GNU and BSD/macOS to be worth avoiding.
MISSING_SCENARIOS_MARKER="${TMPDIR:-/tmp}/chaos-missing-scenarios.$$"

start=$(date +%s)

summary() {
    local total=$(( $(date +%s) - start ))
    echo "## Chaos suite timing"
    echo ""
    printf 'Total chaos wall-clock: **%dm %02ds** (build included)\n\n' $((total / 60)) $((total % 60))
    local tests
    # All reports, live and archived - the summary must count every rep, not just the last.
    tests=$(chaos_all_report_paths | tr -cd '\0' | wc -c | tr -d ' ')
    # Simple (unqualified) class names of every scenario that produced a report, comma-joined - built
    # in the tests>0 branch below and left empty here. Compared against CHAOS_SCENARIOS after both
    # branches, so a shard sharded down to zero tests reports it too, not just a partial miss.
    local found_names=""
    if [ "$tests" -eq 0 ]; then
        echo "### ZERO chaos tests selected - this run measured NOTHING"
        echo ""
        if [ -n "${CHAOS_SCENARIOS:-}" ]; then
            echo "CHAOS_SCENARIOS requested (${CHAOS_SCENARIOS}) but no failsafe report was produced"
            echo "for any of them - a class-name typo, a quarantined scenario assigned to this shard,"
            echo "or a build failure before the tests ran. See below for which requested scenario(s)"
            echo "are missing."
        else
            echo "All chaos-tagged scenarios are currently @Quarantined (excluded here; the"
            echo "Quarantine Lane runs them - see docs/quarantined-tests.md). Real coverage"
            echo "returns when the W4 variants or the quarantine owner fix (astubbs#80) land."
        fi
    else
        # Class 2 lag stagnation REPORTS rather than gates (see ProgressProbe.getObservations and
        # docs/solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md),
        # so a green run is the
        # only place its findings can ever appear. Printing them here is what stops "does not gate"
        # from meaning "nobody reads it" - the peak is the number a timing regression moves.
        # Every read here uses a whole-file `grep` with no early-exiting reader downstream: `| head`
        # would close the pipe and pipefail would promote the writer's EPIPE to a failure, which
        # bin/AGENTS.md bans and the `sigpipe-into-grep-q` rule enforces.
        # One pass over the reports, feeding the loop by process substitution rather than a pipe:
        # `find | while` runs the loop in a SUBSHELL, so per-file counts cannot accumulate and a
        # second full scan was needed to answer "did anything observe?". These XMLs embed captured
        # stdout and reach hundreds of MB, so the extra scan was not free. Same shape as
        # bin/quarantine-lane-report.sh. No early-exiting reader is introduced, so the
        # `sigpipe-into-grep-q` ban is not implicated.
        local any_observations=0
        echo "| Test class | Time | Lag stagnation peak | Class 2 observations |"
        echo "|---|---|---|---|"
        while IFS= read -r -d '' f; do
            local tag n t obs peak_ms peak_line
            tag=$(head -3 "$f" | tr '\n' ' ')
            # `|| n=""`: this loop runs under `set -e` and the missing-scenario guard below depends on
            # it finishing. A report with no name= attribute would otherwise fail the pipeline, abort
            # summary() here, skip the guard, and let a shard that ran less than it was assigned read
            # green - the exact silence the guard exists to remove (review on astubbs#421).
            n=$(grep -o 'name="[^"]*"' <<< "$tag" | head -1 | cut -d'"' -f2) || n=""
            t=$(grep -o 'time="[^"]*"' <<< "$tag" | head -1 | cut -d'"' -f2) || t=""
            obs=$(grep -c "$OBSERVATION_MARKER" "$f") || obs=0
            # MAX, not first match: a class with several test methods emits one `peaks:` line per
            # method, and reporting the first would understate a later method's peak.
            peak_ms=""
            while IFS= read -r peak_line; do
                [ -n "$peak_line" ] || continue
                if [ -z "$peak_ms" ] || [ "$peak_line" -gt "$peak_ms" ]; then peak_ms="$peak_line"; fi
            done <<< "$(grep -o 'maxLagStagnation=[0-9]*ms' "$f" | cut -d= -f2 | tr -d 'ms')"
            # Counted independently of the table-row guard: a report with no name= attribute still
            # observed whatever it observed.
            if [ "$obs" -gt 0 ]; then any_observations=$((any_observations + 1)); fi
            if [ -n "$n" ]; then
                echo "| $n | ${t}s | ${peak_ms:-n/a}${peak_ms:+ms} | ${obs} |"
                # `name=` is the FULLY QUALIFIED class name; CHAOS_SCENARIOS is simple names (see the
                # header), so strip the package here rather than asking every caller to qualify it.
                # `< <(...)` (not a pipe) keeps this loop in summary()'s own scope, not a subshell -
                # accumulation across iterations depends on that, same as `any_observations` above.
                found_names="${found_names},${n##*.}"
            fi
        done < <(chaos_all_report_paths)
        if [ "$any_observations" -gt 0 ]; then
            echo ""
            echo "### Class 2 observations fired in ${any_observations} scenario(s) - this did NOT fail the run"
            echo ""
            echo "\`CLASS2_STALL/LAG_STAGNATION\` measures how long a partition's committed offset stayed"
            echo "pinned. One incomplete record pins it legitimately, so a busy fleet and a wedged one look"
            echo "identical to it - three replays cross this bound and drain completely. Read the peak as a"
            echo "SPEED number: worth noticing if it moves, never a defect on its own. The liveness claim is"
            echo "\`INSTANCE_STALL\`, which gates; if that stayed silent, this run was slow, not stalled."
            echo ""
            echo "One caveat worth knowing before you close the tab: \`INSTANCE_STALL\` is per-INSTANCE, so a"
            echo "single wedged shard beside busy siblings gates nothing. If a watermark froze here while the"
            echo "fleet stayed busy, rule that case out by hand -"
            echo "docs/inflight/test-per-shard-liveness-has-no-gate.md has the shape to look for."
        fi
    fi
    # A shard that silently ran fewer scenarios than it was assigned must not read as a pass - this
    # is the guard `-Dfailsafe.failIfNoSpecifiedTests=false` above makes necessary, and it covers
    # both branches above: zero-selected (found_names empty) and a partial miss alike.
    if [ -n "${CHAOS_SCENARIOS:-}" ]; then
        local requested_name missing=""
        IFS=',' read -ra requested <<< "$CHAOS_SCENARIOS"
        for requested_name in "${requested[@]}"; do
            [ -n "$requested_name" ] || continue
            case ",${found_names}," in
                *",${requested_name},"*) : ;; # found - no action
                *) missing="${missing},${requested_name}" ;;
            esac
        done
        missing="${missing#,}"
        if [ -n "$missing" ]; then
            echo ""
            echo "### MISSING SCENARIO REPORT(S): ${missing}"
            echo ""
            echo "CHAOS_SCENARIOS requested \`${CHAOS_SCENARIOS}\` but no failsafe report was found for"
            echo "the scenario(s) above - a shard doing less than it was assigned must not read as a"
            echo "pass. This fails the run (see emit_summaries)."
            # Existence is the signal - see MISSING_SCENARIOS_MARKER's declaration above. This write
            # happens inside summary()'s own subshell (it runs under \$(summary)), which is fine: a
            # real file write is real I/O, not process-local state, so it is visible to the caller
            # once this subshell exits.
            : > "$MISSING_SCENARIOS_MARKER"
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
    # Build it ONCE. summary() now greps every failsafe report, and CI rendered it twice - to stdout
    # and again into the step summary - doubling the reads of files that reach hundreds of MB.
    local rendered
    rendered=$(summary) || true
    printf '%s\n' "$rendered"
    if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then printf '%s\n' "$rendered" >> "$GITHUB_STEP_SUMMARY" || true; fi
    # summary() marks this file (not $ec, which command substitution cannot set - see the marker's
    # declaration above) when CHAOS_SCENARIOS requested a scenario that produced no report. Turn a
    # 0 exit into 1 for that; a Maven failure already set $ec non-zero and stays as it is - this
    # never masks a real red with a "just missing scenarios" one.
    if [ -f "$MISSING_SCENARIOS_MARKER" ]; then
        rm -f "$MISSING_SCENARIOS_MARKER"
        if [ "$ec" -eq 0 ]; then
            echo "chaos-test.sh: CHAOS_SCENARIOS requested a scenario with no failsafe report - failing a run that would otherwise read GREEN (see the MISSING SCENARIO REPORT(S) section above)." >&2
            ec=1
        fi
    fi
    exit "$ec"
}
trap emit_summaries EXIT

# With CHAOS_REPS > 1 every rep writes the SAME TEST-<class>.xml filenames, so the end-of-run scan
# would describe only the LAST rep: an observation or a higher peak seen in rep 1 vanishes if the
# final rep happens to be quiet, and the summary then under-reports a hunt that did find something.
# That is the exact silent-under-reporting shape this summary exists to prevent. Caught in review on
# astubbs/parallel-consumer#354.
#
# chaos_archive_rep (bin/lib/chaos-reports-common.sh) is the KEEP policy - this script's reports are
# still going to be read, by the summary below and by the workflow's artifact upload. The experiment
# runners use the DELETE policy from the same file for the opposite reason. Both are documented
# there, together, so a third caller picks one rather than meeting the hazard a third time.

for i in $(seq 1 "$REPS"); do
    if [ "$i" -gt 1 ]; then chaos_archive_rep "$((i - 1))"; fi
    echo "=== chaos rep $i/$REPS ==="
    time ./mvnw --batch-mode -Pci -pl parallel-consumer-core -am verify \
        -DskipUTs=true \
        -Dincluded.groups=chaos -Dexcluded.groups=quarantined \
        ${SEED_ARG:+"$SEED_ARG"} ${TEST_ARG[@]+"${TEST_ARG[@]}"}
done
