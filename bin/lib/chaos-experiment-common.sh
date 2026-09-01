#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Shared definitions for the confluentinc#857 experiment runners - THE single home of the maven
# invocation, the JDK pin, the failsafe-report outcome classifier, the detector-verdict classifier
# and the violation-trajectory parsing that exp-audit-stall-detector-silence.sh,
# exp-measure-large-instances-failure-rate.sh, exp-sweep-large-instances-scale.sh and
# exp-batch-857.sh all need. Source this; do not copy
# from it.
#
# Same rule, and the same reason, as bin/lib/quarantine-common.sh: the copies drift, and the drift
# is silent because each copy still runs. It had already happened here before this file existed -
# a now-retired drain-confirmation runner carried a fixed trajectory-parsing bug (`paste -sd'->'`
# treats -d as a character LIST, so it joined with '-', the sed matched nothing, and four runs that
# plainly drained were labelled FLAT) that the two sibling copies of the same parsing never received.
#
# These runners are unattended: nobody is watching when one of them answers wrongly, so every
# classifier here is written to be loud or absent rather than plausible.
#
# WHY EVERY CALLER SETS `set -u` AND NOT `set -euo pipefail`, unlike the rest of bin/. An iteration
# that FAILS is the data these scripts are collecting - a fired probe violation is the expected,
# informative outcome, not a script error - and each of them classifies its run from the failsafe
# XML rather than from maven's exit code. `-e` would abort the hunt on the first failing iteration,
# which is usually the first interesting one. Do not "fix" it into the repo-wide convention.

# --- the JDK -------------------------------------------------------------------------------------
#
# The build needs JDK 17, and `/usr/libexec/java_home -v 17` resolves to JDK 26 on the machine these
# were written on - which fails delombok in a module you never touched. So a real 17 is pinned.
#
# DEGRADE LOUDLY (bin/AGENTS.md): an explicit JAVA_HOME wins; otherwise the SDKMAN candidate is used
# only if it is actually there, and its absence is SAID rather than exported. Exporting a path that
# does not exist does not fail here - it fails minutes later inside maven, in a message about
# something else entirely.
pc_experiment_java_home() {
    if [ -n "${JAVA_HOME:-}" ]; then
        printf '%s' "$JAVA_HOME"
        return 0
    fi
    local candidate="$HOME/.sdkman/candidates/java/17.0.18-tem"
    if [ -x "$candidate/bin/java" ]; then
        printf '%s' "$candidate"
        return 0
    fi
    printf 'chaos-experiment: JAVA_HOME is unset and %s is not present.\n' "$candidate" >&2
    printf '  Falling back to whatever java the maven wrapper finds. This project needs JDK 17;\n' >&2
    printf '  set JAVA_HOME explicitly if that is not what it will pick.\n' >&2
    return 0
}

# Non-essential verify-phase work, skipped on every iteration of every experiment loop. The source
# tree does not change between iterations - only a system property does - so re-running full-effort
# SpotBugs (over main AND test classes, with three plugin packs) and the forbidden-API scan on each
# of the tens of runs these scripts perform buys nothing. copyright/jacoco were already skipped for
# exactly this reason; spotbugs and forbiddenapis were simply missed.
#
# None of these scripts read maven's exit code - each classifies its run from the failsafe XML - so
# suppressing a verify-phase gate cannot change what any of them reports.
PC_EXPERIMENT_SKIPS=(
    -Dfailsafe.failIfNoSpecifiedTests=false
    -Dcopyright.skip=true
    -Djacoco.skip=true
    -Dspotbugs.skip=true
    -Dforbiddenapis.skip=true
)

# --- counting ------------------------------------------------------------------------------------
#
# `grep -c` PRINTS 0 and EXITS 1 when nothing matches, so the habitual `$(grep -c ... || echo 0)`
# captures TWO lines - "0\n0". Every later `[ "$n" -gt 0 ]` then dies with "integer expression
# expected" and falls through to the else branch, and the count is interpolated into a printf, so
# the tally row splits across two lines. Both happen in exactly the no-match case these scripts
# exist to detect. Use this instead of a bare `grep -c`.
pc_count_matches() { # extended-regex file
    local n
    n=$(grep -cE "$1" "$2" 2>/dev/null) || n=0
    printf '%s' "${n:-0}"
}

# --- what actually ran ---------------------------------------------------------------------------
#
# Classify from the failsafe REPORT, never from maven's exit code: a run that executed no test exits
# 0 and is indistinguishable from a pass in a rate. That discipline has changed an answer more than
# once, which is why it is shared rather than remembered.

# Report location and the rep-overwrite hazard are shared with bin/chaos-test.sh, which answers it
# with the KEEP policy because its reports are still going to be read. These runners use the DELETE
# policy: an iteration that dies before failsafe (compile break, bad -Dit.test, dependency failure)
# must be visibly absent rather than silently classified from its predecessor's file. Without it the
# DID-NOT-RUN verdict below is unreachable in exactly the case it exists for - the stale report still
# says tests="1", so a broken tree reports the last good run indefinitely, failing toward "everything
# is fine". Both policies and the two report globs are documented together in that file.
# shellcheck source=bin/lib/chaos-reports-common.sh
. "${BASH_SOURCE[0]%/*}/chaos-reports-common.sh"

pc_clear_failsafe_reports() { # tree-root report-name-fragment
    chaos_clear_reports "$1" "$2"
}

# The last matching report's counts, verbatim - callers print it when there is nothing to classify.
pc_failsafe_stats() { # tree-root report-name-fragment
    grep -ohE 'tests="[0-9]+" errors="[0-9]+" skipped="[0-9]+" failures="[0-9]+"' \
        "$1/parallel-consumer-core/target/failsafe-reports"/TEST-*"$2"*.xml 2>/dev/null | tail -1
}

# Exactly one of DID-NOT-RUN, FAILED, passed. Herestrings rather than `printf | grep -q`: the pipe
# form takes EPIPE from the early-exiting reader and, under `set -o pipefail`, turns a MATCH into a
# failure - see bin/AGENTS.md and bin/check-shell-sigpipe.sh. None of today's callers set pipefail;
# writing it the safe way here means adding it later cannot silently invert a verdict.
pc_classify_failsafe_stats() { # stats-string
    grep -q 'tests="[1-9]' <<< "$1" || { echo DID-NOT-RUN; return; }
    if grep -qE 'errors="[1-9]|failures="[1-9]' <<< "$1"; then echo FAILED; else echo passed; fi
}

pc_failsafe_outcome() { # tree-root report-name-fragment
    pc_classify_failsafe_stats "$(pc_failsafe_stats "$1" "$2")"
}

# --- which detector caught it --------------------------------------------------------------------
#
# Sets PC_VERDICT to one of no-progress, other-probe, UNNAMED-PROBE or LEDGER-ONLY-MISS-CASE, plus
# PC_NO_PROGRESS, PC_OTHER_PROBE and PC_UNNAMED_PROBE. The counts are separate results rather than
# one string because the callers print them beside the verdict - a verdict whose evidence is not in
# the tally cannot be re-checked from it. bin/exp-audit-stall-detector-silence.sh's header owns what
# each verdict MEANS and why it is the question worth asking.
#
# EVERY GATING SIGNAL HAS TO BE HERE, AND ANYTHING UNRECOGNISED HAS TO HAVE SOMEWHERE ELSE TO GO.
# An omission is not an undercount, it is a wrong headline: a signal missing from the list used to
# fall through to LEDGER-ONLY-MISS-CASE - the verdict that says every detector stayed silent - so a
# run a detector DID catch was recorded as a detector miss, in the audit whose entire purpose is
# counting detector misses. Two of the five were missing (DRAIN_OVERDUE, PROBE_DEGRADED) and a sixth
# name was listed that ProgressProbe does not raise at all (LEDGER_KEY_ORDER), which says how the
# list was written: from memory. Regenerate it instead -
# `grep -n 'violate(' <ProgressProbe.java>` is the whole list, and nothing checks that these agree.
# PC_UNNAMED_PROBE is the standing insurance against the next omission.
PC_PROBE_NO_PROGRESS_SIGNAL='NO_PROGRESS'
PC_PROBE_OTHER_SIGNALS='ZOMBIE_MEMBER|INSTANCE_STALL|DRAIN_OVERDUE|PROBE_DEGRADED'

# Anchored on the announcement prefix, not on the bare signal name. ProgressProbe announces a gating
# finding as one line - "[<tag>] VIOLATION: <SIGNAL>: <detail>" - so this counts violations rather
# than mentions, and the three counts below are line counts over the same anchor, which is what makes
# subtracting them sound. The bare-name match it replaces also hit the ambient observer's autopsy
# bullets, which reprint the same names for a probe that was never gating this run.
PC_PROBE_ANNOUNCEMENT='VIOLATION: '

pc_detector_verdict() { # run-log
    local announced
    announced=$(pc_count_matches "$PC_PROBE_ANNOUNCEMENT" "$1")
    PC_NO_PROGRESS=$(pc_count_matches "$PC_PROBE_ANNOUNCEMENT($PC_PROBE_NO_PROGRESS_SIGNAL)" "$1")
    PC_OTHER_PROBE=$(pc_count_matches "$PC_PROBE_ANNOUNCEMENT($PC_PROBE_OTHER_SIGNALS)" "$1")
    PC_UNNAMED_PROBE=$(( announced - PC_NO_PROGRESS - PC_OTHER_PROBE ))
    [ "$PC_UNNAMED_PROBE" -lt 0 ] && PC_UNNAMED_PROBE=0
    if [ "$PC_NO_PROGRESS" -gt 0 ]; then
        PC_VERDICT=no-progress
    elif [ "$PC_OTHER_PROBE" -gt 0 ]; then
        PC_VERDICT=other-probe
    elif [ "$PC_UNNAMED_PROBE" -gt 0 ]; then
        # A detector fired and this file cannot say which. NOT a miss - say so loudly rather than
        # letting it be counted as one, and go and add the signal above.
        PC_VERDICT=UNNAMED-PROBE
    else
        PC_VERDICT=LEDGER-ONLY-MISS-CASE
    fi
}

# Did ONE named signal fire, as opposed to "something did"? The `violations=` reading on the
# [diagnose] line is the probe's whole violation count, so a runner hunting a specific mechanism
# cannot use it to tell its own signal from any other - see pc_first_violation.
pc_signal_fired() { # signal-name run-log
    grep -qE "$PC_PROBE_ANNOUNCEMENT$1" "$2" 2>/dev/null
}

# --- did the probe fire, and what happened next ----------------------------------------------------

# The first `violations=<n>` reading with a non-zero count, or empty when the probe never fired. A
# run that did not fire is not a data point: the recovery diagnostic only engages on a violation.
#
# IT SAYS "SOMETHING FIRED", NEVER WHICH. That reading is ProgressProbe's aggregate violation count
# on the [diagnose] line, so ZOMBIE_MEMBER, INSTANCE_STALL, DRAIN_OVERDUE or PROBE_DEGRADED make it
# non-empty exactly as the async NO_PROGRESS stall does. A runner whose question is about ONE
# mechanism must gate on pc_signal_fired as well, or it will accept a run that answers a different
# question and write the answer up as though it were about its own.
#
# No pipe, deliberately - `grep | head` is the shape bin/check-shell-sigpipe.sh bans under pipefail,
# and a shared helper must stay safe for a caller that adds it.
pc_first_violation() { # run-log
    local all
    all=$(grep -oE 'violations=[1-9][0-9]*' "$1" 2>/dev/null) || return 0
    printf '%s' "${all%%$'\n'*}"
}

# Every consumption reading from the violation onward, INCLUDING the denominator and the scenario's
# own done flag - so a row is readable on its own and the last row settles the run. done=true means
# the backlog DRAINED (the finding was bounded - a timing proxy); done=false at the end means it did
# not, whether consumption climbed on the way there or stayed flat. "Climbing" alone does not
# discriminate: a run that advances once and then stops short is a wedge that climbed.
pc_trajectory_after_violation() { # run-log
    awk '/violations=[1-9]/{f=1} f' "$1" 2>/dev/null \
        | grep -oE 'consumed=[0-9]+/[0-9]+ started=[0-9]+ inFlight=[0-9]+.*done=(true|false)'
}

# Sets PC_CONSUMED_FIRST and PC_CONSUMED_LAST - the ends of that trajectory, as bare integers, empty
# when the run has no readings - plus PC_CONSUMED_EXPECTED (the denominator the scenario was aiming
# at) and PC_DIAGNOSTIC_DONE (the scenario's own completion predicate: consumed the backlog AND the
# ledger covers every expected key).
#
# THE DENOMINATOR AND THE FLAG ARE THE POINT, not decoration. "Last is greater than first" is
# satisfied by a run that advanced once after the violation and then wedged short of the backlog -
# which is a partial-progress wedge, the exact finding these runners exist to catch, and the reading
# that would call it DRAINED is the reading that would report the bug as absent.
#
# READ THE NUMBERS SEPARATELY. An earlier version joined the ends with `paste -sd'->'`, where -d is a
# character LIST and not a string, so it joined with '-'; the sed then looked for '->', matched
# nothing, and the integer test failed silently into the else branch - labelling four runs that
# plainly drained as FLAT. The data was right and every verdict was wrong.
pc_consumed_bounds() { # run-log
    local after readings last
    after=$(awk '/violations=[1-9]/{f=1} f' "$1" 2>/dev/null)
    readings=$(grep -oE 'consumed=[0-9]+' <<< "$after" | cut -d= -f2)
    PC_CONSUMED_FIRST="${readings%%$'\n'*}"
    PC_CONSUMED_LAST="${readings##*$'\n'}"
    # The LAST full [diagnose] reading, which carries both the denominator and the scenario's own
    # done flag. Herestring rather than a pipe from awk, for the EPIPE reason recorded above
    # pc_classify_failsafe_stats.
    last=$(grep -oE 'consumed=[0-9]+/[0-9]+.*done=(true|false)' <<< "$after")
    last="${last##*$'\n'}"
    PC_CONSUMED_EXPECTED=""
    PC_DIAGNOSTIC_DONE=""
    if [ -n "$last" ]; then
        PC_CONSUMED_EXPECTED="${last#consumed=*/}"
        PC_CONSUMED_EXPECTED="${PC_CONSUMED_EXPECTED%% *}"
        PC_DIAGNOSTIC_DONE="${last##*done=}"
    fi
}

# --- running one experiment ------------------------------------------------------------------------
#
# One home for the maven command line, so a flag added for one experiment is not missing from the
# next. Extra `-D` arguments are appended by the caller.

pc_run_chaos() { # tree-root seed logfile [extra maven args...]
    local tree="$1" seed="$2" log="$3"
    shift 3
    pc_clear_failsafe_reports "$tree" ChurnStorm
    JAVA_HOME="$PC_JAVA_HOME" "$tree/mvnw" -f "$tree/pom.xml" -Pci -pl parallel-consumer-core -am \
        verify -DskipUTs=true -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed="$seed" \
        -Dit.test=ChaosChurnStormIT "${PC_EXPERIMENT_SKIPS[@]}" "$@" > "$log" 2>&1
}

pc_run_performance() { # tree-root it-test logfile [extra maven args...]
    local tree="$1" it_test="$2" log="$3"
    shift 3
    pc_clear_failsafe_reports "$tree" "$it_test"
    JAVA_HOME="$PC_JAVA_HOME" "$tree/mvnw" -f "$tree/pom.xml" -Pci -pl parallel-consumer-core -am \
        verify -DskipUTs=true -Dincluded.groups=performance -Dexcluded.groups= \
        -Dit.test="$it_test" "${PC_EXPERIMENT_SKIPS[@]}" "$@" > "$log" 2>&1
}

# --- where things are ------------------------------------------------------------------------------

# UTC stamp for a tally row. One format across every runner, so rows from different experiments sort
# together.
pc_now() { date -u +%FT%TZ; }

# The PRIMARY checkout, whichever worktree this script is running from - `--show-toplevel` would give
# the worktree, and the sibling trees a cross-tree experiment compares live under the primary one.
# Derived rather than hardcoded to one machine's home directory.
pc_main_checkout() {
    local common
    common="$(git rev-parse --git-common-dir)" || return 1
    (cd "$(dirname "$common")" && pwd)
}

# The root every sibling worktree hangs off. AGENTS.md ("Worktree ownership") owns the layout.
pc_worktree_root() {
    printf '%s/.claude/worktrees' "$(pc_main_checkout)"
}

# A cross-tree runner needs sibling worktrees that only exist on the machine somebody cut them on.
# DEGRADE LOUDLY (bin/AGENTS.md): say which tree is missing and exit non-zero, rather than recording
# a "missing tree" row and exiting 0.
#
# Exiting 0 is what made this worth a helper. These runners are unattended and their absence of
# output looks exactly like a run that measured nothing interesting, so a dispatch onto a fresh
# checkout - CI, a new clone, a colleague's machine - produced a green job, an artifact with no rows
# in it, and no statement anywhere that the experiment had not run. Exit 2 is bin/'s "cannot run",
# which is deliberately not the same column as a pass.
pc_require_tree() { # tree-path label
    [ -d "$1" ] && return 0
    printf 'chaos-experiment: required worktree %s is missing (%s).\n' "$2" "$1" >&2
    printf '  This is a CROSS-TREE experiment: it compares this checkout against sibling worktrees\n' >&2
    printf '  that exist only where somebody cut them. It cannot run here, and a run that measured\n' >&2
    printf '  nothing must not be reported as a run that found nothing.\n' >&2
    exit 2
}

PC_JAVA_HOME="$(pc_experiment_java_home)"
