#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Where a chaos run's failsafe reports live, and the ONE hazard both chaos harnesses met
# independently: with more than one repetition, every rep writes the SAME `TEST-<class>.xml`
# filenames, so anything reading them afterwards describes only the LAST rep.
#
# THIS FILE EXISTS BECAUSE THE TWO HARNESSES SOLVED THAT SEPARATELY AND DIFFERENTLY.
# `bin/chaos-test.sh` (the CI gate) archives each finished rep; the experiment runners in
# `bin/lib/chaos-experiment-common.sh` delete before each run. Neither knew about the other, and a
# third caller would have met it a third time. The hazard is stated once here; both policies live
# side by side so the next caller CHOOSES one instead of rediscovering the problem.
#
# THE TWO POLICIES ARE BOTH CORRECT, for different readers - do not collapse them:
#
#   chaos_archive_rep    KEEP every rep. CI renders a summary over all reps and uploads them as
#                        artifacts, so a rep discarded is a finding lost. Used where the reports
#                        are still going to be read.
#   chaos_clear_reports  DELETE before the next run. An experiment has already read and tallied the
#                        previous iteration, and what it needs instead is for an iteration that
#                        never reached failsafe to be visibly absent rather than silently inheriting
#                        its predecessor's verdict. Used where a stale report would be mistaken for
#                        a fresh one.
#
# TWO GLOBS, AND PICKING THE WRONG ONE IS SILENT - which is why they are named rather than inlined:
#
#   chaos_live_report_paths  `*/failsafe-reports/...`  LIVE reports only. The leading slash is what
#                            excludes the archived `rep<N>-failsafe-reports` directories. Archiving
#                            with the other glob would re-archive every earlier rep on each pass and
#                            renumber them under the newest one.
#   chaos_all_report_paths   `*failsafe-reports/...`   live AND archived. No leading slash, so the
#                            prefixed directory name matches too. This is what a summary wants.
#
# No side effects at source time, and no `set` of its own: it is sourced both by a caller running
# under `set -euo pipefail` (the CI gate) and by callers running under bare `set -u` (the experiment
# runners, which classify from the XML precisely because a failing iteration is their data).

# NUL-separated paths of LIVE failsafe reports under the given root (default: the current directory).
chaos_live_report_paths() { # [root]
    find "${1:-.}" -path '*/failsafe-reports/TEST-*.xml' -print0
}

# NUL-separated paths of live AND archived failsafe reports under the given root.
chaos_all_report_paths() { # [root]
    find "${1:-.}" -path '*failsafe-reports/TEST-*.xml' -print0
}

# Move the finished rep's reports aside so the next rep cannot overwrite them.
#
# `rep<N>-failsafe-reports`, beside the live directory, is chosen so BOTH readers still see it:
# chaos_all_report_paths above, and the workflow's artifact glob `**/target/*-reports/*.xml`, which
# requires the directory to sit directly under `target/` and to end in `-reports`. Nesting it deeper
# would keep the first reader and silently lose the artifacts.
chaos_archive_rep() { # n [root]
    local n="$1" root="${2:-.}" f dir dest
    while IFS= read -r -d '' f; do
        dir=$(dirname "$f")
        dest="${dir%/failsafe-reports}/rep${n}-failsafe-reports"
        mkdir -p "$dest"
        mv "$f" "$dest/"
    done < <(chaos_live_report_paths "$root")
}

# Delete the reports matching one test-name fragment, so a run that never reaches failsafe leaves
# nothing behind to be read as its own outcome.
chaos_clear_reports() { # tree-root name-fragment
    rm -f "$1/parallel-consumer-core/target/failsafe-reports"/TEST-*"$2"*.xml
}
