#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/lib/chaos-reports-common.sh - the failsafe-report location and rep-collision
# policies shared by bin/chaos-test.sh (the CI "Chaos Pain Suite" gate) and the experiment runners.
#
# WHY THIS EXISTS. chaos-test.sh is called by two workflows and had NO self-test at all, so the
# extraction that created the shared library had nothing holding its behaviour still. The two globs
# differ by a single leading slash and choosing wrong is SILENT in both directions: archive with the
# permissive one and every earlier rep is re-archived and renumbered under the newest; summarise with
# the strict one and every archived rep vanishes from the count, which is the under-reporting the
# summary exists to prevent.
#
# THE NEGATIVE CONTROL IS BUILT IN. It is not enough to assert the strict glob skips the archive -
# a fixture with no archived reports would pass that vacuously. So the permissive glob is asserted to
# FIND the same file in the same fixture. If a future change makes the fixture stop containing an
# archived report, that assertion fails and says so, rather than the suite passing on a fixture that
# no longer distinguishes the two.
#
# Read-only outside its own temp dir, no network, no maven: safe under the `test-check-*` reviewer
# grant conventions in bin/AGENTS.md.

set -euo pipefail

failures=0

assert() { # <description> <expected> <actual>
    if [ "$2" = "$3" ]; then
        printf 'ok:   %s\n' "$1"
    else
        printf 'FAIL: %s\n        expected: %s\n        actual:   %s\n' "$1" "$2" "$3" >&2
        failures=$((failures + 1))
    fi
}

repo_root=$(cd "$(dirname "$0")/.." && pwd)
# shellcheck source=bin/lib/chaos-reports-common.sh
. "$repo_root/bin/lib/chaos-reports-common.sh"

fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT

live="$fixture/parallel-consumer-core/target/failsafe-reports"
archived="$fixture/parallel-consumer-core/target/rep1-failsafe-reports"
mkdir -p "$live" "$archived"
printf '<testsuite name="ChaosChurnStormIT" time="1.0"/>\n' > "$live/TEST-ChaosChurnStormIT.xml"
printf '<testsuite name="ChaosChurnStormIT" time="1.0"/>\n' > "$archived/TEST-ChaosChurnStormIT.xml"

count_paths() { tr -cd '\0' | wc -c | tr -d ' '; }

echo "--- the two globs must disagree about an archived rep ---"
assert "the LIVE glob sees only the live report" 1 "$(chaos_live_report_paths "$fixture" | count_paths)"
assert "the ALL glob sees live AND archived" 2 "$(chaos_all_report_paths "$fixture" | count_paths)"

echo "--- archiving keeps every rep, and does not re-archive what it already moved ---"
chaos_archive_rep 2 "$fixture"
assert "the live directory is now empty" 0 "$(chaos_live_report_paths "$fixture" | count_paths)"
assert "both reps survive" 2 "$(chaos_all_report_paths "$fixture" | count_paths)"
assert "rep1 was NOT renumbered under rep2" YES \
    "$([ -f "$archived/TEST-ChaosChurnStormIT.xml" ] && echo YES || echo NO)"
assert "the newly finished rep landed in rep2" YES \
    "$([ -f "$fixture/parallel-consumer-core/target/rep2-failsafe-reports/TEST-ChaosChurnStormIT.xml" ] \
        && echo YES || echo NO)"

echo "--- a second archive pass must be a no-op, not a renumbering cascade ---"
chaos_archive_rep 3 "$fixture"
assert "still exactly two reports, still two directories" 2 "$(chaos_all_report_paths "$fixture" | count_paths)"
assert "no rep3 directory was created from already-archived files" NO \
    "$([ -d "$fixture/parallel-consumer-core/target/rep3-failsafe-reports" ] && echo YES || echo NO)"

echo "--- clearing removes only the matching live report ---"
mkdir -p "$live"
printf '<testsuite name="ChaosChurnStormIT"/>\n' > "$live/TEST-ChaosChurnStormIT.xml"
printf '<testsuite name="SomethingElseIT"/>\n' > "$live/TEST-SomethingElseIT.xml"
chaos_clear_reports "$fixture" ChurnStorm
assert "the matching report is gone" NO \
    "$([ -f "$live/TEST-ChaosChurnStormIT.xml" ] && echo YES || echo NO)"
assert "an unrelated report is untouched" YES \
    "$([ -f "$live/TEST-SomethingElseIT.xml" ] && echo YES || echo NO)"
assert "clearing did not touch the archives" YES \
    "$([ -f "$archived/TEST-ChaosChurnStormIT.xml" ] && echo YES || echo NO)"

echo
if [ "$failures" -gt 0 ]; then
    printf '%d chaos-reports self-test(s) failed\n' "$failures" >&2
    exit 1
fi
echo "All chaos-reports self-tests passed"
