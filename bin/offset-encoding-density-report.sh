#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Regenerate docs/offset-encoding-density-benchmark.md - the measured density of every offset-metadata
# encoding, incumbent and candidate, that answers issue astubbs#192.
#
# Usage:
#   bin/offset-encoding-density-report.sh            # rewrite docs/offset-encoding-density-benchmark.md
#   bin/offset-encoding-density-report.sh --check    # exit 1 if the committed report is stale (for CI)
#
# Why generated and not hand-written: the report's numbers ARE the encoders' behaviour, so a hand-kept
# copy is wrong the moment an encoder changes - and wrong in the direction that matters, because the
# whole point of the file is to be the reference someone cites years later. The generator is
# OffsetEncodingDensityBenchmarkTest, which writes the report into the module's target/ directory;
# this script is the bridge from there to docs/, plus the freshness gate.
#
# WHY THIS IS A SCRIPT AND NOT A PATH INSIDE THE TEST: surefire's working directory is the module
# basedir, so a test cannot address the repo root portably. The test writes to target/ (module
# relative) and this script, which knows the repo root because it cd's there, does the copying.
#
# DETERMINISM IS A REQUIREMENT, not a nicety. The report carries no timestamps, no host details and no
# absolute paths, and the benchmark corpus is built from a fixed seed - so two runs on two machines
# produce byte-identical output and `--check` means "stale", never "ran somewhere else".
#
# -am IS REQUIRED on the maven invocation. A bare `-pl parallel-consumer-core` fails the
# enforcer's ReactorModuleConvergence rule ("Module parents have been found which could not be found
# in the reactor") before it ever compiles anything.
#
# In --check mode the density.report.check system property is set, which makes the test itself assert
# the committed file matches. That assertion is belt-and-braces: this script diffs the files too, and
# does the diff even when maven has failed, because a readable diff is more use than an assertion
# stack trace buried in maven output. A plain `mvn test` sets no property and only WARNS on drift -
# running the unit suite must not go red because a doc needs regenerating.

set -euo pipefail

cd "$(dirname "$0")/.."

OUT="docs/offset-encoding-density-benchmark.md"
GENERATED="parallel-consumer-core/target/offset-encoding-density-benchmark.md"
TEST_CLASS="OffsetEncodingDensityBenchmarkTest"

CHECK_MODE=false
[[ "${1:-}" == "--check" ]] && CHECK_MODE=true

LOG=$(mktemp -t density-report.XXXXXX)
trap 'rm -f "$LOG"' EXIT

MAVEN_ARGS=(
    --batch-mode
    -pl parallel-consumer-core -am
    test
    "-Dtest=${TEST_CLASS}"
    # The reactor's parent module has no tests, so -Dtest would otherwise fail the run there.
    -DfailIfNoSpecifiedTests=false
    # Nothing else in this run needs a coverage report, and it is not free.
    -Djacoco.skip=true
)
if $CHECK_MODE; then
    MAVEN_ARGS+=(-Ddensity.report.check=true)
fi

echo "Running ${TEST_CLASS} to generate the density report (this compiles the module first)..."
set +e
./mvnw "${MAVEN_ARGS[@]}" > "$LOG" 2>&1
STATUS=$?
set -e

if [[ ! -f "$GENERATED" ]]; then
    echo "FAILED: ${TEST_CLASS} did not produce ${GENERATED}. Last 40 lines of the build:" >&2
    tail -40 "$LOG" >&2
    exit 1
fi

if $CHECK_MODE; then
    if ! diff -q "$OUT" "$GENERATED" > /dev/null 2>&1; then
        echo "STALE: $OUT does not match the benchmark. Regenerate with: bin/offset-encoding-density-report.sh" >&2
        diff "$OUT" "$GENERATED" | head -60 >&2 || true
        exit 1
    fi
    # Files match, so any failure above is a real one (a compile error, or another assertion in the
    # test) - never report "up to date" on a run that did not actually finish.
    if [[ "$STATUS" -ne 0 ]]; then
        echo "FAILED: the report is up to date but the build did not pass. Last 40 lines:" >&2
        tail -40 "$LOG" >&2
        exit "$STATUS"
    fi
    echo "$OUT is up to date."
else
    if [[ "$STATUS" -ne 0 ]]; then
        echo "FAILED: the benchmark run did not pass, so the report is not trustworthy. Last 40 lines:" >&2
        tail -40 "$LOG" >&2
        exit "$STATUS"
    fi
    cp "$GENERATED" "$OUT"
    echo "Wrote $OUT."
    echo "Verdicts:"
    grep -E '^VERDICT ' "$OUT" || true
fi
