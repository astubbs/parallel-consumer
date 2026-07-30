#!/usr/bin/env bash
#
# Copyright header conformance check for the fork.
#
# Policy (AGENTS.md, "Copyright rules for this fork"):
#   - Files present at the fork point (upstream-derived) keep the Confluent header.
#   - Files added since the fork point (fork-original) must use the fork header:
#       Copyright (C) <years> Antony Stubbs and contributors
#     and must NOT claim Confluent copyright.
#
# Provenance is derived from the pinned fork-point tree below, so the check is fully
# local and deterministic - no network, no upstream remote needed. It requires the
# fork-point commit to be present in history (CI: actions/checkout fetch-depth: 0).
#
# Years are deliberately NOT policed: never bump copyright years as an incidental
# change (see AGENTS.md). This scanner replaces the mycila license-maven-plugin's
# check/format for headers - that plugin knows only ONE header template, so running
# its format goal is exactly what used to stamp Confluent headers onto fork files
# (it is now skipped by default via <license.skip> in the root pom).
#
# Usage: bin/check-copyright-headers.sh
# Exit codes: 0 = conformant, 1 = violations found, 2 = cannot run (shallow clone).
#
# Test-harness overrides (used by bin/test-check-copyright-headers.sh; not needed
# for normal use): COPYRIGHT_CHECK_FORK_POINT pins a different fork-point commit,
# COPYRIGHT_CHECK_EXTRA_EXCEPTIONS appends newline-separated exception paths.

set -eu

cd "$(git rev-parse --show-toplevel)"

# merge-base of the fork's master and confluentinc/parallel-consumer master:
# "Migrate to V3 based sonatype secret (#916)", 2026-03-24.
FORK_POINT="${COPYRIGHT_CHECK_FORK_POINT:-7f2901226bccac68a2a71f0d9da343887b1abb46}"

# Fork-side RENAMES/moves/extractions of upstream files keep the Confluent header
# even though their path is not in the fork-point tree. One path per line.
# - The three example ITs were relocated into integrationTests/ subpackages in fface195.
# - ManagedPCInstance (chaos-suite branch stack, not yet on master) is an EXTRACTION of
#   upstream-derived IT code, so it keeps the Confluent header by design (see PR #83).
#   Listing it before it exists on master is harmless - only tracked files are checked.
UPSTREAM_DERIVED_EXCEPTIONS="
parallel-consumer-examples/parallel-consumer-example-metrics/src/test/java/io/confluent/parallelconsumer/examples/metrics/integrationTests/CoreAppMetricsIntegrationTest.java
parallel-consumer-examples/parallel-consumer-example-metrics/src/test/java/io/confluent/parallelconsumer/examples/metrics/integrationTests/PrometheusContainer.java
parallel-consumer-examples/parallel-consumer-example-streams/src/test/java/io/confluent/parallelconsumer/examples/streams/integrationTests/StreamsAppTest.java
parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/utils/ManagedPCInstance.java
${COPYRIGHT_CHECK_EXTRA_EXCEPTIONS:-}
"

if ! git cat-file -e "${FORK_POINT}^{commit}" 2>/dev/null; then
    echo "ERROR: fork-point commit ${FORK_POINT} not found in history." >&2
    echo "       Fetch full history first (CI: actions/checkout with fetch-depth: 0)." >&2
    exit 2
fi

upstream_files=$(git ls-tree -r --name-only "$FORK_POINT")

fails=0
checked=0
while IFS= read -r f; do
    [ -f "$f" ] || continue
    checked=$((checked + 1))
    header=$(head -5 "$f")

    if printf '%s\n' "$upstream_files" | grep -qxF "$f" \
        || printf '%s' "$UPSTREAM_DERIVED_EXCEPTIONS" | grep -qxF "$f"; then
        # upstream-derived: must keep Confluent attribution
        if ! printf '%s' "$header" | grep -q "Copyright (C)"; then
            echo "FAIL (upstream-derived file has no copyright header): $f"
            fails=$((fails + 1))
        elif ! printf '%s' "$header" | grep -q "Confluent"; then
            echo "FAIL (upstream-derived file lost its Confluent header): $f"
            fails=$((fails + 1))
        fi
    else
        # fork-original: fork header required, Confluent claim forbidden
        if printf '%s' "$header" | grep -q "Confluent"; then
            echo "FAIL (fork-original file claims Confluent copyright): $f"
            fails=$((fails + 1))
        elif ! printf '%s' "$header" | grep -q "Antony Stubbs and contributors"; then
            echo "FAIL (fork-original file missing 'Antony Stubbs and contributors' header): $f"
            fails=$((fails + 1))
        fi
    fi
done <<EOF
$(git ls-files '*.java')
EOF

echo "Checked ${checked} java files against fork point ${FORK_POINT} - ${fails} violation(s)."
[ "$fails" -eq 0 ]
