#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Copyright header conformance check for the fork.
#
# Policy (AGENTS.md, "Copyright rules for this fork"):
#   - Upstream-derived files UNMODIFIED since the fork point keep the Confluent header as-is.
#   - Upstream-derived files MODIFIED since the fork point (including fork-side renames with
#     changes, and extractions of upstream code) must ALSO carry the modifications line:
#       Modifications Copyright (C) <year> Antony Stubbs and contributors
#     (Apache 2.0 s4(b) retain-notices + s4(c) prominent-change-notice, the convention used by
#     e.g. Amazon Corretto and MariaDB for derived files.)
#   - Fork-original files (added after the fork point) must use the fork header:
#       Copyright (C) <year> Antony Stubbs and contributors
#     and must NOT claim Confluent copyright.
#
# Provenance is derived from the pinned fork-point tree below, so the check is fully
# local and deterministic - no network, no upstream remote needed. It requires the
# fork-point commit to be present in history (CI: actions/checkout fetch-depth: 0).
# "Modified" is judged against the WORKING TREE, so local uncommitted edits count.
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
# COPYRIGHT_CHECK_EXTRA_RENAMES appends 'newpath|oldpath' lines,
# COPYRIGHT_CHECK_EXTRA_EXTRACTIONS appends extraction paths.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# merge-base of the fork's master and confluentinc/parallel-consumer master:
# "Migrate to V3 based sonatype secret (#916)", 2026-03-24.
FORK_POINT="${COPYRIGHT_CHECK_FORK_POINT:-7f2901226bccac68a2a71f0d9da343887b1abb46}"
FORK_HOLDER="Antony Stubbs and contributors"
HEADER_WINDOW=8 # lines from the top of the file searched for copyright notices

# Fork-side RENAMES of upstream files: 'newpath|oldpath-at-fork-point'. They keep the
# Confluent header; if their content has diverged from the fork-point blob they must
# also carry the modifications line. (These three were relocated in fface195.)
RENAMED_FROM_UPSTREAM="
parallel-consumer-examples/parallel-consumer-example-metrics/src/test/java/io/confluent/parallelconsumer/examples/metrics/integrationTests/CoreAppMetricsIntegrationTest.java|parallel-consumer-examples/parallel-consumer-example-metrics/src/test/java/io/confluent/parallelconsumer/examples/metrics/CoreAppMetricsIntegrationTest.java
parallel-consumer-examples/parallel-consumer-example-metrics/src/test/java/io/confluent/parallelconsumer/examples/metrics/integrationTests/PrometheusContainer.java|parallel-consumer-examples/parallel-consumer-example-metrics/src/test/java/io/confluent/parallelconsumer/examples/metrics/PrometheusContainer.java
parallel-consumer-examples/parallel-consumer-example-streams/src/test/java/io/confluent/parallelconsumer/examples/streams/integrationTests/StreamsAppTest.java|parallel-consumer-examples/parallel-consumer-example-streams/src/test/java/io/confluent/parallelconsumer/examples/streams/StreamsAppTest.java
${COPYRIGHT_CHECK_EXTRA_RENAMES:-}
"

# EXTRACTIONS of upstream-derived code into new files (no single origin path): always
# Confluent + modifications line. ManagedPCInstance lives on the chaos-suite branch
# stack, not yet on master - listing it early is harmless (only tracked files are
# checked) and stops the check ambushing the stack at merge (see PR #83).
EXTRACTED_FROM_UPSTREAM="
parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/utils/ManagedPCInstance.java
${COPYRIGHT_CHECK_EXTRA_EXTRACTIONS:-}
"

if ! git cat-file -e "${FORK_POINT}^{commit}" 2>/dev/null; then
    # Provenance can't be determined without the fork-point commit (e.g. a shallow clone, or a
    # `mvn validate` build in an environment without full history). Default: WARN and skip rather
    # than fail the build - the authoritative gate is copyright.yml, which fetches full history
    # (fetch-depth: 0). Set COPYRIGHT_CHECK_REQUIRE_FORK_POINT=1 to hard-fail instead (CI does).
    msg="fork-point commit ${FORK_POINT} not in history - need full history (CI: actions/checkout fetch-depth: 0)."
    if [ "${COPYRIGHT_CHECK_REQUIRE_FORK_POINT:-0}" = "1" ]; then
        echo "ERROR: ${msg}" >&2
        exit 2
    fi
    echo "WARNING: ${msg} Skipping copyright header check." >&2
    exit 0
fi

upstream_files=$(git ls-tree -r --name-only "$FORK_POINT")
# vs the working tree, so uncommitted local edits are judged too
modified_since_fork=$(git diff --name-only "$FORK_POINT" -- '*.java')

fails=0
checked=0

require_confluent() { # <file> <header>
    if ! printf '%s' "$2" | grep -q "Copyright (C)"; then
        echo "FAIL (upstream-derived file has no copyright header): $1"
        return 1
    elif ! printf '%s' "$2" | grep -q "Confluent"; then
        echo "FAIL (upstream-derived file lost its Confluent header): $1"
        return 1
    fi
}

require_modifications_line() { # <file> <header> <reason>
    # The phrase and the holder must be on the SAME line: a mere mention of the
    # holder elsewhere in the header (e.g. an @author byline) is not a notice.
    # Years are deliberately not policed (see above).
    if ! printf '%s' "$2" | grep -q "Modifications Copyright (C).*${FORK_HOLDER}"; then
        echo "FAIL ($3 but missing 'Modifications Copyright ... ${FORK_HOLDER}' line): $1"
        return 1
    fi
}

while IFS= read -r f; do
    [ -f "$f" ] || continue
    checked=$((checked + 1))
    header=$(head -"$HEADER_WINDOW" "$f")

    # exact match on the newpath field - a substring match would misroute files
    # whose path is a tail-substring of a registered newpath into the rename branch
    rename_entry=$(printf '%s\n' "$RENAMED_FROM_UPSTREAM" | awk -F'|' -v f="$f" '$1 == f {print; exit}')
    if [ -n "$rename_entry" ]; then
        old_path=${rename_entry#*|}
        require_confluent "$f" "$header" || { fails=$((fails + 1)); continue; }
        if ! git cat-file blob "${FORK_POINT}:${old_path}" 2>/dev/null | cmp -s - "$f"; then
            require_modifications_line "$f" "$header" \
                "renamed upstream file modified since the fork point" || fails=$((fails + 1))
        fi
    elif printf '%s\n' "$EXTRACTED_FROM_UPSTREAM" | grep -qxF "$f"; then
        require_confluent "$f" "$header" || { fails=$((fails + 1)); continue; }
        require_modifications_line "$f" "$header" \
            "extraction of upstream-derived code" || fails=$((fails + 1))
    elif printf '%s\n' "$upstream_files" | grep -qxF "$f"; then
        require_confluent "$f" "$header" || { fails=$((fails + 1)); continue; }
        if printf '%s\n' "$modified_since_fork" | grep -qxF "$f"; then
            require_modifications_line "$f" "$header" \
                "upstream-derived file modified since the fork point" || fails=$((fails + 1))
        fi
    else
        # fork-original: fork header required, Confluent claim forbidden
        if printf '%s' "$header" | grep -q "Confluent"; then
            echo "FAIL (fork-original file claims Confluent copyright): $f"
            fails=$((fails + 1))
        elif ! printf '%s' "$header" | grep -q "$FORK_HOLDER"; then
            echo "FAIL (fork-original file missing '${FORK_HOLDER}' header): $f"
            fails=$((fails + 1))
        fi
    fi
done <<EOF
$(git ls-files '*.java')
EOF

echo "Checked ${checked} java files against fork point ${FORK_POINT} - ${fails} violation(s)."
[ "$fails" -eq 0 ]
