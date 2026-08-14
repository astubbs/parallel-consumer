#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Copyright header conformance check for the fork.
#
# Policy (docs/copyright.md):
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
# PROVENANCE SURVIVES THE PACKAGE MOVE, AND THAT IS A DESIGN CONSTRAINT, NOT A DETAIL.
# A file's provenance is a fact about the fork-point tree, so it has to be resolved
# against the path the file had THERE - not the path it has today. Looking the CURRENT
# path up in the fork-point listing worked only while the two spellings coincided; the
# fork's io.confluent.* -> bz.stub.* rename (bin/rename-packages.sh) ends that, and the
# failure is an INVERSION rather than a degradation: every upstream-derived file misses
# the lookup, is judged fork-original, and its REQUIRED Confluent header becomes a
# violation. Measured on a renamed throwaway clone: 0 -> 197 violations. This script is
# bound to maven's `validate` phase, so that is not one red CI job, it is every `./mvnw`
# invocation on the tree dying before it starts.
#
# So each path is mapped back through PACKAGE_MOVES below BEFORE any lookup. Both
# spellings resolve to the same fork-point path, which is what lets the check pass on a
# renamed branch, an un-renamed branch, and the half-renamed tree that exists between
# the rename's two commits - all of which are live at once while the rename rolls out
# across the open branches. See docs/plans/2026-08-11-001-refactor-package-rename-plan.md
# for why a ~200-entry RENAMED_FROM_UPSTREAM manifest was rejected in favour of a rule.
#
# Years are deliberately NOT policed: never bump copyright years as an incidental
# change (see AGENTS.md). This scanner IS the header enforcement. It replaced the
# mycila license-maven-plugin, which knew only ONE header template - running its
# format goal is what used to stamp Confluent headers onto fork-original files. That
# plugin was skipped from PR astubbs#90 and removed outright once nothing invoked it.
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
# "Migrate to V3 based sonatype secret (confluentinc#916)", 2026-03-24.
FORK_POINT="${COPYRIGHT_CHECK_FORK_POINT:-7f2901226bccac68a2a71f0d9da343887b1abb46}"
FORK_HOLDER="Antony Stubbs and contributors"
HEADER_WINDOW=8 # lines from the top of the file searched for copyright notices

# The fork's package move, as PATH prefixes: 'current-form|fork-point-form'. Every path this
# script handles - the files it scans and both halves of the manifests below - is mapped through
# this table before it is looked up, so a file keeps its provenance across the move.
#
# A SMALL TABLE, NOT ~200 RENAMED_FROM_UPSTREAM ENTRIES. The move is a rule, so it is written as
# one (or a handful): a per-file manifest of everything that moved would be unreviewable, and the
# next file to move under a moved package would silently not be in it. Derived, this needs no
# maintenance.
#
# THE NESTED ENTRIES MUST STAY ABOVE THE GENERAL ONE. Both packages under the second upstream-owned
# prefix fold INTO the library's internals - `io/confluent/csid/utils` to
# `bz/stub/parallelconsumer/internal/utils` and `io/confluent/csid/testcontainers` to
# `bz/stub/parallelconsumer/internal/testcontainers` (see bin/rename-packages.sh for the reasoning).
# Their bz/stub halves sit INSIDE the general `bz/stub/parallelconsumer` prefix on the last line, and
# fork_point_path() below returns on the FIRST prefix match it finds - so if the general rule were
# checked first, every file the nested rules actually moved would match it too and resolve to a
# fork-point path under parallelconsumer that upstream never owned. That is not a near miss: the
# resulting path holds no blob at the fork point, so the file is judged fork-original and its
# REQUIRED upstream header is reported as a violation. bin/test-check-copyright-headers.sh runs that
# reordering as a control arm and asserts the extra violations appear, so the ordering is measured
# rather than asserted in a comment.
#
# THERE IS NO GENERAL RULE FOR THE SECOND PREFIX ANY MORE. It used to map onto a same-named prefix
# under bz/stub, which carried upstream's mark into the new namespace; bin/rename-packages.sh
# deleted the equivalent rule and now REFUSES to run on a package no rule names. Both tables list
# every package explicitly, and that is the invariant the drift guard below protects.
#
# NOT SHARED WITH bin/rename-packages.sh, DELIBERATELY. That script is a migration tool and is
# deleted once the rename has landed on every branch; this table describes the fork point, which is
# immutable history, so it must outlive it. Wiring a permanent gate - one bound to maven's validate
# phase - into a disposable script buys DRY at the cost of the gate breaking when the tool goes.
# bin/test-check-copyright-headers.sh cross-checks EVERY rule here against bin/rename-packages.sh's
# PKG_MAP while both exist, so the only real drift risk (someone changing a target package before
# the rename lands) fails loudly. It reads this one with an anchored grep, which is why entries sit
# flush against the opening quote.
PACKAGE_MOVES="
bz/stub/parallelconsumer/internal/utils|io/confluent/csid/utils
bz/stub/parallelconsumer/internal/testcontainers|io/confluent/csid/testcontainers
bz/stub/parallelconsumer|io/confluent/parallelconsumer"

# Sets FP_PATH rather than echoing it: this runs once per java file, and a command substitution
# would fork a subshell each time on a script that maven runs at the start of every build.
FP_PATH=""
fork_point_path() { # <path in the working tree> -> FP_PATH = the path it had at the fork point
    local p="$1" new old
    while IFS='|' read -r new old; do
        [ -n "$new" ] || continue
        case "$p" in
            "$new"/*)   FP_PATH="${old}${p#"$new"}"; return 0 ;;
            */"$new"/*) FP_PATH="${p%%/"$new"/*}/${old}/${p#*/"$new"/}"; return 0 ;;
        esac
    done <<EOF
$PACKAGE_MOVES
EOF
    FP_PATH="$p"
}

fp_path_of() { # <path> -> prints the fork-point path (for the manifest normalisation below)
    fork_point_path "$1"
    printf '%s' "$FP_PATH"
}

# Fork-side RENAMES of upstream files: 'newpath|oldpath-at-fork-point'. They keep the
# Confluent header; if their content has diverged from the fork-point blob they must
# also carry the modifications line. (The first three were relocated in fface195; the
# MockConsumer* trio was renamed so surefire would actually collect them - their old
# names matched none of its include patterns, so they had never run in CI.)
#
# These paths move with the package rename too, and BOTH halves are normalised through
# PACKAGE_MOVES before use, so an entry matches whichever spelling it is written in. The
# newpath half is a current path and bin/rename-packages.sh retargets it; the oldpath
# half names a path in the UPSTREAM tree and must keep saying io/confluent - normalising
# it as well is a no-op on a correct manifest and repairs one that a bulk rewrite got at.
RENAMED_FROM_UPSTREAM="
parallel-consumer-examples/parallel-consumer-example-metrics/src/test/java/bz/stub/parallelconsumer/examples/metrics/integrationTests/CoreAppMetricsIntegrationTest.java|parallel-consumer-examples/parallel-consumer-example-metrics/src/test/java/io/confluent/parallelconsumer/examples/metrics/CoreAppMetricsIntegrationTest.java
parallel-consumer-examples/parallel-consumer-example-metrics/src/test/java/bz/stub/parallelconsumer/examples/metrics/integrationTests/PrometheusContainer.java|parallel-consumer-examples/parallel-consumer-example-metrics/src/test/java/io/confluent/parallelconsumer/examples/metrics/PrometheusContainer.java
parallel-consumer-examples/parallel-consumer-example-streams/src/test/java/bz/stub/parallelconsumer/examples/streams/integrationTests/StreamsAppTest.java|parallel-consumer-examples/parallel-consumer-example-streams/src/test/java/io/confluent/parallelconsumer/examples/streams/StreamsAppTest.java
parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/MockConsumerCommitTimeoutTest.java|parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/MockConsumerTestWithCommitTimeoutException.java
parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/MockConsumerSaslAuthenticationTest.java|parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/MockConsumerTestWithSaslAuthenticationException.java
parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/MockConsumerEarlyCloseTest.java|parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/MockConsumerTestWithEarlyClose.java
${COPYRIGHT_CHECK_EXTRA_RENAMES:-}
"

# EXTRACTIONS of upstream-derived code into new files (no single origin path): always
# Confluent + modifications line. ManagedPCInstance lives on the chaos-suite branch
# stack, not yet on master - listing it early is harmless (only tracked files are
# checked) and stops the check ambushing the stack at merge (see PR astubbs#83).
EXTRACTED_FROM_UPSTREAM="
parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/utils/ManagedPCInstance.java
parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/MockConsumerTestBase.java
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

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
TAB=$'\t'

# The fork-point tree as 'blob-sha<TAB>path', java only - it is the only extension asked about.
# Both membership ("was this file upstream?") and modification ("has it changed since?") are
# answered from this ONE listing, which is what makes the answer independent of the file's current
# path: a moved file is looked up under the path it had HERE. The old design answered the second
# question from `git diff --name-only`, a list of CURRENT names, which cannot describe a file that
# has moved - the two halves of the model disagreed the moment a package moved.
git ls-tree -r "$FORK_POINT" > "$TMP/fork-tree.txt"
awk -F'\t' '$2 ~ /\.java$/ { split($1, a, " "); print a[3] "\t" $2 }' "$TMP/fork-tree.txt" \
    | LC_ALL=C sort -t"$TAB" -k2,2 > "$TMP/fork-blobs.tsv"

# Working-tree blob hashes for every java file that exists on disk, in ONE `git hash-object` call
# (0.06s for 233 files) rather than a `git cat-file | cmp` per file. That matters: this script is
# bound to maven's validate phase, so its cost is paid by every build. Hashing the working tree,
# not the index, keeps the documented behaviour that uncommitted local edits count as modified.
: > "$TMP/files.txt"
while IFS= read -r f; do
    [ -f "$f" ] || continue
    printf '%s\n' "$f" >> "$TMP/files.txt"
done <<EOF
$(git ls-files '*.java')
EOF
# `if`, not `[ -s f ] && cmd`: under `set -e` that list exits the script when the test is false.
: > "$TMP/shas.txt"
if [ -s "$TMP/files.txt" ]; then
    git hash-object --stdin-paths < "$TMP/files.txt" > "$TMP/shas.txt"
fi
paste "$TMP/files.txt" "$TMP/shas.txt" > "$TMP/pairs.tsv"

fork_point_sha() { # <fork-point path> -> blob sha, empty if that path held no java blob there
    # reads a FILE, not a pipe: `awk ... exit` on the reading end of a pipe would SIGPIPE the
    # writer, and under pipefail a MATCH would then read as failure
    awk -F'\t' -v p="$1" '$2 == p { print $1; exit }' "$TMP/fork-blobs.tsv"
}

# Resolve each file's fork-point path and pair it with its fork-point blob in ONE `join`, rather
# than an `awk` lookup per file: 233 extra process spawns doubled this script's runtime when it
# was written that way, and maven pays it on every build. LC_ALL=C so both sides collate alike.
while IFS="$TAB" read -r f cur_sha; do
    fork_point_path "$f"
    printf '%s\t%s\t%s\n' "$FP_PATH" "$f" "$cur_sha"
done < "$TMP/pairs.tsv" | LC_ALL=C sort -t"$TAB" -k1,1 > "$TMP/by-fork-path.tsv"
# -a 1: keep files with no fork-point counterpart (fork-original), with an empty sha field.
LC_ALL=C join -t"$TAB" -1 1 -2 2 -a 1 -e '' -o 1.1,1.2,1.3,2.1 \
    "$TMP/by-fork-path.tsv" "$TMP/fork-blobs.tsv" > "$TMP/resolved.tsv"

# Normalise both halves of the manifests to fork-point spelling, once, so the per-file lookups
# below compare like with like whichever spelling the entries happen to be written in. Held in
# arrays and searched in pure bash: these lists are a handful of entries, and a `grep`/`awk` per
# scanned file is a process spawn per file for a seven-line lookup.
rename_new=(); rename_old=(); n_renames=0
while IFS= read -r entry; do
    case "$entry" in *'|'*) ;; *) continue ;; esac
    rename_new[$n_renames]="$(fp_path_of "${entry%%|*}")"
    rename_old[$n_renames]="$(fp_path_of "${entry#*|}")"
    n_renames=$((n_renames + 1))
done <<EOF
$RENAMED_FROM_UPSTREAM
EOF

extracted=(); n_extracted=0
while IFS= read -r entry; do
    [ -n "$entry" ] || continue
    extracted[$n_extracted]="$(fp_path_of "$entry")"
    n_extracted=$((n_extracted + 1))
done <<EOF
$EXTRACTED_FROM_UPSTREAM
EOF

registered_rename() { # <fork-point path> -> sets RENAME_ORIGIN; 1 if the path is not registered
    # exact match on the newpath field - a substring match would misroute files whose path is a
    # tail-substring of a registered newpath into the rename branch
    local i=0
    RENAME_ORIGIN=""
    while [ "$i" -lt "$n_renames" ]; do
        if [ "${rename_new[$i]}" = "$1" ]; then RENAME_ORIGIN="${rename_old[$i]}"; return 0; fi
        i=$((i + 1))
    done
    return 1
}

registered_extraction() { # <fork-point path>
    local i=0
    while [ "$i" -lt "$n_extracted" ]; do
        if [ "${extracted[$i]}" = "$1" ]; then return 0; fi
        i=$((i + 1))
    done
    return 1
}

fails=0
checked=0

require_confluent() { # <file> <header>
    # Membership/pattern tests use herestrings, never `printf | grep -q`: grep -q exits at first
    # match, printf takes SIGPIPE, and under pipefail the pipeline then reads as FALSE - randomly
    # misclassifying files that DID match (seen in CI: an upstream file flagged as fork-original).
    if ! grep -q "Copyright (C)" <<< "$2"; then
        echo "FAIL (upstream-derived file has no copyright header): $1"
        return 1
    elif ! grep -q "Confluent" <<< "$2"; then
        echo "FAIL (upstream-derived file lost its Confluent header): $1"
        return 1
    fi
}

require_modifications_line() { # <file> <header> <reason>
    # The phrase and the holder must be on the SAME line: a mere mention of the
    # holder elsewhere in the header (e.g. an @author byline) is not a notice.
    # Years are deliberately not policed (see above).
    if ! grep -q "Modifications Copyright (C).*${FORK_HOLDER}" <<< "$2"; then
        echo "FAIL ($3 but missing 'Modifications Copyright ... ${FORK_HOLDER}' line): $1"
        return 1
    fi
}

# fp_path is THE line the whole provenance model turns on: every lookup below asks about the path
# the file had at the FORK POINT, never the path it has now. It is identity for a file that has
# not moved, so an un-renamed tree behaves exactly as it did before this was introduced.
while IFS=$'\t' read -r fp_path f cur_sha fp_sha; do
    [ -n "$f" ] || continue
    checked=$((checked + 1))
    header=$(head -"$HEADER_WINDOW" "$f")

    if registered_rename "$fp_path"; then
        require_confluent "$f" "$header" || { fails=$((fails + 1)); continue; }
        if [ "$(fork_point_sha "$RENAME_ORIGIN")" != "$cur_sha" ]; then
            require_modifications_line "$f" "$header" \
                "renamed upstream file modified since the fork point" || fails=$((fails + 1))
        fi
    elif registered_extraction "$fp_path"; then
        require_confluent "$f" "$header" || { fails=$((fails + 1)); continue; }
        require_modifications_line "$f" "$header" \
            "extraction of upstream-derived code" || fails=$((fails + 1))
    elif [ -n "$fp_sha" ]; then
        require_confluent "$f" "$header" || { fails=$((fails + 1)); continue; }
        if [ "$fp_sha" != "$cur_sha" ]; then
            require_modifications_line "$f" "$header" \
                "upstream-derived file modified since the fork point" || fails=$((fails + 1))
        fi
    else
        # fork-original: fork header required, Confluent claim forbidden
        if grep -q "Confluent" <<< "$header"; then
            echo "FAIL (fork-original file claims Confluent copyright): $f"
            fails=$((fails + 1))
        elif ! grep -q "$FORK_HOLDER" <<< "$header"; then
            echo "FAIL (fork-original file missing '${FORK_HOLDER}' header): $f"
            fails=$((fails + 1))
        fi
    fi
done < "$TMP/resolved.tsv"

echo "Checked ${checked} java files against fork point ${FORK_POINT} - ${fails} violation(s)."
[ "$fails" -eq 0 ]
