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
# EVERY TRACKED FILE IS CLASSIFIED, AND "NOT CLASSIFIED" IS A FAILURE. This scanner covered
# `.java` only until the fork grew seven client libraries in seven other languages; the non-Java
# tree was being hand-checked, which is to say not checked. The replacement is not a longer
# extension list - a list alone rots the moment an eighth language arrives, silently, which is the
# failure mode this repo has now recorded several times. It is three EXHAUSTIVE sets:
#
#   EXEMPT_PATHS    - path globs that carry no header, each with the REASON it does not
#   ENFORCED_TYPES  - filename globs that must, each with the comment syntax it uses
#   everything else - reported as a VIOLATION, naming the file and both tables
#
# So adding a `.zig` file, or a new generated-code directory, fails the build with a message
# telling you which table to add it to. A quiet skip is not reachable from here.
#
# GRANDFATHERING: AN UPSTREAM FILE THAT NEVER HAD A NOTICE IS NOT REQUIRED TO GAIN ONE. Upstream
# marked its `.java`, its poms, its shell scripts and some of its resources - but not its
# workflows, its IDE run configurations, its maven wrapper, or its prose. Demanding a Confluent
# header on those would mean STAMPING a notice onto a file Confluent chose not to mark, which
# neither Apache 2.0 s4(b) (retain the notices that exist) nor this repo's standing rule (never
# touch an existing file's header without a substantive change) supports. So a file whose
# fork-point blob carried no notice in its header window is passed and COUNTED as grandfathered.
# The test reads the fork-point blob, so it is a fact about immutable history and cannot rot; a
# hand-maintained exclusion list of the same files could.
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
# THE CONFLUENT TEST IS SAME-LINE, NOT A BARE MENTION OF THE WORD. `Confluent` appears in the
# PROSE of fork-original files that talk about the fork's own provenance - .github/workflows/
# copyright.yml opens by explaining the two-header policy, and bin/deps-version-rules.xml names
# the upstream artifacts. Under the old "does the header window contain the word" test, extending
# the scan past `.java` reported both as fork-original files claiming Confluent copyright. A claim
# is `Copyright (C) ... Confluent` on ONE line; the same reasoning already governed the
# modifications line, and this brings the other half of the check into line with it.
#
# Years are deliberately NOT policed: never bump copyright years as an incidental
# change (see AGENTS.md). This scanner IS the header enforcement. It replaced the
# mycila license-maven-plugin, which knew only ONE header template - running its
# format goal is what used to stamp Confluent headers onto fork-original files. That
# plugin was skipped from PR astubbs#90 and removed outright once nothing invoked it.
#
# Usage: bin/check-copyright-headers.sh [--report]
#        --report prints every tracked file with its classification, then exits 0. Use it to see
#        what the three tables actually cover, rather than trusting that they do.
# Exit codes: 0 = conformant, 1 = violations found, 2 = cannot run (shallow clone).
#
# Test-harness overrides (used by bin/test-check-copyright-headers.sh; not needed
# for normal use): COPYRIGHT_CHECK_FORK_POINT pins a different fork-point commit,
# COPYRIGHT_CHECK_EXTRA_RENAMES appends 'newpath|oldpath' lines,
# COPYRIGHT_CHECK_EXTRA_EXTRACTIONS appends extraction paths,
# COPYRIGHT_CHECK_EXTRA_RECOVERIES appends 'path|origin-commit' lines.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

REPORT=0
case "${1:-}" in
    --report) REPORT=1 ;;
    "") ;;
    *) echo "usage: $0 [--report]" >&2; exit 2 ;;
esac

# merge-base of the fork's master and confluentinc/parallel-consumer master:
# "Migrate to V3 based sonatype secret (confluentinc#916)", 2026-03-24.
FORK_POINT="${COPYRIGHT_CHECK_FORK_POINT:-7f2901226bccac68a2a71f0d9da343887b1abb46}"
FORK_HOLDER="Antony Stubbs and contributors"
HEADER_WINDOW=8 # lines from the top of the file searched for copyright notices

# --- WHAT CARRIES NO HEADER, AND WHY -------------------------------------------------------
# 'path glob|reason'. Matched against the FULL path, first match wins, and checked BEFORE
# ENFORCED_TYPES - so a rule here is an exception to the type rules below, not a gap in them.
#
# Every entry states why the file cannot or should not carry a notice. Four reasons recur, and
# they are the only ones that qualify: the file is REWRITTEN by a generator, so a header would be
# destroyed on the next run; its FORMAT has no comment syntax; it is VENDORED, so the header is
# its author's business and not ours; or it is PROSE, where the notice would render into the
# document a reader sees. "Nobody got round to it" is not on that list - it makes the file a
# violation, which is the point.
EXEMPT_PATHS="
*/_generated/*|generated by protoc - regeneration overwrites the whole directory
*/generated/*|generated by protoc/ts-proto - regeneration overwrites the whole directory
*_pb.rb|generated by protoc (ruby), regenerated in place
*.lock|dependency lockfile, written by the package manager
*/go.sum|dependency checksum file, written by the go toolchain
*/Package.resolved|SwiftPM dependency lockfile, written by swift package resolve - JSON, so no comment syntax either
*.json|JSON has no comment syntax - see docs/copyright.md for the in-band conventions that fit
*.sln|Visual Studio solution format carries no comment syntax
*.bin|binary test fixture
*/py.typed|PEP 561 marker, required by the spec to be empty
*/META-INF/services/*|JDK ServiceLoader registry - the file's entire content is the provider list
*.md|prose documentation - the notice would render into the document
*.adoc|prose documentation - the notice would render into the document
*.html|prose documentation - the notice would render into the document
LICENSE|the licence text itself
NOTICE|the fork's attribution notice itself - it is the thing headers point at
mvnw|vendored Maven wrapper, regenerated by mvn wrapper:wrapper
mvnw.cmd|vendored Maven wrapper, regenerated by mvn wrapper:wrapper
.mvn/*|vendored Maven wrapper configuration, regenerated by mvn wrapper:wrapper
.idea/*|IDE configuration, rewritten by IntelliJ
.claude/*|local agent-harness configuration, not shipped
*.gitignore|VCS configuration, not authored source
*.editorconfig|editor configuration, not authored source
*/.swift-format|swift-format configuration, and JSON has no comment syntax
.gitmessage|VCS commit template, not authored source
.github/CODEOWNERS|VCS configuration, not authored source
*/.bundle/config|bundler configuration, written by bundle config
"

# --- WHAT MUST CARRY ONE, AND IN WHICH COMMENT SYNTAX --------------------------------------
# 'filename glob|comment style'. Matched against the BASENAME, first match wins.
#
# The style is checked, not just the presence of the words: the notice line must actually be
# inside a comment for that language, and must not sit above a line that has to come first. A
# header pasted above a `#!` shebang or above an `<?xml ...?>` declaration breaks the file while
# still containing every string this scanner looks for, so both are named violations rather than
# something the next build discovers.
#
#   slash - // line comments, or a /* */ block whose continuation lines start with *
#   hash  - # line comments
#   xml   - the notice sits inside an <!-- --> block
#   cmd   - REM / @REM / :: (Windows batch)
#
# A file type with NO usable comment syntax does not belong here - it belongs in EXEMPT_PATHS
# with that as its reason. JSON is the classic: strict JSON has no comments at all, so the
# TypeScript client puts its notice in a `"//"` key (tsconfig.json) or the `description` string
# (package.json), neither of which a scanner can demand in general.
ENFORCED_TYPES="
*.java|slash
*.kt|slash
*.kts|slash
*.scala|slash
*.sbt|slash
*.go|slash
go.mod|slash
*.rs|slash
*.ts|slash
*.tsx|slash
*.js|slash
*.mjs|slash
*.cjs|slash
*.cs|slash
*.swift|slash
*.c|slash
*.cc|slash
*.cpp|slash
*.h|slash
*.hpp|slash
*.proto|slash
*.py|hash
*.pyi|hash
*.rb|hash
*.sh|hash
*.bash|hash
*.yml|hash
*.yaml|hash
*.toml|hash
*.properties|hash
*.cmake|hash
CMakeLists.txt|hash
Dockerfile|hash
Dockerfile.*|hash
Makefile|hash
Gemfile|hash
Rakefile|hash
*.xml|xml
*.csproj|xml
*.props|xml
*.targets|xml
*.cmd|cmd
*.bat|cmd
"

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
# deleted once the rename has landed; this table describes the fork point, which is
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

# Sets FP_PATH rather than echoing it: this runs once per file, and a command substitution
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

# RECOVERIES of upstream code from a branch that never reached the fork point:
# 'path|origin-commit'. Treated exactly as an extraction is - Confluent + modifications line -
# because that is what the file IS; the separate table exists because the REASON differs and the
# claim here is checkable, where an extraction's is not.
#
# WHY A THIRD TABLE AND NOT A LINE IN EXTRACTED_FROM_UPSTREAM. An extraction has no single origin
# path, so its provenance can only be asserted. A recovery has one, and the blob is still in this
# repository's history - just on a ref that is not an ancestor of the fork point. So the claim is
# VERIFIED below rather than trusted: the origin commit must actually contain the file at its
# fork-point path, and a manifest entry naming a commit that does not is a FAILURE. Filing these
# as extractions would have thrown that check away to save a table.
#
# THE ORIGIN PATH IS DERIVED, NOT LISTED. It is the file's fork-point path - PACKAGE_MOVES already
# maps bz/stub/parallelconsumer back to io/confluent/parallelconsumer, which is the spelling every
# pre-fork branch uses. A recovery that also MOVED within the tree does not fit this table; give it
# a RENAMED_FROM_UPSTREAM-shaped entry when one turns up, rather than bending this one.
#
# Demo.java is the code behind the asciinema cast README_TEMPLATE.adoc links
# (https://asciinema.org/a/404299), written at Confluent in 2021 on `origin/presentation` and never
# merged to any master. docs/inflight/branch-classic-comparison-demo.md is its rescue ledger, and
# records that the branch is to be archived as a tag once the rescue lands - the tag is what keeps
# this commit reachable, so deleting it without one turns the verification below into a warning.
RECOVERED_FROM_UPSTREAM_BRANCH="
parallel-consumer-vertx/src/test-integration/java/bz/stub/parallelconsumer/vertx/integrationTests/Demo.java|ffda9c6a3a9e06d948cc6130d7694b3562f63b92
${COPYRIGHT_CHECK_EXTRA_RECOVERIES:-}
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

# The two coverage tables, parsed ONCE into arrays. classify() runs per tracked file and a
# heredoc per file would be a temp file per file; the tables are a few dozen entries, so a bash
# loop over an array beats any process this could spawn instead.
ex_glob=(); ex_reason=(); n_exempt=0
while IFS='|' read -r g r; do
    [ -n "$g" ] || continue
    ex_glob[$n_exempt]="$g"; ex_reason[$n_exempt]="$r"; n_exempt=$((n_exempt + 1))
done <<EOF
$EXEMPT_PATHS
EOF

en_glob=(); en_style=(); n_enforced=0
while IFS='|' read -r g s; do
    [ -n "$g" ] || continue
    en_glob[$n_enforced]="$g"; en_style[$n_enforced]="$s"; n_enforced=$((n_enforced + 1))
done <<EOF
$ENFORCED_TYPES
EOF

CLASS=""; STYLE=""; EXEMPT_REASON=""
classify() { # <path> -> CLASS in enforce|exempt|unknown, plus STYLE or EXEMPT_REASON
    local p="$1" base="${1##*/}" i=0 first
    CLASS=""; STYLE=""; EXEMPT_REASON=""
    while [ "$i" -lt "$n_exempt" ]; do
        # shellcheck disable=SC2254  # the glob is the pattern, deliberately unquoted
        case "$p" in ${ex_glob[$i]}) CLASS=exempt; EXEMPT_REASON="${ex_reason[$i]}"; return 0 ;; esac
        i=$((i + 1))
    done
    i=0
    while [ "$i" -lt "$n_enforced" ]; do
        # shellcheck disable=SC2254
        case "$base" in ${en_glob[$i]}) CLASS=enforce; STYLE="${en_style[$i]}"; return 0 ;; esac
        i=$((i + 1))
    done
    # An extensionless file that opens with a shebang names its own language, so it needs no entry
    # in the table above - .githooks/pre-commit and any future one are covered without being listed.
    case "$base" in
        *.*) ;;
        *)  if [ -f "$p" ]; then
                first=""
                IFS= read -r first < "$p" || true
                case "$first" in '#!'*) CLASS=enforce; STYLE=hash; return 0 ;; esac
            fi ;;
    esac
    CLASS=unknown
}

# The fork-point tree as 'blob-sha<TAB>path', EVERY path - the scan is no longer java-only, and
# a filter here would silently make every non-java upstream file look fork-original (the same
# inversion the package move causes, from a different direction).
# Both membership ("was this file upstream?") and modification ("has it changed since?") are
# answered from this ONE listing, which is what makes the answer independent of the file's current
# path: a moved file is looked up under the path it had HERE. The old design answered the second
# question from `git diff --name-only`, a list of CURRENT names, which cannot describe a file that
# has moved - the two halves of the model disagreed the moment a package moved.
git ls-tree -r "$FORK_POINT" > "$TMP/fork-tree.txt"
awk -F'\t' '{ split($1, a, " "); print a[3] "\t" $2 }' "$TMP/fork-tree.txt" \
    | LC_ALL=C sort -t"$TAB" -k2,2 > "$TMP/fork-blobs.tsv"

# Classify every tracked file. Enforced ones go on to the header checks; exempt ones are counted
# by reason; anything in neither table is a violation in its own right.
git ls-files > "$TMP/tracked.txt"
: > "$TMP/files.txt"
: > "$TMP/styles-only.txt"
: > "$TMP/report.txt"
exempt_count=0
unknown_files=""
while IFS= read -r f; do
    [ -f "$f" ] || continue
    classify "$f"
    case "$CLASS" in
        enforce)
            printf '%s\n' "$f" >> "$TMP/files.txt"
            printf '%s\n' "$STYLE" >> "$TMP/styles-only.txt"
            if [ "$REPORT" = 1 ]; then printf 'enforce\t%s\t%s\n' "$STYLE" "$f" >> "$TMP/report.txt"; fi
            ;;
        exempt)
            exempt_count=$((exempt_count + 1))
            if [ "$REPORT" = 1 ]; then printf 'exempt\t%s\t%s\n' "$EXEMPT_REASON" "$f" >> "$TMP/report.txt"; fi
            ;;
        *)
            unknown_files="${unknown_files}${f}"$'\n'
            if [ "$REPORT" = 1 ]; then printf 'UNCLASSIFIED\t-\t%s\n' "$f" >> "$TMP/report.txt"; fi
            ;;
    esac
done < "$TMP/tracked.txt"

if [ "$REPORT" = 1 ]; then
    LC_ALL=C sort "$TMP/report.txt"
    exit 0
fi

# Working-tree blob hashes for every scanned file, in ONE `git hash-object` call rather than a
# `git cat-file | cmp` per file. That matters: this script is bound to maven's validate phase, so
# its cost is paid by every build. Hashing the working tree, not the index, keeps the documented
# behaviour that uncommitted local edits count as modified.
# `if`, not `[ -s f ] && cmd`: under `set -e` that list exits the script when the test is false.
: > "$TMP/shas.txt"
if [ -s "$TMP/files.txt" ]; then
    git hash-object --stdin-paths < "$TMP/files.txt" > "$TMP/shas.txt"
fi
# The comment style travels WITH the file through the join, rather than being looked up per file:
# a lookup is a process spawn per file, and this scan is now the whole tree.
paste "$TMP/files.txt" "$TMP/shas.txt" "$TMP/styles-only.txt" > "$TMP/pairs.tsv"

fork_point_sha() { # <fork-point path> -> blob sha, empty if that path held no blob there
    # reads a FILE, not a pipe: `awk ... exit` on the reading end of a pipe would SIGPIPE the
    # writer, and under pipefail a MATCH would then read as failure
    awk -F'\t' -v p="$1" '$2 == p { print $1; exit }' "$TMP/fork-blobs.tsv"
}

# Resolve each file's fork-point path and pair it with its fork-point blob in ONE `join`, rather
# than an `awk` lookup per file: 233 extra process spawns doubled this script's runtime when it
# was written that way, and maven pays it on every build. LC_ALL=C so both sides collate alike.
while IFS="$TAB" read -r f cur_sha style; do
    fork_point_path "$f"
    printf '%s\t%s\t%s\t%s\n' "$FP_PATH" "$f" "$cur_sha" "$style"
done < "$TMP/pairs.tsv" | LC_ALL=C sort -t"$TAB" -k1,1 > "$TMP/by-fork-path.tsv"
# -a 1: keep files with no fork-point counterpart (fork-original), with an empty sha field.
LC_ALL=C join -t"$TAB" -1 1 -2 2 -a 1 -e '' -o 1.1,1.2,1.3,1.4,2.1 \
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

recovered_path=(); recovered_commit=(); n_recovered=0
while IFS='|' read -r path commit; do
    [ -n "$path" ] || continue
    [ -n "$commit" ] || continue
    recovered_path[$n_recovered]="$(fp_path_of "$path")"
    recovered_commit[$n_recovered]="$commit"
    n_recovered=$((n_recovered + 1))
done <<EOF
$RECOVERED_FROM_UPSTREAM_BRANCH
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

registered_recovery() { # <fork-point path> -> sets RECOVERY_COMMIT; 1 if the path is not registered
    local i=0
    RECOVERY_COMMIT=""
    while [ "$i" -lt "$n_recovered" ]; do
        if [ "${recovered_path[$i]}" = "$1" ]; then
            RECOVERY_COMMIT="${recovered_commit[$i]}"; return 0
        fi
        i=$((i + 1))
    done
    return 1
}

# --- header reading and the four facts every rule needs --------------------------------------
# Read with bash, not `head`: the scan grew from 304 java files to the whole tree, and one spawn
# per file - plus the two or three `grep`s the checks used to run per file - is a cost maven pays
# on every single build. HDR_LINES is the window; the flags below are derived from it in one pass.
HDR_LINES=()
HAS_NOTICE=0; HAS_CONFLUENT_NOTICE=0; HAS_MODS=0; HAS_FORK_HOLDER=0; NOTICE_IDX=-1
read_header() { # <file>
    local line n=0
    HDR_LINES=()
    while IFS= read -r line || [ -n "$line" ]; do
        HDR_LINES+=("$line")
        n=$((n + 1))
        [ "$n" -ge "$HEADER_WINDOW" ] && break
    done < "$1"

    HAS_NOTICE=0; HAS_CONFLUENT_NOTICE=0; HAS_MODS=0; HAS_FORK_HOLDER=0; NOTICE_IDX=-1
    local i=0
    while [ "$i" -lt "${#HDR_LINES[@]}" ]; do
        line="${HDR_LINES[$i]}"
        case "$line" in
            *"Copyright (C)"*)
                HAS_NOTICE=1
                [ "$NOTICE_IDX" -lt 0 ] && NOTICE_IDX=$i
                # A CLAIM is the holder on the SAME line as the notice. A bare mention of
                # "Confluent" elsewhere in the header is prose, not a claim - see the header.
                case "$line" in *Confluent*) HAS_CONFLUENT_NOTICE=1 ;; esac
                case "$line" in
                    *"Modifications Copyright (C)"*)
                        case "$line" in *"$FORK_HOLDER"*) HAS_MODS=1 ;; esac ;;
                esac ;;
        esac
        # The fork-holder test is deliberately window-wide, not same-line: it answers "is this
        # file marked for the fork at all", and an @author byline is excluded by the mods-line
        # rule above, which is the check that actually needs a notice rather than a mention.
        case "$line" in *"$FORK_HOLDER"*) HAS_FORK_HOLDER=1 ;; esac
        i=$((i + 1))
    done
}

# The notice has to be inside a comment, and it has to be below anything the format requires to
# come first. Both are ways a header can be present and the file still broken - a `#` block above
# a shebang stops the script being executable, a comment above `<?xml ...?>` makes the document
# malformed - and neither is visible to a check that only looks for the words.
SYNTAX_REASON=""
syntax_violation() { # <style> -> sets SYNTAX_REASON; empty means the header is well placed
    # Sets a variable rather than printing one: `$(...)` would fork a subshell per scanned file,
    # and this now runs over the whole tree rather than the java subset.
    local style="$1" notice="${HDR_LINES[$NOTICE_IDX]}" trimmed i open=0
    trimmed="${notice#"${notice%%[![:space:]]*}"}" # strip leading whitespace
    SYNTAX_REASON=""
    case "$style" in
        hash)
            case "$trimmed" in
                '#'*) ;;
                *) SYNTAX_REASON='notice is not in a # comment'; return 0 ;;
            esac
            i=1
            while [ "$i" -lt "${#HDR_LINES[@]}" ]; do
                case "${HDR_LINES[$i]}" in
                    '#!'*) SYNTAX_REASON='header sits above the #! shebang, which must be the first line'
                           return 0 ;;
                esac
                i=$((i + 1))
            done ;;
        slash)
            case "$trimmed" in
                '//'*|'/*'*|'*'*) ;;
                *) SYNTAX_REASON='notice is not in a // or /* */ comment'; return 0 ;;
            esac ;;
        xml)
            # inside an <!-- --> block: some comment opened at or before the notice line and
            # nothing closed it in between
            i=0
            while [ "$i" -le "$NOTICE_IDX" ]; do
                case "${HDR_LINES[$i]}" in *'<!--'*) open=1 ;; esac
                if [ "$i" -lt "$NOTICE_IDX" ]; then
                    case "${HDR_LINES[$i]}" in *'-->'*) open=0 ;; esac
                fi
                i=$((i + 1))
            done
            if [ "$open" != 1 ]; then
                SYNTAX_REASON='notice is not inside an <!-- --> comment'; return 0
            fi
            i=1
            while [ "$i" -lt "${#HDR_LINES[@]}" ]; do
                case "${HDR_LINES[$i]}" in
                    '<?xml'*) SYNTAX_REASON='header sits above the <?xml ...?> declaration, which must be the first line'
                              return 0 ;;
                esac
                i=$((i + 1))
            done ;;
        cmd)
            case "$trimmed" in
                REM*|rem*|@REM*|@rem*|'::'*) ;;
                *) SYNTAX_REASON='notice is not in a REM / :: comment'; return 0 ;;
            esac ;;
    esac
}

# An upstream file that carried NO notice at the fork point is not required to grow one - see the
# grandfathering paragraph in this script's header. Judged from the fork-point blob, so it is a
# fact about immutable history. Evaluated LAZILY, only for a file that would otherwise be
# reported, so a green tree pays nothing for it.
grandfathered() { # <fork-point blob sha> -> 0 if that blob had no notice in its header window
    [ -n "$1" ] || return 1
    local content line n=0
    # No `| head`: head would exit early, SIGPIPE `git cat-file`, and pipefail would turn a
    # perfectly good read into a failure. Blobs here are source files; reading one whole is fine.
    content="$(git cat-file blob "$1" 2>/dev/null || true)"
    while IFS= read -r line; do
        case "$line" in *"Copyright"*) return 1 ;; esac
        n=$((n + 1))
        [ "$n" -ge "$HEADER_WINDOW" ] && break
    done <<EOF
$content
EOF
    return 0
}

fails=0
checked=0
grandfathered_count=0

require_confluent() { # <file> <fork-point blob sha of its origin>
    if [ "$HAS_NOTICE" = 0 ]; then
        echo "FAIL (upstream-derived file has no copyright header): $1"
        return 1
    elif [ "$HAS_CONFLUENT_NOTICE" = 0 ]; then
        echo "FAIL (upstream-derived file lost its Confluent header): $1"
        return 1
    fi
}

require_modifications_line() { # <file> <reason>
    # The phrase and the holder must be on the SAME line: a mere mention of the
    # holder elsewhere in the header (e.g. an @author byline) is not a notice.
    # Years are deliberately not policed (see above).
    if [ "$HAS_MODS" = 0 ]; then
        echo "FAIL ($2 but missing 'Modifications Copyright ... ${FORK_HOLDER}' line): $1"
        return 1
    fi
}

require_comment_syntax() { # <file> <style>
    [ "$NOTICE_IDX" -ge 0 ] || return 0
    syntax_violation "$2"
    if [ -n "$SYNTAX_REASON" ]; then
        echo "FAIL ($SYNTAX_REASON): $1"
        return 1
    fi
}

# fp_path is THE line the whole provenance model turns on: every lookup below asks about the path
# the file had at the FORK POINT, never the path it has now. It is identity for a file that has
# not moved, so an un-renamed tree behaves exactly as it did before this was introduced.
while IFS=$'\t' read -r fp_path f cur_sha style fp_sha; do
    [ -n "$f" ] || continue
    checked=$((checked + 1))
    read_header "$f"

    if registered_rename "$fp_path"; then
        origin_sha="$(fork_point_sha "$RENAME_ORIGIN")"
        if ! require_confluent "$f" > "$TMP/msg" ; then
            if grandfathered "$origin_sha"; then
                grandfathered_count=$((grandfathered_count + 1)); continue
            fi
            cat "$TMP/msg"; fails=$((fails + 1)); continue
        fi
        if [ "$origin_sha" != "$cur_sha" ]; then
            require_modifications_line "$f" \
                "renamed upstream file modified since the fork point" || fails=$((fails + 1))
        fi
    elif registered_extraction "$fp_path"; then
        require_confluent "$f" || { fails=$((fails + 1)); continue; }
        require_modifications_line "$f" \
            "extraction of upstream-derived code" || fails=$((fails + 1))
    elif registered_recovery "$fp_path"; then
        # Verify the claim before enforcing on it. A missing COMMIT is the shallow-clone case the
        # fork-point guard above already tolerates, so it warns; a commit that is present but does
        # not hold the file is a WRONG entry, and that fails.
        if ! git cat-file -e "${RECOVERY_COMMIT}^{commit}" 2>/dev/null; then
            echo "WARNING: recovery origin ${RECOVERY_COMMIT} not in history - provenance of $f unverified." >&2
        elif ! git cat-file -e "${RECOVERY_COMMIT}:${fp_path}" 2>/dev/null; then
            echo "FAIL (recovery origin ${RECOVERY_COMMIT} does not contain ${fp_path}): $f"
            fails=$((fails + 1)); continue
        fi
        require_confluent "$f" || { fails=$((fails + 1)); continue; }
        require_modifications_line "$f" \
            "recovery of upstream code from an unmerged branch" || fails=$((fails + 1))
    elif [ -n "$fp_sha" ]; then
        if ! require_confluent "$f" > "$TMP/msg" ; then
            if grandfathered "$fp_sha"; then
                grandfathered_count=$((grandfathered_count + 1)); continue
            fi
            cat "$TMP/msg"; fails=$((fails + 1)); continue
        fi
        if [ "$fp_sha" != "$cur_sha" ]; then
            require_modifications_line "$f" \
                "upstream-derived file modified since the fork point" || fails=$((fails + 1))
        fi
    else
        # fork-original: fork header required, Confluent claim forbidden
        if [ "$HAS_CONFLUENT_NOTICE" = 1 ]; then
            echo "FAIL (fork-original file claims Confluent copyright): $f"
            fails=$((fails + 1))
            continue
        elif [ "$HAS_FORK_HOLDER" = 0 ]; then
            echo "FAIL (fork-original file missing '${FORK_HOLDER}' header): $f"
            fails=$((fails + 1))
            continue
        fi
    fi
    require_comment_syntax "$f" "$style" || fails=$((fails + 1))
done < "$TMP/resolved.tsv"

# A file in neither table is a violation, not a skip: see the three-sets paragraph in the header.
while IFS= read -r f; do
    [ -n "$f" ] || continue
    echo "FAIL (unclassified file type - add it to ENFORCED_TYPES or EXEMPT_PATHS in bin/check-copyright-headers.sh): $f"
    fails=$((fails + 1))
done <<EOF
$unknown_files
EOF

echo "Checked ${checked} file(s) against fork point ${FORK_POINT} - ${fails} violation(s)."
echo "  ${grandfathered_count} upstream file(s) grandfathered (carried no notice at the fork point); ${exempt_count} path(s) exempt by rule - run with --report to see every classification."
[ "$fails" -eq 0 ]
