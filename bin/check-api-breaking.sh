#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Java API-compatibility gate for the published modules - the sibling of bin/check-proto-breaking.sh,
# which does the same job for the proxy protocol's schema. Same problem, different schema language:
# what a downstream caller compiled against must keep working, and any break must be deliberate.
#
# Compares each published module's built jar against the SAME module last published to Maven Central.
# The baseline is master's snapshot (publish.yml redeploys it on every green push to master), so the
# question this answers today - before any release exists - is "does this branch change the published
# surface?". Point JAPICMP_BASELINE_VERSION at a release once one exists and the question becomes
# "does this release break users?"; both are the same comparison against a different left-hand side.
#
# BEFORE THE FIRST PUBLISH THERE IS NOTHING TO COMPARE AGAINST. The check says so and passes, arming
# itself the moment a baseline exists on the remote - the same grace branch bin/check-proto-breaking.sh
# uses, and for the same reason: a gate that cannot exist yet must not block every PR until it can.
#
# WHY BOTH SIDES ARE RESOLVED TO EXPLICIT FILES, AND WHY THE HASHES ARE COMPARED.
# The published baseline carries the SAME Maven version string as the working tree (both are
# 0.6.0.0-SNAPSHOT). A coordinate-based comparison therefore resolves BOTH sides to the local
# reactor/.m2 artifact, and japicmp cheerfully compares the new jar against itself and prints
# "No changes." - green, forever, whatever you break. Three separate ways of getting this wrong were
# hit while building this script, and every one of them was GREEN:
#   1. <oldVersion> as a coordinate            -> compared the new jar to itself
#   2. the rebuild failed (missing -am)        -> compared a STALE jar, break invisible
#   3. the rebuild failed (test compile error) -> compared a STALE jar, break invisible
# So this script (a) resolves both sides to paths, (b) refuses to run if they are the same file or
# the same bytes, and (c) refuses to run if the "new" jar is older than its own sources. Each exits 2
# - "cannot run" - which is NOT a pass. The repo's name for this failure class:
# docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md
#
# The self-test that proves the gate can still say no is bin/test-check-api-breaking.sh, and CI runs
# it BEFORE this script, the same order as the proto, docs-data and copyright scanners.
#
# Env overrides (the self-test uses them):
#   JAPICMP_BASELINE_VERSION  baseline Maven version (default: the project's own, i.e. master's snapshot)
#   JAPICMP_BASELINE_JAR      a jar to use as the baseline, replacing the download entirely
#   JAPICMP_MODULES           space-separated module list to check
#
# Usage: bin/check-api-breaking.sh
# Exit codes: 0 = compatible (or no baseline published yet), 1 = breaking change,
#             2 = cannot run (no network, jar missing, or both sides resolved to the same artifact).

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

SNAPSHOT_REPO=https://central.sonatype.com/repository/maven-snapshots
GROUP_PATH=bz/stub/parallelconsumer

# The published modules. parallel-consumer-examples and its children are deploy-skipped (see the
# examples pom's skipPublishing), so they have no published surface and are deliberately absent.
DEFAULT_MODULES="parallel-consumer-core parallel-consumer-vertx parallel-consumer-reactor parallel-consumer-mutiny"
MODULES=${JAPICMP_MODULES:-$DEFAULT_MODULES}

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

die_cannot_run() {
    echo "::error::api-compat gate CANNOT RUN: $1" >&2
    echo "A gate that cannot compare what it guards has not run, and must not report success." >&2
    exit 2
}

project_version=$(./mvnw -q help:evaluate -Dexpression=project.version -DforceStdout 2>/dev/null | tail -1)
[ -n "$project_version" ] || die_cannot_run "could not read project.version"
BASELINE_VERSION=${JAPICMP_BASELINE_VERSION:-$project_version}

echo "Project version: $project_version"
echo "Baseline version: $BASELINE_VERSION"
echo "Modules: $MODULES"
echo

# Resolve the newest published jar for a module, or print nothing when none is published.
resolve_baseline_url() {
    local module=$1 meta ts
    meta="$SNAPSHOT_REPO/$GROUP_PATH/$module/$BASELINE_VERSION/maven-metadata.xml"
    ts=$(curl -sS --fail "$meta" 2>/dev/null | sed -n 's|.*<value>\(.*\)</value>.*|\1|p' | head -1) || true
    [ -n "$ts" ] || return 0
    echo "$SNAPSHOT_REPO/$GROUP_PATH/$module/$BASELINE_VERSION/$module-$ts.jar"
}

overall=0
checked=0
ungated=0

for module in $MODULES; do
    new_jar="$module/target/$module-$project_version.jar"
    if [ ! -f "$new_jar" ]; then
        die_cannot_run "$module: built jar not found at $new_jar - run a package build first"
    fi

    # (c) the built jar must not predate its own sources: a stale jar is how a failed rebuild
    # turns into a green gate (failure 2 and 3 in the header).
    newest_src=$(find "$module/src/main" -name '*.java' -newer "$new_jar" -print -quit 2>/dev/null || true)
    if [ -n "$newest_src" ]; then
        die_cannot_run "$module: $new_jar is older than $newest_src - the rebuild did not happen"
    fi

    if [ -n "${JAPICMP_BASELINE_JAR:-}" ]; then
        base_jar=$JAPICMP_BASELINE_JAR
    else
        url=$(resolve_baseline_url "$module")
        if [ -z "$url" ]; then
            echo "  $module: nothing published at $BASELINE_VERSION yet - nothing to compare against."
            ungated=$((ungated + 1))
            continue
        fi
        base_jar="$WORK/$module-baseline.jar"
        curl -sS --fail -o "$base_jar" "$url" || die_cannot_run "$module: could not download baseline $url"
    fi

    [ -f "$base_jar" ] || die_cannot_run "$module: baseline jar $base_jar does not exist"

    # (a)/(b) the two sides must be genuinely different artifacts.
    if [ "$(realpath "$base_jar")" = "$(realpath "$new_jar")" ]; then
        die_cannot_run "$module: baseline and new resolved to THE SAME FILE ($new_jar)"
    fi

    report="$module/target/japicmp/api-breaking.diff"
    set +e
    ./mvnw --batch-mode -Papi-compat -pl "$module" -Dmaven.test.skip=true -Dcopyright.skip=true \
        -Djapicmp.baseline.jar="$(realpath "$base_jar")" \
        -Djapicmp.new.jar="$(realpath "$new_jar")" \
        japicmp:cmp >"$WORK/$module.log" 2>&1
    mvn_rc=$?
    set -e
    [ $mvn_rc -eq 0 ] || { cat "$WORK/$module.log" >&2; die_cannot_run "$module: japicmp itself failed"; }

    generated="$module/target/japicmp/default-cli.diff"
    [ -f "$generated" ] || die_cannot_run "$module: japicmp produced no report at $generated"
    mkdir -p "$(dirname "$report")" && cp "$generated" "$report"

    checked=$((checked + 1))

    # japicmp marks binary/source-incompatible entries with a leading '!'. Lines starting '---!' are
    # removals, '***!' modified classes. Presence of any '!' marker is a break.
    if grep -qE '^\s*(---|\+\+\+|\*\*\*)!' "$report"; then
        echo "  $module: BREAKING CHANGES against $BASELINE_VERSION"
        grep -E '^\s*(---|\+\+\+|\*\*\*)!|^Semantic versioning' "$report" | sed 's/^/      /'
        overall=1
    else
        echo "  $module: compatible"
    fi
done

echo
if [ "$checked" -eq 0 ]; then
    echo "No module had a published baseline ($ungated skipped). The gate arms itself once one exists."
    exit 0
fi
[ "$overall" -eq 0 ] && echo "All $checked checked module(s) are API-compatible with $BASELINE_VERSION."
exit "$overall"
