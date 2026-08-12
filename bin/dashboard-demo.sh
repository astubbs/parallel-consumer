#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# The one command behind the Parallel Consumer dashboard demo: brings up a Kafka broker, a topic, a
# workload, a fleet of Parallel Consumer instances and the dashboard itself, then drives the whole
# thing through the showcase scenario while you watch the graphs move.
#
#   bin/dashboard-demo.sh                 # loop forever, Ctrl-C to stop - the thing you watch
#   bin/dashboard-demo.sh --once          # one deterministic sweep; exit 0 only if every phase
#                                         # produced the condition it declared - the thing CI runs
#   bin/dashboard-demo.sh --seed=12345    # replay a run exactly (every run logs its own seed)
#   bin/dashboard-demo.sh --port=9000     # first port to try (the server walks upward from there)
#   bin/dashboard-demo.sh --no-open       # do not open a browser (loop mode opens one by default)
#   bin/dashboard-demo.sh --rebuild       # build even if nothing changed - see "Skipping the build"
#
# Exit codes:
#   0  everything the scenario declared actually happened
#   1  a phase postcondition failed, or the demo threw - the run is named in the output
#   2  bad arguments
#   3  cannot run at all (no Docker, or the build failed) - see the single line printed
#
# The only prerequisite is a running Docker daemon: the broker is a Testcontainers container, so
# there is nothing to install, configure or clean up afterwards.
#
# SKIPPING THE BUILD
#
# Maven costs about 20 seconds on this reactor whether or not anything changed, which is most of the
# gap between "let me show you this" and showing it. So the build runs only when its output is
# actually out of date, and the resolved classpath is cached in the module's own target/ rather than
# re-resolved into a fresh mktemp on every run.
#
# A cache you keep is a cache that can lie, so every way this one could go stale is checked before it
# is trusted - see build_is_fresh(). `--rebuild` forces the build regardless, for when you want
# certainty rather than a check.
#
# The scenario itself lives in ShowcaseScenario (parallel-consumer-dashboard test-integration) and
# is asserted by ShowcaseScenarioIT - this script is only the front door.

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

MODULE="parallel-consumer-dashboard"
MAIN_CLASS="bz.stub.parallelconsumer.dashboard.integrationTests.DemoMain"
# Pinned, and matched to the version the root pom manages - an unpinned goal invocation would
# silently float to whatever is newest on Central the day someone runs the demo.
DEPENDENCY_PLUGIN="org.apache.maven.plugins:maven-dependency-plugin:3.11.0"

# Both live under the module's target/, which is the point: `mvn clean` deletes the compiled output
# and the cache that describes it in the same stroke, so the two can never disagree about whether a
# build happened. Absolute, because maven-dependency-plugin resolves a relative mdep.outputFile
# against each reactor module's own basedir - a relative path here would scatter one file per module.
BUILD_DIR="$REPO_ROOT/$MODULE/target"
CP_FILE="$BUILD_DIR/dashboard-demo-classpath.txt"
STAMP_FILE="$BUILD_DIR/dashboard-demo-build.stamp"
# The one class the whole run turns on, so "is it built?" can be answered about the actual artefact
# rather than about a directory that happens to exist.
# The replacement is an UNESCAPED `/`: `${x//./\/}` inserts literal backslashes, which silently
# produces a path that never exists - so the freshness check would fail forever and Maven would run
# every time, looking exactly like the cache does not work.
MAIN_CLASS_FILE="$BUILD_DIR/test-classes/${MAIN_CLASS//.//}.class"

# Everything whose change could make the previous build's output wrong. The first five are exactly
# what the reactor `-pl $MODULE -am` builds - the parent pom, core, and the dashboard. Editing another
# module (vertx, reactor, mutiny, examples) cannot change what this demo runs, so it must not cost a
# rebuild. Directories are listed whole: `find -newer` reports a directory whose own mtime moved,
# which is how a deleted or renamed source is caught as well as an edited one.
#
# The last three are the build's instructions rather than its inputs. This script is on the list
# because it names the Maven goals and the pinned plugin version - change those and the cached
# classpath was produced by a command that no longer exists.
WATCHED_INPUTS=(
    "pom.xml"
    "parallel-consumer-core/pom.xml"
    "$MODULE/pom.xml"
    "parallel-consumer-core/src"
    "$MODULE/src"
    "bin/dashboard-demo.sh"
    "mvnw"
    ".mvn"
)

# NO EXIT TRAP, deliberately. This script used to mktemp a classpath file and remove it in a trap;
# now that the classpath lives in target/ there is nothing to clean up, and no trap is the safest
# state to be in. If you ever add one back: an EXIT trap's own last command becomes the script's exit
# status, so it MUST capture $? first and re-exit with it, or a failed --once run reports as a pass.
# Same foot-gun documented at length in bin/chaos-test.sh.

# One line, actionable, no stack trace. A Testcontainers failure with no daemon is 40 lines of
# reflection that never says "start Docker", which is the only thing the reader needs to know.
die() {
    echo "dashboard-demo: $*" >&2
    exit 3
}

# `--rebuild` is this script's, not the demo's; everything else is passed straight through so the
# JVM stays the single authority on what an argument means.
FORCE_REBUILD=false
DEMO_ARGS=()
for arg in "$@"; do
    case "$arg" in
        --rebuild) FORCE_REBUILD=true ;;
        *) DEMO_ARGS+=("$arg") ;;
    esac
done

if ! command -v docker >/dev/null 2>&1; then
    die "Docker is not installed, and the demo needs it to run the Kafka broker. Install Docker Desktop (or any Docker daemon) and re-run bin/dashboard-demo.sh"
fi
if ! docker info >/dev/null 2>&1; then
    die "Docker is installed but not running, and the demo needs it to run the Kafka broker. Start Docker (e.g. open Docker Desktop, or 'sudo systemctl start docker') and re-run bin/dashboard-demo.sh"
fi

# Every entry the cached classpath names must still exist. This is the check that makes the cache
# safe to keep across runs rather than across one: the file records absolute paths into ~/.m2 AND
# into the reactor's own target/classes directories, so a pruned local repository, a deleted
# artifact, or a `mvn clean` in core all show up here as a missing path rather than as a
# NoClassDefFoundError twenty seconds into a demo someone is watching.
classpath_entries_all_exist() {
    local entry
    # `|| [ -n "$entry" ]` is load-bearing: build-classpath writes the file with NO trailing newline,
    # and `read` returns non-zero on that final partial line - so a plain `while read` silently skips
    # the LAST entry on the classpath. Verified: without it, appending a nonexistent jar to the cache
    # was not detected.
    while IFS= read -r entry || [ -n "$entry" ]; do
        [ -n "$entry" ] || continue
        [ -e "$entry" ] || return 1
    done < <(tr ':' '\n' <"$CP_FILE")
    return 0
}

# The full list of ways the previous build's output could be wrong for this run. If ANY of them is
# unproven, we build. Cheapest checks first, so the common "nothing changed" path is a handful of
# stats and one find.
build_is_fresh() {
    # Written only after a build that exited 0, so its absence means "no build has succeeded here".
    [ -f "$STAMP_FILE" ] || return 1
    [ -s "$CP_FILE" ] || return 1
    # What the demo actually runs from. Cheap, and catches a hand-deleted target/ or a half-clean.
    [ -d "$MODULE/target/classes" ] || return 1
    [ -f "$MAIN_CLASS_FILE" ] || return 1

    # The stamp carries the time the build STARTED (see run_build), so a source edited while Maven
    # was running is newer than it and gets rebuilt next time, rather than being masked by output
    # that never saw the edit.
    # find's errors are NOT silenced: a missing or unreadable watched path has to read as "cannot
    # prove freshness", because swallowing it would turn "find printed nothing because it failed"
    # into "nothing changed" - a false pass, which is the only failure mode a cache must not have.
    local changed
    changed="$(find "${WATCHED_INPUTS[@]}" -newer "$STAMP_FILE" -print)" || return 1
    [ -z "$changed" ] || return 1

    classpath_entries_all_exist || return 1
}

run_build() {
    echo "==> Building $MODULE (only the first run, and runs after an edit, are slow)"
    mkdir -p "$BUILD_DIR"
    # Stamp the START of the build, and only publish it once Maven has exited 0. A stamp taken at the
    # end would date the output later than an edit that landed mid-build, and the next run would
    # trust output compiled from the older source. A failed build publishes nothing, so the next run
    # builds again.
    local pending="$STAMP_FILE.pending"
    rm -f "$STAMP_FILE" "$pending"
    : >"$pending"

    # The lifecycle phase is not optional here. Without it, build-classpath resolves
    # parallel-consumer-core from ~/.m2 and the demo silently runs against whatever was last
    # installed; with it, the reactor resolves to this checkout's target/classes. Verified both ways.
    if ! ./mvnw --batch-mode -pl "$MODULE" -am test-compile \
            "$DEPENDENCY_PLUGIN:build-classpath" -Dmdep.outputFile="$CP_FILE"; then
        rm -f "$pending" "$CP_FILE"
        die "the build failed - fix the errors above and re-run bin/dashboard-demo.sh"
    fi
    # rename(2), so the stamp keeps the build's start time rather than taking "now".
    mv "$pending" "$STAMP_FILE"
}

if [ "$FORCE_REBUILD" = true ]; then
    run_build
elif build_is_fresh; then
    echo "==> Build is up to date, skipping Maven (--rebuild to force it)"
else
    run_build
fi

RESOLVED_CLASSPATH="$(cat "$CP_FILE")"
[ -n "$RESOLVED_CLASSPATH" ] || die "the build produced an empty classpath - re-run with ./mvnw -pl $MODULE -am test-compile to see why"

# build-classpath writes the file once per reactor module, so the surviving content is whichever
# module was built last. Assert it really was the dashboard's: a parent-module classpath would get
# all the way to a NoClassDefFoundError before saying anything useful. Checked on EVERY run, cached
# classpath included - the guard is worth nothing if skipping the build also skips it.
# Herestring, never a pipe into `grep -q` - see bin/check-shell-sigpipe.sh.
if ! grep -q "vertx-web" <<<"$RESOLVED_CLASSPATH"; then
    die "the resolved classpath is not $MODULE's (vertx-web is missing from it) - the reactor build order changed, so the demo would fail with NoClassDefFoundError"
fi

DEMO_CLASSPATH="$MODULE/target/test-classes:$MODULE/target/classes:$RESOLVED_CLASSPATH"
JAVA_BIN="java"
if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA_BIN="$JAVA_HOME/bin/java"
fi

echo "==> Starting the demo (the dashboard URL is printed below, in a box)"
# Not `exec`: the demo's exit code is the script's verdict. Ctrl-C reaches the JVM anyway - it is in
# this script's foreground process group, and its shutdown hook stops the scenario and releases the
# broker.
# The `${x[@]+...}` guard is for bash 3.2 (macOS's /bin/bash), where expanding an empty array under
# `set -u` is an unbound-variable error rather than nothing.
set +e
"$JAVA_BIN" -cp "$DEMO_CLASSPATH" "$MAIN_CLASS" ${DEMO_ARGS[@]+"${DEMO_ARGS[@]}"}
demo_status=$?
set -e

exit "$demo_status"
