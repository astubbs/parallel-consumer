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
# The scenario itself lives in ShowcaseScenario (parallel-consumer-dashboard test-integration) and
# is asserted by ShowcaseScenarioIT - this script is only the front door.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

MODULE="parallel-consumer-dashboard"
MAIN_CLASS="io.confluent.parallelconsumer.dashboard.integrationTests.DemoMain"
# Pinned, and matched to the version the root pom manages - an unpinned goal invocation would
# silently float to whatever is newest on Central the day someone runs the demo.
DEPENDENCY_PLUGIN="org.apache.maven.plugins:maven-dependency-plugin:3.11.0"

# One line, actionable, no stack trace. A Testcontainers failure with no daemon is 40 lines of
# reflection that never says "start Docker", which is the only thing the reader needs to know.
die() {
    echo "dashboard-demo: $*" >&2
    exit 3
}

if ! command -v docker >/dev/null 2>&1; then
    die "Docker is not installed, and the demo needs it to run the Kafka broker. Install Docker Desktop (or any Docker daemon) and re-run bin/dashboard-demo.sh"
fi
if ! docker info >/dev/null 2>&1; then
    die "Docker is installed but not running, and the demo needs it to run the Kafka broker. Start Docker (e.g. open Docker Desktop, or 'sudo systemctl start docker') and re-run bin/dashboard-demo.sh"
fi

CP_FILE="$(mktemp -t dashboard-demo-classpath.XXXXXX)"
# MUST capture $? first and re-exit with it: an EXIT trap's own last command otherwise becomes the
# script's exit status, which would report a failed --once run as a pass (same foot-gun documented
# in bin/chaos-test.sh).
cleanup() {
    ec=$?
    rm -f "$CP_FILE" || true
    exit "$ec"
}
trap cleanup EXIT

echo "==> Building $MODULE (incremental - only the first run is slow)"
# The lifecycle phase is not optional here. Without it, build-classpath resolves
# parallel-consumer-core from ~/.m2 and the demo silently runs against whatever was last installed;
# with it, the reactor resolves to this checkout's target/classes. Verified both ways.
if ! ./mvnw --batch-mode -pl "$MODULE" -am test-compile \
        "$DEPENDENCY_PLUGIN:build-classpath" -Dmdep.outputFile="$CP_FILE"; then
    die "the build failed - fix the errors above and re-run bin/dashboard-demo.sh"
fi

RESOLVED_CLASSPATH="$(cat "$CP_FILE")"
[ -n "$RESOLVED_CLASSPATH" ] || die "the build produced an empty classpath - re-run with ./mvnw -pl $MODULE -am test-compile to see why"

# build-classpath writes the file once per reactor module, so the surviving content is whichever
# module was built last. Assert it really was the dashboard's: a parent-module classpath would get
# all the way to a NoClassDefFoundError before saying anything useful.
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
# Not `exec`: the demo's exit code is the script's verdict, and the EXIT trap still has a temp file
# to remove. Ctrl-C reaches the JVM anyway - it is in this script's foreground process group, and
# its shutdown hook stops the scenario and releases the broker.
set +e
"$JAVA_BIN" -cp "$DEMO_CLASSPATH" "$MAIN_CLASS" "$@"
demo_status=$?
set -e

exit "$demo_status"
