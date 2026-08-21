#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# RUNS THE REFERENCE DEMO THROUGH BOTH OF ITS ENTRY POINTS, because a demo with one tested entry
# point is a demo with an untested entry point (astubbs#242).
#
# WHY THIS EXISTS SEPARATELY FROM ReferenceDemoIT. That integration test calls the demo's own
# runFor(), which proves the arms work. It does not go through demo/run.sh, so it proves nothing
# about how a READER starts the demo: not the Maven build-classpath step, not the forked JVM the
# spawned sidecar's classpath depends on, not the image build, not the compose broker, not the exit
# code a scripted caller sees. Every failure this demo has actually had lived in exactly that gap:
#
#   - `./mvnw` dying inside the maven: image because that image sets MAVEN_CONFIG and the wrapper
#     appends it to its own command line;
#   - the compose broker refusing to format its KRaft storage because advertised.listeners resolved
#     to a non-routable address;
#   - an earlier runner aborting when given no arguments at all.
#
# None of them is a logic error, so no unit test was ever going to catch one. They are "the thing
# does not run" errors, and the only thing that catches those is running the thing.
#
# THE VOLUME IS CHOSEN TO PROVE THE MACHINERY, NOT TO MEASURE ANYTHING. Twenty records, no big
# replay. Throughput here is meaningless and deliberately unasserted.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

RUN_SH="parallel-consumer-proxy-clients/parallel-consumer-proxy-client-java/demo/run.sh"
DEMO_DIR="$(dirname "$RUN_SH")"
LOG_DIR="$(mktemp -d)"
SMALL=(--records 20 --delay-ms 1 --concurrency 4 --partitions 2 --replay-factor 1)

# The arms every platform runs. java-grpc-uds is asserted separately, because whether it can run is
# a property of the platform rather than of the demo - see the container assertion below.
REQUIRED_ARMS=("AK core" "pc-core" "java-direct" "java-grpc" "java-raw-grpc")

fail() {
    echo "ci-demo-test: $1" >&2
    exit 1
}

# Compose prefixes every line with its service name, and both modes colour their output, so one
# normalisation lets a single set of assertions serve both entry points.
normalise() {
    sed -e 's/^demo-1 *| *//' -e 's/\x1b\[[0-9;]*m//g' "$1" > "$2"
}

assert_demo_ran() {
    local mode="$1" log="$2" plain="$2.plain"
    normalise "$log" "$plain"

    grep -q "Small replay - every arm" "$plain" \
        || fail "$mode: printed no comparison table - see $plain"

    local arm
    for arm in "${REQUIRED_ARMS[@]}"; do
        # A row is the arm name followed by its elapsed FIGURE. Matching the name alone would also
        # hit prose - the big replay's own title says "AK core is serial" - so the trailing digit is
        # what distinguishes a reported row from a mention of one.
        #
        # Leading whitespace is optional, and that is not laziness: stripping compose's "demo-1 | "
        # prefix takes the row's indentation with it, so requiring indentation passed natively and
        # failed in the container. Found by running this script, which is the point of it.
        grep -qE "^[[:space:]]*${arm}[[:space:]]+[0-9]" "$plain" \
            || fail "$mode: the '$arm' arm reported no row - see $plain"
    done
    echo "ci-demo-test: $mode ran every required arm"
}

echo "ci-demo-test: === native entry point ==="
"$RUN_SH" "${SMALL[@]}" > "$LOG_DIR/native.log" 2>&1 \
    || fail "native: run.sh exited non-zero - see $LOG_DIR/native.log"
assert_demo_ran "native" "$LOG_DIR/native.log"

echo "ci-demo-test: === container entry point ==="
"$RUN_SH" --docker "${SMALL[@]}" > "$LOG_DIR/docker.log" 2>&1 \
    || fail "container: run.sh --docker exited non-zero - see $LOG_DIR/docker.log"
assert_demo_ran "container" "$LOG_DIR/docker.log"

# The container is Linux whatever the host is, so the domain-socket arm MUST be there. This is the
# assertion that keeps the UDS path covered on a macOS developer machine, where it cannot run
# natively and would otherwise be exercised by nobody until CI.
grep -qE "^[[:space:]]*java-grpc-uds[[:space:]]+[0-9]" "$LOG_DIR/docker.log.plain" \
    || fail "container: the java-grpc-uds arm is missing, and inside a Linux container it cannot be
        legitimately absent - see $LOG_DIR/docker.log.plain"
echo "ci-demo-test: container ran the java-grpc-uds arm"

docker compose -f "$DEMO_DIR/docker-compose.yml" down --remove-orphans >/dev/null 2>&1 || true

echo "ci-demo-test: BOTH entry points ran the demo end to end"
