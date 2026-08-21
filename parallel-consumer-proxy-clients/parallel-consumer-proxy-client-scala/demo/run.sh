#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# The Scala demo: the same records through Scala's own Kafka client and through Scala over the
# sidecar. One command, no setup.
#
# This mirrors the reference demo's interface exactly - same flags, same defaults, same two tables
# out. The contract is parallel-consumer-proxy/demo/README.md; the reference is
# parallel-consumer-proxy-clients/parallel-consumer-proxy-client-java/demo/run.sh.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

MODULE_DIR="parallel-consumer-proxy-clients/parallel-consumer-proxy-client-scala"
DEMO_DIR="$MODULE_DIR/demo"
MODULE=":parallel-consumer-proxy-client-scala"
MAIN_CLASS="bz.stub.parallelconsumer.client.scaladsl.demo.ScalaDemo"

usage() {
    cat <<'USAGE'
usage: demo/run.sh [options]

  --records N        records in the comparison replay   (default 2000)
  --delay-ms N       simulated work per record, ms      (default 2)
  --concurrency N    max in-flight records              (default 100)
  --partitions N     partitions on the demo topic       (default 10)
  --replay-factor N  big replay = records x N; 1 skips  (default 20)
  --bootstrap ADDR   an existing broker; omit to start one
  --topic NAME       an existing topic; omit to create one

  --docker           run in a container, broker as a compose sibling
  --native           run on this machine's own JDK toolchain
  -h, --help         this

With neither --docker nor --native, the demo picks: native when a JDK toolchain is present,
a container otherwise. It says which it chose, and why, on the first line of output.

Every flag also has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.
USAGE
}

# ${args[@]+...} rather than "${args[@]}": under `set -u`, bash 3.2 - which is what macOS ships,
# and what a first-time user runs this with - treats an EMPTY array expansion as an unbound
# variable and aborts. That fails exactly the no-argument case, which is the double-click case.
args=()
mode="auto"
while [ $# -gt 0 ]; do
    case "$1" in
        --records|--delay-ms|--concurrency|--partitions|--replay-factor|--bootstrap|--topic)
            if [ $# -lt 2 ]; then
                echo "$1 needs a value" >&2
                exit 2
            fi
            args+=("$1" "$2"); shift 2 ;;
        --docker)  mode="docker"; shift ;;
        --native)  mode="native"; shift ;;
        -h|--help) usage; exit 0 ;;
        *)         echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

have_jdk_toolchain() {
    # `mvnw -v` rather than `command -v java`: the wrapper needs a JDK it can actually run, and a
    # JRE-only or absent JAVA_HOME fails here rather than three minutes into a build.
    [ -x ./mvnw ] && ./mvnw -v >/dev/null 2>&1
}

if [ "$mode" = "auto" ]; then
    if have_jdk_toolchain; then
        mode="native"
        echo "Mode: native - a JDK toolchain is present on this machine."
    else
        mode="docker"
        echo "Mode: container - no JDK toolchain here, so the demo brings its own."
    fi
else
    echo "Mode: $mode - asked for explicitly."
fi

if ! docker info >/dev/null 2>&1; then
    if [ "$mode" = "docker" ]; then
        echo "Docker is not running - the demo needs it for the broker and for its own image." >&2
    else
        echo "Docker is not running - the demo starts its broker in a container." >&2
    fi
    exit 1
fi

if [ "$mode" = "docker" ]; then
    # The broker is a COMPOSE SIBLING and the demo container is never given the host Docker socket:
    # a documented socket mount is root-equivalent host access taught as the normal way to run the
    # product (plan unit U35). PC_DEMO_ARGS carries the flags through to the container, where the
    # same parser reads them.
    PC_DEMO_ARGS="${args[*]+${args[*]}}" \
        exec docker compose -f "$DEMO_DIR/docker-compose.yml" up \
            --build --abort-on-container-exit --exit-code-from demo
fi

# -Dpc.scalaDemo activates the module's scala-demo profile, which is what adds the demo's sources,
# its Kafka client, its broker, the sidecar jar it hands its child - and the classpath file read
# below. Outside that profile none of them exist, which is the point: an unconditional edge to the
# engine would put parallel-consumer-proxy in this module's ordinary reactor, and `bin/build.sh`
# opens with `clean`.
#
# -am is not optional: this module's parent is not in a single-module reactor and the enforcer's
# ReactorModuleConvergence rule fails the build before anything runs without it.
#
# test-compile rather than package: the demo is a test source root, so it is compiled by then, and
# the profile's own dependency:build-classpath execution has already written the classpath file.
./mvnw --batch-mode -q -pl "$MODULE" -am -DskipTests -Dpc.scalaDemo test-compile

# A REAL FORKED JVM, and this is load-bearing rather than stylistic. The client library spawns the
# sidecar as a child process and this demo hands it System.getProperty("java.class.path"). Under
# `mvn exec:java` the main class runs inside MAVEN's JVM, so that property would be Maven's own
# classpath and the child sidecar would come up without the engine on it. The container path forks
# for the same reason.
#
# -Dlogback.configurationFile is what lets the two tables be seen. With no logback configuration
# anywhere on the classpath, logback's fallback is root at DEBUG, and a small run buries its own
# output under thousands of lines of Netty frames and docker-java headers. demo/logback.xml explains
# what it turns down and why one of those is a rule rather than a preference.
exec java -Dlogback.configurationFile="$DEMO_DIR/logback.xml" \
    -cp "$(cat "$MODULE_DIR/target/demo-classpath.txt"):$MODULE_DIR/target/classes:$MODULE_DIR/target/test-classes" \
    "$MAIN_CLASS" ${args[@]+"${args[@]}"}
