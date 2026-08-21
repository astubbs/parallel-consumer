#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# The reference demo: the same records through the Apache Kafka client, through Parallel Consumer
# in process, and through Parallel Consumer reached over the sidecar. One command, no setup.
#
# Every per-language demo mirrors THIS file's interface: same flags, same defaults, same two
# tables out. The contract is parallel-consumer-proxy/demo/README.md.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

DEMO_DIR="parallel-consumer-proxy-clients/parallel-consumer-proxy-client-java/demo"
MODULE=":parallel-consumer-proxy-client-java-demo"

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
    # The broker is a COMPOSE SIBLING and the demo container is never given the host Docker
    # socket: a documented socket mount is root-equivalent host access taught as the normal way to
    # run the product (plan unit U35). PC_DEMO_ARGS carries the flags through to the container,
    # where the same parser reads them.
    PC_DEMO_ARGS="${args[*]+${args[*]}}" \
        exec docker compose -f "$DEMO_DIR/docker-compose.yml" up \
            --build --abort-on-container-exit --exit-code-from demo
fi

# -am is not optional: this module's parent is not in a single-module reactor and the enforcer's
# ReactorModuleConvergence rule fails the build before anything runs without it.
#
# build-classpath in the SAME invocation, writing into each module's own target directory: the
# ${project.build.directory} expression is evaluated per module, so the demo's file is the demo's
# classpath rather than whichever module Maven happened to build last.
./mvnw --batch-mode -q -pl "$MODULE" -am -DskipTests package dependency:build-classpath \
    '-Dmdep.outputFile=${project.build.directory}/demo-classpath.txt'

MODULE_DIR="parallel-consumer-proxy-clients/parallel-consumer-proxy-client-java/parallel-consumer-proxy-client-java-demo"

# A REAL FORKED JVM, and this is load-bearing rather than stylistic. The demo spawns the sidecar as
# a child process and hands it System.getProperty("java.class.path"). Under `mvn exec:java` the
# main class runs inside MAVEN's JVM, so that property would be Maven's own classpath and the child
# sidecar would come up without the engine on it. The container path forks for the same reason.
exec java -cp "$(cat "$MODULE_DIR/target/demo-classpath.txt"):$MODULE_DIR/target/classes" \
    bz.stub.parallelconsumer.client.demo.ReferenceDemo ${args[@]+"${args[@]}"}
