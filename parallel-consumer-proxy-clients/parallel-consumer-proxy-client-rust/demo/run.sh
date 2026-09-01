#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# The Rust demo: the same records through Rust's own Kafka client and through Rust over the
# Parallel Consumer sidecar. One command, no setup.
#
# This mirrors the reference demo's interface exactly - same flags, same defaults, same two tables
# out. The contract is parallel-consumer-proxy/demo/README.md.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

MODULE_DIR="parallel-consumer-proxy-clients/parallel-consumer-proxy-client-rust"
DEMO_DIR="$MODULE_DIR/demo"

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
  --native           run on this machine's own Rust and JDK toolchains
  -h, --help         this

With neither --docker nor --native, the demo picks: native when both toolchains are present, a
container otherwise. It says which it chose, and why, on the first line of output.

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

# BOTH toolchains, because the native path needs both: cargo builds the application, and the
# sidecar it spawns is a Java program this repository builds from source. `mvnw -v` rather than
# `command -v java`, because the wrapper needs a JDK it can actually run - a JRE-only or absent
# JAVA_HOME fails here rather than three minutes into a build.
have_native_toolchains() {
    command -v cargo >/dev/null 2>&1 && [ -x ./mvnw ] && ./mvnw -v >/dev/null 2>&1
}

if [ "$mode" = "auto" ]; then
    if have_native_toolchains; then
        mode="native"
        echo "Mode: native - a Rust toolchain and a JDK are present on this machine."
    else
        mode="docker"
        echo "Mode: container - no Rust toolchain and JDK here, so the demo brings its own."
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

# THE SIDECAR IS A JAVA PROGRAM, so the native path builds it first and writes its classpath where
# the demo looks for it. -am is not optional: this module's parent is not in a single-module reactor
# and the enforcer's ReactorModuleConvergence rule fails the build before anything runs without it.
echo "Building the sidecar the demo will spawn..."
./mvnw --batch-mode -q -pl :parallel-consumer-proxy -am -DskipTests package dependency:build-classpath \
    '-Dmdep.outputFile=${project.build.directory}/demo-proxy-classpath.txt'

# Release, not debug: the demo's entire output is throughput, and a debug-profile number would be a
# measurement of the optimiser being switched off. The first build also compiles librdkafka from
# source, which takes a minute; every later one is cached.
echo "Building the demo..."
(cd "$DEMO_DIR" && cargo build --release)

# The demo binary spawns the sidecar itself, through the client library - there is no wrapper
# process between them, because the pipe the library holds is the sidecar's parent-death signal.
exec "$MODULE_DIR/target/demo/release/demo" ${args[@]+"${args[@]}"}
