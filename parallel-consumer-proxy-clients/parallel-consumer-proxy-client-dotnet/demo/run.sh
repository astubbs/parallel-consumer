#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE .NET DEMO: the same records through .NET's own Kafka client, one at a time, and through this
# module's client library over a sidecar it spawns itself. One command, no setup.
#
# This mirrors the reference demo's interface EXACTLY - same flags, same defaults, same environment
# variables, same two tables out. The contract is parallel-consumer-proxy/demo/README.md; the
# reference implementation of it is the Java demo's run.sh, one directory family over.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

MODULE_DIR="parallel-consumer-proxy-clients/parallel-consumer-proxy-client-dotnet"
DEMO_DIR="$MODULE_DIR/demo"
PROJECT="$DEMO_DIR/Bz.Stub.ParallelConsumer.Proxy.Client.Demo/Bz.Stub.ParallelConsumer.Proxy.Client.Demo.csproj"
DEMO_BIN="$DEMO_DIR/Bz.Stub.ParallelConsumer.Proxy.Client.Demo/bin/Debug/net8.0/pc-dotnet-demo"

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
  --native           run on this machine's own toolchains
  -h, --help         this

With neither --docker nor --native, the demo picks: native when a .NET SDK and a JDK are both
present, a container otherwise. It says which it chose, and why, on the first line of output.

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

have_dotnet_sdk() {
    # `dotnet --list-sdks` rather than `command -v dotnet`: a machine can carry the RUNTIME alone,
    # which runs an app but cannot build one, and that fails three minutes in rather than here.
    command -v dotnet >/dev/null 2>&1 && [ -n "$(dotnet --list-sdks 2>/dev/null)" ]
}

have_jdk_toolchain() {
    # The sidecar is a JVM program in this repository, so the native path needs a JDK as well as a
    # .NET SDK - `mvnw -v` proves the wrapper has one it can actually run.
    [ -x ./mvnw ] && ./mvnw -v >/dev/null 2>&1
}

if [ "$mode" = "auto" ]; then
    if have_dotnet_sdk && have_jdk_toolchain; then
        mode="native"
        echo "Mode: native - a .NET SDK and a JDK toolchain are both present on this machine."
    else
        mode="docker"
        echo "Mode: container - this machine is missing a .NET SDK or a JDK, so the demo brings both."
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

# THE SIDECAR, BUILT BEFORE THE DEMO THAT SPAWNS IT. The client library launches a binary; in this
# repository that binary is a JVM and the proxy is a classpath, so the classpath has to exist before
# the demo runs. It is written into the proxy module's own target/ by ${project.build.directory},
# which is evaluated PER MODULE - without that, `-am` has every module in the reactor write the same
# file and the last one to build wins, which is whichever module Maven happened to finish with.
#
# -am is not optional: this module's parent is not in a single-module reactor and the enforcer's
# ReactorModuleConvergence rule fails the build before anything runs without it.
echo "Building the sidecar (parallel-consumer-proxy) and its classpath..."
./mvnw --batch-mode -q -pl :parallel-consumer-proxy -am -DskipTests package \
    dependency:build-classpath -Dmdep.includeScope=runtime \
    '-Dmdep.outputFile=${project.build.directory}/sidecar-classpath.txt'

# NoSummary as well as quiet: the "Build succeeded / 0 Warning(s) / Time Elapsed" block is four
# lines of scaffolding immediately before the demo's own banner, and the banner is the first thing a
# reader is meant to see. Build ERRORS are not part of that summary and still print.
echo "Building the demo..."
dotnet build --nologo --verbosity quiet -consoleLoggerParameters:NoSummary "$PROJECT"

# The built executable, not `dotnet run`, and not because it is faster: `dotnet run` interposes a
# process between this script and the demo, and the demo is the process whose lifecycle pipe keeps
# the spawned sidecar alive. Launching the apphost directly keeps the parent-death signal a
# straight line, and gives a scripted caller the demo's own exit code rather than a wrapper's.
exec "$DEMO_BIN" ${args[@]+"${args[@]}"}
