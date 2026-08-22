#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# The Go demo: the same records through Go's own Kafka client and through Go over the sidecar. One
# command, no setup.
#
# THE CONTRACT IS parallel-consumer-proxy/demo/README.md and the reference implementation is the
# Java seed's run.sh beside the Java client - same flags, same defaults, same two tables out. What
# is specific to Go is in README.md beside this file; the largest of it is here, in that this
# script starts the broker rather than the demo binary doing it.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

DEMO_DIR="parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go/demo"
COMPOSE_FILE="$DEMO_DIR/docker-compose.yml"

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
  --native           run on this machine's own Go and JDK toolchains
  -h, --help         this

With neither --docker nor --native, the demo picks: native when both toolchains are present,
a container otherwise. It says which it chose, and why, on the first line of output.

Every flag also has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.
USAGE
}

# ${args[@]+...} rather than "${args[@]}": under `set -u`, bash 3.2 - which is what macOS ships,
# and what a first-time user runs this with - treats an EMPTY array expansion as an unbound
# variable and aborts. That fails exactly the no-argument case, which is the double-click case.
args=()
mode="auto"
bootstrap_supplied="${PC_DEMO_BOOTSTRAP:-}"
while [ $# -gt 0 ]; do
    case "$1" in
        --records|--delay-ms|--concurrency|--partitions|--replay-factor|--topic)
            if [ $# -lt 2 ]; then
                echo "$1 needs a value" >&2
                exit 2
            fi
            args+=("$1" "$2"); shift 2 ;;
        --bootstrap)
            if [ $# -lt 2 ]; then
                echo "$1 needs a value" >&2
                exit 2
            fi
            # Remembered as well as forwarded: it is what decides whether this script starts a
            # broker at all. It is never echoed - own-cluster mode puts a real address here.
            bootstrap_supplied="$2"
            args+=("$1" "$2"); shift 2 ;;
        --docker)  mode="docker"; shift ;;
        --native)  mode="native"; shift ;;
        -h|--help) usage; exit 0 ;;
        *)         echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

# `./mvnw -v` rather than `command -v java`: the wrapper needs a JDK it can actually run, and a
# JRE-only or absent JAVA_HOME fails here rather than three minutes into a build.
have_jdk_toolchain() {
    [ -x ./mvnw ] && ./mvnw -v >/dev/null 2>&1
}

# Asked of the module rather than of `go version`, because the answer depends on the module's own
# `go` directive and on GOTOOLCHAIN, and a version comparison in shell would have to know both.
have_go_toolchain() {
    command -v go >/dev/null 2>&1 || return 1
    if (cd "$DEMO_DIR" && go list -m >/dev/null 2>&1); then
        return 0
    fi
    # This machine's Go is older than the module needs. Fetching a matching toolchain is Go's OWN
    # DEFAULT behaviour, disabled only where GOTOOLCHAIN has been pinned to local - so the demo
    # asks for it explicitly for this run rather than telling the reader to install something.
    if (cd "$DEMO_DIR" && GOTOOLCHAIN=auto go list -m >/dev/null 2>&1); then
        export GOTOOLCHAIN=auto
        echo "This machine's Go is older than the demo needs; using Go's own toolchain download" \
             "(GOTOOLCHAIN=auto) for this run."
        return 0
    fi
    return 1
}

if [ "$mode" = "auto" ]; then
    if have_go_toolchain && have_jdk_toolchain; then
        mode="native"
        echo "Mode: native - a Go toolchain and a JDK toolchain are both present on this machine."
    else
        mode="docker"
        echo "Mode: container - this machine is missing a Go or JDK toolchain, so the demo brings its own."
    fi
else
    echo "Mode: $mode - asked for explicitly."
    # An explicit --native still has to CHECK, and for a reason beyond a nicer error: the Go probe
    # is also what turns GOTOOLCHAIN on when this machine's Go is too old, so skipping it here would
    # let the run reach `go build` and fail there on a machine the auto path would have handled.
    if [ "$mode" = "native" ]; then
        if ! have_go_toolchain; then
            echo "--native needs a Go toolchain this module can build with, and this machine has none." >&2
            exit 1
        fi
        if ! have_jdk_toolchain; then
            echo "--native needs a JDK toolchain to build the sidecar, and this machine has none." >&2
            exit 1
        fi
    fi
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
        exec docker compose -f "$COMPOSE_FILE" up \
            --build --abort-on-container-exit --exit-code-from demo
fi

# ---------------------------------------------------------------------------------------------
# Native. Three things have to exist before the demo binary runs: the sidecar, the broker, and the
# binary itself.
# ---------------------------------------------------------------------------------------------

# THE SIDECAR IS A JVM APPLICATION, so "spawn the sidecar binary" means "spawn the JVM launcher
# with the proxy on its classpath". Computed here, once, and handed to the demo in a file - the
# alternative is Maven in the startup path of a demo whose whole promise is one command.
#
# -am is not optional: this module's parent is not in a single-module reactor and the enforcer's
# ReactorModuleConvergence rule fails the build before anything runs without it.
echo "Building the sidecar..."
./mvnw --batch-mode -q -pl :parallel-consumer-proxy -am -DskipTests \
    package dependency:build-classpath \
    '-Dmdep.outputFile=${project.build.directory}/sidecar-classpath.txt'

SIDECAR_CLASSPATH_FILE="$PWD/$DEMO_DIR/../target/demo-sidecar-classpath.txt"
mkdir -p "$(dirname "$SIDECAR_CLASSPATH_FILE")"
{
    tr -d '\n' < parallel-consumer-proxy/target/sidecar-classpath.txt
    printf ':%s/parallel-consumer-proxy/target/classes' "$PWD"
} > "$SIDECAR_CLASSPATH_FILE"
export PC_DEMO_SIDECAR_CLASSPATH="$SIDECAR_CLASSPATH_FILE"

# THE BROKER, when the caller did not bring one. The demo binary never starts a broker in either
# mode - see README.md - so this is the only place it happens natively.
#
# The image is read out of the compose file rather than pinned twice: a compose file cannot derive
# anything, so the literal lives there and this reads it. Exactly one `image:` line is expected;
# anything else is a change to the compose file that has to be looked at rather than guessed past.
BROKER_CONTAINER=""
cleanup() {
    if [ -n "$BROKER_CONTAINER" ]; then
        docker rm -f "$BROKER_CONTAINER" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

if [ -z "$bootstrap_supplied" ]; then
    BROKER_IMAGE="$(grep -E '^[[:space:]]*image:[[:space:]]' "$COMPOSE_FILE" | awk '{print $2}')"
    if [ "$(printf '%s\n' "$BROKER_IMAGE" | grep -c .)" -ne 1 ]; then
        echo "expected exactly one image: line in $COMPOSE_FILE, found: $BROKER_IMAGE" >&2
        exit 1
    fi
    echo "No broker supplied, starting one in a container: $BROKER_IMAGE"

    # A random high port, retried on collision, because five demos may be running at once on one
    # machine and a fixed port makes the second one fail. Docker itself is the free-port test: if
    # the port is taken the run fails and the next attempt picks another.
    started=""
    for _ in 1 2 3 4 5; do
        BROKER_PORT=$(( 20000 + (RANDOM % 20000) ))
        # CONTROLLER binds a ROUTABLE name rather than 0.0.0.0: Kafka treats an unadvertised
        # listener's bind address as its advertised one when it formats KRaft storage, and refuses
        # the meta-address there. Measured - 0.0.0.0 fails at the format step, before the broker
        # starts, with "advertised.listeners cannot use the nonroutable meta-address".
        if BROKER_CONTAINER=$(docker run -d \
                -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
                -e KAFKA_NODE_ID=1 \
                -e KAFKA_PROCESS_ROLES=broker,controller \
                -e KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:${BROKER_PORT},CONTROLLER://localhost:9093" \
                -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://localhost:${BROKER_PORT}" \
                -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
                -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
                -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
                -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
                -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
                -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
                -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
                -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=500 \
                -p "127.0.0.1:${BROKER_PORT}:${BROKER_PORT}" \
                "$BROKER_IMAGE" 2>/dev/null); then
            started="yes"
            break
        fi
        BROKER_CONTAINER=""
    done
    if [ -z "$started" ]; then
        echo "could not start a broker container on any of five candidate ports" >&2
        exit 1
    fi

    # Ready means "can serve an API request", not "the port is open" - the same thing the compose
    # sibling's healthcheck asserts, for the same reason.
    ready=""
    for _ in $(seq 1 60); do
        if docker exec "$BROKER_CONTAINER" \
                kafka-topics --bootstrap-server "localhost:${BROKER_PORT}" --list >/dev/null 2>&1; then
            ready="yes"
            break
        fi
        sleep 1
    done
    if [ -z "$ready" ]; then
        echo "the broker did not become ready within 60s:" >&2
        docker logs "$BROKER_CONTAINER" 2>&1 | tail -20 >&2
        exit 1
    fi
    args+=(--bootstrap "localhost:${BROKER_PORT}")
fi

# Built rather than `go run`: `go run` puts the compiler's output in a temp directory and adds a
# process between the shell and the demo, and the demo spawns a child whose parent-death pipe must
# come from the demo itself.
DEMO_BINARY="$PWD/$DEMO_DIR/../target/pc-go-demo"
echo "Building the demo..."
# PC_DEMO_EMBEDDED adds a third arm that runs the engine INSIDE this process, over the C ABI of a
# GraalVM shared library. It is a build tag rather than a runtime flag because cgo has to be
# compiled in, and it is off by default because bin/ci-demo-conformance.sh compares every language's
# output skeleton and knows only the AK-CORE and SIDECAR roles.
#
# An environment variable rather than an eighth flag, matching PC_DEMO_SIDECAR and the Python
# demo's PC_DEMO_EMBEDDED - the demo contract fixes the flag table at seven across eleven languages.
FFI_DIR="$PWD/$DEMO_DIR/../ffi"
GO_BUILD_TAGS=()
case "${PC_DEMO_EMBEDDED:-}" in
    1 | true | yes | TRUE | YES)
        if [ -z "${PC_EMBEDDED_LIBRARY:-}" ] && [ ! -e "$FFI_DIR/build/libpc.dylib" ] \
           && [ ! -e "$FFI_DIR/build/libpc.so" ]; then
            echo "PC_DEMO_EMBEDDED is set but no shared library was found. Build it with:" >&2
            echo "  $FFI_DIR/build-shared-library.sh session" >&2
            echo "or point PC_EMBEDDED_LIBRARY at one." >&2
            exit 1
        fi
        GO_BUILD_TAGS=(-tags pcffi)
        echo "Embedded arm enabled: building with -tags pcffi"
        ;;
esac

(cd "$DEMO_DIR" && go build "${GO_BUILD_TAGS[@]}" -o "$DEMO_BINARY" .)

"$DEMO_BINARY" ${args[@]+"${args[@]}"}
