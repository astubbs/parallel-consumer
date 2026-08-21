#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# The TypeScript demo: the same records through kafkajs one at a time, and through this module's
# client library over a sidecar. One command, no setup.
#
# It mirrors the reference demo's interface exactly - same flags, same defaults, same environment
# variables, same two tables out. The contract is parallel-consumer-proxy/demo/README.md and the
# reference implementation is
# parallel-consumer-proxy-clients/parallel-consumer-proxy-client-java/demo/run.sh.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

MODULE_DIR="parallel-consumer-proxy-clients/parallel-consumer-proxy-client-typescript"
DEMO_DIR="$MODULE_DIR/demo"

# Pinned as a literal here and in docker-compose.yml, tracking <kafka.version> in the root pom the
# same way the Java demo does: CP major = AK major + 4, so AK 3.9.x means CP 7.9.
BROKER_IMAGE="confluentinc/cp-kafka:7.9.0"

# The name the broker container answers to, inside itself and on the demo's own network. Every
# listener binds to it - see start_broker below for why 0.0.0.0 is not an option here.
BROKER_HOSTNAME="pc-typescript-demo-broker"

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
  --native           run on this machine's own Node and JDK toolchains
  -h, --help         this

With neither --docker nor --native, the demo picks: native when Node and a JDK toolchain are both
present, a container otherwise. It says which it chose, and why, on the first line of output.

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
        --bootstrap)
            if [ $# -lt 2 ]; then echo "$1 needs a value" >&2; exit 2; fi
            bootstrap_supplied="$2"
            args+=("$1" "$2"); shift 2 ;;
        --records|--delay-ms|--concurrency|--partitions|--replay-factor|--topic)
            if [ $# -lt 2 ]; then echo "$1 needs a value" >&2; exit 2; fi
            args+=("$1" "$2"); shift 2 ;;
        --docker)  mode="docker"; shift ;;
        --native)  mode="native"; shift ;;
        -h|--help) usage; exit 0 ;;
        *)         echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

have_node_toolchain() {
    command -v node >/dev/null 2>&1 \
        && command -v npm >/dev/null 2>&1 \
        && [ "$(node -e 'process.stdout.write(String(process.versions.node.split(".")[0] >= 20))')" = "true" ]
}

have_jdk_toolchain() {
    # `mvnw -v` rather than `command -v java`: the sidecar is built from the proxy module, so the
    # wrapper needs a JDK it can actually run. A JRE-only or absent JAVA_HOME fails here rather
    # than three minutes into a build.
    [ -x ./mvnw ] && ./mvnw -v >/dev/null 2>&1
}

# Native needs BOTH toolchains, and that is the one place this script's decision differs from the
# Java demo's: the demo itself is Node, and the sidecar it spawns is a JVM the proxy module builds.
if [ "$mode" = "auto" ]; then
    if have_node_toolchain && have_jdk_toolchain; then
        mode="native"
        echo "Mode: native - Node 20+ and a JDK toolchain are both present on this machine."
    else
        mode="docker"
        echo "Mode: container - this machine is missing Node 20+ or a JDK, so the demo brings both."
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

# --- native ---------------------------------------------------------------------------------
#
# THE SIDECAR IS BUILT, NOT DOWNLOADED. `bz.stub.parallelconsumer.proxy.Main` is the real sidecar
# in the proxy module; build-classpath writes what it needs to run into that module's own target
# directory, and the client library spawns it from there as a child process. It is NOT a service,
# and it is NOT installed: the library spawns and supervises it, so a user never operates a process.
#
# -am is not optional: this module's parent is not in a single-module reactor and the enforcer's
# ReactorModuleConvergence rule fails the build before anything runs without it.
#
# -DincludeScope=runtime IS NOT A TIDY-UP. Without it build-classpath writes every scope, which puts
# TEST-scope logback on the sidecar's classpath - and logback is test-scoped repository-wide, so the
# sidecar a user actually deploys has no SLF4J provider at all. The container path never had one and
# says so ("No SLF4J providers were found"); scoping here is what stops the two entry points running
# the sidecar with different classpaths and calling it the same demo.
echo "Building the sidecar from the proxy module..."
./mvnw --batch-mode -q -pl :parallel-consumer-proxy -am -DskipTests package dependency:build-classpath \
    -DincludeScope=runtime '-Dmdep.outputFile=${project.build.directory}/sidecar-classpath.txt'

# The classes directory is appended because build-classpath writes DEPENDENCIES only - the proxy's
# own compiled output is not on it, and a sidecar without its main class does not start.
export PC_PROXY_SIDECAR_CLASSPATH="$(cat parallel-consumer-proxy/target/sidecar-classpath.txt):$PWD/parallel-consumer-proxy/target/classes"

# The library first, then the demo: the demo depends on it through `file:..`, which npm resolves to
# a symlink, so it loads the built dist/ a user would install rather than the sources.
echo "Building the client library and the demo..."
( cd "$MODULE_DIR" && npm ci --no-audit --no-fund --silent && npm run --silent compile )
( cd "$DEMO_DIR" && npm ci --no-audit --no-fund --silent && npm run --silent compile )

broker_container=""

stop_broker() {
    if [ -n "$broker_container" ]; then
        docker rm --force "$broker_container" >/dev/null 2>&1 || true
    fi
}

free_port() {
    # Racy by construction, exactly as every ephemeral-port picker is; the window is the few
    # hundred milliseconds until the broker binds it.
    node -e 'const s=require("net").createServer();s.listen(0,"127.0.0.1",()=>{const p=s.address().port;s.close(()=>{process.stdout.write(String(p));});});'
}

start_broker() {
    local port
    port="$(free_port)"
    broker_container="pc-typescript-demo-broker-$$"
    trap stop_broker EXIT INT TERM

    # TWO PLAINTEXT LISTENERS, AND BOTH ARE LOAD-BEARING. The published one advertises
    # 127.0.0.1:$port so the demo and the sidecar on this host can reach it; the BROKER one
    # advertises a container-local address for the broker's own inter-broker traffic, which would
    # otherwise be sent to a host port that does not exist inside the container. It is what
    # Testcontainers does for the Java demo, spelled out here because a shell script has nothing
    # that does it for us.
    #
    # EVERY LISTENER BINDS TO $BROKER_HOSTNAME, NEVER 0.0.0.0, and that is a bug fix rather than a
    # style: this image formats its KRaft storage with the config it is about to boot with, and
    # that step refuses a non-routable advertised address - including the CONTROLLER listener,
    # which has no entry in advertised.listeners and is therefore taken from `listeners`. Measured:
    # with 0.0.0.0 the container exits 1 during preflight with "advertised.listeners cannot use the
    # nonroutable meta-address 0.0.0.0". The compose sibling binds to its service name for the same
    # reason. Docker still forwards the published port to a listener bound to the container's own
    # address.
    echo "Starting a broker in a container: $BROKER_IMAGE"
    docker run --detach --name "$broker_container" --hostname "$BROKER_HOSTNAME" \
        --publish "127.0.0.1:$port:9092" \
        --env CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
        --env KAFKA_NODE_ID=1 \
        --env KAFKA_PROCESS_ROLES=broker,controller \
        --env "KAFKA_LISTENERS=PLAINTEXT://$BROKER_HOSTNAME:9092,BROKER://$BROKER_HOSTNAME:9091,CONTROLLER://$BROKER_HOSTNAME:9093" \
        --env "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:$port,BROKER://$BROKER_HOSTNAME:9091" \
        --env "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT" \
        --env KAFKA_INTER_BROKER_LISTENER_NAME=BROKER \
        --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
        --env "KAFKA_CONTROLLER_QUORUM_VOTERS=1@$BROKER_HOSTNAME:9093" \
        --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
        --env KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
        --env KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
        --env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=500 \
        "$BROKER_IMAGE" >/dev/null

    # Listing topics is the cheapest call that proves the broker can serve an API request, and it
    # proves more than a port being open does. The same check guards the compose sibling.
    local waited=0
    until docker exec "$broker_container" \
            kafka-topics --bootstrap-server "$BROKER_HOSTNAME:9091" --list >/dev/null 2>&1; do
        waited=$((waited + 1))
        if [ "$waited" -gt 120 ]; then
            echo "The broker did not become ready within 120s." >&2
            docker logs "$broker_container" >&2 || true
            exit 1
        fi
        sleep 1
    done

    args+=(--bootstrap "127.0.0.1:$port")
}

# Only when the caller supplied neither the flag nor the environment variable - starting a broker
# over the top of a user's own would be the demo overriding its own precedence rule.
if [ -z "$bootstrap_supplied" ]; then
    start_broker
fi

# Not `exec`: the broker container above is this shell's to remove, and an exec'd process has no
# EXIT trap to run.
status=0
node "$DEMO_DIR/dist/src/main.js" ${args[@]+"${args[@]}"} || status=$?
exit "$status"
