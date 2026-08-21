#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE PYTHON DEMO: the same records through Python's own Kafka client, and through Parallel
# Consumer reached over the sidecar. One command, no setup.
#
# This mirrors the Java seed's run.sh - same flags, same defaults but one, same two tables out.
# The contract is parallel-consumer-proxy/demo/README.md; what is specific to Python is in
# README.md beside this file.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

MODULE_DIR="parallel-consumer-proxy-clients/parallel-consumer-proxy-client-python"
DEMO_DIR="$MODULE_DIR/demo"
COMPOSE_FILE="$DEMO_DIR/docker-compose.yml"

# The host port the compose broker publishes for the NATIVE path. The container path never uses it -
# there the demo reaches the broker as a compose sibling on the demo's own network. It is a variable
# because eleven per-language demos will eventually exist and a reader may have two brokers up.
BROKER_PORT="${PC_DEMO_BROKER_PORT:-19095}"

# THE FIRST THING ON SCREEN NAMES THE PRODUCT. That is contract
# (parallel-consumer-proxy/demo/README.md), and it has to be printed HERE rather than only by the
# demo: a native run builds a classpath and starts a broker first, so a reader would otherwise watch
# a minute of Maven output without being told what any of it is for. reference_demo.py prints the
# same banner when nothing else has - `docker compose up` and running it by hand - and
# PC_DEMO_BANNER_PRINTED below is how this script tells it not to print a second one.
banner() {
    cat <<'BANNER'
================================================================
  PARALLEL CONSUMER  -  Python demo
  The same records, twice: one at a time, then all at once.
================================================================
BANNER
}

usage() {
    cat <<'USAGE'
usage: demo/run.sh [options]

  --records N        records in the comparison replay   (default 2000)
  --delay-ms N       simulated work per record, ms      (default 2)
  --concurrency N    max in-flight records              (default 16)
  --partitions N     partitions on the demo topic       (default 10)
  --replay-factor N  big replay = records x N; 1 skips  (default 20)
  --bootstrap ADDR   an existing broker; omit to start one
  --topic NAME       an existing topic; omit to create one

  --docker           run in a container, broker as a compose sibling
  --native           run on this machine's own Python and JDK toolchains
  -h, --help         this

With neither --docker nor --native, the demo picks: native when a Python 3.10+ interpreter and a
JDK toolchain are both present, a container otherwise. It says which it chose, and why, on the
first line of output.

--concurrency defaults to 16 rather than the seed's 100, and that is the one number this demo
does not inherit: in Python, in-flight records are worker PROCESSES. See demo/README.md.

Every flag also has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.
USAGE
}

banner
# Every path below has now printed it exactly once, so the demo itself must not print a second.
export PC_DEMO_BANNER_PRINTED=1

# ${args[@]+...} rather than "${args[@]}": under `set -u`, bash 3.2 - which is what macOS ships,
# and what a first-time user runs this with - treats an EMPTY array expansion as an unbound
# variable and aborts. That fails exactly the no-argument case, which is the double-click case.
args=()
mode="auto"
bootstrap_given="${PC_DEMO_BOOTSTRAP:-}"
while [ $# -gt 0 ]; do
    case "$1" in
        --bootstrap)
            if [ $# -lt 2 ]; then echo "$1 needs a value" >&2; exit 2; fi
            bootstrap_given="$2"; args+=("$1" "$2"); shift 2 ;;
        --records|--delay-ms|--concurrency|--partitions|--replay-factor|--topic)
            if [ $# -lt 2 ]; then echo "$1 needs a value" >&2; exit 2; fi
            args+=("$1" "$2"); shift 2 ;;
        --docker)  mode="docker"; shift ;;
        --native)  mode="native"; shift ;;
        -h|--help) usage; exit 0 ;;
        *)         echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

have_jdk_toolchain() {
    # `mvnw -v` rather than `command -v java`: the wrapper needs a JDK it can actually run, and a
    # JRE-only or absent JAVA_HOME fails here rather than three minutes into a build. The demo
    # needs one because the sidecar is a JVM binary today.
    [ -x ./mvnw ] && ./mvnw -v >/dev/null 2>&1
}

have_python() {
    # The floor is the package's own requires-python. Asked of the interpreter rather than parsed
    # out of `python3 --version`, so a 3.10.0rc or a vendor build answers correctly.
    command -v python3 >/dev/null 2>&1 \
        && python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>/dev/null
}

if [ "$mode" = "auto" ]; then
    if have_python && have_jdk_toolchain; then
        mode="native"
        echo "Mode: native - a Python 3.10+ interpreter and a JDK toolchain are both present."
    else
        mode="docker"
        echo "Mode: container - this machine is missing Python 3.10+ or a JDK toolchain, so the"
        echo "      demo brings both."
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
    # run the product (plan unit U35). The SIDECAR is not a service either - the client library
    # spawns it as a child process inside the demo's own container, exactly as it does natively.
    # PC_DEMO_ARGS carries the flags through to the container, where the same parser reads them.
    PC_DEMO_ARGS="${args[*]+${args[*]}}" \
        exec docker compose -f "$COMPOSE_FILE" up \
            --build --abort-on-container-exit --exit-code-from demo
fi

# ---------------------------------------------------------------------------------------------
# The native path.
# ---------------------------------------------------------------------------------------------

# THE SIDECAR'S CLASSPATH, BUILT ONCE AND HANDED OVER AS A PATH. The sidecar is a JVM binary today,
# so the "absolute executable" the client library demands is `java`, and the classpath is an
# argument about the BINARY rather than configuration - bootstrap servers, credentials, ordering
# and concurrency still travel only in the connect-time handshake (R39).
#
# -am is not optional: this module's parent is not in a single-module reactor and the enforcer's
# ReactorModuleConvergence rule fails the build before anything runs without it.
#
# -DincludeScope=runtime, and it is a bug fix rather than tidiness: the default scope drags in
# parallel-consumer-core's TEST jar, whose logback-test.xml then configures logging for the sidecar
# and prints logback's own status report to STDOUT - ahead of the `port: <n>` line the client library
# scans for. It survives only because that scan tolerates preceding lines.
echo "Building the sidecar's classpath..."
./mvnw --batch-mode -q -pl :parallel-consumer-proxy -am -DskipTests \
    -DincludeScope=runtime package dependency:build-classpath \
    '-Dmdep.outputFile=${project.build.directory}/proxy-classpath.txt'

PC_DEMO_SIDECAR_CLASSPATH="$(cat parallel-consumer-proxy/target/proxy-classpath.txt):$PWD/parallel-consumer-proxy/target/classes"
export PC_DEMO_SIDECAR_CLASSPATH

# The venv, with the demo's own extra on top of the package's. `make` is the module's one recipe,
# so a developer and this script install identically.
echo "Preparing the Python environment..."
make -C "$MODULE_DIR" demo-build

# THE BROKER, WHEN THE CALLER DID NOT NAME ONE. The seed starts its own with Testcontainers; this
# demo starts the same compose broker its container path uses, because the alternative is putting
# a Docker client library into a demo of a Kafka client library. From the reader's side the promise
# is the contract's: omit --bootstrap and a broker appears.
if [ -z "$bootstrap_given" ]; then
    echo "Starting the broker (compose), publishing 127.0.0.1:$BROKER_PORT..."
    PC_DEMO_BROKER_PORT="$BROKER_PORT" docker compose -f "$COMPOSE_FILE" up -d --wait broker
    export PC_DEMO_BOOTSTRAP="127.0.0.1:$BROKER_PORT"

    # Stopped on the way out, because Testcontainers stops the seed's broker and a demo should not
    # leave a container running that the reader never started. PC_DEMO_KEEP_BROKER=1 keeps it, which
    # is worth about ten seconds on every repeat run while developing.
    if [ -z "${PC_DEMO_KEEP_BROKER:-}" ]; then
        # shellcheck disable=SC2317,SC2329  # invoked by the trap below, not by any call site
        stop_broker() {
            PC_DEMO_BROKER_PORT="$BROKER_PORT" docker compose -f "$COMPOSE_FILE" down \
                >/dev/null 2>&1 || true
        }
        trap stop_broker EXIT
    fi
fi

# NOT `exec`, unlike the seed: the trap above has to run, and exec would replace this shell before
# it could. The demo's exit code is carried out by hand instead - `|| status=$?` rather than a bare
# call, so `set -e` does not exit before the trap has stopped the broker - and a scripted caller
# sees exactly what the demo returned.
status=0
"$MODULE_DIR/.venv/bin/python" "$DEMO_DIR/reference_demo.py" ${args[@]+"${args[@]}"} || status=$?
exit "$status"
