#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE RUBY DEMO: the same records through Ruby's own Kafka client and through Ruby over the
# sidecar. One command, no setup.
#
# This file mirrors the reference demo's interface - same flags, same defaults, same two tables out.
# The contract is parallel-consumer-proxy/demo/README.md; the reference implementation is
# parallel-consumer-proxy-clients/parallel-consumer-proxy-client-java/demo/run.sh.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

MODULE_DIR="parallel-consumer-proxy-clients/parallel-consumer-proxy-client-ruby"
DEMO_DIR="$MODULE_DIR/demo"
# Absolute, because the native path changes directory into the demo before it runs, and the EXIT
# trap that stops the broker fires after that.
COMPOSE="$PWD/$DEMO_DIR/docker-compose.yml"
PROXY_TARGET="parallel-consumer-proxy/target"

# The port the compose broker publishes for a NATIVE run, and the one its HOST listener advertises.
# It must match docker-compose.yml, which cannot derive it. Deliberately not 9092: a reader running
# this is quite likely to have their own broker on the usual port already.
BROKER_HOST_PORT=29092

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
  --native           run on this machine's own Ruby and JDK toolchains
  -h, --help         this

With neither --docker nor --native, the demo picks: native when Ruby 3.2+ and a JDK toolchain are
both present, a container otherwise. It says which it chose, and why, on the first line of output.

Every flag also has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.
USAGE
}

# ${args[@]+...} rather than "${args[@]}": under `set -u`, bash 3.2 - which is what macOS ships, and
# what a first-time user runs this with - treats an EMPTY array expansion as an unbound variable and
# aborts. That fails exactly the no-argument case, which is the double-click case.
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

# 3.2 is the client library's floor (Thread::Queue#pop with a timeout:), so it is the demo's too.
# `ruby -e` rather than a version string comparison in the shell: the interpreter that will run the
# demo is the one that answers.
have_ruby() {
    command -v ruby >/dev/null 2>&1 \
        && ruby -e 'exit(Gem::Version.new(RUBY_VERSION) >= Gem::Version.new("3.2") ? 0 : 1)' 2>/dev/null
}

# `mvnw -v` rather than `command -v java`: the wrapper needs a JDK it can actually run, and a
# JRE-only or absent JAVA_HOME fails here rather than three minutes into a build. The demo needs one
# even though it is not a JVM demo - the sidecar it spawns is a JVM program, and nothing else in
# this repository can build it.
have_jdk_toolchain() {
    [ -x ./mvnw ] && ./mvnw -v >/dev/null 2>&1
}

if [ "$mode" = "auto" ]; then
    if have_ruby && have_jdk_toolchain; then
        mode="native"
        echo "Mode: native - Ruby 3.2+ and a JDK toolchain are both present on this machine."
    elif have_ruby; then
        mode="docker"
        echo "Mode: container - no JDK toolchain here, and the sidecar this demo spawns is a JVM program."
    else
        mode="docker"
        echo "Mode: container - no Ruby 3.2+ here, so the demo brings its own."
    fi
else
    echo "Mode: $mode - asked for explicitly."
fi

if ! docker info >/dev/null 2>&1; then
    echo "Docker is not running - the demo needs it for the broker, and for its own image." >&2
    exit 1
fi

if [ "$mode" = "docker" ]; then
    # The broker is a COMPOSE SIBLING and the demo container is never given the host Docker socket:
    # a documented socket mount is root-equivalent host access taught as the normal way to run the
    # product (plan unit U35). PC_DEMO_ARGS carries the flags through to the container, where the
    # same parser reads them.
    PC_DEMO_ARGS="${args[*]+${args[*]}}" \
        exec docker compose -f "$COMPOSE" up \
            --build --abort-on-container-exit --exit-code-from demo
fi

# ── The native path ─────────────────────────────────────────────────────────────────────────────
#
# THE SIDECAR IS BUILT WITH MAVEN, and there is no way round it: the sidecar is a JVM program, this
# is a Ruby demo, and a Ruby toolchain cannot produce one. The classpath is computed here and handed
# to the demo, because Ruby has nothing equivalent to the java.class.path the Java seed reads out of
# its own runtime.
#
# -am is not optional: the proxy module's parent is not in a single-module reactor and the
# enforcer's ReactorModuleConvergence rule fails the build before anything runs without it.
echo "Building the sidecar (Maven; the first run downloads its dependencies)..."
./mvnw --batch-mode -q -pl :parallel-consumer-proxy -am -DskipTests \
    package dependency:build-classpath \
    '-Dmdep.outputFile=${project.build.directory}/sidecar-classpath.txt'

sidecar_jar="$(ls "$PROXY_TARGET"/*.jar \
    | grep -v -e '-tests\.jar$' -e '-sources\.jar$' -e '-javadoc\.jar$' | head -1)"
if [ -z "$sidecar_jar" ]; then
    echo "the Maven build produced no sidecar jar in $PROXY_TARGET" >&2
    exit 1
fi

# Absolute, because the JVM this becomes the classpath of is spawned by the client library and
# inherits whatever working directory the demo happened to have.
export PC_DEMO_SIDECAR_CLASSPATH="$PWD/$sidecar_jar:$(cat "$PROXY_TARGET/sidecar-classpath.txt")"
if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    export PC_DEMO_SIDECAR_JAVA="$JAVA_HOME/bin/java"
fi

# THE BROKER IS THE COMPOSE ONE, EVEN NATIVELY, and that is one broker definition rather than two.
# The Java seed starts a Testcontainers broker on this path; Ruby has no equivalent this demo would
# rather depend on, and reusing the compose service means the native and container paths measure
# against a broker configured identically - including the 500ms rebalance delay, which either path
# would otherwise be silently charged three seconds for on every group it forms.
started_broker=""
cleanup() {
    if [ -n "$started_broker" ]; then
        echo "Stopping the demo broker..."
        docker compose -f "$COMPOSE" down --remove-orphans >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

if [ -z "$bootstrap_supplied" ]; then
    echo "No broker supplied, starting one with compose..."
    docker compose -f "$COMPOSE" up --detach --wait broker
    started_broker="yes"
    # Deliberately not echoed: the same variable carries a user's real broker in own-cluster mode,
    # and the credential-hygiene rule that binds the proxy binds a demo too.
    export PC_DEMO_BOOTSTRAP="127.0.0.1:$BROKER_HOST_PORT"
fi

cd "$DEMO_DIR"
echo "Installing the demo's gems..."
bundle install --quiet
bundle exec ruby demo.rb ${args[@]+"${args[@]}"}
