#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# The Swift demo: the same records through Swift's own Kafka client, and through Swift over the
# sidecar. One command, no setup, and nothing to install but Docker.
#
# It mirrors the Java seed's interface - same flags, same defaults, same two tables out. The
# contract is parallel-consumer-proxy/demo/README.md, and what is specific to Swift is in the
# README beside this file.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

DEMO_DIR="parallel-consumer-proxy-clients/parallel-consumer-proxy-client-swift/demo"

usage() {
    cat <<'USAGE'
usage: demo/run.sh [options]

  --records N        records in the comparison replay   (default 2000)
  --delay-ms N       simulated work per record, ms      (default 2)
  --concurrency N    max in-flight records              (default 100)
  --partitions N     partitions on the demo topic       (default 10)
  --replay-factor N  big replay = records x N; 1 skips  (default 20)
  --bootstrap ADDR   an existing broker; omit for the compose sibling
  --topic NAME       an existing topic; omit to name one

  --docker           run in a container (the only mode Swift has here)
  -h, --help         this

Every flag also has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.
Flags beat the environment beats the defaults.
USAGE
}

# ${args[@]+...} rather than "${args[@]}": under `set -u`, bash 3.2 - which is what macOS ships, and
# what a first-time reader runs this with - treats an EMPTY array expansion as an unbound variable
# and aborts. That fails exactly the no-argument case, which is the double-click case.
args=()
# Seeded from the environment, not blanked: the contract says every flag has an environment
# variable, and unconditionally exporting an empty one below would silently beat the caller's own
# PC_DEMO_PARTITIONS. A flag still wins, because it overwrites this.
partitions="${PC_DEMO_PARTITIONS:-}"
topic="${PC_DEMO_TOPIC:-}"
while [ $# -gt 0 ]; do
    case "$1" in
        --records|--delay-ms|--concurrency|--replay-factor|--bootstrap)
            if [ $# -lt 2 ]; then
                echo "$1 needs a value" >&2
                exit 2
            fi
            args+=("$1" "$2"); shift 2 ;;
        # These two are forwarded to the demo like every other flag AND captured here, because the
        # compose broker needs them too: the demo's topic is created by the broker on first produce
        # (swift-kafka-client has no admin client), so --partitions has to reach the broker's own
        # num.partitions. See docker-compose.yml, which states the divergence in full.
        --partitions)
            if [ $# -lt 2 ]; then echo "$1 needs a value" >&2; exit 2; fi
            partitions="$2"; args+=("$1" "$2"); shift 2 ;;
        --topic)
            if [ $# -lt 2 ]; then echo "$1 needs a value" >&2; exit 2; fi
            topic="$2"; args+=("$1" "$2"); shift 2 ;;
        --docker)  shift ;;
        --native)
            echo "There is no native mode for the Swift demo: Swift.org publishes Linux toolchains" >&2
            echo "for Ubuntu, Amazon Linux and RHEL only, so this project builds Swift in a" >&2
            echo "container and there is no toolchain on a developer machine to run natively on." >&2
            exit 2 ;;
        -h|--help) usage; exit 0 ;;
        *)         echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

# Said on the first line, like the Java demo's, because a reader should never have to work out
# which of two modes produced the numbers below.
echo "Mode: container - Swift has no host toolchain here, so the demo brings its own."

if ! docker info >/dev/null 2>&1; then
    echo "Docker is not running - the demo needs it for its own image and for the broker." >&2
    exit 1
fi

# The broker is a COMPOSE SIBLING and the demo container is never given the host Docker socket: a
# documented socket mount is root-equivalent host access taught as the normal way to run the product
# (plan unit U35). PC_DEMO_ARGS carries the flags through to the container, where the same parser
# reads them; PC_DEMO_PARTITIONS and PC_DEMO_TOPIC are read by the compose file itself as well.
PC_DEMO_ARGS="${args[*]+${args[*]}}" \
PC_DEMO_PARTITIONS="$partitions" \
PC_DEMO_TOPIC="$topic" \
    exec docker compose -f "$DEMO_DIR/docker-compose.yml" up \
        --build --abort-on-container-exit --exit-code-from demo
