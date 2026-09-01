#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# The C++ demo: the same records through C++'s own Kafka client, and through C++ over the sidecar.
# One command, no setup.
#
# It mirrors the reference demo's interface - same flags, same defaults, same two tables out. The
# contract is parallel-consumer-proxy/demo/README.md; what is specific to C++ is demo/README.md
# beside this file.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

DEMO_DIR="parallel-consumer-proxy-clients/parallel-consumer-proxy-client-cpp/demo"

usage() {
    cat <<'USAGE'
usage: demo/run.sh [options]

  --records N        records in the comparison replay   (default 2000)
  --delay-ms N       simulated work per record, ms      (default 2)
  --concurrency N    max in-flight records              (default 100)
  --partitions N     partitions on the demo topic       (default 10)
  --replay-factor N  big replay = records x N; 1 skips  (default 20)
  --bootstrap ADDR   an existing broker; omit to use the compose sibling
  --topic NAME       an existing topic; omit to create one

  --docker           run in a container - the only mode C++ has, and the default
  -h, --help         this

Needs Docker and nothing else: no C++ toolchain, no gRPC dev packages, no JDK. Everything is
built in the image, including the sidecar, which the client library spawns as a child process.

Every flag also has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.
USAGE
}

# ${args[@]+...} rather than "${args[@]}": under `set -u`, bash 3.2 - which is what macOS ships, and
# what a first-time user runs this with - treats an EMPTY array expansion as an unbound variable and
# aborts. That fails exactly the no-argument case, which is the double-click case.
args=()
while [ $# -gt 0 ]; do
    case "$1" in
        --records|--delay-ms|--concurrency|--partitions|--replay-factor|--bootstrap|--topic)
            if [ $# -lt 2 ]; then
                echo "$1 needs a value" >&2
                exit 2
            fi
            args+=("$1" "$2"); shift 2 ;;
        --docker)  shift ;;
        --native)
            # Answered rather than rejected as unknown, because a reader arriving from the Java
            # demo will type it.
            cat >&2 <<'NATIVE'
There is no native mode for the C++ demo, and that is the one place it diverges from the
reference. C++ needs gRPC, protobuf and librdkafka as system development packages rather than
as a versioned toolchain, so the container IS the toolchain here - the same reason
bin/build-client.sh builds this language in an image. Run it without --native.
NATIVE
            exit 2 ;;
        -h|--help) usage; exit 0 ;;
        *)         echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

echo "Mode: container - C++ has no host toolchain path, so the demo brings its whole toolchain."

if ! docker info >/dev/null 2>&1; then
    echo "Docker is not running - the demo needs it for the broker and for its own image." >&2
    exit 1
fi

# The broker is a COMPOSE SIBLING and the demo container is never given the host Docker socket: a
# documented socket mount is root-equivalent host access taught as the normal way to run the product
# (plan unit U35). PC_DEMO_ARGS carries the flags through to the container, where the same parser
# reads them.
PC_DEMO_ARGS="${args[*]+${args[*]}}" \
    exec docker compose -f "$DEMO_DIR/docker-compose.yml" up \
        --build --abort-on-container-exit --exit-code-from demo
