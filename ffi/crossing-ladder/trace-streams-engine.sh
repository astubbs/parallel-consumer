#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# A `java` stand-in that runs the Streams engine under GraalVM's tracing agent.
#
# The Python Streams demo launches its engine as `<java> -cp <classpath> StreamsMain`, choosing the
# binary through PC_DEMO_JAVA - so pointing that at this script records what a REAL session touches.
# That distinction is the whole point: an agent run that merely starts and stops the engine records
# nothing about the reflection Kafka Streams does while assembling and running a topology, which is
# exactly where the sidecar's first native build failed
# (docs/inflight/perf-native-image-sidecar-works.md).
#
#   PC_DEMO_JAVA=<this script> demo/run.sh --streams --native
#
# Output goes to PC_STREAMS_TRACE_DIR (default: streams-native/trace beside this script), merging
# across runs so several sessions can be traced into one config.

set -euo pipefail

PROBE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TRACE_DIR="${PC_STREAMS_TRACE_DIR:-$PROBE_DIR/streams-native/trace}"

if [ -z "${GRAALVM_HOME:-}" ]; then
    for candidate in "$HOME"/.local/share/mise/installs/java/graalvm-community-* \
                     "$HOME"/.sdkman/candidates/java/*-graal; do
        [ -x "$candidate/bin/java" ] && GRAALVM_HOME="$candidate" && break
    done
fi
if [ -z "${GRAALVM_HOME:-}" ] || [ ! -x "$GRAALVM_HOME/bin/java" ]; then
    echo "no GraalVM found for the tracing agent. Set GRAALVM_HOME." >&2
    exit 1
fi

mkdir -p "$TRACE_DIR"

# config-merge-dir rather than config-output-dir: a second traced run then ADDS to the first rather
# than replacing it, which is what lets the failure path and the happy path share one config.
exec "$GRAALVM_HOME/bin/java" \
    "-agentlib:native-image-agent=config-merge-dir=$TRACE_DIR" \
    "$@"
