#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# The reference demo: the same records through the Apache Kafka client and through Parallel
# Consumer reached over the sidecar. One command, no setup - the broker is a container this
# starts for you, and the sidecar is spawned as a child process you never have to install.
#
# Every per-language demo mirrors THIS file's interface: same flags, same defaults, same two
# tables out. See README.md in this directory for the contract.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

usage() {
    cat <<'USAGE'
usage: parallel-consumer-proxy/demo/run.sh [options]

  --records N        records in the comparison replay   (default 2000)
  --delay-ms N       simulated work per record, ms      (default 2)
  --concurrency N    max in-flight records              (default 100)
  --partitions N     partitions on the demo topic       (default 10)
  --replay-factor N  big replay = records x N; 1 skips  (default 20)
  -h, --help         this

Needs Docker (the broker runs in a container) and a JDK 17 toolchain.
USAGE
}

# ${props[@]+...} rather than "${props[@]}": under `set -u`, bash 3.2 - which is what macOS ships,
# and what a first-time user runs this with - treats an EMPTY array expansion as an unbound variable
# and aborts. That fails exactly the no-argument case, which is the double-click case.
props=()
while [ $# -gt 0 ]; do
    case "$1" in
        --records)       props+=("-Ddemo.records=$2"); shift 2 ;;
        --delay-ms)      props+=("-Ddemo.delayMs=$2"); shift 2 ;;
        --concurrency)   props+=("-Ddemo.maxConcurrency=$2"); shift 2 ;;
        --partitions)    props+=("-Ddemo.partitions=$2"); shift 2 ;;
        --replay-factor) props+=("-Ddemo.replayFactor=$2"); shift 2 ;;
        -h|--help)       usage; exit 0 ;;
        *)               echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if ! docker info >/dev/null 2>&1; then
    echo "Docker is not running - the demo starts its broker in a container." >&2
    exit 1
fi

# -am is not optional: this module's parent is not in a single-module reactor and the enforcer's
# ReactorModuleConvergence rule fails the build before any test runs without it. And the no-tests
# escape is failsafe's own property, not surefire's -DfailIfNoTests.
exec ./mvnw verify \
    -pl parallel-consumer-proxy -am \
    -Dit.test=SidecarDemo \
    -Dpc.demo=true \
    -Dfailsafe.failIfNoSpecifiedTests=false \
    -DskipUTs=true \
    ${props[@]+"${props[@]}"}
