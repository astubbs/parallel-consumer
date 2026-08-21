#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Runs the cross-language conformance suite for one container-toolchain language INSIDE that
# language's own image.
#
# WHY THIS EXISTS. `bin/build-client.sh` builds C++ and Swift in a container and extracts the
# artifacts to the host. That is sound when the host is Linux, and impossible when it is not: the
# artifact is a Linux binary, and on macOS the shell answers **126** - "found, but cannot execute" -
# for every scenario, twenty times, with an empty stdout and nothing saying "wrong operating system".
# So those two languages could not be driven from a developer's laptop at all.
#
# WHY THE WHOLE SUITE MOVES, NOT JUST THE RUNNER. The client spawns the sidecar as its own child
# process and reaches it over loopback (KTD41), and the suite's SidecarShim announces a bare `port:`
# with no host - deliberately, so the client exercises its real spawn-and-reap path instead of gaining
# a connect-to-an-existing-port option that would bind ten languages. Client, shim and engine
# therefore have to share ONE loopback. Measured on Docker Desktop 27.4.0: a container cannot reach
# the host's 127.0.0.1 even with `--network host`, while `host.docker.internal` works - an address no
# client can be told to use without changing the protocol and R29's authority allowlist. Moving the
# whole run inside one container makes that shared loopback the container's own, and needs no product
# change whatsoever.
#
# WHAT RUNS: this suite, the engine it hosts in its own JVM, the shim it writes, and the runner it
# spawns - all in the image, all on one loopback.
#
# WHY IT IS NOT `--network none`, which would have been a tidy proof that nothing crosses the
# boundary: the mounted ~/.m2 is a macOS repository, and Maven needs PLATFORM-CLASSIFIED artifacts
# for the Linux container - `com.google.protobuf:protoc:exe:linux-aarch_64`, which a host cache has
# never had a reason to hold. A cold run therefore has to fetch, and offline fails before the suite
# is even reached. Set PC_CONFORMANCE_OFFLINE=1 once the cache is warm to get that proof back; the
# classifiers coexist, so warming it costs the host nothing.
#
# CI DOES NOT USE THIS AND NEVER WILL, because its runners are Linux and the extracted artifact is
# native there. That is the rot risk to design against: this path is exercised only by developers on
# macOS, so a scheduled run is what would keep it honest. Recorded in
# docs/inflight/parked-containerised-toolchains-and-runtime.md.
#
# ONLY the conformance suite runs, not every test in every module `-am` builds. `-am` is still
# required - the suite depends on those modules' jars and test-jars - but running parallel-consumer-core's
# whole suite inside a resource-constrained container adds minutes and imports its contention-sensitive
# tests into a result that is supposed to be about C++. The first run here failed on three of them,
# including `processInKeyOrder`, which the flake ledger already records as contention.
#
# Run: bin/run-conformance-in-container.sh <cpp|swift> [extra maven args...]

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
readonly REPO_ROOT
readonly CONFORMANCE_MODULE=":parallel-consumer-proxy-conformance"

die() { # <exit code> <message...>
    local code="$1"
    shift
    printf 'bin/run-conformance-in-container.sh: %s\n' "$*" >&2
    exit "$code"
}

LANGUAGE="${1:-}"
case "$LANGUAGE" in
    cpp | swift) shift ;;
    "") die 2 "usage: bin/run-conformance-in-container.sh <cpp|swift> [maven args...]" ;;
    *) die 2 "$LANGUAGE does not build in a container - only cpp and swift do, and every other
        language's runner already executes on this host. Run the suite the ordinary way:
        ./mvnw test -pl $CONFORMANCE_MODULE -am -Dpc.conformance.language=$LANGUAGE" ;;
esac

command -v docker >/dev/null 2>&1 \
    || die 2 "docker is not installed, and this path exists precisely because $LANGUAGE cannot run
        natively here. Nothing was run - this is NOT a pass."
docker info >/dev/null 2>&1 \
    || die 2 "the docker daemon is not reachable. Nothing was run - this is NOT a pass."

MODULE_DIR="$REPO_ROOT/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-$LANGUAGE"
[ -f "$MODULE_DIR/Dockerfile" ] || die 3 "no Dockerfile in $MODULE_DIR"

IMAGE="pc-conformance-$LANGUAGE"

printf '==> %s: building the conformance image (%s)\n' "$LANGUAGE" "$IMAGE"
# The same build context and named proto context bin/build-client.sh uses, so this image and the
# artifacts CI extracts come from one Dockerfile and one `build` stage.
docker buildx build \
    --target conformance \
    --build-context "proto=$REPO_ROOT/parallel-consumer-proxy-protocol/src/main/proto" \
    --tag "$IMAGE" \
    --load \
    "$MODULE_DIR" \
    || die 1 "$LANGUAGE: conformance image build failed"

# The Maven repository is mounted rather than baked: the image would otherwise carry a copy of every
# dependency and go stale the moment one moved.
M2_DIR="${HOME}/.m2"
mkdir -p "$M2_DIR"

printf '==> %s: running the suite inside the container\n' "$LANGUAGE"
# Network and offline travel together: with PC_CONFORMANCE_OFFLINE=1 the run proves nothing crossed
# the boundary, and without it Maven may fetch the Linux-classified artifacts a macOS ~/.m2 lacks.
network_args=()
maven_offline=()
if [ "${PC_CONFORMANCE_OFFLINE:-0}" = "1" ]; then
    network_args=(--network none)
    maven_offline=(--offline)
    printf '==> %s: offline - no network at all, so a pass proves the run is self-contained\n' "$LANGUAGE"
fi

docker run --rm \
    "${network_args[@]+"${network_args[@]}"}" \
    --volume "$REPO_ROOT:/repo" \
    --volume "$M2_DIR:/root/.m2" \
    --workdir /repo \
    "$IMAGE" \
    ./mvnw --batch-mode "${maven_offline[@]+"${maven_offline[@]}"}" test \
        -pl "$CONFORMANCE_MODULE" -am \
        -Dpc.conformance.language="$LANGUAGE" \
        -Dtest=ConformanceSuiteTest \
        -Dsurefire.failIfNoSpecifiedTests=false \
        -Dcopyright.skip=true \
        "$@"
status=$?

if [ "$status" -ne 0 ]; then
    die "$status" "$LANGUAGE: the conformance suite failed inside the container.
        That is a real result - the runner and the engine were on the same loopback, so this is the
        language's behaviour rather than the host's incompatibility."
fi

printf '==> %s: conformance passed inside the container\n' "$LANGUAGE"
