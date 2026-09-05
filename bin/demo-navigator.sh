#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Watch the navigator's global rate limiting work across JVMs, live: two child processes in one
# consumer group split one 2-credits/sec resource at ~1 record/sec each - each holding one of two
# partitions, so each reports a share of 0.500 - while an untagged bystander process drains
# flat-out beside them. Then one tagged JVM is KILLED outright and the survivor inherits the whole
# rate. Printed as a clean per-second dashboard with no log noise.
#
# The storyline runs about 30 seconds once the broker is up - a little longer than the in-process
# demo it replaced, because a killed member is only rebalanced away after session.timeout.ms, which
# the children run at the broker's floor (~6s). Those six seconds of the group not yet knowing are
# part of the story, not dead air.
#
# Runs NavigatorDemo (failsafe-collected, off by default behind -Dpc.demo=true - the same
# discipline as the classic Demo and AdaptiveConcurrencyDemo). The asserted version of the same
# storyline is NavigatorPartitionShareIT (its AE1 and AE2 scenarios), which CI runs on every PR;
# this one is for eyes.
#
# Needs: JDK 17 on JAVA_HOME (Jabel - see the root AGENTS.md), and Docker running
# (TestContainers starts a real Kafka broker).
#
# Any extra arguments are passed through to mvnw.

set -euo pipefail

cd "$(dirname "$0")/.."

if ! docker info >/dev/null 2>&1; then
    echo "demo-navigator: Docker does not appear to be running - the demo starts a real Kafka" >&2
    echo "demo-navigator: broker via TestContainers and cannot run without it." >&2
    exit 2
fi

echo "demo-navigator: building and starting (a real Kafka broker spins up first - allow ~a minute)..."

exec ./mvnw -q verify -pl parallel-consumer-core -am \
    -Dpc.demo=true \
    -Dit.test=NavigatorDemo \
    -Dtest=skipall -DfailIfNoTests=false \
    -Dsurefire.failIfNoSpecifiedTests=false \
    -Dfailsafe.failIfNoSpecifiedTests=false \
    "$@"
