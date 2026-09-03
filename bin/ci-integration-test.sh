#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run integration tests only (failsafe, requires Docker for TestContainers).
# Skips unit tests to avoid duplicate work.
# Usage: bin/ci-integration-test.sh [extra-maven-args...]
#
# Extra args are forwarded to Maven. Forked per-broker mode - each JVM fork gets its
# own broker, reliable AND parallel - is requested with -DforkCount=N -DreuseForks=true,
# and .github/workflows/maven.yml's gating Integration Tests lane passes forkCount=4.
# This script itself passes none, so a bare invocation runs failsafe at its default of 1.
#
# forkCount=4 is a measured ceiling, not a floor: raising it to 6 was measured on the hosted
# runner at 469s of failsafe against a 420s baseline, because the forks contend (the same
# tests cost 11% more CPU time), and it produced a first-ever timeout failure in
# ManagedPCInstanceLifecycleTest. See docs/plans/2026-09-03-001-investigate-integration-gate-wall-time.md
# before raising it. See also docs/self-hosted-runner.md.

set -euo pipefail

# Group exclusions are HARDCODED here, not inherited from the pom default - enforced by
# QuarantinedAnnotationContractTest (pom inheritance once made the quarantine exclusion a
# silent no-op for unit tests). Keep in sync with the pom excluded.groups default - the two lists
# drifting is silent in the direction that matters: a lane the pom excludes but this does not runs
# here, in the GATING suite, which is how the Lincheck lane would have arrived if nobody looked.
./mvnw --batch-mode \
  -Pci \
  clean verify \
  -DskipUTs=true \
  -Dexcluded.groups=performance,chaos,quarantined,lincheck \
  "$@"
