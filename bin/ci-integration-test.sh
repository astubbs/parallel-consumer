#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run integration tests only (failsafe, requires Docker for TestContainers).
# Skips unit tests to avoid duplicate work.
# Usage: bin/ci-integration-test.sh [extra-maven-args...]
#
# Extra args are forwarded to Maven. The self-hosted runner workflow passes
# -DforkCount=4 -DreuseForks=true to run integration in forked per-broker mode
# (each JVM fork gets its own broker - reliable AND parallel; the ci profile
# runs sequentially on GitHub-hosted 2-core runners). See
# docs/self-hosted-runner.md.

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
