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

# --fail-at-end so one run reports every independent module's verdict rather than stopping at the
# first failure; the reasoning, and why the exit code survives it, is in bin/ci-build.sh's header.
# It matters most in this lane: an integration run costs ~11.5 minutes uncontended, so each failure
# learned separately is a round trip nobody gets back.
#
# Group exclusions are HARDCODED here, not inherited from the pom default - enforced by
# QuarantinedAnnotationContractTest (pom inheritance once made the quarantine exclusion a
# silent no-op for unit tests). Keep in sync with the pom excluded.groups default.
./mvnw --batch-mode \
  -Pci \
  --fail-at-end \
  clean verify \
  -DskipUTs=true \
  -Dexcluded.groups=performance,chaos,quarantined \
  "$@"
