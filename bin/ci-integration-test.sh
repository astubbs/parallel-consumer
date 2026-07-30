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
# docs/SELF_HOSTED_RUNNER.md.

set -euo pipefail

./mvnw --batch-mode \
  -Pci \
  clean verify \
  -DskipUTs=true \
  -Dlicense.skip \
  -Dexcluded.groups=performance,quarantined \
  -Dsurefire.rerunFailingTestsCount=2 \
  "$@"
