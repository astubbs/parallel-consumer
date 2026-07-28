#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run integration tests only (failsafe, requires Docker for TestContainers).
# Skips unit tests to avoid duplicate work.
# Usage: bin/ci-integration-test.sh [extra-maven-args...]
#
# Extra args are forwarded to Maven. The self-hosted runner workflow passes
# -Dparallel-tests=true to re-enable JUnit's concurrent execution (the ci
# profile disables it for GitHub-hosted 2-core runners). See
# docs/SELF_HOSTED_RUNNER.md.

set -euo pipefail

./mvnw --batch-mode \
  -Pci \
  clean verify \
  -DskipUTs=true \
  -Dlicense.skip \
  -Dexcluded.groups=performance \
  -Dsurefire.rerunFailingTestsCount=2 \
  "$@"
