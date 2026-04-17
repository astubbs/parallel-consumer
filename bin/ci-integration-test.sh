#!/usr/bin/env bash
#
# Copyright (C) 2020-2026 Confluent, Inc.
#

# Run integration tests only (failsafe, requires Docker for TestContainers).
# Skips unit tests to avoid duplicate work.
# Usage: bin/ci-integration-test.sh

set -euo pipefail

./mvnw --batch-mode \
  -Pci \
  clean verify \
  -DskipUTs=true \
  -Dlicense.skip \
  -Dexcluded.groups=performance \
  -Dsurefire.rerunFailingTestsCount=2
