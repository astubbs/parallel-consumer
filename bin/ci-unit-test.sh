#!/usr/bin/env bash
#
# Copyright (C) 2020-2026 Confluent, Inc.
#

# Run unit tests only (surefire, no Docker/TestContainers needed).
# Usage: bin/ci-unit-test.sh

set -euo pipefail

./mvnw --batch-mode \
  -Pci \
  clean test \
  -Dlicense.skip \
  -Dexcluded.groups=performance \
  -Dsurefire.rerunFailingTestsCount=2
