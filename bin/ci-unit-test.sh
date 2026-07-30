#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run unit tests only (surefire, no Docker/TestContainers needed).
# Usage: bin/ci-unit-test.sh

set -euo pipefail

./mvnw --batch-mode \
  -Pci \
  clean test \
  -Dlicense.skip \
  -Dexcluded.groups=performance,quarantined \
  -Dsurefire.rerunFailingTestsCount=2
