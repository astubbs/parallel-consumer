#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run unit tests only (surefire, no Docker/TestContainers needed).
# Usage: bin/ci-unit-test.sh

set -euo pipefail

# Test-group exclusion (performance,chaos) comes from pom.xml's excluded.groups default -
# the single source of truth; pass -Dexcluded.groups=... explicitly to deviate.
./mvnw --batch-mode \
  -Pci \
  clean test \
  -Dlicense.skip \
  -Dsurefire.rerunFailingTestsCount=2
