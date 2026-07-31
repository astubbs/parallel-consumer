#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run unit tests only (surefire, no Docker/TestContainers needed).
# Usage: bin/ci-unit-test.sh

set -euo pipefail

# Group exclusions are HARDCODED here, not inherited from the pom default - enforced by
# QuarantinedAnnotationContractTest (pom inheritance once made the quarantine exclusion a
# silent no-op for unit tests). Keep in sync with the pom excluded.groups default.
./mvnw --batch-mode \
  -Pci \
  clean test \
  -Dlicense.skip \
  -Dexcluded.groups=performance,chaos,quarantined \
  -Dsurefire.rerunFailingTestsCount=2
