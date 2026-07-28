#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run only the performance test suite (tests tagged @Tag("performance")).
# These are excluded from the regular CI build because they take a long time
# and need substantial hardware. The self-hosted runner workflow
# (.github/workflows/self-hosted-tests.yml) calls this script.
#
# Usage: bin/performance-test.sh [extra-maven-args...]

set -euo pipefail

./mvnw --batch-mode \
  -Pci \
  clean verify \
  -DskipUTs=true \
  -Dincluded.groups=performance \
  -Dexcluded.groups= \
  -Dlicense.skip \
  "$@"
