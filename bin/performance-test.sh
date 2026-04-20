#!/usr/bin/env bash
#
# Copyright (C) 2020-2026 Confluent, Inc.
#

# Run only the performance test suite (tests tagged @Tag("performance")).
# These are excluded from the regular CI build because they take a long time
# and need substantial hardware. The self-hosted runner workflow
# (.github/workflows/performance.yml) calls this script.
#
# Usage: bin/performance-test.sh [extra-maven-args...]

set -euo pipefail

./mvnw --batch-mode \
  -Pci \
  clean verify \
  -Dincluded.groups=performance \
  -Dexcluded.groups= \
  -Dlicense.skip \
  "$@"
