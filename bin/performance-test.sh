#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run only the performance test suite (tests tagged @Tag("performance")).
# These are excluded from the regular CI build because they take a long time
# and need substantial hardware. Called by the "Performance Tests" leg of the
# `test` matrix in .github/workflows/maven.yml (a required check on every PR),
# and by pr-highcpu-fast-feedback.yml on the self-hosted highcpu runners.
#
# Usage: bin/performance-test.sh [extra-maven-args...]

set -euo pipefail

./mvnw --batch-mode \
  -Pci \
  clean verify \
  -DskipUTs=true \
  -Dincluded.groups=performance \
  -Dexcluded.groups= \
  "$@"
