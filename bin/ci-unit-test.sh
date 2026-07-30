#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run unit tests only (surefire, no Docker/TestContainers needed).
# Usage: bin/ci-unit-test.sh [extra-maven-args...]
#
# Extra args are forwarded to Maven (mirrors bin/ci-integration-test.sh). The self-hosted high-CPU runners
# pass -DforkCount=<n> to CAP per-core forking so several concurrent jobs share the box without
# oversubscribing (the ci profile default is forkCount=1C = all cores, ideal for a single job).

set -euo pipefail

./mvnw --batch-mode \
  -Pci \
  clean test \
  -Dlicense.skip \
  -Dexcluded.groups=performance,quarantined \
  -Dsurefire.rerunFailingTestsCount=2 \
  "$@"
