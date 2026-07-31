#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run unit tests only (surefire, no Docker/TestContainers needed).
# Usage: bin/ci-unit-test.sh [extra-maven-args...]
#
# Extra args are forwarded to Maven (mirrors bin/ci-integration-test.sh). To cap per-core forking so
# several concurrent jobs can share a self-hosted box without oversubscribing, pass
# -Dsurefire.forkCount=<n> (NOT bare -DforkCount: the pom pins <forkCount>${surefire.forkCount}</forkCount>,
# so surefire ignores the un-namespaced property). The ci profile default is surefire.forkCount=1C
# (one fork per core), ideal for a single job.

set -euo pipefail

./mvnw --batch-mode \
  -Pci \
  clean test \
  -Dlicense.skip \
  -Dexcluded.groups=performance,quarantined \
  -Dsurefire.rerunFailingTestsCount=2 \
  "$@"
