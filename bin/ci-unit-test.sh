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

# Group exclusions are HARDCODED here, not inherited from the pom default - enforced by
# QuarantinedAnnotationContractTest (pom inheritance once made the quarantine exclusion a
# silent no-op for unit tests). Keep in sync with the pom excluded.groups default.
./mvnw --batch-mode \
  -Pci \
  clean test \
  -Dlicense.skip \
  -Dexcluded.groups=performance,chaos,quarantined \
  -Dsurefire.rerunFailingTestsCount=2 \
  "$@"
