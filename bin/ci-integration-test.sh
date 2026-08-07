#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run integration tests only (failsafe, requires Docker for TestContainers).
# Skips unit tests to avoid duplicate work.
# Usage: bin/ci-integration-test.sh [extra-maven-args...]
#
# Extra args are forwarded to Maven. The self-hosted runner workflow passes
# -DforkCount=4 -DreuseForks=true to run integration in forked per-broker mode
# (each JVM fork gets its own broker - reliable AND parallel; the ci profile
# runs sequentially on GitHub-hosted 2-core runners). See
# docs/SELF_HOSTED_RUNNER.md.

set -euo pipefail

# Group exclusions are HARDCODED here, not inherited from the pom default - enforced by
# QuarantinedAnnotationContractTest (pom inheritance once made the quarantine exclusion a
# silent no-op for unit tests). Keep in sync with the pom excluded.groups default.
# The dashboard's browser tests SKIP when no Chrome is present, so that a developer without one
# still gets a green local build. In CI that default is wrong: ubuntu-latest ships Chrome today, so
# the suite runs - but if the image ever drops it, the tests would vanish silently and the lane would
# stay green with nothing checking the rendered page at all. Requiring the browser here turns that
# into a loud failure. A skipped suite is indistinguishable from a passing one, which is the whole
# reason this flag exists.
./mvnw --batch-mode \
  -Pci \
  clean verify \
  -DskipUTs=true \
  -Dexcluded.groups=performance,chaos,quarantined \
  -Dsurefire.rerunFailingTestsCount=2 \
  -Ddashboard.ui.requireBrowser=true \
  "$@"
