#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run ONLY the quarantined tests (@Quarantined / @Tag("quarantined")) - the known-failing-on-master
# tests excluded from the gating suites. Red here is EXPECTED while the owning fix PR is open; the
# point of running them anyway is to see when they start passing (fix landed), when they get worse,
# and to keep their signal alive (a "known flake" can be a real product bug - see
# docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md - lands with PR astubbs#80).
#
# Usage: bin/quarantined-test.sh [extra-maven-args...]
#
# The audit listing (every @Quarantined + its owning fix PR) is printed first so unowned
# quarantines are visible even when the run itself is red.

set -euo pipefail

cd "$(dirname "$0")/.."
source bin/lib/quarantine-common.sh

# QUARANTINE_SKIP_CHECKS=1 skips the rule checks (CI sets it when the same checks already ran as
# separate fail-fast gating steps - re-running them inside a continue-on-error step would swallow
# their failures; ce-review finding).
if [ "${QUARANTINE_SKIP_CHECKS:-0}" != "1" ]; then
  echo "=== Quarantine registry check (docs/quarantined-tests.md must match the annotations) ==="
  bin/check-quarantine-registry.sh
  echo "=== Quarantine owner-claim check (needs gh; skipped when unavailable) ==="
  if gh auth status >/dev/null 2>&1; then
    bin/check-quarantine-owners.sh
  else
    echo "(gh unavailable/unauthenticated - owner claims not verified locally; CI verifies them)"
  fi
fi
echo "=== Quarantine audit (entries are diagnosed, or recorded rule-1 exceptions; empty fixedBy = unowned) ==="
quarantined_audit || echo "(no @Quarantined tests - this lane is empty)"
echo "==================================================================================================="

# WHY -Dmaven.test.failure.ignore=true, AND WHY ITS ABSENCE WAS A SILENT HOLE.
#
# This lane exists to RUN known-failing tests, so a failing test must not stop the run - but by
# default a surefire failure fails the module and the reactor never reaches failsafe. With only
# quarantined UNIT tests that was invisible: surefire ran them, the build failed, and everything the
# lane needed had already been recorded. Add one quarantined INTEGRATION test and the hole opens -
# `ProducerManagerTest` fails in surefire, `parallel-consumer-core` goes FAILURE, and failsafe never
# executes, so the integration test runs NOWHERE while the job still reports success (the workflow
# step is continue-on-error). That is `@Disabled` with extra steps, which is the exact outcome
# quarantining is meant to prevent, and nothing goes red to say so.
#
# Ignoring failures makes both surefire and failsafe run to completion and write their XML, which is
# what the verdict is actually read from - bin/quarantine-lane-report.sh parses the reports, and the
# workflow's own comment already says a green tick here does not mean the tests passed. So the exit
# code was never the verdict for this lane; it is now explicitly not one.
./mvnw --batch-mode \
  -Pci \
  clean verify \
  -Dincluded.groups=quarantined \
  -Dexcluded.groups= \
  -Dmaven.test.failure.ignore=true \
  "$@"
