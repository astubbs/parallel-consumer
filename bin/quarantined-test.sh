#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run ONLY the quarantined tests (@Quarantined / @Tag("quarantined")) - the known-failing-on-master
# tests excluded from the gating suites. Red here is EXPECTED while the owning fix PR is open; the
# point of running them anyway is to see when they start passing (fix landed), when they get worse,
# and to keep their signal alive (a "known flake" can be a real product bug - see
# docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md).
#
# Usage: bin/quarantined-test.sh [extra-maven-args...]
#
# The audit listing (every @Quarantined + its owning fix PR) is printed first so unowned
# quarantines are visible even when the run itself is red.

set -euo pipefail

echo "=== Quarantine registry check (docs/QUARANTINED_TESTS.md must match the annotations) ==="
"$(dirname "$0")/check-quarantine-registry.sh"
echo "=== Quarantine owner-claim check (needs gh; skipped when unavailable) ==="
if gh auth status >/dev/null 2>&1; then
  "$(dirname "$0")/check-quarantine-owners.sh"
else
  echo "(gh unavailable/unauthenticated - owner claims not verified locally; CI verifies them)"
fi
echo "=== Quarantine audit (every entry must be diagnosed; empty fixedBy = unowned, needs an owner) ==="
grep -rn --include='*.java' --exclude-dir=target -A 4 '@Quarantined(' . || echo "(no @Quarantined tests - this lane is empty)"
echo "==================================================================================================="

./mvnw --batch-mode \
  -Pci \
  clean verify \
  -Dincluded.groups=quarantined \
  -Dexcluded.groups= \
  -Dlicense.skip \
  "$@"
