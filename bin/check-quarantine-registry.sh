#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Enforce that docs/QUARANTINED_TESTS.md (the live registry / task list) matches the @Quarantined
# annotations in the code, in BOTH directions:
#   - every test class carrying @Quarantined must have a registry entry
#   - every registry entry must correspond to a class still carrying @Quarantined
# Comparison is at test-CLASS granularity (registry entries name `Class.method`; the class is the
# token before the first dot). Exits non-zero on drift - run by bin/quarantined-test.sh and the
# non-gating "Quarantined Tests" CI job (drift turns that job red: a real actionable error, unlike
# quarantined-test failures which are expected).

set -euo pipefail

# QUARANTINE_CHECK_ROOT overrides the scan root - used by QuarantineRegistryScriptTest to exercise
# this script against temp fixtures; defaults to the repo root.
cd "${QUARANTINE_CHECK_ROOT:-$(dirname "$0")/..}"

REGISTRY=docs/QUARANTINED_TESTS.md

code_classes=$(grep -rlE --include='*.java' --exclude-dir=target '^[[:space:]]*@Quarantined\(' . 2>/dev/null \
  | xargs -n1 basename 2>/dev/null | sed 's/\.java$//' | sort -u || true)

registry_classes=$(grep -E '^- \[ \] `' "$REGISTRY" 2>/dev/null \
  | sed -E 's/^- \[ \] `([^`.]+).*/\1/' | sort -u || true)

drift=0

for c in $code_classes; do
  if ! echo "$registry_classes" | grep -qx "$c"; then
    echo "DRIFT: $c carries @Quarantined but has NO entry in $REGISTRY - add one (rule 1: no quarantine without diagnosis)."
    drift=1
  fi
done

for c in $registry_classes; do
  if ! echo "$code_classes" | grep -qx "$c"; then
    echo "DRIFT: $REGISTRY lists $c but no @Quarantined annotation found - stale entry; delete it (rule 3: annotation and entry go together)."
    drift=1
  fi
done

if [ "$drift" -eq 0 ]; then
  n=$(echo "$registry_classes" | grep -c . || true)
  echo "Quarantine registry consistent ($n class(es) quarantined)."
fi
exit "$drift"
