#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Enforce that docs/QUARANTINED_TESTS.md (the live registry / task list) matches the @Quarantined
# annotations in the code, at METHOD granularity, in BOTH directions:
#   - every annotated class must have registry entries matching its annotation COUNT (a second
#     undiagnosed quarantine cannot ride along on the first entry)
#   - every registry entry must correspond to a still-annotated class, and its named method must
#     still exist in that class (stale method entries are flagged)
# Exits non-zero on drift - run by bin/quarantined-test.sh, the per-PR Quarantine Audit job, and the
# Quarantine Lane itself, which runs on every PR push and every merge to master (drift turns those red:
# real actionable errors, unlike quarantined-test failures).

set -euo pipefail

# QUARANTINE_CHECK_ROOT overrides the scan root - used by QuarantineRegistryScriptTest fixtures.
cd "${QUARANTINE_CHECK_ROOT:-$(dirname "$0")/..}"
# shellcheck source=bin/lib/quarantine-common.sh
source "${BASH_SOURCE[0]%/*}/lib/quarantine-common.sh" 2>/dev/null || source bin/lib/quarantine-common.sh

drift=0

# --- code -> registry: per-class annotation count must equal entry count ---
for f in $(quarantined_files); do
    cls=$(basename "$f" .java)
    count=$(quarantined_occurrences "$f")
    entries=$(registry_entries | awk -F. -v c="$cls" '$1 == c' | wc -l | tr -d ' ')
    if [ "$entries" -eq 0 ]; then
        echo "DRIFT: $cls carries @Quarantined but has NO entry in $REGISTRY - add one (rule 1: no quarantine without diagnosis)."
        drift=1
    elif [ "$entries" -ne "$count" ]; then
        echo "DRIFT: $cls has $count @Quarantined annotation(s) but $entries registry entr(ies) - every quarantined test needs its own diagnosed entry."
        drift=1
    fi
done

# --- registry -> code: entry's class must still be annotated; named method must still exist ---
for e in $(registry_entries); do
    cls=${e%%.*}
    method=""
    case "$e" in *.*) method=${e#*.} ;; esac
    f=$(quarantined_files | while read -r qf; do
            [ "$(basename "$qf" .java)" = "$cls" ] && { echo "$qf"; break; }
        done)
    if [ -z "$f" ]; then
        echo "DRIFT: $REGISTRY lists $e but no @Quarantined annotation found in a class named $cls - stale entry; delete it (rule 3: annotation and entry go together)."
        drift=1
    elif [ -n "$method" ] && ! grep -qE "[[:space:]]$method[[:space:]]*\(" "$f"; then
        echo "DRIFT: $REGISTRY lists $e but no method '$method' exists in $f - stale method entry; fix or delete it."
        drift=1
    fi
done

if [ "$drift" -eq 0 ]; then
    n=$(registry_entries | wc -l | tr -d ' ')
    echo "Quarantine registry consistent ($n entr(ies), method-granularity checked)."
fi
exit "$drift"
