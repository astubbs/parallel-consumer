#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Verify each quarantined test's OWNER CLAIM against reality (needs `gh` + network).
# For every `- [ ] `Class.method`` entry in docs/QUARANTINED_TESTS.md with an `Owner: PR #NN` marker:
#
#   ERROR (exit 1 - the closed loop is broken, fix the registry):
#     - the owning PR does not exist
#     - the owning PR was closed without merging          -> entry is orphaned, needs a new owner
#     - the owning PR has MERGED but the test is still
#       quarantined                                       -> re-enable is OVERDUE (delete annotation + entry)
#
#   ADVISORY (informational only):
#     - entry has no owner                                -> diagnosed-but-unowned, find it an owner
#     - owner is open and its merge preview still carries the annotation -> normal until the owner
#       merges master; must delete annotation + entry before it merges
#     - quarantine not yet on the owner's base branch     -> preview check n/a, re-check after bases update
#
# Run by the non-gating "Quarantined Tests" CI job; locally via bin/quarantined-test.sh (skipped
# there when gh is absent/unauthenticated).

set -euo pipefail

cd "$(dirname "$0")/.."

REGISTRY=docs/QUARANTINED_TESTS.md
# this repo is a fork - pin gh to the origin remote's repo, or it resolves PR numbers upstream
REPO=$(git remote get-url origin | sed -E 's#\.git$##; s#.*[:/]([^/]+/[^/]+)$#\1#')
fail=0

entries=$(grep -E '^- \[ \] `' "$REGISTRY" 2>/dev/null | sed -E 's/^- \[ \] `([^`]+)`.*/\1/' || true)
[ -z "$entries" ] && { echo "Registry has no entries - nothing to verify."; exit 0; }

for t in $entries; do
  cls=${t%%.*}
  # the entry's continuation lines (until the next entry/blank block) carry the Owner marker
  block=$(awk -v pat="^- \\\\[ \\\\] \`$t\`" '$0 ~ pat {f=1} f && /^- \[ \]/ && $0 !~ pat {exit} f {print}' "$REGISTRY")
  pr=$(echo "$block" | grep -oE 'Owner: PR #[0-9]+' | grep -oE '[0-9]+' | head -1 || true)

  if [ -z "$pr" ]; then
    echo "ADVISORY: $t has no owning PR - diagnosed-but-unowned, find it an owner."
    continue
  fi

  state=$(gh pr view "$pr" -R "$REPO" --json state -q .state 2>/dev/null || echo MISSING)
  case "$state" in
    MISSING)
      echo "ERROR: $t claims owner PR #$pr, but that PR does not exist."; fail=1 ;;
    CLOSED)
      echo "ERROR: $t owner PR #$pr was closed without merging - entry is orphaned, needs a new owner."; fail=1 ;;
    MERGED)
      echo "ERROR: $t owner PR #$pr has MERGED but the test is still quarantined - re-enable is OVERDUE (delete the @Quarantined annotation + registry entry)."; fail=1 ;;
    OPEN)
      file=$(find . -name "$cls.java" -not -path '*/target/*' | head -1)
      relpath=${file#./}
      base=$(gh pr view "$pr" -R "$REPO" --json baseRefName -q .baseRefName)
      if ! git fetch --quiet --depth=1 origin "$base" 2>/dev/null; then
        echo "ADVISORY: $t owner PR #$pr is open; could not fetch its base '$base' to verify - skipping preview check."
        continue
      fi
      if ! git show "FETCH_HEAD:$relpath" 2>/dev/null | grep -q '@Quarantined('; then
        echo "ADVISORY: $t owner PR #$pr is open, but the quarantine is not yet on its base '$base' - preview check n/a, re-check after the base updates."
        continue
      fi
      if ! git fetch --quiet --depth=1 origin "pull/$pr/merge" 2>/dev/null; then
        echo "ADVISORY: $t owner PR #$pr is open but has no merge preview (conflicts?) - cannot verify it removes the quarantine."
        continue
      fi
      if git show "FETCH_HEAD:$relpath" 2>/dev/null | grep -q '@Quarantined('; then
        echo "ADVISORY: $t owner PR #$pr is open and does NOT yet remove the quarantine - it must delete the @Quarantined annotation + registry entry before merging."
      else
        echo "OK: $t owner PR #$pr is open and its merge result removes the quarantine - loop closed."
      fi
      ;;
  esac
done

[ "$fail" -eq 0 ] && echo "Quarantine owner claims verified."
exit "$fail"
