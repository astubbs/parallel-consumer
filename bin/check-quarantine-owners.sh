#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Verify each quarantined test's OWNER CLAIM against reality (needs `gh` + network).
# For every registry entry with an `Owner: PR #NN` / `Owner: PR astubbs#NN` marker:
#
#   ERROR (exit 1 - the closed loop is broken, fix the registry):
#     - the owning PR does not exist (confirmed not-found, NOT a transient gh failure)
#     - the owning PR was closed without merging          -> entry is orphaned, needs a new owner
#     - the owning PR has MERGED but the test is still
#       quarantined                                       -> re-enable is OVERDUE (delete annotation + entry)
#
#   ADVISORY (informational only):
#     - entry has no owner                                -> diagnosed-but-unowned, find it an owner
#     - gh unavailable after retries (rate limit / 5xx / auth) -> transient infra weather must NOT
#       red the audit ("red here is real" guarantee); skipped with a note
#     - owner open, quarantine not yet on its base        -> preview check n/a, re-check later
#     - owner open, merge preview still carries the annotation -> must remove it before merging
#     - the test file is absent from the merge preview (renamed?) -> cannot verify removal
#     - annotation's fixedBy PR number disagrees with the registry's Owner line
#
# Run by the per-PR Quarantine Audit job and by the Quarantine Lane (every PR push + every merge to
# master); locally via bin/quarantined-test.sh
# (skipped there when gh is absent/unauthenticated).

set -euo pipefail

cd "${QUARANTINE_CHECK_ROOT:-$(dirname "$0")/..}"
# shellcheck source=bin/lib/quarantine-common.sh
source "${BASH_SOURCE[0]%/*}/lib/quarantine-common.sh" 2>/dev/null || source bin/lib/quarantine-common.sh

# this repo is a fork - pin gh to the origin remote's repo, or it resolves PR numbers upstream
REPO=$(git remote get-url origin | sed -E 's#\.git$##; s#.*[:/]([^/]+/[^/]+)$#\1#')
fail=0

# gh with retry + error classification: echoes the value, or MISSING (confirmed not-found), or
# TRANSIENT (still failing after retries - infra weather, never an ERROR).
gh_query() { # $1=pr  $2=jq field (e.g. .state)
    local err out rc attempt
    err=$(mktemp)
    for attempt in 1 2 3; do
        if out=$(gh pr view "$1" -R "$REPO" --json "${2#.}" -q "$2" 2>"$err"); then
            rm -f "$err"; echo "$out"; return 0
        fi
        if grep -qiE 'could not resolve|not found|no pull requests' "$err"; then
            rm -f "$err"; echo MISSING; return 0
        fi
        sleep "$attempt"
    done
    rm -f "$err"; echo TRANSIENT
}

entries=$(registry_entries)
[ -z "$entries" ] && { echo "Registry has no entries - nothing to verify."; exit 0; }

for t in $entries; do
    cls=${t%%.*}
    block=$(registry_entry_block "$t")
    # Accepts `Owner: PR #NN`, `Owner: PR astubbs#NN` and the fully qualified
    # `Owner: PR astubbs/parallel-consumer#NN`. The qualified forms exist because
    # bin/check-issue-refs.sh rejects a bare `#NN` below its threshold - the fork's numbers sit
    # inside confluentinc's range - so the registry could not satisfy both gates at once. Extract
    # from the `#NN` tail, never from the whole match, or a digit in the qualifier would win.
    pr=$(echo "$block" | grep -oE 'Owner: PR (astubbs/parallel-consumer|astubbs)?#[0-9]+' | grep -oE '#[0-9]+' | tr -d '#' | head -1 || true)

    if [ -z "$pr" ]; then
        echo "ADVISORY: $t has no owning PR - diagnosed-but-unowned, find it an owner."
        continue
    fi

    state=$(gh_query "$pr" .state)
    case "$state" in
        TRANSIENT)
            echo "ADVISORY: $t owner PR #$pr - gh unavailable after retries (rate limit/network?); owner claim NOT verified this run. Not an error: transient infra must not red the audit." ;;
        MISSING)
            echo "ERROR: $t claims owner PR #$pr, but that PR does not exist."; fail=1 ;;
        CLOSED)
            echo "ERROR: $t owner PR #$pr was closed without merging - entry is orphaned, needs a new owner."; fail=1 ;;
        MERGED)
            echo "ERROR: $t owner PR #$pr has MERGED but the test is still quarantined - re-enable is OVERDUE (delete the @Quarantined annotation + registry entry)."; fail=1 ;;
        OPEN)
            file=$(find . -name "$cls.java" -not -path '*/target/*' | head -1)
            relpath=${file#./}
            # fixedBy cross-check (annotation attribute vs registry Owner line) - advisory only,
            # and only when the class carries exactly one annotation (else ambiguous)
            if [ -n "$file" ] && [ "$(quarantined_occurrences "$file")" = "1" ]; then
                # Same three forms the Owner marker accepts - see the parse above. Matching only
                # `PR #NN` here silently disabled this cross-check the moment an annotation used a
                # qualified reference: no match, empty `declared`, advisory never fires. A check that
                # quietly stops checking is worse than one that never existed.
                declared=$(grep -oE 'fixedBy = "(PR )?(astubbs/parallel-consumer|astubbs)?#?[0-9]+' "$file" \
                    | grep -oE '[0-9]+$' | head -1 || true)
                if [ -n "$declared" ] && [ "$declared" != "$pr" ]; then
                    echo "ADVISORY: $t annotation says fixedBy PR #$declared but the registry Owner line says PR #$pr - align them."
                fi
            fi
            base=$(gh_query "$pr" .baseRefName)
            if [ "$base" = "TRANSIENT" ] || [ "$base" = "MISSING" ]; then
                echo "ADVISORY: $t owner PR #$pr is open; could not resolve its base branch - skipping preview check."
                continue
            fi
            if ! git fetch --quiet --depth=1 origin "$base" 2>/dev/null; then
                echo "ADVISORY: $t owner PR #$pr is open; could not fetch its base '$base' to verify - skipping preview check."
                continue
            fi
            # Herestring: `git show | grep -q` under pipefail turns a MATCH into a failure once
            # the file exceeds the 64 KiB pipe buffer. The largest source file here is already
            # within a few hundred bytes of that.
            if ! grep -qE "$QUARANTINE_ANNOTATION_ERE" <<<"$(git show "FETCH_HEAD:$relpath" 2>/dev/null)"; then
                echo "ADVISORY: $t owner PR #$pr is open, but the quarantine is not yet on its base '$base' - preview check n/a, re-check after the base updates."
                continue
            fi
            if ! git fetch --quiet --depth=1 origin "pull/$pr/merge" 2>/dev/null; then
                echo "ADVISORY: $t owner PR #$pr is open but has no merge preview (conflicts?) - cannot verify it removes the quarantine."
                continue
            fi
            if ! git cat-file -e "FETCH_HEAD:$relpath" 2>/dev/null; then
                echo "ADVISORY: $t owner PR #$pr merge preview does not contain $relpath (file renamed/moved?) - cannot verify removal; check manually."
                continue
            fi
            if grep -qE "$QUARANTINE_ANNOTATION_ERE" <<<"$(git show "FETCH_HEAD:$relpath" 2>/dev/null)"; then
                echo "ADVISORY: $t owner PR #$pr is open and does NOT yet remove the quarantine - it must delete the @Quarantined annotation + registry entry before merging."
            else
                echo "OK: $t owner PR #$pr is open and its merge result removes the quarantine - loop closed."
            fi
            ;;
        *)
            echo "ADVISORY: $t owner PR #$pr returned unexpected state '$state' - not verified this run." ;;
    esac
done

[ "$fail" -eq 0 ] && echo "Quarantine owner claims verified."
exit "$fail"
