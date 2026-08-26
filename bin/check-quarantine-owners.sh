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
#     - entry has no owner                                -> advisory: diagnosed-but-unowned (find it an
#                                                            owner), or UNDIAGNOSED when the entry records
#                                                            a rule-1 exception (complete the diagnosis)
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
ORIGIN_URL=$(git remote get-url origin)
REPO=$(sed -E 's#\.git$##; s#.*[:/]([^/]+/[^/]+)$#\1#' <<<"$ORIGIN_URL")
fail=0

# REF PREVIEWS ARE FETCHED INTO A THROWAWAY GIT DIR, NEVER INTO THIS CLONE.
#
# The preview check below needs two remote refs - the owner PR's base, and its merge preview - and
# it used to reach them with `git fetch --depth=1` right here. A depth-limited fetch writes the
# `shallow` file, and that file lives in the SHARED --git-common-dir, so one run truncated history
# for every worktree of the clone at once. This gate is swept by bin/check-all.sh, which made the
# mandated pre-push sweep an instruction to corrupt the clone.
#
# The damage lands on OTHER commands and nothing goes red: `git merge-base` returns empty,
# ahead/behind counts read in the hundreds, and a commit that plainly landed reports "not an
# ancestor of master" - all of which read as a rewritten history rather than as missing objects.
# .claude/hooks/check-shallow-history.sh denies those queries while shallow; this is the other half.
#
# FETCHING ELSEWHERE RATHER THAN FETCHING DEEPER HERE is what makes that unconditional. Choosing the
# depth from `git rev-parse --is-shallow-repository` would still SAMPLE shared state a sibling
# worktree can change between the sample and the fetch. A scratch git dir has no shared state to
# race on; it deepens nothing when the clone arrived shallow on purpose (CI checks out at depth 1);
# and if this script is killed mid-fetch the clone is exactly as it was, because it was never the
# fetch target. Cost of the isolation, measured on this repo: ~1.4s and ~2.3MB for the first fetch,
# and the dir is reused for the rest of the run.
#
# ONE SCRATCH DIR FOR THE WHOLE RUN, created eagerly below and removed by a single trap. Lazily is
# what it wants to be, and cannot: gh_query runs inside `$(...)`, so a dir it created would be
# recorded in a subshell and leak on every exit. It also holds gh_query stderr capture, which used
# to be a per-call `mktemp` that survived an interrupted run.
scratch_dir=""
# IDEMPOTENT, AND IT DISARMS FIRST: a second signal arriving during teardown re-enters the handler
# and abandons the `rm -rf` half-done.
scratch_cleanup() { trap '' INT TERM; [ -n "$scratch_dir" ] && rm -rf "$scratch_dir"; scratch_dir=""; return 0; }
trap scratch_cleanup EXIT
# THE SIGNAL HANDLERS CLEAN UP THEMSELVES rather than relying on `exit` to reach the EXIT trap.
# `exit` from inside a trap handler is documented to run the EXIT trap and MEASURABLY does not
# always: instrumented, the TERM handler ran in the main shell and the EXIT trap did not follow, on
# roughly one run in five of the self-test's interrupt arm. Calling it directly is one word and
# removes the question. Nothing about the CLONE depends on any of this - it was never the fetch
# target - so this is tidiness, not correctness.
trap 'scratch_cleanup; exit 130' INT
trap 'scratch_cleanup; exit 143' TERM

# Fetch one ref for inspection. The result is FETCH_HEAD *in the scratch repo*, read back by
# preview_show/preview_has - never by a bare `git show FETCH_HEAD`, which would read this clone's.
#
# `GIT_DIR=` AS A COMMAND PREFIX, NOT `--git-dir`, THOUGH THEY MEAN THE SAME THING. The global-option
# spelling pushes the subcommand out of `$1`, and CheckQuarantineOwnersScriptTest stubs `git` on PATH
# with a `case "$1" in fetch|show|cat-file)` dispatcher - so `git --git-dir=X show` matched nothing,
# the stub printed nothing, and three of its assertions failed with the script behaving perfectly.
# The stub is naive, but the property is worth keeping for free: the subcommand stays where every
# wrapper looks for it. Same walk-past that let `git -C DIR rev-list` through
# .claude/hooks/check-shallow-history.sh, and `git -c k=v fetch` through the hazard row.
preview_git() { GIT_DIR="$scratch_dir/preview" git "$@"; }
preview_fetch() { # $1 = ref to fetch from origin
    [ -d "$scratch_dir/preview" ] || git init -q --bare "$scratch_dir/preview" || return 1
    # The scratch repo is thrown away with its `shallow` file, which is the entire point of the
    # indirection. The marker must be the line IMMEDIATELY above; the gate reads one line back.
    # hazard-ok: fetches into the scratch repo above, never into this clone.
    preview_git fetch --quiet --depth=1 --no-tags "$ORIGIN_URL" "$1" 2>/dev/null
}
preview_show() { preview_git show "FETCH_HEAD:$1" 2>/dev/null; }
preview_has()  { preview_git cat-file -e "FETCH_HEAD:$1" 2>/dev/null; }

# gh with retry + error classification: echoes the value, or MISSING (confirmed not-found), or
# TRANSIENT (still failing after retries - infra weather, never an ERROR).
gh_query() { # $1=pr  $2=jq field (e.g. .state)
    # A FIXED PATH IN THE RUN SCRATCH DIR, not a per-call `mktemp`. This function runs inside
    # `$(...)`, so it cannot register anything for cleanup; every redirect below truncates the file,
    # and the trap above removes the directory, interrupted run included.
    local err out attempt
    err="$scratch_dir/gh-stderr"
    for attempt in 1 2 3; do
        if out=$(gh pr view "$1" -R "$REPO" --json "${2#.}" -q "$2" 2>"$err"); then
            echo "$out"; return 0
        fi
        if grep -qiE 'could not resolve|not found|no pull requests' "$err"; then
            echo MISSING; return 0
        fi
        sleep "$attempt"
    done
    echo TRANSIENT
}

entries=$(registry_entries)
[ -z "$entries" ] && { echo "Registry has no entries - nothing to verify."; exit 0; }

scratch_dir=$(mktemp -d) || { echo "ERROR: cannot create a scratch directory." >&2; exit 1; }

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
        # An unowned entry is legal in two distinct states, and the advisory must not report the
        # wrong one: a diagnosed entry needs an owner found for its fix, while a recorded rule-1
        # exception is UNDIAGNOSED and needs the diagnosis itself completed - telling a maintainer
        # to "find an owner" for a failure nobody understands points them at the wrong task.
        if grep -qi 'rule-1 exception' <<<"$block"; then
            echo "ADVISORY: $t has no owning PR - UNDIAGNOSED (recorded rule-1 exception); completing the diagnosis is the open task."
        else
            echo "ADVISORY: $t has no owning PR - diagnosed-but-unowned, find it an owner."
        fi
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
            if ! preview_fetch "$base"; then
                echo "ADVISORY: $t owner PR #$pr is open; could not fetch its base '$base' to verify - skipping preview check."
                continue
            fi
            # Herestring: `git show | grep -q` under pipefail turns a MATCH into a failure once
            # the file exceeds the 64 KiB pipe buffer. The largest source file here is already
            # within a few hundred bytes of that.
            if ! grep -qE "$QUARANTINE_ANNOTATION_ERE" <<<"$(preview_show "$relpath")"; then
                echo "ADVISORY: $t owner PR #$pr is open, but the quarantine is not yet on its base '$base' - preview check n/a, re-check after the base updates."
                continue
            fi
            if ! preview_fetch "pull/$pr/merge"; then
                echo "ADVISORY: $t owner PR #$pr is open but has no merge preview (conflicts?) - cannot verify it removes the quarantine."
                continue
            fi
            if ! preview_has "$relpath"; then
                echo "ADVISORY: $t owner PR #$pr merge preview does not contain $relpath (file renamed/moved?) - cannot verify removal; check manually."
                continue
            fi
            if grep -qE "$QUARANTINE_ANNOTATION_ERE" <<<"$(preview_show "$relpath")"; then
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
