#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Quarantine lane PR reporter - runs after the lane executes the @Quarantined tests.
#
#   1. Classifies each registry entry's outcome from the surefire/failsafe reports:
#        FAILED               -> expected while the owner PR is open (report only)
#        PASSED, flapping     -> proves nothing (report only)
#        PASSED, deterministic-> ACTION: the fix landed; the annotation + registry entry must be
#                                deleted. Posts a MERGE-BLOCKING review thread on the PR (repo
#                                requires conversation resolution), anchored at the annotation's file
#                                when it is part of the PR diff, else on the first changed file with a
#                                pointer to the annotation location.
#        NOT_RUN              -> infra/report anomaly, flagged.
#   2. LANE-LEAK SELF-CHECK: every testcase found in the reports must correspond to a registry
#      entry - if ANY non-quarantined test executed in the lane, the group filtering has regressed
#      (e.g. a plugin config losing the <groups> binding, the real ce-review P1) and the script
#      exits 1, turning the lane job RED. The lane proves on every run that it runs ONLY the
#      quarantined tests.
#   3. Upserts ONE sticky PR comment (marker-identified) with the per-test table - never spams.
#
# Env: PR_NUMBER (empty = log-only, e.g. push-to-master runs), HEAD_SHA (for review comments),
#      GH_TOKEN in CI. DRY_RUN=1 prints intended gh actions instead of calling gh (test seam).
# Exit: 0 unless the script breaks OR the lane-leak self-check fires - test OUTCOMES never red the
#       job (the thread is the gate); a leaked non-quarantined test always does.

set -euo pipefail

cd "${QUARANTINE_CHECK_ROOT:-$(dirname "$0")/..}"
# shellcheck source=bin/lib/quarantine-common.sh
source "${BASH_SOURCE[0]%/*}/lib/quarantine-common.sh" 2>/dev/null || source bin/lib/quarantine-common.sh

STICKY_MARKER="<!-- quarantine-lane-report -->"
DRY_RUN="${DRY_RUN:-0}"
PR_NUMBER="${PR_NUMBER:-}"

gh_do() {
    if [ "$DRY_RUN" = "1" ]; then
        echo "DRY-RUN gh $*"
    else
        gh "$@"
    fi
}

# outcome <Class> <method>: FAILED / PASSED / NOT_RUN, from surefire+failsafe XML reports.
# Parameterized tests emit multiple <testcase> entries (name="method(...)[N]") - ANY failure => FAILED.
outcome_of() {
    local cls=$1 method=$2 f found=0 failed=0
    while IFS= read -r f; do
        [ -n "$f" ] || continue
        # awk: within <testcase> blocks whose classname ends with .cls (or equals) and name starts
        # with the method, detect a <failure/<error child before </testcase>
        local res
        res=$(awk -v cls="$cls" -v m="$method" '
            /<testcase /{
                match($0, /classname="[^"]*"/); cn=substr($0, RSTART+11, RLENGTH-12)
                match($0, /name="[^"]*"/);      nm=substr($0, RSTART+6,  RLENGTH-7)
                inCase = (cn ~ ("(^|\\.)" cls "$")) && (index(nm, m) == 1)
                if (inCase) {
                    seen=1
                    # same-line children (single-line testcase XML)
                    if ($0 ~ /<failure/ || $0 ~ /<error/) bad=1
                    if ($0 ~ /\/>/ || $0 ~ /<\/testcase>/) inCase=0
                }
                next
            }
            inCase && (/<failure/ || /<error/) { bad=1 }
            /<\/testcase>/ { inCase=0 }
            END { if (!seen) print "ABSENT"; else if (bad) print "FAILED"; else print "PASSED" }
        ' "$f")
        if [ "$res" != "ABSENT" ]; then
            found=1
            [ "$res" = "FAILED" ] && failed=1
        fi
    done < <(find . -path '*/surefire-reports/*.xml' -o -path '*/failsafe-reports/*.xml' 2>/dev/null)
    if [ "$found" = "0" ]; then echo "NOT_RUN"; elif [ "$failed" = "1" ]; then echo "FAILED"; else echo "PASSED"; fi
}

# is_flapping <Class>: 1 if the class's annotation carries flapping = true (single-annotation files only)
is_flapping() {
    local f
    f=$(quarantined_files | while read -r qf; do
            [ "$(basename "$qf" .java)" = "$1" ] && { echo "$qf"; break; }
        done)
    [ -n "$f" ] && grep -q 'flapping = true' "$f" && echo 1 || echo 0
}

annotation_location() { # <Class> -> path:line of the @Quarantined( line
    local f
    f=$(quarantined_files | while read -r qf; do
            [ "$(basename "$qf" .java)" = "$1" ] && { echo "$qf"; break; }
        done)
    [ -n "$f" ] || { echo "unknown"; return; }
    local line
    line=$(grep -nE "$QUARANTINE_ANNOTATION_ERE" "$f" | head -1 | cut -d: -f1)
    echo "${f#./}:${line:-1}"
}

entries=$(registry_entries)
[ -z "$entries" ] && { echo "Quarantine lane empty - nothing to report."; exit 0; }

# --- lane-leak self-check: every executed testcase must match a registry entry ---
leak=0
while IFS=$'\t' read -r tc_class tc_name; do
    [ -n "$tc_class" ] || continue
    simple=${tc_class##*.}
    matched=0
    for e in $entries; do
        ecls=${e%%.*}; emethod=${e#*.}
        [ "$emethod" = "$e" ] && emethod=""
        if [ "$simple" = "$ecls" ] && { [ -z "$emethod" ] || [ "${tc_name#"$emethod"}" != "$tc_name" ]; }; then
            matched=1; break
        fi
    done
    if [ "$matched" = "0" ]; then
        echo "LANE_LEAK: $tc_class.$tc_name executed in the quarantine lane but is NOT a quarantined test - group filtering has regressed (check the surefire/failsafe <groups> bindings)."
        leak=1
    fi
done < <(find . -path '*/surefire-reports/*.xml' -o -path '*/failsafe-reports/*.xml' 2>/dev/null | while IFS= read -r f; do
        awk '/<testcase /{
            match($0, /classname="[^"]*"/); cn=substr($0, RSTART+11, RLENGTH-12)
            match($0, /name="[^"]*"/);      nm=substr($0, RSTART+6,  RLENGTH-7)
            print cn "\t" nm
        }' "$f"
    done | sort -u)
if [ "$leak" = "1" ]; then
    echo "FATAL: non-quarantined tests ran in the lane - failing the job (this is the self-check that the lane runs ONLY quarantined tests)."
    exit 1
fi
echo "Lane-leak self-check passed: every executed testcase matches a registry entry."

table=""
action_needed=""
for t in $entries; do
    cls=${t%%.*}; method=${t#*.}
    [ "$method" = "$t" ] && method=""
    block=$(registry_entry_block "$t")
    owner=$(echo "$block" | grep -oE 'Owner: PR #[0-9]+' | grep -oE '#[0-9]+' | head -1 || true)
    outcome=$(outcome_of "$cls" "$method")
    flapping=$(is_flapping "$cls")
    case "$outcome" in
        FAILED)
            table+="| \`$t\` | 🔴 failing (expected) | ${owner:-⚠️ unowned} | quarantine holding |"$'\n' ;;
        PASSED)
            if [ "$flapping" = "1" ]; then
                table+="| \`$t\` | 🟡🎲 passed (flapper) | ${owner:-⚠️ unowned} | proves nothing - passes most runs by nature |"$'\n'
            else
                table+="| \`$t\` | 🚨✅ **PASSED - ACTION REQUIRED** | ${owner:-⚠️ unowned} | fix landed → delete annotation + registry entry |"$'\n'
                action_needed+="$t"$'\n'
            fi ;;
        NOT_RUN)
            table+="| \`$t\` | ⚪ not run | ${owner:-⚠️ unowned} | report missing - check the lane job |"$'\n' ;;
    esac
    echo "lane-report: $t -> $outcome (flapping=$flapping, owner=${owner:-none})"
done

[ -z "$PR_NUMBER" ] && { echo "No PR context (push/dispatch run) - report logged only."; exit 0; }

body="$STICKY_MARKER
## 🧪🔒 Quarantine Lane Report

| Quarantined test | Outcome | Owner | Meaning |
|---|---|---|---|
$table
<sub>🔴 expected while the owner PR is open · 🟡🎲 flapper, pass proves nothing · 🚨 a deterministic quarantined test passing means its fix landed: delete its \`@Quarantined\` annotation + \`docs/QUARANTINED_TESTS.md\` entry (a merge-blocking review thread has been opened). Lane: non-gating; rules: see the Quarantine Audit check.</sub>"

# --- upsert the sticky comment ---
if [ "$DRY_RUN" = "1" ]; then
    echo "DRY-RUN sticky comment body:"; echo "$body"
else
    existing=$(gh api "repos/{owner}/{repo}/issues/$PR_NUMBER/comments" --paginate \
        -q ".[] | select(.body | startswith(\"$STICKY_MARKER\")) | .id" | head -1 || true)
    if [ -n "$existing" ]; then
        gh api -X PATCH "repos/{owner}/{repo}/issues/comments/$existing" -f body="$body" >/dev/null
        echo "Sticky comment updated ($existing)."
    else
        gh api "repos/{owner}/{repo}/issues/$PR_NUMBER/comments" -f body="$body" >/dev/null
        echo "Sticky comment created."
    fi
fi

# --- merge-blocking review threads for unexpected passes ---
while IFS= read -r t; do
    [ -n "$t" ] || continue
    cls=${t%%.*}
    marker="<!-- quarantine-unexpected-pass:$t -->"
    loc=$(annotation_location "$cls")
    anchor_path=${loc%%:*}
    if [ "$DRY_RUN" = "1" ]; then
        echo "DRY-RUN would ensure review thread for $t anchored near $loc"
        continue
    fi
    already=$(gh api "repos/{owner}/{repo}/pulls/$PR_NUMBER/comments" --paginate \
        -q ".[] | select(.body | contains(\"$marker\")) | .id" | head -1 || true)
    [ -n "$already" ] && { echo "Thread for $t already exists ($already)."; continue; }
    # anchor at the annotation's file when the PR touches it, else the first changed file
    changed=$(gh api "repos/{owner}/{repo}/pulls/$PR_NUMBER/files" --paginate -q '.[].filename')
    target="$anchor_path"
    # Herestrings: a SIGPIPE here would take the `||` branch, silently retargeting.
    grep -qx "$anchor_path" <<<"$changed" || target=$(head -1 <<<"$changed")
    thread_body="$marker
🚨✅ **Quarantined test \`$t\` PASSED** - its fix appears to have landed.

Reality no longer matches the quarantine ledger. Before merging, either:
1. **Re-enable it**: delete the \`@Quarantined\` annotation at \`$loc\` **and** its entry in \`docs/QUARANTINED_TESTS.md\` (same commit), or
2. Resolve this thread with a reason (e.g. one lucky pass on a test that should be marked \`flapping = true\`).

<sub>Posted by the Quarantine Lane · this thread blocks merge until resolved (repo requires conversation resolution)</sub>"
    gh api "repos/{owner}/{repo}/pulls/$PR_NUMBER/comments" \
        -f body="$thread_body" -f commit_id="$HEAD_SHA" -f path="$target" -f subject_type=file >/dev/null \
        && echo "Merge-blocking thread created for $t on $target." \
        || echo "WARN: could not create review thread for $t (anchor $target)."
done <<< "$action_needed"

exit 0
