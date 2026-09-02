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
#   3. WRITES the per-test table into target/, with a one-line machine-readable payload. It does NOT
#      post it: .github/workflows/quarantine-lane.yml hands that file to
#      .github/scripts/quarantine-report-comment.js, which reads the previous comment's payload back
#      to render a DELTA, updates in place for ordinary pushes, and posts a FRESH comment when an
#      outcome CHANGES. The mechanics under that are .github/scripts/sticky-report-comment.js,
#      shared with the throughput report.
#
#      WHY THE POSTING LEFT THIS SCRIPT. It used to upsert the comment itself with `gh api`, and that
#      copy had two of the three defects astubbs/parallel-consumer#407 fixed for the throughput
#      comment: no author filter on the lookup, and - worse - a `gh api` failure under `set -e` took
#      the whole step down, so a rate limit while posting could red a job whose lane had run fine.
#      The third was the one the operator noticed from outside: a test going from failing to PASSING
#      - which means its fix landed and the annotation plus registry entry must be deleted - was
#      announced by silently editing a comment nobody was looking at.
#
#      Writing a file also makes the report TESTABLE without a network, from both sides:
#      QuarantineLaneReportScriptTest runs this script against fixture reports and asserts what the
#      file contains, and .github/scripts/quarantine-report-comment.test.js runs it for real and
#      feeds its actual output through the reader - which is the only test that fails if the two
#      ever disagree about the payload's shape.
#
# Env: PR_NUMBER (empty = log-only, e.g. push-to-master runs, but the report file is still written),
#      HEAD_SHA (for review comments), GH_TOKEN in CI. DRY_RUN=1 prints intended gh actions instead
#      of calling gh (test seam). QUARANTINE_REPORT_FILE overrides the output path (test seam).
# Exit: 0 unless the script breaks OR the lane-leak self-check fires - test OUTCOMES never red the
#       job (the thread is the gate); a leaked non-quarantined test always does.

set -euo pipefail

cd "${QUARANTINE_CHECK_ROOT:-$(dirname "$0")/..}"
# shellcheck source=bin/lib/quarantine-common.sh
source "${BASH_SOURCE[0]%/*}/lib/quarantine-common.sh" 2>/dev/null || source bin/lib/quarantine-common.sh

# The name of the payload this report embeds in its own body. WRITTEN HERE AND READ IN
# .github/scripts/quarantine-report-comment.js, which parses it off the previous comment. A rename
# on either side is caught by that module's end-to-end self-test rather than in production, but
# `grep -rn quarantine-lane-data` is still the list to change.
DATA_MARKER="quarantine-lane-data"
REPORT_FILE="${QUARANTINE_REPORT_FILE:-target/quarantine-lane-report.md}"
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
    # Same class of defect as bin/check-quarantine-registry.sh carried: the inline
    # `quarantined_files | while ... break` this replaces exits 1 when NO file matches, and under
    # this script's `set -e` that killed the report at the assignment. quarantine-common.sh's
    # `quarantined_file_for_class` header owns the mechanism.
    f=$(quarantined_file_for_class "$1")
    [ -n "$f" ] && grep -q 'flapping = true' "$f" && echo 1 || echo 0
}

annotation_location() { # <Class> -> path:line of the @Quarantined( line
    local f
    f=$(quarantined_file_for_class "$1")
    # Reachable only now: with the inline lookup, `set -e` killed the script one line above and this
    # documented fallback never ran.
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

# JSON-safe: a registry entry is a Java identifier path, so anything outside that set is either a
# malformed entry or a quoting hazard in the payload. Strip rather than escape - the payload is a
# machine key, and a name that needs escaping is one this report should not be keying on.
json_key() { printf '%s' "$1" | tr -cd 'A-Za-z0-9._$-'; }

table=""
action_needed=""
# The payload the next run reads back off this comment to render a delta, and the digest that
# decides whether an outcome CHANGED. Sorted, so the digest is stable across runs when nothing
# moved - an unsorted digest would read every run as a status change and post a fresh comment on
# every push, which is the fifteen-comments problem the stickiness exists to prevent.
outcome_pairs=""
for t in $(printf '%s\n' $entries | sort); do
    cls=${t%%.*}; method=${t#*.}
    [ "$method" = "$t" ] && method=""
    block=$(registry_entry_block "$t")
    # Same three accepted forms as bin/check-quarantine-owners.sh - keep the two in step.
    owner=$(echo "$block" | grep -oE 'Owner: PR (astubbs/parallel-consumer|astubbs)?#[0-9]+' | grep -oE '#[0-9]+' | head -1 || true)
    outcome=$(outcome_of "$cls" "$method")
    flapping=$(is_flapping "$cls")
    # THE PAYLOAD RECORDS THE REPORTED OUTCOME, NOT THE RAW ONE. A flapping test that passes is
    # reported as `PASSED_FLAPPER` and a deterministic one as `PASSED_ACTION`, because those two
    # rows say opposite things to a reader and a transition between them is a real change.
    # Collapsing both to PASSED would make an annotation gaining `flapping = true` invisible to
    # the delta, and that flag is exactly what decides whether a pass demands action.
    reported="$outcome"
    case "$outcome" in
        FAILED)
            table+="| \`$t\` | 🔴 failing (expected) | ${owner:-⚠️ unowned} | quarantine holding |"$'\n' ;;
        PASSED)
            if [ "$flapping" = "1" ]; then
                reported="PASSED_FLAPPER"
                table+="| \`$t\` | 🟡🎲 passed (flapper) | ${owner:-⚠️ unowned} | proves nothing - passes most runs by nature |"$'\n'
            else
                reported="PASSED_ACTION"
                table+="| \`$t\` | 🚨✅ **PASSED - ACTION REQUIRED** | ${owner:-⚠️ unowned} | fix landed → delete annotation + registry entry |"$'\n'
                action_needed+="$t"$'\n'
            fi ;;
        NOT_RUN)
            table+="| \`$t\` | ⚪ not run | ${owner:-⚠️ unowned} | report missing - check the lane job |"$'\n' ;;
    esac
    outcome_pairs+="$(json_key "$t")=$reported"$'\n'
    echo "lane-report: $t -> $outcome (reported=$reported, flapping=$flapping, owner=${owner:-none})"
done

# --- the report file, which is what the workflow posts -------------------------------------------
#
# BUILT AND WRITTEN BEFORE THE PR CHECK BELOW, deliberately: a push-to-master run has no PR to
# comment on but its report is still the canonical master-state record, and it lands in the job log
# and the workspace either way. The old order returned early and built nothing, so a log-only run
# printed a bare list of outcomes and no table at all.
status_digest=$(printf '%s' "$outcome_pairs" | paste -sd ';' -)

# The payload is assembled with printf rather than jq, so this script keeps working on a runner that
# has no jq - and every key has already been through json_key while every value is one of five enum
# words, so there is nothing here that needs escaping.
#
# NO `grep -v` IN EITHER PIPELINE. Under `set -euo pipefail` a grep that matches nothing exits 1,
# pipefail promotes it, the assignment takes it and the script dies at the assignment - the exact
# defect quarantine-common.sh's `quarantined_occurrences` header documents, one file away. There are
# never blank lines here anyway, so the filter was buying nothing and risking everything.
outcomes_json=$(printf '%s' "$outcome_pairs" | while IFS='=' read -r k v; do
    [ -n "$k" ] || continue
    printf '"%s":"%s",' "$k" "$v"
done)
outcomes_json="{${outcomes_json%,}}"

body="## 🧪🔒 Quarantine Lane Report

| Quarantined test | Outcome | Owner | Meaning |
|---|---|---|---|
$table
<sub>🔴 expected while the owner PR is open · 🟡🎲 flapper, pass proves nothing · 🚨 a deterministic quarantined test passing means its fix landed: delete its \`@Quarantined\` annotation + \`docs/quarantined-tests.md\` entry (a merge-blocking review thread has been opened). Lane: non-gating; rules: see the Quarantine Audit check.</sub>

<!-- $DATA_MARKER: {\"status\":\"$status_digest\",\"outcomes\":$outcomes_json} -->
"

mkdir -p "$(dirname "$REPORT_FILE")"
printf '%s' "$body" > "$REPORT_FILE"
echo "Lane report written to $REPORT_FILE (status digest: ${status_digest:-<empty>})."

[ -z "$PR_NUMBER" ] && { echo "No PR context (push/dispatch run) - report logged only."; exit 0; }

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
1. **Re-enable it**: delete the \`@Quarantined\` annotation at \`$loc\` **and** its entry in \`docs/quarantined-tests.md\` (same commit), or
2. Resolve this thread with a reason (e.g. one lucky pass on a test that should be marked \`flapping = true\`).

<sub>Posted by the Quarantine Lane · this thread blocks merge until resolved (repo requires conversation resolution)</sub>"
    gh api "repos/{owner}/{repo}/pulls/$PR_NUMBER/comments" \
        -f body="$thread_body" -f commit_id="$HEAD_SHA" -f path="$target" -f subject_type=file >/dev/null \
        && echo "Merge-blocking thread created for $t on $target." \
        || echo "WARN: could not create review thread for $t (anchor $target)."
done <<< "$action_needed"

exit 0
