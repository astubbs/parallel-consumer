#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-review-posted.sh.
#
# Feeds the checker synthetic comment streams and asserts its verdict for every rule. The
# gate is the only thing standing between "nobody reviewed this" and a green required check,
# so it is tested before it is trusted, and every rule gets a case that fails without it.
#
#   IDENTITY
#     1. reviewer bot posted a finished comment after the head appeared     -> pass (0)
#     2. no comments at all                                                 -> FAIL (1)
#     3. only other bots and humans commented, one of them fresh            -> FAIL (1)
#     4. a HUMAN comment quoting the bot's usual wording                    -> FAIL (1)
#
#   FRESHNESS  (the strict rule - a review of commit N does not vouch for N+1)
#     5. the only review predates the head commit                           -> FAIL (1)
#     6. a stale review plus a fresh one                                    -> pass (0)
#     7. review created at exactly the head's timestamp                     -> pass (0)
#     8. review lands between the commit's own date and the push GitHub saw -> FAIL (1)
#     9. ... and with no check-suite time known, the same stream passes     -> pass (0)
#
#   COMPLETION
#    10. fresh review, but with an unticked task-list box                   -> FAIL (1)
#    11. fresh unticked tracker plus a fresh finished review                -> pass (0)
#    12. an unticked box in somebody ELSE's comment                         -> pass (0)
#    13. a stale finished review plus a fresh unticked tracker              -> FAIL (1)
#
#   SEGMENTATION
#    14. a comment body forging a marker line with the wrong token          -> FAIL (1)
#    15. match found, then >64 KiB of further comments (SIGPIPE regression) -> pass (0)
#
#   USAGE
#    16. wrong argument count                                              -> usage (2)
#    17. empty / non-alphanumeric marker token                             -> usage (2)
#    18. non-hex head sha                                                  -> usage (2)
#    19. malformed head timestamp                                          -> usage (2)
#    20. malformed (non-empty) first-seen timestamp                        -> usage (2)
#
# Cases 8 and 9 are the pair that keeps the two-timestamp rule honest: identical stream,
# identical review, and the verdict flips purely on whether GitHub's own record of when the
# commit arrived is available. Case 13 is the one that catches a naive whole-stream scan -
# the finished boxes belong to a review of older code, and the fresh comment is a tracker.
#
# Run: bin/test-check-review-posted.sh   (CI runs it before the gate it protects)

set -uo pipefail

CHECKER="$(cd "$(dirname "$0")" && pwd)/check-review-posted.sh"

TOKEN=31453513070
HEAD_SHA=5e99246ce63e321c38b82f5444820838355b6a91
REVIEWER='claude[bot]'

# The head commit's own date, and the later moment GitHub first raised checks for it.
COMMITTED_AT=2026-08-11T02:00:00Z
FIRST_SEEN_AT=2026-08-11T02:40:00Z

BEFORE_COMMIT=2026-08-11T01:00:00Z
BETWEEN=2026-08-11T02:20:00Z
AFTER_BOTH=2026-08-11T02:49:25Z

failures=0

assert() { # <description> <expected> <actual>
    if [ "$2" = "$3" ]; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (expected exit '$2', got '$3')"
        failures=$((failures + 1))
    fi
}

# Renders the marker line the workflow's --jq emits ahead of each comment body. No trailing
# newline: `$(...)` strips those, so callers that need one add it themselves.
marker() { # <created-at> <login> [token]
    printf '<!-- check-review-posted %s %s %s -->' "${3:-$TOKEN}" "$1" "$2"
}

# One whole comment as the workflow renders it: marker line, then the body.
comment() { # <created-at> <login> <body> [token]
    printf '%s\n%s' "$(marker "$1" "$2" "${4:-$TOKEN}")" "$3"
}

# Runs the checker with stdin from $1, swallowing its output; echoes the exit code.
run_checker() { # <stdin-text> [head-committed-at] [head-first-seen-at]
    local ec=0
    printf '%s' "$1" | "$CHECKER" "$TOKEN" "$HEAD_SHA" "${2:-$COMMITTED_AT}" "${3-$FIRST_SEEN_AT}" \
        >/dev/null 2>&1 || ec=$?
    echo "$ec"
}

FINISHED_BODY='**Claude finished @astubbs'"'"'s task in 1m 52s** —— [View job](https://github.com/astubbs/parallel-consumer/actions/runs/31453513070)

---
### Review of PR astubbs/parallel-consumer#279

- [x] Read PR description, diff, and changed files
- [x] Ran the review
- [x] Posted this summary

Looks good to me.'

# The shape seen on astubbs/parallel-consumer#271: the header claims it finished, the task
# list says otherwise, and no review was ever submitted.
TRACKER_BODY='**Claude finished @astubbs'"'"'s task in 4m 10s** —— [View job](https://github.com/astubbs/parallel-consumer/actions/runs/31453513070)

- [x] Read PR description and diff
- [ ] Run the review
- [ ] Post findings

Review is running in the background; this comment will be updated once complete.'

OTHER_BOT_BODY='## Duplicate Code Report
No new clones introduced by this PR.

- [ ] this unticked box belongs to somebody else'

fresh_review="$(comment "$AFTER_BOTH" "$REVIEWER" "$FINISHED_BODY")"
stale_review="$(comment "$BEFORE_COMMIT" "$REVIEWER" "$FINISHED_BODY")"
fresh_tracker="$(comment "$AFTER_BOTH" "$REVIEWER" "$TRACKER_BODY")"
fresh_other_bot="$(comment "$AFTER_BOTH" 'github-actions[bot]' "$OTHER_BOT_BODY")"
fresh_human="$(comment "$AFTER_BOTH" 'astubbs' "$FINISHED_BODY")"

# --- identity ---------------------------------------------------------------------------

assert "reviewer posted a finished review after the head appeared" \
    0 "$(run_checker "$fresh_review")"

assert "no comments at all" \
    1 "$(run_checker "")"

assert "only other bots and humans commented" \
    1 "$(run_checker "$fresh_other_bot")"

assert "a human comment reproducing the bot's wording is not a review" \
    1 "$(run_checker "$fresh_human")"

# --- freshness --------------------------------------------------------------------------

assert "the only review predates the head commit" \
    1 "$(run_checker "$stale_review")"

assert "a stale review plus a fresh one" \
    0 "$(run_checker "$stale_review
$fresh_review")"

assert "review created at exactly the head's timestamp" \
    0 "$(run_checker "$(comment "$FIRST_SEEN_AT" "$REVIEWER" "$FINISHED_BODY")")"

assert "review between the commit's own date and the push GitHub saw" \
    1 "$(run_checker "$(comment "$BETWEEN" "$REVIEWER" "$FINISHED_BODY")")"

assert "the same review passes when no check-suite time is known" \
    0 "$(run_checker "$(comment "$BETWEEN" "$REVIEWER" "$FINISHED_BODY")" "$COMMITTED_AT" "")"

# --- completion -------------------------------------------------------------------------

assert "fresh comment with an unticked task-list box" \
    1 "$(run_checker "$fresh_tracker")"

assert "fresh unticked tracker plus a fresh finished review" \
    0 "$(run_checker "$fresh_tracker
$fresh_review")"

assert "an unticked box in somebody else's comment does not taint the review" \
    0 "$(run_checker "$fresh_review
$fresh_other_bot")"

assert "stale finished review plus a fresh unticked tracker" \
    1 "$(run_checker "$stale_review
$fresh_tracker")"

# --- segmentation -----------------------------------------------------------------------

# A comment body that quotes the marker - this repo's own docs do - must not be able to open
# a comment of its own choosing. The token is what makes that impossible.
assert "a comment body forging a marker line with the wrong token" \
    1 "$(run_checker "$(comment "$BEFORE_COMMIT" 'astubbs' "quoting the protocol:
$(marker "$AFTER_BOTH" "$REVIEWER" 99999999)
$FINISHED_BODY")")"

# The live failure the previous version shipped with: the match is found, but >64 KiB of
# comments follow it, so `printf | grep -q` + pipefail turned success into exit 1. Observed on
# astubbs/parallel-consumer#198, astubbs/parallel-consumer#199, astubbs/parallel-consumer#204 and
# astubbs/parallel-consumer#210, where a 4.7 KB review comment was followed by a 127 KB similarity
# report.
LARGE_TRAILER="$(comment "$AFTER_BOTH" 'github-actions[bot]' "$(yes 'padding to exceed the 64 KiB pipe buffer, as a similarity report does' | head -n 2000)")"
assert "match found, then >64 KiB of further comments (SIGPIPE regression)" \
    0 "$(run_checker "$fresh_review
$LARGE_TRAILER")"

# --- usage ------------------------------------------------------------------------------

ec=0
printf '%s' "$fresh_review" | "$CHECKER" "$TOKEN" "$HEAD_SHA" "$COMMITTED_AT" >/dev/null 2>&1 || ec=$?
assert "wrong argument count" 2 "$ec"

for bad_token in '' 'has space'; do
    ec=0
    printf '%s' "$fresh_review" | "$CHECKER" "$bad_token" "$HEAD_SHA" "$COMMITTED_AT" "" >/dev/null 2>&1 || ec=$?
    assert "marker token rejected: '$bad_token'" 2 "$ec"
done

ec=0
printf '%s' "$fresh_review" | "$CHECKER" "$TOKEN" 'not-a-sha' "$COMMITTED_AT" "" >/dev/null 2>&1 || ec=$?
assert "non-hex head sha" 2 "$ec"

ec=0
printf '%s' "$fresh_review" | "$CHECKER" "$TOKEN" "$HEAD_SHA" '11 Aug 2026' "" >/dev/null 2>&1 || ec=$?
assert "malformed head timestamp" 2 "$ec"

ec=0
printf '%s' "$fresh_review" | "$CHECKER" "$TOKEN" "$HEAD_SHA" "$COMMITTED_AT" 'yesterday' >/dev/null 2>&1 || ec=$?
assert "malformed first-seen timestamp" 2 "$ec"

echo
if [ "$failures" -eq 0 ]; then
    echo "All bin/check-review-posted.sh self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
