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
#     7. review created in the SAME SECOND the head appeared                -> FAIL (1)
#     8. review lands between the commit's own date and the push GitHub saw -> FAIL (1)
#     9. ... and with no check-suite time known, the same stream passes     -> pass (0)
#    9b. a FUTURE-dated committer clock does not hold a real review stale   -> pass (0)
#    9c. ... but with no check-suite time, a pre-commit review is STILL stale -> FAIL (1)
#
#   COMPLETION
#    10. fresh review, but with an unticked task-list box                   -> FAIL (1)
#    11. fresh unticked tracker plus a fresh finished review                -> pass (0)
#    12. an unticked box in somebody ELSE's comment                         -> pass (0)
#    13. a stale finished review plus a fresh unticked tracker              -> FAIL (1)
#   13b. an unticked box inside a fenced code block                         -> pass (0)
#   13c. ... inside a 3-backtick example nested in a 4-backtick fence       -> pass (0)
#   13d. ... behind an EQUAL-length fence carrying an info string           -> pass (0)
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
# Case 7 is the one-second tie: whole-second timestamps make equality ambiguous, so the gate
# resolves it as stale rather than risk a false green. Cases 8, 9 and 9b pin the timestamp
# choice - the server-side check-suite time is PREFERRED and the committer date is only a
# fallback - and 9b is why it is a preference rather than a max(): under max(), a commit
# dated in the future would hold the check red forever with no review able to clear it. 9c is
# 9's negative control: without it, a fallback that stopped comparing timestamps at all would
# still satisfy 9.
# Case 13 catches a naive whole-stream scan - the finished boxes belong to a review of older
# code, and the fresh comment is a tracker. Case 13b is the self-referential
# hazard: a review DISCUSSING this rule shows an unticked box, and a posted comment never
# changes, so counting it would stick the check red with no way at all to clear it. 13c and 13d
# pin the two remaining CommonMark closing-fence requirements - long enough, and no info string -
# each of which arrived as its own review round; together with "same character" they are the
# complete rule, so there is no third case of this shape left to find.
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

assert "review created in the same second the head appeared is ambiguous, so stale" \
    1 "$(run_checker "$(comment "$FIRST_SEEN_AT" "$REVIEWER" "$FINISHED_BODY")")"

assert "review between the commit's own date and the push GitHub saw" \
    1 "$(run_checker "$(comment "$BETWEEN" "$REVIEWER" "$FINISHED_BODY")")"

assert "the same review passes when no check-suite time is known" \
    0 "$(run_checker "$(comment "$BETWEEN" "$REVIEWER" "$FINISHED_BODY")" "$COMMITTED_AT" "")"

# NEGATIVE CONTROL for the case above, which on its own only proves the empty-first-seen path
# accepts something. It would still pass if that path stopped comparing timestamps altogether and
# waved every review through - this repo's named failure class, a check reporting success without
# having run. So: same empty first-seen, a review OLDER than the committer date, must be stale.
# The fallback has to be a different clock, not the absence of one.
assert "with no check-suite time, a review older than the commit is still stale" \
    1 "$(run_checker "$(comment "$BEFORE_COMMIT" "$REVIEWER" "$FINISHED_BODY")" "$COMMITTED_AT" "")"

# The committer date is written by the contributor's own clock, so a skewed or deliberately
# future-dated commit would, under a max() of the two timestamps, outrank every real review and
# hold the required check red until that date arrived - unfixable by reviewing. Preferring the
# server-side check-suite time is what stops that.
assert "a future-dated committer clock does not hold a real review stale" \
    0 "$(run_checker "$(comment "$AFTER_BOTH" "$REVIEWER" "$FINISHED_BODY")" "2099-01-01T00:00:00Z" "$FIRST_SEEN_AT")"

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

# A finished review that DISPLAYS an unticked box - as a fenced example, or quoting feedback
# back - is not a reviewer that stopped partway. This is the self-referential case: a review of
# the PR that introduced the unticked-box rule quotes one while discussing it, and a posted
# comment never changes, so counting it would stick the check red permanently.
FENCED_BODY='**Claude finished @astubbs'"'"'s task in 1m 52s** —— [View job](https://github.com/astubbs/parallel-consumer/actions/runs/31453513070)

- [x] Reviewed the completion rule

The gate treats a line like this as an unfinished tracker:

```markdown
- [ ] Run the review
```

That is the behaviour under discussion.'

assert "an unticked box inside a fenced code block is displayed, not a tracker" \
    0 "$(run_checker "$(comment "$AFTER_BOTH" "$REVIEWER" "$FENCED_BODY")")"

# A closing fence must match its opener's character and length. A review showing a Markdown
# example wraps it in a FOUR-backtick fence precisely so the three-backtick block inside
# survives; a parser that toggles on any fence reads that inner opener as the close, puts the
# example back in scope, and counts the box it was only displaying. That is a permanent red on
# the very comment shape most likely to discuss this rule.
NESTED_FENCE_BODY='**Claude finished @astubbs'"'"'s task in 1m 52s** —— [View job](https://github.com/astubbs/parallel-consumer/actions/runs/31453513070)

- [x] Reviewed the completion rule

Example of a body the gate must NOT read as unfinished:

````markdown
```markdown
- [ ] Run the review
```
````

Done.'

assert "an unticked box in a 3-backtick example nested inside a 4-backtick fence" \
    0 "$(run_checker "$(comment "$AFTER_BOTH" "$REVIEWER" "$NESTED_FENCE_BODY")")"

# The third CommonMark closing-fence requirement: a fence carrying an INFO STRING is content, not
# a close. Same character and same length as the opener, so requirements 1 and 2 both pass and
# only this one stands between the displayed box and a permanent red. Equal length is the point -
# the four-backtick wrapper above cannot express it.
INFO_STRING_FENCE_BODY='**Claude finished @astubbs'"'"'s task in 1m 52s** —— [View job](https://github.com/astubbs/parallel-consumer/actions/runs/31453513070)

- [x] Reviewed the completion rule

Showing how a tracker renders, inside one 3-backtick block:

```markdown
```text
- [ ] Run the review
```
```

Done.'

assert "an equal-length fence with an info string does not close the block" \
    0 "$(run_checker "$(comment "$AFTER_BOTH" "$REVIEWER" "$INFO_STRING_FENCE_BODY")")"

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
