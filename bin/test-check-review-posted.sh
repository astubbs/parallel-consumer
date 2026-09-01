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
#     1. reviewer bot posted a finished comment                             -> pass (0)
#     2. no comments at all                                                 -> FAIL (1)
#     3. only other bots and humans commented                               -> FAIL (1)
#     4. a HUMAN comment quoting the bot's usual wording                    -> FAIL (1)
#     5. a bot whose login merely CONTAINS the reviewer's                   -> FAIL (1)
#
#   COMPLETION
#     6. the only review has an unticked task-list box                      -> FAIL (1)
#     7. an unticked tracker plus a finished review                         -> pass (0)
#     8. an unticked box in somebody ELSE's comment                         -> pass (0)
#     9. an unticked box inside a fenced code block                         -> pass (0)
#    9b. ... inside a 3-backtick example nested in a 4-backtick fence       -> pass (0)
#    9c. ... behind an EQUAL-length fence carrying an info string           -> pass (0)
#    9d. a TICKED box is not an unticked one                                -> pass (0)
#
#   AGE IS NOT A RULE  (the deliberate reversal - see check-review-posted.sh)
#    10. a review far older than anything else on the PR                    -> pass (0)
#    11. an ancient finished review plus a recent unticked tracker          -> pass (0)
#
#   SEGMENTATION
#    12. a comment body forging a marker line with the wrong token          -> FAIL (1)
#    13. a forged marker with the RIGHT shape but inside a review body      -> pass (0)
#    14. match found, then >64 KiB of further comments (SIGPIPE regression) -> pass (0)
#
#   USAGE
#    15. wrong argument count                                              -> usage (2)
#    16. empty / non-alphanumeric marker token                             -> usage (2)
#
# Cases 10 and 11 are the ones that pin the REVERSAL, and they are written as positive
# assertions rather than deletions so that silently re-adding a freshness comparison breaks
# the suite. 11 is the sharper of the two: under the old strict rule that stream FAILED,
# because the newest comment was an unfinished tracker. It passes now, because "a finished
# review exists on this PR" is the whole question.
#
# Case 7 catches a naive whole-stream scan - the tracker's unticked boxes must not be charged
# to the finished review sitting beside it. Case 9 is the self-referential hazard: a review
# DISCUSSING this rule shows an unticked box, and a posted comment never changes, so counting
# it would stick the check red with no way at all to clear it. 9b and 9c pin the two remaining
# CommonMark closing-fence requirements - long enough, and no info string - each of which
# arrived as its own review round; together with "same character" they are the complete rule.
#
# Run: bin/test-check-review-posted.sh   (CI runs it before the gate it protects)

set -uo pipefail

CHECKER="$(cd "$(dirname "$0")" && pwd)/check-review-posted.sh"

TOKEN=31453513070
REVIEWER='claude[bot]'

# Timestamps are carried through to the message and never compared, so these only need to be
# distinguishable from each other by eye.
LONG_AGO=2026-01-02T03:04:05Z
RECENTLY=2026-08-11T02:49:25Z

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
run_checker() { # <stdin-text>
    local ec=0
    printf '%s' "$1" | "$CHECKER" "$TOKEN" >/dev/null 2>&1 || ec=$?
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

finished_review="$(comment "$RECENTLY" "$REVIEWER" "$FINISHED_BODY")"
ancient_review="$(comment "$LONG_AGO" "$REVIEWER" "$FINISHED_BODY")"
tracker="$(comment "$RECENTLY" "$REVIEWER" "$TRACKER_BODY")"
other_bot="$(comment "$RECENTLY" 'github-actions[bot]' "$OTHER_BOT_BODY")"
human="$(comment "$RECENTLY" 'astubbs' "$FINISHED_BODY")"

# ---------------------------------------------------------------- IDENTITY

assert "1. a finished review from the reviewer bot passes" \
    0 "$(run_checker "$finished_review")"

assert "2. no comments at all fails" \
    1 "$(run_checker "")"

assert "3. only other bots and humans fails" \
    1 "$(run_checker "$other_bot
$human")"

assert "4. a human quoting the bot's wording fails" \
    1 "$(run_checker "$human")"

assert "5. a login merely containing the reviewer's fails" \
    1 "$(run_checker "$(comment "$RECENTLY" 'not-claude[bot]' "$FINISHED_BODY")")"

# ---------------------------------------------------------------- COMPLETION

assert "6. an unfinished tracker alone fails" \
    1 "$(run_checker "$tracker")"

assert "7. an unfinished tracker beside a finished review passes" \
    0 "$(run_checker "$tracker
$finished_review")"

assert "8. an unticked box in somebody else's comment is not charged to the review" \
    0 "$(run_checker "$other_bot
$finished_review")"

FENCED_BODY='Explaining the rule:

```
- [ ] this box is an example, not a real one
```

- [x] Done'
assert "9. an unticked box inside a fenced block does not count" \
    0 "$(run_checker "$(comment "$RECENTLY" "$REVIEWER" "$FENCED_BODY")")"

NESTED_FENCE_BODY='Quoting a document that itself shows a fence:

````
```
- [ ] still an example
```
````

- [x] Done'
assert "9b. a 3-backtick example nested in a 4-backtick fence does not count" \
    0 "$(run_checker "$(comment "$RECENTLY" "$REVIEWER" "$NESTED_FENCE_BODY")")"

INFO_STRING_BODY='An equal-length fence carrying an info string does not close:

```markdown
- [ ] example
```

- [x] Done'
assert "9c. an equal-length fence with an info string does not close the block" \
    0 "$(run_checker "$(comment "$RECENTLY" "$REVIEWER" "$INFO_STRING_BODY")")"

assert "9d. a ticked box is not an unticked one" \
    0 "$(run_checker "$(comment "$RECENTLY" "$REVIEWER" '- [x] all done')")"

# ---------------------------------------------------------------- AGE IS NOT A RULE

assert "10. a review far older than the rest of the PR still passes" \
    0 "$(run_checker "$ancient_review
$other_bot")"

assert "11. an ancient finished review beside a recent unfinished tracker passes" \
    0 "$(run_checker "$ancient_review
$tracker")"

# ---------------------------------------------------------------- SEGMENTATION

FORGED_BODY="I think the gate should accept this:
$(marker "$RECENTLY" "$REVIEWER" deadbeefdeadbeef)
- [x] pretending to be finished"
assert "12. a forged marker with the wrong token cannot open a comment" \
    1 "$(run_checker "$(comment "$RECENTLY" 'astubbs' "$FORGED_BODY")")"

assert "13. a real review is unaffected by a wrong-token marker in a later comment" \
    0 "$(run_checker "$finished_review
$(comment "$RECENTLY" 'astubbs' "$FORGED_BODY")")"

# The checker must not die of SIGPIPE when it stops reading early: bin/AGENTS.md, and the
# reason bin/check-shell-sigpipe.sh exists.
big_tail=""
for _ in $(seq 1 400); do
    big_tail="$big_tail
$(comment "$RECENTLY" 'github-actions[bot]' "$(head -c 200 /dev/zero | tr '\0' 'x')")"
done
assert "14. a large trailing comment stream does not break the scan" \
    0 "$(run_checker "$finished_review$big_tail")"

# ---------------------------------------------------------------- USAGE

ec=0; printf '' | "$CHECKER" >/dev/null 2>&1 || ec=$?
assert "15. no arguments is a usage error" 2 "$ec"

ec=0; printf '' | "$CHECKER" "$TOKEN" extra >/dev/null 2>&1 || ec=$?
assert "15b. too many arguments is a usage error" 2 "$ec"

ec=0; printf '' | "$CHECKER" '' >/dev/null 2>&1 || ec=$?
assert "16. an empty marker token is a usage error" 2 "$ec"

ec=0; printf '' | "$CHECKER" 'not a token' >/dev/null 2>&1 || ec=$?
assert "16b. a non-alphanumeric marker token is a usage error" 2 "$ec"

# ----------------------------------------------------------------

if [ "$failures" -ne 0 ]; then
    echo "FAILED: $failures case(s)"
    exit 1
fi
echo "ok:   all review-gate cases passed"
