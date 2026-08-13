#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-human-lgtm.sh.
#
# Feeds the checker synthetic review streams and asserts its verdict for every clause of the
# matching rule. Half of this file is negative controls, on purpose: this check is the only
# thing standing between "the owner never actually looked at it" and a green required check,
# and an assertion that has never been seen to fail proves nothing (docs/investigating.md, and
# docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md).
#
#   SOURCE - it must be a submitted review, from the reviews endpoint
#     1. an owner review whose body says LGTM                              -> pass (0)
#     2. no reviews at all                                                 -> FAIL (1)
#     3. the same LGTM body arriving as an ISSUE COMMENT                   -> FAIL (1)
#     4. a PENDING review (started, never submitted) saying LGTM           -> FAIL (1)
#
#   IDENTITY
#     5. LGTM from somebody else entirely                                  -> FAIL (1)
#     6. LGTM from a login merely CONTAINING the owner's                   -> FAIL (1)
#     7. LGTM from the owner's login in a different case                   -> pass (0)
#
#   TOKEN
#     8. lower-case lgtm, and the two real forms this repo has received    -> pass (0)
#     9. LGTM?, LGTM ? and lgtm?                                           -> FAIL (1)
#    10. NOT LGTM / not LGTM / never LGTM / isn't LGTM                     -> FAIL (1)
#    11. LGTM glued to a letter or digit either side                       -> FAIL (1)
#    12. lgtm inside a fenced code block only                              -> FAIL (1)
#    12b. ... inside a 3-backtick example nested in a 4-backtick fence     -> FAIL (1)
#    12c. ... behind an EQUAL-length fence carrying an info string         -> FAIL (1)
#    13. LGTM inside a blockquote only                                     -> FAIL (1)
#    14. LGTM wearing ordinary punctuation - (LGTM) LGTM! -- LGTM. LGTM,   -> pass (0)
#    15. a rejected near-miss beside a real LGTM in the same body          -> pass (0)
#
#   STATE - not consulted, except PENDING
#    16. APPROVED with an EMPTY body                                       -> FAIL (1)
#    17. DISMISSED review saying LGTM                                      -> pass (0)
#    18. CHANGES_REQUESTED review saying LGTM                              -> pass (0)
#
#   HEAD-INSENSITIVITY - the deliberate reversal, pinned as positive assertions
#    19. an ancient LGTM beside newer owner reviews that do not say it     -> pass (0)
#    20. an LGTM long predating every other review on the PR               -> pass (0)
#
#   SEGMENTATION
#    21. a body forging a marker line with the wrong token                 -> FAIL (1)
#    22. a real LGTM is unaffected by a forged marker in a later review    -> pass (0)
#    23. match found, then >64 KiB of further reviews (SIGPIPE regression) -> pass (0)
#
#   USAGE
#    24. wrong argument count, empty / non-alphanumeric token              -> usage (2)
#
#   COUPLING - the checker is fed the right stream by the workflow
#    25. the gate workflow reads the REVIEWS endpoint into this checker, with a marker
#        matching the one this checker parses
#
# Cases 17 and 19 are the ones that pin head-insensitivity, and they are written as positive
# assertions rather than as deleted code so that re-adding a freshness rule breaks the suite.
# 17 is the sharper of the two: the master ruleset dismisses approvals on push, so a version of
# this check that consulted review state would pass that stream today and fail it after an
# unrelated typo fix - head-sensitivity arriving through a side door.
#
# Cases 12-13 are the self-referential hazard: the review of the PR that introduced this rule
# necessarily quotes the token, and a posted review never changes, so counting a quoted mention
# would stamp a PR nobody stamped. Case 3 is the one that keeps the stamp ceremonial - a
# passing remark in the comment box is not a review.
#
# Run: bin/test-check-human-lgtm.sh   (CI runs it before the gate it protects)

set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CHECKER="$HERE/check-human-lgtm.sh"
GATE_WORKFLOW="$HERE/../.github/workflows/claude-code-review.yml"

TOKEN=31453513070
OWNER='astubbs'

# Timestamps reach the failure message and are never compared for a verdict, so these only need
# to be distinguishable from each other by eye.
LONG_AGO=2026-01-02T03:04:05Z
RECENTLY=2026-08-13T09:15:00Z

failures=0

assert() { # <description> <expected> <actual>
    if [ "$2" = "$3" ]; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (expected exit '$2', got '$3')"
        failures=$((failures + 1))
    fi
}

# Renders the marker line the workflow's --jq emits ahead of each review body. No trailing
# newline: `$(...)` strips those, so callers that need one add it themselves.
marker() { # <submitted-at> <login> <state> [token]
    printf '<!-- check-human-lgtm %s %s %s %s -->' "${4:-$TOKEN}" "$1" "$2" "$3"
}

# One whole review as the workflow renders it: marker line, then the body.
review() { # <submitted-at> <login> <state> <body> [token]
    printf '%s\n%s' "$(marker "$1" "$2" "$3" "${5:-$TOKEN}")" "$4"
}

# The shorthand almost every case wants: a submitted COMMENTED review by the owner.
owner_review() { # <body>
    review "$RECENTLY" "$OWNER" COMMENTED "$1"
}

# Runs the checker with stdin from $1, swallowing its output; echoes the exit code.
run_checker() { # <stdin-text>
    local ec=0
    printf '%s' "$1" | "$CHECKER" "$TOKEN" >/dev/null 2>&1 || ec=$?
    echo "$ec"
}

# The harness has to be able to fail, or every "ok:" line below is decoration. Proven here
# rather than assumed: a stream that must pass and a stream that must fail are run through the
# same helper and required to disagree.
sanity_pass=$(run_checker "$(owner_review 'LGTM')")
sanity_fail=$(run_checker "")
if [ "$sanity_pass" = "$sanity_fail" ]; then
    echo "FAIL: the test harness cannot distinguish pass from fail (both '$sanity_pass')"
    exit 1
fi

# ---------------------------------------------------------------- SOURCE

assert "1. an owner review saying LGTM passes" \
    0 "$(run_checker "$(owner_review 'Read the whole diff. LGTM.')")"

assert "2. no reviews at all fails" \
    1 "$(run_checker "")"

# The endpoint separation is what really enforces this - the workflow feeds this checker
# `pulls/<n>/reviews` and nothing else, asserted by case 25 - but the marker name enforces it a
# second time, so an issue comment cannot be smuggled into this stream either.
issue_comment="$(printf '<!-- check-review-posted %s %s %s -->\n%s' \
    "$TOKEN" "$RECENTLY" "$OWNER" 'LGTM, ship it')"
assert "3. LGTM in an ordinary issue comment is not a review" \
    1 "$(run_checker "$issue_comment")"

assert "4. a PENDING review saying LGTM does not count" \
    1 "$(run_checker "$(review "$RECENTLY" "$OWNER" PENDING 'LGTM')")"

# ---------------------------------------------------------------- IDENTITY

assert "5. LGTM from somebody else fails" \
    1 "$(run_checker "$(review "$RECENTLY" 'some-contributor' COMMENTED 'LGTM')")"

assert "6. LGTM from a login containing the owner's fails" \
    1 "$(run_checker "$(review "$RECENTLY" 'astubbs-bot' COMMENTED 'LGTM')
$(review "$RECENTLY" 'not-astubbs' COMMENTED 'LGTM')")"

assert "7. the owner's login in a different case still counts" \
    0 "$(run_checker "$(review "$RECENTLY" 'AStubbs' COMMENTED 'LGTM')")"

# ---------------------------------------------------------------- TOKEN

# THE FORM THE OWNER ACTUALLY USES, and the reason the token match is case-insensitive. Every
# owner LGTM this repo has received - 18, across astubbs#210 to astubbs#292 - is the lower-case
# bare word, seventeen alone on a line and one as "lgtm to try". Both real forms are asserted
# here, so a future tightening to capitals cannot land without failing the case that says why.
assert "8. the lower-case bare word - the form 18 real reviews used - passes" \
    0 "$(run_checker "$(owner_review 'lgtm')")"

assert "8b. the one real variant, 'lgtm to try', passes" \
    0 "$(run_checker "$(owner_review 'lgtm to try ')")"

assert "8c. mixed case passes" \
    0 "$(run_checker "$(owner_review 'Lgtm')")"

assert "9. LGTM followed by a question mark fails" \
    1 "$(run_checker "$(owner_review 'LGTM?')")"

assert "9b. LGTM followed by a spaced question mark fails" \
    1 "$(run_checker "$(owner_review 'LGTM ?')")"

# The two forms the request named explicitly as things that must not false-positive, in the
# case the owner actually types.
assert "9c. lower-case lgtm? fails - the question mark, not the case, is what rejects it" \
    1 "$(run_checker "$(owner_review 'lgtm?')")"

assert "10. NOT LGTM fails" \
    1 "$(run_checker "$(owner_review 'This is NOT LGTM until the flake is fixed')")"

assert "10b. lower-case negation fails" \
    1 "$(run_checker "$(owner_review 'not LGTM')")"

assert "10c. never LGTM fails" \
    1 "$(run_checker "$(owner_review 'never LGTM')")"

assert "10d. a contraction negation fails" \
    1 "$(run_checker "$(owner_review "isn't LGTM yet")")"

assert "11. LGTM glued to a letter or digit either side fails" \
    1 "$(run_checker "$(owner_review 'ALGTM LGTMx LGTM2 xLGTM')")"

FENCED_BODY='Explaining the rule to a contributor:

```
LGTM
```

Still working through the concurrency change.'
assert "12. LGTM inside a fenced code block does not stamp" \
    1 "$(run_checker "$(owner_review "$FENCED_BODY")")"

NESTED_FENCE_BODY='Quoting a document that itself shows a fence:

````
```
LGTM
```
````

Not done reading.'
assert "12b. a 3-backtick example nested in a 4-backtick fence does not stamp" \
    1 "$(run_checker "$(owner_review "$NESTED_FENCE_BODY")")"

INFO_STRING_BODY='An equal-length fence carrying an info string does not close:

```markdown
LGTM
```

Still reading.'
assert "12c. an equal-length fence with an info string does not close the block" \
    1 "$(run_checker "$(owner_review "$INFO_STRING_BODY")")"

QUOTED_BODY='> LGTM

That was Bob, not me. I have not read it yet.'
assert "13. LGTM inside a blockquote does not stamp" \
    1 "$(run_checker "$(owner_review "$QUOTED_BODY")")"

assert "14. LGTM wearing ordinary punctuation passes" \
    0 "$(run_checker "$(owner_review '(LGTM)')")"

assert "14b. an exclaimed LGTM passes" \
    0 "$(run_checker "$(owner_review 'read it end to end -- LGTM!')")"

assert "14c. a trailing-clause LGTM passes" \
    0 "$(run_checker "$(owner_review 'LGTM, ship it')")"

NEAR_MISS_THEN_REAL='lgtm? not yet - one question about the retry path.

...and having read your answer: LGTM.'
assert "15. a near-miss beside a real LGTM in the same body passes" \
    0 "$(run_checker "$(owner_review "$NEAR_MISS_THEN_REAL")")"

# ---------------------------------------------------------------- STATE

assert "16. an APPROVED review with an empty body does not count" \
    1 "$(run_checker "$(review "$RECENTLY" "$OWNER" APPROVED '')")"

assert "17. a DISMISSED review saying LGTM still counts" \
    0 "$(run_checker "$(review "$RECENTLY" "$OWNER" DISMISSED 'LGTM')")"

assert "18. a CHANGES_REQUESTED review saying LGTM counts" \
    0 "$(run_checker "$(review "$RECENTLY" "$OWNER" CHANGES_REQUESTED 'LGTM apart from the typo')")"

# ---------------------------------------------------------------- HEAD-INSENSITIVITY

assert "19. an ancient LGTM beside newer owner reviews that do not say it passes" \
    0 "$(run_checker "$(review "$LONG_AGO" "$OWNER" APPROVED 'LGTM')
$(owner_review 'one more thought about the poller')")"

assert "20. an LGTM predating every other review on the PR passes" \
    0 "$(run_checker "$(review "$LONG_AGO" "$OWNER" COMMENTED 'LGTM')
$(review "$RECENTLY" 'some-contributor' COMMENTED 'I have some questions')")"

# ---------------------------------------------------------------- SEGMENTATION

FORGED_BODY="I think the gate should accept this:
$(marker "$RECENTLY" "$OWNER" APPROVED deadbeefdeadbeef)
LGTM"
assert "21. a forged marker with the wrong token cannot open a review" \
    1 "$(run_checker "$(review "$RECENTLY" 'some-contributor' COMMENTED "$FORGED_BODY")")"

assert "22. a real LGTM is unaffected by a wrong-token marker in a later review" \
    0 "$(run_checker "$(owner_review 'LGTM')
$(review "$RECENTLY" 'some-contributor' COMMENTED "$FORGED_BODY")")"

# The checker must not die of SIGPIPE when it stops reading early: bin/AGENTS.md, and the
# reason bin/check-shell-sigpipe.sh exists.
big_tail=""
for _ in $(seq 1 400); do
    big_tail="$big_tail
$(review "$RECENTLY" 'some-contributor' COMMENTED "$(head -c 200 /dev/zero | tr '\0' 'x')")"
done
assert "23. a large trailing review stream does not break the scan" \
    0 "$(run_checker "$(owner_review 'LGTM')$big_tail")"

# ---------------------------------------------------------------- USAGE

ec=0; printf '' | "$CHECKER" >/dev/null 2>&1 || ec=$?
assert "24. no arguments is a usage error" 2 "$ec"

ec=0; printf '' | "$CHECKER" "$TOKEN" extra >/dev/null 2>&1 || ec=$?
assert "24b. too many arguments is a usage error" 2 "$ec"

ec=0; printf '' | "$CHECKER" '' >/dev/null 2>&1 || ec=$?
assert "24c. an empty marker token is a usage error" 2 "$ec"

ec=0; printf '' | "$CHECKER" 'not a token' >/dev/null 2>&1 || ec=$?
assert "24d. a non-alphanumeric marker token is a usage error" 2 "$ec"

# ---------------------------------------------------------------- COUPLING

# Every case above proves what the checker does with a stream. None of them proves the WORKFLOW
# hands it the right stream - and a gate fed the wrong endpoint would report success without
# having asked the question, which is this repo's most-repeated defect
# (docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md). The
# marker shape is documented as living in three places; this is what makes that a rule rather
# than a hope.
coupling() { # <description> <pattern>
    if grep -Fq "$2" "$GATE_WORKFLOW"; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (no line matching '$2' in $GATE_WORKFLOW)"
        failures=$((failures + 1))
    fi
}

coupling "25. the gate reads the reviews endpoint" 'pulls/$PR_NUMBER/reviews'
coupling "25b. the gate pipes it into this checker" 'bin/check-human-lgtm.sh'
coupling "25c. the gate emits the marker this checker parses" '<!-- check-human-lgtm \(env.MARKER_TOKEN) \(.submitted_at'
coupling "25d. the marker still carries login and state" '\(.user.login // "unknown") \(.state // "UNKNOWN") -->'

# ----------------------------------------------------------------

if [ "$failures" -ne 0 ]; then
    echo "FAILED: $failures case(s)"
    exit 1
fi
echo "ok:   all human-LGTM cases passed"
