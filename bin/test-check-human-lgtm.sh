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
#   TOKEN - every form below is a body this repo has actually received, or a mutation of one
#     8. lower-case lgtm, and the real forms this repo has received        -> pass (0)
#     9. LGTM?, LGTM ? and lgtm?                                           -> FAIL (1)
#     9d. a question mark across a SOFT LINE BREAK - 'LGTM\n?'             -> FAIL (1)
#     9e. ... but across a BLANK line, which is a paragraph break          -> pass (0)
#    10. NOT LGTM / not LGTM / never LGTM / isn't LGTM                     -> FAIL (1)
#    10e. 'not LGTM/LGTM' - where the negator rule deliberately stops      -> pass (0)
#    10f. a negator across a soft line break - 'not\nLGTM'                 -> FAIL (1)
#    10g. ... but not across a blank line                                  -> pass (0)
#    11. LGTM glued to a letter or digit either side                       -> FAIL (1)
#    11b. LGTM glued to ANOTHER LGTM - LGTMLGTM, xLGTMLGTM                 -> FAIL (1)
#    12. lgtm inside a fenced code block only                              -> FAIL (1)
#    12b. ... inside a 3-backtick example nested in a 4-backtick fence     -> FAIL (1)
#    12c. ... behind an EQUAL-length fence carrying an info string         -> FAIL (1)
#    12d. an info-string fence does not CLOSE an already-open fence        -> FAIL (1)
#    12e. a `~~~` line does not close a backtick fence                     -> FAIL (1)
#    12f. a CRLF body still closes its fence, so a later LGTM counts       -> pass (0)
#    12g. a fence opened BEHIND A LIST MARKER still hides the LGTM         -> FAIL (1)
#    12h. ... but a marker-prefixed line inside a block does not close it  -> FAIL (1)
#    13. LGTM inside a blockquote only                                     -> FAIL (1)
#    13b-d. a blockquote NESTED IN A LIST ITEM, three ways                 -> FAIL (1)
#    13e-f. a bulleted LGTM, and an `->` arrow, both still stamp           -> pass (0)
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
#    21b. a 7-field marker naming a DIFFERENT checker                      -> FAIL (1)
#    21c. a marker with this run's token but a lost field                  -> FAIL (1)
#    22. a real LGTM is unaffected by a forged marker in a later review    -> pass (0)
#    23. match found, then >64 KiB of further reviews (SIGPIPE regression) -> pass (0)
#
#   USAGE
#    24. wrong argument count, empty / non-alphanumeric token              -> usage (2)
#
#   COUPLING - the checker is fed the right stream by the workflow, by a step that RUNS
#    25.  a real (uncommented) step in the gate workflow runs this checker
#    25b. it reads the REVIEWS endpoint, with the marker this checker parses (25c, 25d)
#    25e. its `if:` guard is not a never-true constant, and 25f. is still the intended one
#    25g. nothing in it swallows the checker's exit status
#    25h. the checker invocation is the step's last command
#    26.  both of the gate's JOBS are guarded on the PR AUTHOR, not the event sender
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

# THE FORMS THE OWNER ACTUALLY USES. Every body below is a real one, copied from the survey of
# every owner LGTM this repo has received - the survey itself is stated once, in
# bin/check-human-lgtm.sh, and deliberately not summarised here, because the number that used to
# be summarised here was wrong. What matters to this suite is that each of these was a stamp the
# owner really gave, so a tightening that rejects one of them is rejecting a review that already
# happened, and cannot land without failing the case beside it that says so.
assert "8. the lower-case bare word - overwhelmingly the form the owner uses - passes" \
    0 "$(run_checker "$(owner_review 'lgtm')")"

assert "8b. a real variant, 'lgtm to try' (astubbs#280), passes" \
    0 "$(run_checker "$(owner_review 'lgtm to try ')")"

# astubbs#84, and the reason case-insensitivity is load-bearing rather than merely generous:
# without it this repo has a stamp on record that the checker would call missing.
assert "8c. the one capitalised real form, 'Lgtm' (astubbs#84), passes" \
    0 "$(run_checker "$(owner_review 'Lgtm')")"

# astubbs#73, and the reason the `?` clause rejects only a `?` TOUCHING the token. A rule that
# refused a question mark anywhere in the body would refuse this, which is a real stamp.
assert "8d. a real body that ends in a question mark (astubbs#73) still passes" \
    0 "$(run_checker "$(owner_review 'lgtm, @claude how about you?')")"

assert "9. LGTM followed by a question mark fails" \
    1 "$(run_checker "$(owner_review 'LGTM?')")"

assert "9b. LGTM followed by a spaced question mark fails" \
    1 "$(run_checker "$(owner_review 'LGTM ?')")"

# The two forms the request named explicitly as things that must not false-positive, in the
# case the owner actually types.
assert "9c. lower-case lgtm? fails - the question mark, not the case, is what rejects it" \
    1 "$(run_checker "$(owner_review 'lgtm?')")"

# ACROSS A SOFT LINE BREAK. Markdown renders a newline inside a paragraph as a space, so this
# body reads "LGTM ?" to every human who opens it - and the scanner, which looked at one
# physical line at a time, called it a stamp. Hard-wrapped prose is the ordinary way to reach
# this. Reported on astubbs/parallel-consumer#298 with 10f.
assert "9d. a question mark on the next line still refuses the stamp" \
    1 "$(run_checker "$(owner_review 'LGTM
?')")"

# WHERE THE LOOKAROUND STOPS, so that nobody widens it into the sentiment analysis this rule
# refuses: a blank line is a paragraph break, the two are not adjacent in the rendered text, and
# the LGTM stands. Delete the blank line and 9d is what you get.
assert "9e. a question mark in a LATER paragraph does not reach back" \
    0 "$(run_checker "$(owner_review 'LGTM

?')")"

assert "10. NOT LGTM fails" \
    1 "$(run_checker "$(owner_review 'This is NOT LGTM until the flake is fixed')")"

assert "10b. lower-case negation fails" \
    1 "$(run_checker "$(owner_review 'not LGTM')")"

assert "10c. never LGTM fails" \
    1 "$(run_checker "$(owner_review 'never LGTM')")"

assert "10d. a contraction negation fails" \
    1 "$(run_checker "$(owner_review "isn't LGTM yet")")"

# WHERE THE NEGATOR RULE STOPS, pinned so that nobody widens it by accident while fixing 11b
# below. Only the word TOUCHING the token is considered, so the second token here - whose
# preceding character is a slash, not a negator - is a match, and this body passes. That reads
# odd in isolation and is the deliberate choice: the alternative is a general negation detector,
# which is unbounded, and half of one firing unpredictably is worse than a rule you can state in
# a sentence. The person who could write this is the owner, about his own PR.
assert "10e. only the word touching the token negates it - 'not LGTM/LGTM' passes, on purpose" \
    0 "$(run_checker "$(owner_review 'not LGTM/LGTM')")"

# The negator's half of the soft-line-break clause: wrapped prose puts "not" at the end of one
# line and the token at the start of the next, which renders as "not LGTM" and used to pass.
assert "10f. a negator on the previous line still refuses the stamp" \
    1 "$(run_checker "$(owner_review "I have read the retry path and this is not
LGTM until the flake is fixed")")"

assert "10g. a negator in an EARLIER paragraph does not reach forward" \
    0 "$(run_checker "$(owner_review 'The first version of this was not

LGTM, but this one is')")"

assert "11. LGTM glued to a letter or digit either side fails" \
    1 "$(run_checker "$(owner_review 'ALGTM LGTMx LGTM2 xLGTM')")"

# The whole-word guard used to have a hole exactly where two tokens touch. The scanner advanced
# PAST each token it had examined, so the next one started a fresh string with no character
# before it - and a token at the start of a string looks like a token at a word boundary. The
# effect was that `LGTMLGTM` passed a rule under which neither half of it does, and `xLGTMLGTM`
# passed one that refuses `xLGTM`. Glued repeats are the only inputs that reach it, which is why
# case 11 above never did.
assert "11b. two LGTMs glued together fail - neither is at a word boundary" \
    1 "$(run_checker "$(owner_review 'LGTMLGTM')")"

assert "11c. a glued repeat behind a letter fails, as the single form does" \
    1 "$(run_checker "$(owner_review 'xLGTMLGTM')")"

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

# 12c proves an info string does not OPEN-then-close in one line. It does not prove the clause
# is load-bearing, because the fence in it is the opening one, where the info string is ignored
# anyway - delete the clause and 12c stays green. This is the input that flips: the info-string
# fence here arrives while a fence is already open, so treating it as a close would put the LGTM
# outside the block and stamp the PR.
INFO_STRING_CLOSE_BODY='Explaining the fence rule to a contributor:

```
here is what a nested example looks like
```markdown
LGTM
```

Still reading, back tomorrow.'
assert "12d. an info-string fence does not CLOSE an already-open fence" \
    1 "$(run_checker "$(owner_review "$INFO_STRING_CLOSE_BODY")")"

# Same shape for the fence-CHARACTER clause. Nothing in this suite used a `~~~` fence at all, so
# `ch == fence_ch` could be deleted with every case still green.
TILDE_FENCE_BODY='Both fence characters, in one block:

```
~~~
LGTM
```

Not finished.'
assert "12e. a tilde fence does not close a backtick fence" \
    1 "$(run_checker "$(owner_review "$TILDE_FENCE_BODY")")"

# GitHub stores a body as it was typed, and a CRLF body used to be unreadable: the fence-close
# test wants nothing but whitespace after the backticks, and a carriage return is not nothing.
# So the fence never closed and swallowed every LGTM after it - including the one being given.
# No real body has done this (0 of 365 owner review bodies carry a CR), which is exactly why it
# needs a case rather than a wait.
CRLF_BODY=$'The rule looks like this:\r\n```\r\nLGTM\r\n```\r\n\r\nAnd having now read the diff: LGTM\r'
assert "12f. a CRLF body still closes its fence, so the LGTM after it counts" \
    0 "$(run_checker "$(owner_review "$CRLF_BODY")")"

# A FENCE OPENED BEHIND A LIST MARKER IS STILL A FENCE. CommonMark lets a block start straight
# after the marker, so this renders as a code block exactly as 12 does - but the fence test only
# allowed leading whitespace, so the `- ` carried the LGTM out of the block and stamped the PR.
# Reported on astubbs/parallel-consumer#298 alongside 13b.
LIST_FENCE_BODY='Two ways to write it:

- ```
  LGTM
  ```

Still reading.'
assert "12g. a fence opened behind a list marker still hides the LGTM" \
    1 "$(run_checker "$(owner_review "$LIST_FENCE_BODY")")"

# The asymmetry the fix has to keep: a marker may precede an OPENING fence, but between the two
# fences every line is literal code, where `- ` is text rather than a marker. Read the closing
# rule the same way as the opening one and this body's block ends early, putting its LGTM in
# open prose. Nothing else in this suite distinguishes the two directions.
LIST_FENCE_CLOSE_BODY='Explaining what a bulleted fence looks like:

```
- ```
LGTM
```

Still reading.'
assert "12h. a marker-prefixed fence line inside a block does not close it" \
    1 "$(run_checker "$(owner_review "$LIST_FENCE_CLOSE_BODY")")"

QUOTED_BODY='> LGTM

That was Bob, not me. I have not read it yet.'
assert "13. LGTM inside a blockquote does not stamp" \
    1 "$(run_checker "$(owner_review "$QUOTED_BODY")")"

# THE SAME QUOTE, ONE BULLET DEEP. `- > LGTM` is a blockquote nested in a list item and renders
# identically to 13; the predicate looked for `>` as the first non-whitespace character, so a
# leading `- ` walked a quoted LGTM straight past the exclusion and turned the required check
# green with nobody having stamped anything.
assert "13b. a blockquote nested in a list item does not stamp either" \
    1 "$(run_checker "$(owner_review '- > LGTM')")"

assert "13c. the same in an ordered list does not stamp" \
    1 "$(run_checker "$(owner_review '1. > LGTM')")"

assert "13d. two levels of list nesting does not stamp" \
    1 "$(run_checker "$(owner_review '  - - > LGTM')")"

# THE OTHER SIDE OF THAT FIX, and the reason it strips markers rather than any leading
# punctuation: a bulleted LGTM is a perfectly ordinary stamp, and a rule that swallowed the
# whole line prefix would refuse it.
assert "13e. an LGTM in a plain list item still stamps" \
    0 "$(run_checker "$(owner_review '- LGTM')")"

# A marker needs whitespace (or end of line) after it, which is what makes `->` an arrow rather
# than a bullet followed by a blockquote - CommonMark says the same. It has to start the LINE to
# reach the rule at all, which is why this body is two lines: a mid-sentence `->` never gets
# near the marker-stripping. Drop the whitespace clause and this ordinary stamp is read as
# quoted text and refused.
assert "13f. a line-leading arrow is prose, not a bullet and a blockquote" \
    0 "$(run_checker "$(owner_review 'Read it end to end.
-> LGTM')")"

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

# Case 3 feeds a `check-review-posted` marker, but that one has six fields, so it is refused by
# the field count before the NAME is ever looked at - delete `$2 == "check-human-lgtm"` and case
# 3 stays green. This is the input that flips it: a marker of the right SHAPE and the right
# token, naming the other checker. The two streams are not interchangeable and the marker name
# is the second line of defence that says so.
OTHER_CHECKER_MARKER="$(printf '<!-- check-review-posted %s %s %s COMMENTED -->\n%s' \
    "$TOKEN" "$RECENTLY" "$OWNER" 'LGTM')"
assert "21b. a full-shape marker naming a different checker cannot open a review" \
    1 "$(run_checker "$OTHER_CHECKER_MARKER")"

# A marker carrying THIS RUN's token but a lost field - what an innocent `--jq` edit, or a
# `state` that came back empty, would produce. It used to be read as more of the PREVIOUS
# review's body, so the next reviewer's words were merged into the owner's segment and this
# exact stream reported "astubbs submitted a review containing LGTM". Nobody wrote the LGTM but
# mallory. It now closes the segment instead, and fails closed.
MALFORMED_MARKER="$(review "$RECENTLY" "$OWNER" COMMENTED 'Not read it yet - will look tonight.')
$(printf '<!-- check-human-lgtm %s %s mallory -->\n%s' "$TOKEN" "$RECENTLY" 'LGTM')"
assert "21c. a marker of ours that lost a field closes the segment, it does not extend it" \
    1 "$(run_checker "$MALFORMED_MARKER")"

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
#
# THESE CASES ASSERT ON THE STEP, NOT ON THE FILE'S TEXT, and that distinction is the whole
# point of them. They were four `grep -F` substring searches over the whole workflow, and a
# substring search cannot tell a step that RUNS from one that merely APPEARS. Measured, on a
# scratch copy, before this was rewritten: `if: false` on the step, `|| true` on its pipeline,
# `; true`, `set +e`, `continue-on-error: true`, and COMMENTING THE ENTIRE STEP OUT all left the
# suite green at 42/42. One of them - `grep -F 'bin/check-human-lgtm.sh'` - was satisfied by two
# header COMMENTS alone, so it stayed green with the step deleted outright.
#
# That is the failure class in
# docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md occurring
# INSIDE the guard written to prevent it, which is why it is worth more than a substring search
# and a hope. So: parse the step out of the workflow, drop comment lines first, and assert on
# its structure.
#
# Deliberately awk and not python3 + PyYAML, though a real YAML parse was the obvious reach and
# PyYAML does import on this machine. This suite runs as a step of a REQUIRED check, ahead of
# the gate it protects, so anything it depends on is a thing that can brick every open PR by
# being absent from a runner image. A module that is present today and not tomorrow buys a
# tidier parse at the price of that. The indentation rules below are the only YAML this file
# needs to understand, and the workflow they read is in the same repository.

# Prints the workflow step whose body contains <substring>, with every comment line removed, or
# nothing at all when there is no such step. Comments are dropped FIRST and on purpose: a step
# that has been commented out must read as an ABSENT step, and a mention of the checker in a
# header comment must not stand in for the step existing.
gate_step() { # <workflow-file> <substring>
    awk -v want="$2" '
        # YAML comments and shell comments inside a `run:` block alike.
        /^[ \t]*#/ { next }

        {
            match($0, /^ */)
            ind = RLENGTH

            # The indent of the step list is read from the first step rather than assumed, so
            # reindenting the file does not silently turn every case below green.
            if (!have_ind && $0 ~ /^ *- name:/) { have_ind = 1; step_ind = ind }

            # A non-blank line shallower than the step list ends it - the next job starts here.
            if (have_ind && $0 !~ /^[ \t]*$/ && ind < step_ind) {
                if (index(buf, want)) { printf "%s", buf; found = 1; exit }
                buf = ""; have_ind = 0
            }

            # A list item at exactly the step indent starts the next step.
            if (have_ind && ind == step_ind && $0 ~ /^ *- /) {
                if (index(buf, want)) { printf "%s", buf; found = 1; exit }
                buf = ""
            }

            if (have_ind) buf = buf $0 "\n"
        }

        END { if (!found && index(buf, want)) printf "%s", buf }
    ' "$1"
}

LGTM_STEP=$(gate_step "$GATE_WORKFLOW" 'bash bin/check-human-lgtm.sh')

# Herestrings rather than `printf | grep`, throughout this section: this file runs under
# `pipefail`, an early-exiting reader takes the writer down with EPIPE, and `pipefail` promotes
# that 141 to the pipeline's status - so matching would become failing. bin/AGENTS.md, and
# bin/check-shell-sigpipe.sh, which fails the build over it.

# The `if:` guard, whitespace-squeezed so reflowing the line is not a failure but changing it is.
LGTM_STEP_IF=$(sed -n 's/^[ \t]*if:[ \t]*//p' <<<"$LGTM_STEP" | tr -s ' \t' '  ')
LGTM_STEP_IF=${LGTM_STEP_IF#"${LGTM_STEP_IF%%[! ]*}"}
LGTM_STEP_IF=${LGTM_STEP_IF%"${LGTM_STEP_IF##*[! ]}"}

# The guard this step is supposed to carry, spelled out so that ANY change to it - including one
# that can never be true - has to come past a red test and a person who meant it.
EXPECTED_IF="\${{ !cancelled() && steps.automated-review.conclusion != 'skipped' }}"

# The last command of the step, trimmed. `|| true` and friends appended to the pipeline change
# this line, which is what makes them detectable at all.
LGTM_STEP_LAST=$(grep -v '^[ \t]*$' <<<"$LGTM_STEP" | tail -1 | sed 's/^[ \t]*//;s/[ \t]*$//')

coupling() { # <description> <substring>   (searched in the parsed step, not the file)
    if [ -n "$LGTM_STEP" ] && grep -Fq -- "$2" <<<"$LGTM_STEP"; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (not found in the human-LGTM step of $GATE_WORKFLOW)"
        failures=$((failures + 1))
    fi
}

if [ -n "$LGTM_STEP" ]; then
    echo "ok:   25. the gate has a step that runs this checker - a real step, not a comment"
else
    echo "FAIL: 25. the gate has no step running bin/check-human-lgtm.sh (deleted, or commented out) in $GATE_WORKFLOW"
    failures=$((failures + 1))
fi

coupling "25b. that step reads the REVIEWS endpoint, not the comments one" 'pulls/$PR_NUMBER/reviews'
coupling "25c. it emits the marker this checker parses" '<!-- check-human-lgtm \(env.MARKER_TOKEN) \(.submitted_at'
coupling "25d. the marker still carries login and state" '\(.user.login // "unknown") \(.state // "UNKNOWN") -->'

# A guard that can never be true is a deleted step that still reads as present. These are the
# spellings that disable a step outright; the case below pins the guard exactly, and this one
# exists so that the reason is legible even to somebody who "fixes" that case by pasting the new
# value in.
if [ -z "$LGTM_STEP" ]; then
    echo "FAIL: 25e. there is no step whose guard could be checked"
    failures=$((failures + 1))
else
    case "$LGTM_STEP_IF" in
        false | 'true == false' | '${{ false }}' | '${{ !true }}' | '${{ false == true }}' | 0)
            echo "FAIL: 25e. the step's guard is never true ('$LGTM_STEP_IF') - the step can never run, so the gate asserts nothing while still looking like it does"
            failures=$((failures + 1))
            ;;
        *)
            echo "ok:   25e. the step's guard is not a never-true constant"
            ;;
    esac
fi

if [ "$LGTM_STEP_IF" = "$EXPECTED_IF" ]; then
    echo "ok:   25f. the step's guard is still the one this gate intends"
else
    echo "FAIL: 25f. the step's guard is '$LGTM_STEP_IF', expected '$EXPECTED_IF'. If you changed it deliberately, change it here too - the point of this case is that no edit to the guard, least of all one that can never be true, can quietly stop the gate asking the question."
    failures=$((failures + 1))
fi

# Nothing may swallow the checker's exit status. `set +e` alone does not actually neuter this
# particular step (the pipeline is its last command, so its status is still the step's), but it
# is refused anyway: the next person to add a line after the pipeline would make it live, and a
# rule you can state in one sentence beats one with a footnote.
swallow=""
for pattern in '|| true' '|| :' '; true' '; :' '&& true' 'set +e' 'continue-on-error'; do
    if grep -Fq -- "$pattern" <<<"$LGTM_STEP"; then
        swallow="$swallow '$pattern'"
    fi
done
if [ -z "$swallow" ] && [ -n "$LGTM_STEP" ]; then
    echo "ok:   25g. nothing in the step swallows the checker's exit status"
else
    echo "FAIL: 25g. the step swallows the checker's exit status -${swallow:- (step not found)}. A gate that cannot go red is not a gate."
    failures=$((failures + 1))
fi

EXPECTED_LAST='| bash bin/check-human-lgtm.sh "$MARKER_TOKEN"'
if [ "$LGTM_STEP_LAST" = "$EXPECTED_LAST" ]; then
    echo "ok:   25h. the checker invocation is the last command in the step"
else
    echo "FAIL: 25h. the step's last command is '$LGTM_STEP_LAST', expected '$EXPECTED_LAST'. Anything after the checker decides the step's exit status instead of the checker."
    failures=$((failures + 1))
fi

# THE JOB'S GUARD, for the same reason 25e and 25f pin the step's, and one the step-level cases
# cannot reach: a job that does not run reports a `skipped` check run, and GitHub counts skipped
# as SATISFYING a required check. So the job guard is the one line in this workflow that can
# turn the gate green with nothing asserted, and it is not hypothetical - it read
# `github.event.sender.type != 'Bot'` until astubbs/parallel-consumer#298, where `sender` is
# whoever triggered the event rather than the PR's author. A bot pushing to a human's branch
# (this repo has one that does) made the sender a Bot and skipped the entire gate.
#
# Both jobs are asserted, including the transitional `claude-review` one, because that is the
# job the ruleset currently requires - a hole left open there is open in practice. The case
# lives in this file rather than a third one because this is the file that already parses this
# workflow.
job_if() { # <workflow-file> <job-id>
    awk -v job="$2" '
        /^[ \t]*#/ { next }
        $0 ~ "^  " job ":[ \t]*$" { inside = 1; next }
        inside && /^  [^ ]/ { exit }
        inside && /^    steps:/ { exit }
        inside && /^    if:/ {
            sub(/^[ \t]*if:[ \t]*/, ""); sub(/[ \t]+$/, ""); print; exit
        }
    ' "$1"
}

# Spelled out so that ANY edit to it - a widened exemption, a never-true constant, or a quiet
# return to `sender` - has to come past a red test and a person who meant it.
EXPECTED_JOB_IF="github.event.pull_request.user.type != 'Bot'"

for job in review-gate claude-review; do
    guard=$(job_if "$GATE_WORKFLOW" "$job")
    if [ "$guard" = "$EXPECTED_JOB_IF" ]; then
        echo "ok:   26. the '$job' job is guarded on the PR author, not the event sender"
    else
        echo "FAIL: 26. the '$job' job's guard is '$guard', expected '$EXPECTED_JOB_IF'. A job that does not run reports a skipped check run, and GitHub counts that as satisfying a required check - so this line can turn the whole gate green with neither half asserted. 'github.event.sender' is whoever triggered the event, not the PR's author, and a bot pushing to a human's branch is enough to skip it."
        failures=$((failures + 1))
    fi
done

# ----------------------------------------------------------------

if [ "$failures" -ne 0 ]; then
    echo "FAILED: $failures case(s)"
    exit 1
fi
echo "ok:   all human-LGTM cases passed"
