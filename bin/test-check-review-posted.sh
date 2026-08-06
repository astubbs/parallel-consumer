#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-review-posted.sh.
#
# Feeds the checker synthetic comment streams and asserts its verdict for every rule:
#    1. review step succeeded, a comment cites this run              -> pass (0)
#    2. review step succeeded, no comments at all                    -> FAIL (1)
#    3. review step succeeded, comments cite a DIFFERENT run         -> FAIL (1)
#    4. review step succeeded, this run's id is only a PREFIX of a
#       longer run id in the comments                                -> FAIL (1)
#    5. run url sits deep inside a multi-line body, not line 1       -> pass (0)
#    6. run url is the last thing in the stream, no trailing text    -> pass (0)
#    7. review step FAILED, even with a matching comment             -> FAIL (1)
#    8. review step SKIPPED, even with a matching comment            -> FAIL (1)
#    9. review step CANCELLED, even with a matching comment          -> FAIL (1)
#   10. wrong argument count                                         -> usage (2)
#   11. non-numeric run id                                           -> usage (2)
#   12. empty review outcome                                         -> usage (2)
#   13. match found, then >64 KiB of further comments                -> pass (0)
#
# Case 4 is the one worth keeping honest: a plain substring search for "actions/runs/3096"
# matches run 30965089954, which would pass this run's check on a different run's review.
#
# Run: bin/test-check-review-posted.sh   (CI runs it before the gate it protects)

set -uo pipefail

CHECKER="$(cd "$(dirname "$0")" && pwd)/check-review-posted.sh"

failures=0

assert() { # <description> <expected> <actual>
    if [ "$2" = "$3" ]; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (expected exit '$2', got '$3')"
        failures=$((failures + 1))
    fi
}

# Runs the checker with stdin from $1, swallowing its output; echoes the exit code.
run_checker() { # <stdin-text> <run-id> <outcome>
    local ec=0
    printf '%s' "$1" | "$CHECKER" "$2" "$3" >/dev/null 2>&1 || ec=$?
    echo "$ec"
}

POSTED_COMMENT='**Claude finished @astubbs'"'"'s task in 1m 52s** —— [View job](https://github.com/astubbs/parallel-consumer/actions/runs/30965089954)

---
### Reviewing PR #124
Looks good to me.'

OTHER_BOT_COMMENTS='## Duplicate Code Report
No new clones introduced by this PR.
## SpotBugs Report
No bugs found.'

assert "matching comment, step succeeded" \
    0 "$(run_checker "$POSTED_COMMENT" 30965089954 success)"

assert "no comments at all" \
    1 "$(run_checker "" 30965089954 success)"

assert "only other bots commented" \
    1 "$(run_checker "$OTHER_BOT_COMMENTS" 30965089954 success)"

assert "this run's id is a prefix of another run's id" \
    1 "$(run_checker "$POSTED_COMMENT" 3096 success)"

assert "run url buried in a multi-line body" \
    0 "$(run_checker "$OTHER_BOT_COMMENTS
$POSTED_COMMENT" 30965089954 success)"

assert "run url at the very end of the stream" \
    0 "$(run_checker "see https://github.com/astubbs/parallel-consumer/actions/runs/30965089954" 30965089954 success)"

# The live failure this script shipped with: the match is found, but >64 KiB of
# comments follow it, so `printf | grep -q` + pipefail turned success into exit 1.
# Cases 5 and 6 missed it - case 6 puts the match LAST, so nothing follows to
# fill the pipe buffer. Observed on #198, #199, #204 and #210; on #210 a 4.7 KB
# review comment was followed by a 127 KB similarity report.
LARGE_TRAILER="$(yes 'padding to exceed the 64 KiB pipe buffer, as a similarity report does' | head -n 2000)"
assert "match found, then >64 KiB of further comments (SIGPIPE regression)" \
    0 "$(run_checker "$POSTED_COMMENT
$LARGE_TRAILER" 30965089954 success)"

for outcome in failure skipped cancelled; do
    assert "review step '$outcome' is never a review, matching comment or not" \
        1 "$(run_checker "$POSTED_COMMENT" 30965089954 "$outcome")"
done

ec=0
printf '%s' "$POSTED_COMMENT" | "$CHECKER" 30965089954 >/dev/null 2>&1 || ec=$?
assert "wrong argument count" 2 "$ec"

assert "non-numeric run id" \
    2 "$(run_checker "$POSTED_COMMENT" not-a-run-id success)"

assert "empty review outcome" \
    2 "$(run_checker "$POSTED_COMMENT" 30965089954 "")"

# --- Structural guard: no SIGPIPE-prone pipes in the checker -------------------
# The same rule bin/test-check-copyright-headers.sh enforces on the copyright
# scanner, for the same reason. This checker shipped without it and hit the bug
# live on four PRs before anyone noticed.
if grep -vE '^[[:space:]]*#' "$CHECKER" | grep -nE '\|[[:space:]]*grep -q|\|[[:space:]]*awk '; then
    echo "FAIL: checker pipes into an early-exiting reader (SIGPIPE + pipefail misclassification risk) - use a herestring"
    failures=$((failures + 1))
else
    echo "ok:   checker has no SIGPIPE-prone pipes into grep -q / awk"
fi

echo
if [ "$failures" -eq 0 ]; then
    echo "All bin/check-review-posted.sh self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
