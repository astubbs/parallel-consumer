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
#   14. this run's comment still has unticked task-list boxes        -> FAIL (1)
#   15. this run's comment has a task list, all boxes ticked         -> pass (0)
#   16. two comments from this run, a stale tracker and a finished
#       review, separated by the boundary marker                     -> pass (0)
#   17. this run's finished review plus somebody ELSE's in-progress
#       tracker, separated by the boundary marker                    -> pass (0)
#   18. the case-16 stream with no boundary markers at all, so the
#       checker has to fall back to run-url segmentation             -> pass (0)
#   19. the FIRST comment in a bounded stream is an unfinished
#       tracker that cites this run below its task list              -> FAIL (1)
#
# Case 4 is the one worth keeping honest: a plain substring search for "actions/runs/3096"
# matches run 30965089954, which would pass this run's check on a different run's review.
#
# Case 14 is the astubbs#271 failure, verbatim: the action reported "finished", stamped the run
# URL into its sticky comment, and left its own task list half unticked. No review was ever
# submitted - pulls/271/reviews was empty - and `claude-review` went green anyway.
#
# Cases 16-18 are the other half of that fix and matter just as much: the checker is handed every
# comment on the PR as one stream, so a rule that merely asks "does this stream contain an
# unticked box anywhere" fails runs that DID review. Case 16 is real - on astubbs#257, run
# 31177418588 posted a full 2376-character review AND left an earlier self-written tracker with
# unticked boxes behind, both citing that run. Case 17 is the same shape from the other direction:
# a `@claude` tracker posted by an unrelated run while this one is finishing.
#
# Case 19 is a bug the first draft of that segmentation had: it only noticed boundaries on
# reaching one, so the first comment in the stream was still segmented by the fallback rule.
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
### Reviewing PR astubbs#124
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
# fill the pipe buffer. Observed on astubbs#198, astubbs#199, astubbs#204 and
# astubbs#210; on astubbs#210 a 4.7 KB
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

# --- the astubbs#271 family: the reviewer says, in its own words, that it did not finish -------

# Must match COMMENT_BOUNDARY in check-review-posted.sh and the `gh api --jq` in
# .github/workflows/claude-code-review.yml. If the three drift apart, cases 16 and 17 stop
# segmenting and go red, which is what they are for.
BOUNDARY='<!-- check-review-posted: comment boundary -->'

# Verbatim from astubbs#271 (comment 5237812937), em-dashes and all. It is 832 characters of
# "finished" that finished nothing.
# shellcheck disable=SC2016  # the backticks are markdown in the bot's body, not substitutions
UNFINISHED_COMMENT='**Claude finished @astubbs'"'"'s task in 4m 10s** —— [View job](https://github.com/astubbs/parallel-consumer/actions/runs/31370656609)

---
### Claude is reviewing this PR <img src="https://github.com/user-attachments/assets/5ac382c7-e004-429b-8e35-7feb3e8f9c6f" width="14px" height="14px" style="vertical-align: middle; margin-left: 4px;" />

**Task list**
- [x] Gather context (PR diff, changed files)
- [x] Understand the request (code review via `/code-review`)
- [ ] Run code review across the diff (line-by-line scan, cross-file trace, removed-behavior audit, cleanup/efficiency scan) — in progress
- [ ] Verify findings and post results

This is a large PR (44 files, ~10.9k additions) adding a new `parallel-consumer-streams` module. Review is running in the background; this comment will be updated with findings once complete.'

FINISHED_COMMENT='**Claude finished @astubbs'"'"'s task in 4m 33s** —— [View job](https://github.com/astubbs/parallel-consumer/actions/runs/31370656609)

---
### Code review

**Task list**
- [x] Gather context (PR diff, changed files)
- [x] Run code review across the diff
- [x] Post findings

One non-blocking nit, otherwise this looks correct.'

# The shape claude.yml leaves behind when somebody asks for a review in a comment: unticked
# boxes, and no run URL at all until it finishes.
OTHER_RUN_TRACKER='### Claude is reviewing this PR

**Task list**
- [x] Gather context
- [ ] Run code review
- [ ] Post findings

Review in progress - this comment will be updated once complete.'

assert "reviewer's own task list still has unticked boxes" \
    1 "$(run_checker "$UNFINISHED_COMMENT" 31370656609 success)"

assert "task list present and fully ticked" \
    0 "$(run_checker "$FINISHED_COMMENT" 31370656609 success)"

assert "stale tracker and finished review, both from this run" \
    0 "$(run_checker "$UNFINISHED_COMMENT
$BOUNDARY
$FINISHED_COMMENT" 31370656609 success)"

assert "another run's tracker is still in progress, this run's review is not" \
    0 "$(run_checker "$FINISHED_COMMENT
$BOUNDARY
$OTHER_RUN_TRACKER" 31370656609 success)"

assert "no boundary markers, so segmentation falls back to run urls" \
    0 "$(run_checker "$UNFINISHED_COMMENT
$FINISHED_COMMENT" 31370656609 success)"

# Whether the stream carries boundaries has to be settled before the FIRST line is judged. A
# checker that only notices boundaries when it reaches one segments the first comment by the
# fallback rule, and this shape - the astubbs#257 tracker, where the run URL comes AFTER the
# boxes - then has its unticked boxes filed under the comment before it and passes.
# shellcheck disable=SC2016  # the backticks are markdown in the bot's body, not substitutions
TRAILING_URL_TRACKER='### Review in progress

- [x] Gather context (PR diff, description, prior review comments)
- [ ] Run `/code-review` on the current state of the PR
- [ ] Post findings

[View job run](https://github.com/astubbs/parallel-consumer/actions/runs/31370656609)'

assert "first comment in the stream is an unfinished tracker citing this run" \
    1 "$(run_checker "$TRAILING_URL_TRACKER
$BOUNDARY
$OTHER_BOT_COMMENTS" 31370656609 success)"

# "Does this stream carry boundaries" has to ask the same question the scan asks. A comment can
# MENTION the marker without being one - quoting this script, or reviewing the PR that adds it -
# and a substring test then reports boundaries that the scan will never find. Segmentation is
# switched off with nothing put in its place, the whole stream collapses into one comment, and a
# genuine review is failed by its own predecessor's unticked boxes. That is the control-arm
# behaviour this change exists to avoid, re-entered through the back door.
MARKER_MENTIONED_INLINE="Quoting the script: \`COMMENT_BOUNDARY='$BOUNDARY'\` is the marker it emits."

assert "a comment mentions the marker mid-line without being a boundary" \
    0 "$(run_checker "$UNFINISHED_COMMENT
$MARKER_MENTIONED_INLINE
$FINISHED_COMMENT" 31370656609 success)"

echo
if [ "$failures" -eq 0 ]; then
    echo "All bin/check-review-posted.sh self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
