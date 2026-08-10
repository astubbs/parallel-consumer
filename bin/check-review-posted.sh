#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Assert that an automated review actually landed on this PR.
#
# WHY THIS EXISTS
#
# `claude-code-action` exits 0 in several situations where it reviews nothing, so the
# `claude-review` check goes green having certified nothing - indistinguishable from
# "reviewed, no findings". This repo has been bitten three times:
#
#   1. The workflow ran green for months while never posting a single review, because the
#      plugin stops before commenting unless `--comment` is passed (see the note on the
#      `prompt:` key in .github/workflows/claude-code-review.yml).
#   2. PR astubbs#124 edited .github/workflows/claude-code-review.yml itself, which trips the
#      action's workflow-validation guard - it refuses to run unless the workflow file is
#      identical to the version on the default branch, so a PR cannot rewrite its own
#      reviewer. The action logged "Exiting due to workflow validation skip", exited 0, and
#      the PR sat mergeable with a green `claude-review` and no review of any kind.
#   3. The reviewer announced it would review in the background, and then the job ended. On
#      PR astubbs#271 the sticky comment's header read "Claude finished @astubbs's task in
#      4m 10s" while its own task list still had two boxes unticked and its last line said
#      "Review is running in the background; this comment will be updated with findings once
#      complete". pulls/271/reviews was empty. astubbs#266 has the same shape three times over.
#      The earlier version of this script passed all four, because `track_progress: true`
#      makes the action post its tracking comment - stamped with the run URL - at the START
#      of the run. Rule 1 below was therefore satisfied before any review was attempted, and
#      stayed satisfied whatever happened next.
#
# A green check has to mean "this PR was reviewed", not "the job finished".
#
# HOW
#
# Two rules, both of which the reviewer has to satisfy in its own output.
#
#   1. A comment cites THIS run. The action stamps its own run URL into every comment it
#      posts - "[View job](https://github.com/<owner>/<repo>/actions/runs/<id>)" - so a
#      comment citing this run id is proof that THIS run posted something. The id is matched
#      with a trailing non-digit boundary: a bare substring search for "actions/runs/3096"
#      would happily match run 30965089954 and pass on someone else's review.
#   2. That comment has no unticked task-list box left in it. The action writes itself a task
#      list and ticks the boxes as it goes, so an unticked `- [ ]` is the reviewer's own
#      statement that it did not get to the end. This is not us grading the review; it is us
#      declining to believe a reviewer that says it is not finished.
#
# Comment bodies arrive on stdin, which keeps the network call in the workflow and leaves
# this script testable offline.
#
# WHY THE UNTICKED BOX AND NOT THE WORDING
#
# The same runs also say "Review in progress", "Code review in progress", "Review is running
# in the background", "Will update once complete" and "this comment will be updated once
# complete" - five phrasings across five samples, all of them upstream's to change without
# notice. The checkbox is structure rather than prose, it survives rewording, and every stale
# comment observed - on astubbs#257, astubbs#262, astubbs#266, astubbs#269 and astubbs#271 -
# carried one. Matching the prose as well would add nothing those runs were not already caught
# by, and would newly fail a COMPLETED review that happens to quote the phrase.
#
# Length is deliberately not a rule either. The stalled comments run 462-832 characters and
# real reviews run 2164-8443, which looks separable until you notice a genuine finished
# comment on astubbs#269 at 175 characters. A floor there is a guess about how much a
# reviewer ought to write, and "no findings" is a legitimate short answer.
#
# WHY THE STREAM IS SEGMENTED FIRST
#
# stdin is every comment on the PR, concatenated. Asking "does this stream contain an
# unticked box anywhere" would fail runs that DID review: on astubbs#257 run 31177418588
# posted a full 2376-character review AND left an earlier self-written tracker with unticked
# boxes behind, both citing that run, and a `@claude` request during a review leaves a
# tracker from a different run sitting in the same stream. So the stream is split into
# comments and the rules are applied per comment: the check passes when SOME comment citing
# this run is free of unticked boxes.
#
# The workflow supplies the split, emitting COMMENT_BOUNDARY (below) between bodies. When no
# boundary appears in the stream at all - a hand-run pipe, or a workflow not yet carrying the
# marker - the script falls back to treating each line that cites any run URL as the start of
# a new comment. That is an approximation, not a second opinion: it gets every case in the
# self-test right, but it cannot tell that a trailing tracker citing no run at all belongs to
# somebody else. Keep the three copies of the marker in step - here, in the workflow's
# `gh api --jq`, and in bin/test-check-review-posted.sh.
#
# LIMIT, STATED PLAINLY
#
# This proves the reviewer posted something from this run and called itself done. It does not
# read the review, and it never will - a check that judges review quality is a check nobody
# can keep honest. A run that posts a fully ticked task list over an empty review still
# passes, and the answer to that is the reviewer's prompt, not this script. Note too that
# both rules are satisfiable by anyone who can comment on the PR: this is a guard against the
# action failing quietly, not against a hostile author.
#
# WHEN IT FAILS ON A WORKFLOW-EDITING PR
#
# That is the guard working, not a false alarm: the action genuinely cannot review a PR that
# modifies its own workflow file. Get a real review by asking for one in a PR comment
# (`@claude review this`), which runs via .github/workflows/claude.yml - a different file,
# unmodified, so validation passes - or split the workflow edit into its own PR.
#
# Usage: gh api repos/OWNER/REPO/issues/N/comments --paginate \
#            --jq '.[] | .body, "<!-- check-review-posted: comment boundary -->"' \
#            | bin/check-review-posted.sh <run-id> <review-step-outcome>
#
# Exit: 0 a review from this run was posted
#       1 no review (or the review step itself did not succeed)
#       2 bad usage

set -euo pipefail

# Emitted by the workflow between comment bodies so this script can tell them apart. Kept as a
# markdown-invisible HTML comment: if it ever ends up rendered, it renders as nothing.
COMMENT_BOUNDARY='<!-- check-review-posted: comment boundary -->'

fail() { # <message>
    if [ -n "${GITHUB_ACTIONS:-}" ]; then
        echo "::error::$1"
    else
        echo "ERROR: $1" >&2
    fi
}

if [ "$#" -ne 2 ]; then
    echo "usage: $(basename "$0") <run-id> <review-step-outcome>   (comment bodies on stdin)" >&2
    exit 2
fi

run_id=$1
review_outcome=$2

case "$run_id" in
    '' | *[!0-9]*)
        echo "usage: run-id must be a number, got '$run_id'" >&2
        exit 2
        ;;
esac

if [ -z "$review_outcome" ]; then
    echo "usage: review-step-outcome must not be empty" >&2
    exit 2
fi

comment_bodies=$(cat)

# A crashed, timed-out or skipped review step is not a review, whatever it left behind.
if [ "$review_outcome" != "success" ]; then
    fail "The review step did not succeed (outcome: '$review_outcome'), so this PR has not been reviewed. Refusing to report a green review check for a job that reviewed nothing."
    exit 1
fi

# Split the stream into comments and judge each one, because the two rules are about a single
# comment and stdin holds all of them (see "WHY THE STREAM IS SEGMENTED FIRST" above).
#
# Which split to use is settled here, before a single line is judged. Deciding it lazily - on
# reaching the first boundary - would segment the FIRST comment by the fallback rule, so a
# tracker that puts its run URL after its task list would have its unticked boxes filed under
# whatever came before it, and pass. That is exactly the shape astubbs#257 posted.
have_boundary=0
if grep -qF "$COMMENT_BOUNDARY" <<<"$comment_bodies"; then
    have_boundary=1
fi

#
# Herestring, not `printf | awk`: an early-exiting reader leaves the writer with EPIPE, and
# `pipefail` (above) promotes 141 to the pipeline's status - so FINDING the review would fail
# the check. That is not hypothetical, it shipped here once and misreported four PRs; it bites
# once >64 KiB of comments follow the match. awk reads to EOF, but the herestring is what makes
# that irrelevant rather than merely lucky.
#
# awk's verdicts: 0 a complete comment from this run, 3 only incomplete ones, 4 none at all.
scan_verdict=0
awk -v boundary="$COMMENT_BOUNDARY" -v have_boundary="$have_boundary" -v run_id="$run_id" '
    function settle() {
        if (cites) {
            cited = 1
            if (!unticked) complete = 1
        }
        cites = 0
        unticked = 0
    }
    BEGIN {
        # Anchored on a trailing non-digit so run 3096 does not match run 30965089954.
        cite_re = "actions/runs/" run_id "([^0-9]|$)"
        any_run_re = "actions/runs/[0-9]"
    }
    {
        if ($0 == boundary) { settle(); next }
        # Fallback segmentation, for a stream that carries no boundaries at all: a line citing
        # any run URL opens a new comment, and belongs to it rather than to the one before.
        if (have_boundary == 0 && $0 ~ any_run_re) settle()
        if ($0 ~ cite_re) cites = 1
        # A GitHub task list is `- [ ]` / `* [ ]` / `+ [ ]`, optionally indented. Ticked boxes
        # hold an x, so only a whitespace-filled box means "not done".
        if ($0 ~ /^[ \t]*[-*+][ \t]+\[[ \t]\]([ \t]|$)/) unticked = 1
    }
    END {
        settle()
        if (complete) exit 0
        if (cited) exit 3
        exit 4
    }
' <<<"$comment_bodies" || scan_verdict=$?

case "$scan_verdict" in
    0)
        echo "Review posted by run ${run_id}."
        exit 0
        ;;
    3)
        fail "The review step reported success, but the comment it posted for run ${run_id} still has unticked task-list boxes - the reviewer's own record that it stopped partway. With track_progress enabled the action posts that comment at the START of the run, so its mere existence proves nothing; a finished review ticks every box. Seen on astubbs#271, where the comment said the review was 'running in the background' and no review was ever submitted. Re-run the review, or ask for one in a PR comment ('@claude review this'). See bin/check-review-posted.sh for the full rationale."
        exit 1
        ;;
    4)
        fail "The review step reported success but posted no review: no comment on this PR cites run ${run_id}. The most likely cause is the action's workflow-validation guard - it skips itself (and still exits 0) when .github/workflows/claude-code-review.yml differs from the default branch, which is every PR that edits the reviewer. Ask for a review in a PR comment ('@claude review this'), which runs from an unmodified workflow file, or split the workflow edit into its own PR. See bin/check-review-posted.sh for the full rationale."
        exit 1
        ;;
    *)
        fail "Could not scan the PR comments for run ${run_id}: the comment scan exited ${scan_verdict}. Treating that as unreviewed rather than guessing."
        exit 1
        ;;
esac
