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
# "reviewed, no findings". This repo has been bitten twice:
#
#   1. The workflow ran green for months while never posting a single review, because the
#      plugin stops before commenting unless `--comment` is passed (see the note on the
#      `prompt:` key in .github/workflows/claude-code-review.yml).
#   2. PR #124 edited .github/workflows/claude-code-review.yml itself, which trips the
#      action's workflow-validation guard - it refuses to run unless the workflow file is
#      identical to the version on the default branch, so a PR cannot rewrite its own
#      reviewer. The action logged "Exiting due to workflow validation skip", exited 0, and
#      the PR sat mergeable with a green `claude-review` and no review of any kind.
#
# A green check has to mean "this PR was reviewed", not "the job finished".
#
# HOW
#
# The action stamps its own run URL into every comment it posts - "[View job](https://
# github.com/<owner>/<repo>/actions/runs/<id>)" - so a comment citing THIS run id is proof
# that THIS run posted something. Comment bodies arrive on stdin, which keeps the network
# call in the workflow and leaves this script testable offline.
#
# The run id is matched with a trailing non-digit boundary: a bare substring search for
# "actions/runs/3096" would happily match run 30965089954 and pass on someone else's review.
#
# LIMIT, STATED PLAINLY
#
# This proves a comment from this run exists, not that its contents are a good review. An
# action that posts its progress comment and then produces an empty review would still pass.
# It closes the failure mode that has actually bitten this repo - the silent no-op that
# reports success - and does not pretend to judge review quality.
#
# WHEN IT FAILS ON A WORKFLOW-EDITING PR
#
# That is the guard working, not a false alarm: the action genuinely cannot review a PR that
# modifies its own workflow file. Get a real review by asking for one in a PR comment
# (`@claude review this`), which runs via .github/workflows/claude.yml - a different file,
# unmodified, so validation passes - or split the workflow edit into its own PR.
#
# Usage: gh api repos/OWNER/REPO/issues/N/comments --paginate --jq '.[].body' \
#            | bin/check-review-posted.sh <run-id> <review-step-outcome>
#
# Exit: 0 a review from this run was posted
#       1 no review (or the review step itself did not succeed)
#       2 bad usage

set -euo pipefail

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

if printf '%s\n' "$comment_bodies" | grep -qE "actions/runs/${run_id}([^0-9]|$)"; then
    echo "Review posted by run ${run_id}."
    exit 0
fi

fail "The review step reported success but posted no review: no comment on this PR cites run ${run_id}. The most likely cause is the action's workflow-validation guard - it skips itself (and still exits 0) when .github/workflows/claude-code-review.yml differs from the default branch, which is every PR that edits the reviewer. Ask for a review in a PR comment ('@claude review this'), which runs from an unmodified workflow file, or split the workflow edit into its own PR. See bin/check-review-posted.sh for the full rationale."
exit 1
