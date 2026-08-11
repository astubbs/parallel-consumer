#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Assert that this PR HAS HAD a deliberate automated review.
#
# WHAT THIS PROVES, AND WHAT IT DELIBERATELY DOES NOT
#
# Two rules, both about a single comment:
#
#   1. IDENTITY   - a comment authored by the reviewer bot exists on this PR. The author comes
#                   from the GitHub API (the `.user.login` field), not from anything inside the
#                   comment body, so no amount of writing in a comment makes it look like the
#                   bot.
#   2. COMPLETION - that comment has no unticked task-list box left in it. The action writes
#                   itself a task list and ticks the boxes as it goes, so an unticked `- [ ]`
#                   is the reviewer's own record that it stopped partway. We are not grading
#                   the review; we are declining to believe a reviewer that says it is not
#                   finished. Prior art and the samples behind it:
#                   astubbs/parallel-consumer#275, which found stale trackers on
#                   astubbs/parallel-consumer#257, astubbs/parallel-consumer#262,
#                   astubbs/parallel-consumer#266, astubbs/parallel-consumer#269 and
#                   astubbs/parallel-consumer#271.
#
# It does NOT prove the review covered the current head. That is a deliberate reversal - see
# "WHY THE FRESHNESS RULE WAS REMOVED" below - and it means the check answers "has this PR had
# a deliberate Claude review?", not "was every commit reviewed?".
#
# WHY IT EXISTS AT ALL
#
# `claude-code-action` exits 0 in several situations where it reviews nothing, so a check
# wired straight to it goes green having certified nothing - indistinguishable from
# "reviewed, no findings". This repo has been bitten three times:
#
#   1. The workflow ran green for months while never posting a single review, because the
#      review plugin stops before commenting unless `--comment` is passed.
#   2. astubbs/parallel-consumer#124 edited .github/workflows/claude-code-review.yml itself,
#      which tripped the action's workflow-validation guard - it refuses to run unless the
#      workflow file matches the default branch. It logged "Exiting due to workflow validation
#      skip", exited 0, and the PR sat mergeable with a green `claude-review` and no review of
#      any kind.
#   3. The reviewer announced it would review in the background, and then the job ended -
#      header claiming "Claude finished ... in 4m 10s" over a task list with unticked boxes.
#
# Now that the reviewer is on demand, a FOURTH way to certify nothing appears, and it is the
# one this gate is really about: nobody asks for a review at all. Rule 1 is what stands in for
# the old always-on trigger; rule 3 above is why rule 2 exists.
#
# WHY THE FRESHNESS RULE WAS REMOVED
#
# There used to be a third rule: the review had to be newer than the current head, so a push
# after a review turned the check red and wanted a fresh one. It is gone, and the reasoning is
# worth keeping, because "a review of commit N does not vouch for commit N+1" is TRUE and will
# be re-proposed by anyone who reads only that sentence.
#
# It was not abandoned because it was wrong. It was abandoned because of what ENFORCING it
# cost, and because the coverage it was protecting turned out to come from somewhere else:
#
#   * PER-PUSH COVERAGE ALREADY EXISTS. A separate reviewer auto-reviews every push. Strictness
#     here was buying a guarantee that was already being provided, at a price paid on every PR.
#   * IT NEEDED A WRITE TOKEN NEXT TO THE REVIEWER. The check is produced by a `pull_request`
#     workflow, so a review posted after the last push could not clear it without something
#     re-running the gate - which meant a job holding `actions: write`, on both review routes.
#     That token can dispatch any workflow in the repo, `release.yml` included. Strictness was
#     the only thing that made the freshness comparison worth having, and the freshness
#     comparison was a large part of what those jobs existed to serve.
#   * IT NEEDED CONTESTABLE TIMESTAMPS. Deciding "newer than the head" meant choosing between
#     the contributor-controlled committer date and the server-side check-suite time, handling
#     same-second ties, and paginating an endpoint with undocumented ordering - three rounds of
#     review findings, all of them about the comparison rather than about reviewing.
#
# So the trade was: a guarantee already covered elsewhere, against a standing privilege
# escalation surface and a page of timestamp plumbing. The gate now means "somebody deliberately
# asked for a review of this PR and it finished", which is the thing it was actually needed for.
#
# What did NOT change: there is still no skip word, label, or "trivial change" escape. Any such
# escape is asserted by the same person who wants to use it, which makes it exactly as strong as
# not having the gate. The honest escape already exists and is loud: merge with the required
# check red, which leaves a permanent record that somebody chose to merge unreviewed.
#
# A red `claude-review` on a PR nobody has asked to review yet remains the EXPECTED state, not a
# fault, and not something to fix by editing this script. It is fixed by asking for a review.
# See docs/ci.md.
#
# HOW THE STREAM IS SEGMENTED
#
# Both rules are about a SINGLE comment - a stale tracker with unticked boxes sitting next to a
# real review must not lend it its boxes. So the workflow hands over one marker line per
# comment carrying that comment's created-at and author, and everything up to the next marker
# is that comment's body:
#
#     <!-- check-review-posted <token> <created-at> <login> -->
#
# The token is an UNGUESSABLE random value generated by the workflow for each run, and that
# property is load-bearing rather than incidental. A comment whose body carries a valid marker
# opens a comment of its own choosing in the stream - with an author field of its own choosing -
# so anyone who can predict the token can have their own comment counted as a finished review by
# the bot. It was the run id at first, which is published the instant the run starts and so
# guaranteed exactly nothing against a deliberate forgery; it now comes from `openssl rand`,
# generated inside the step so it never reaches the public run log.
#
# The weaker property still matters too: a comment innocently QUOTING this marker - this file's
# own documentation, a doc describing the protocol, a review of the PR that introduced it -
# cannot open a comment either.
#
# The created-at field is still carried, and is still used - for the human-readable message
# only, never for a verdict. Keep the marker's shape in step in three places: here, the `--jq`
# in .github/workflows/claude-code-review.yml, and bin/test-check-review-posted.sh.
#
# WHAT THIS SCRIPT STILL DOES NOT PROVE
#
# That the review was any good, or that it looked at anything in particular. It reads comment
# metadata and task-list boxes. A reviewer that posts a finished, empty review satisfies it.
# That boundary is unchanged by the freshness removal and is described in docs/ci.md.

set -euo pipefail

REVIEWER_LOGIN='claude[bot]'

fail() { # <message>
    if [ -n "${GITHUB_ACTIONS:-}" ]; then
        echo "::error::$1"
    else
        echo "ERROR: $1" >&2
    fi
}

if [ "$#" -ne 1 ]; then
    echo "usage: $(basename "$0") <marker-token>" >&2
    echo "       (marked-up comment stream on stdin)" >&2
    exit 2
fi

marker_token=$1

case "$marker_token" in
    '' | *[![:alnum:]_-]*)
        echo "usage: marker-token must be non-empty and alphanumeric, got '$marker_token'" >&2
        exit 2
        ;;
esac

comment_stream=$(cat)

verdict=0
latest_review_at=$(awk \
    -v token="$marker_token" \
    -v reviewer="$REVIEWER_LOGIN" '
    function settle() {
        if (open && login == reviewer) {
            saw_reviewer = 1
            if (!unticked) complete = 1
            if (created > latest) latest = created
        }
        open = 0; unticked = 0; fenced = 0; fence_ch = ""; fence_len = 0; created = ""; login = ""
    }
    NF == 6 && $1 == "<!--" && $2 == "check-review-posted" && $3 == token && $6 == "-->" {
        settle(); open = 1; created = $4; login = $5; next
    }
    open && match($0, /^[ \t]*(`{3,}|~{3,})/) {
        fence = substr($0, RSTART, RLENGTH)
        sub(/^[ \t]+/, "", fence)
        ch = substr(fence, 1, 1)
        len = length(fence)
        after = substr($0, RSTART + RLENGTH)
        if (!fenced) { fenced = 1; fence_ch = ch; fence_len = len }
        else if (ch == fence_ch && len >= fence_len && after ~ /^[ \t]*$/) { fenced = 0 }
        next
    }
    open && !fenced && /^[ \t]*[-*+][ \t]+\[[ \t]\]([ \t]|$)/ { unticked = 1 }
    END {
        settle()
        print latest
        if (complete) exit 0
        if (saw_reviewer) exit 3
        exit 5
    }
' <<<"$comment_stream") || verdict=$?

case "$verdict" in
    0)
        echo "Reviewed: ${REVIEWER_LOGIN} posted a finished review on this PR (latest at ${latest_review_at})."
        exit 0
        ;;
    3)
        fail "The reviewer commented on this PR at ${latest_review_at} but left unticked task-list boxes in every comment - its own record that it stopped partway, so this PR has not actually been reviewed. Ask for another review (see docs/ci.md). See bin/check-review-posted.sh."
        exit 1
        ;;
    5)
        fail "This PR has never been reviewed: ${REVIEWER_LOGIN} has not commented on it. THIS IS THE EXPECTED STATE for a new or in-progress PR - the review does not run automatically on push, precisely so that work in progress does not spend one. Ask for a review when the PR is ready - see docs/ci.md."
        exit 1
        ;;
    *)
        fail "Could not scan the PR comments for a review: the comment scan exited ${verdict}. Treating that as unreviewed rather than guessing."
        exit 1
        ;;
esac
