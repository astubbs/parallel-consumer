#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Assert that an automated review exists FOR THE CURRENT HEAD of this PR.
#
# WHAT THIS NOW PROVES (it changed - read this before trusting an old memory of it)
#
# It used to prove "the reviewer step in THIS workflow run posted something", because the
# reviewer and the gate were two steps of one job that fired on every push. The reviewer no
# longer runs on push - it is dispatched by hand from
# .github/workflows/claude-code-review-dispatch.yml - so there is no sibling step in this run
# whose id to match.
#
# What it proves instead, and each rule's job:
#
#   1. IDENTITY   - a comment authored by the reviewer bot exists. The author comes from the
#                   GitHub API (the `.user.login` field), not from anything inside the comment
#                   body, so no amount of writing in a comment makes it look like the bot.
#   2. FRESHNESS  - that comment was created strictly after the current head appeared, so the
#                   reviewer cannot have been looking at older code. This is the rule that
#                   makes the gate mean "this PR was reviewed" rather than "this PR was
#                   reviewed once, a while ago, before the last four commits".
#   3. COMPLETION - that comment has no unticked task-list box left in it. The action writes
#                   itself a task list and ticks the boxes as it goes, so an unticked `- [ ]`
#                   is the reviewer's own record that it stopped partway. We are not grading
#                   the review; we are declining to believe a reviewer that says it is not
#                   finished. Prior art and the samples behind it:
#                   astubbs/parallel-consumer#275, which found stale trackers on
#                   astubbs/parallel-consumer#257, astubbs/parallel-consumer#262,
#                   astubbs/parallel-consumer#266, astubbs/parallel-consumer#269 and
#                   astubbs/parallel-consumer#271.
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
#      workflow file matches the default branch, so a PR could not rewrite its own reviewer.
#      It logged "Exiting due to workflow validation skip", exited 0, and the PR sat mergeable
#      with a green `claude-review` and no review of any kind.
#   3. The reviewer announced it would review in the background, and then the job ended -
#      header claiming "Claude finished ... in 4m 10s" over a task list with unticked boxes.
#
# Now that the reviewer is on demand, a FOURTH way to certify nothing appears, and it is the
# one this rewrite is really about: nobody asks for a review at all. Rules 1 and 2 are what
# stand in for the old always-on trigger.
#
# STRICT, DELIBERATELY - AND WHAT A RED CHECK MEANS
#
# Rule 2 is the strict reading: a review of commit N does not vouch for commit N+1, so a push
# after a review turns the check red again and wants a fresh review dispatch. The
# lenient reading - any review on the PR, ever, satisfies the gate - was rejected because it
# reports success for code nothing has looked at, which is the exact defect class named in
# docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md. A gate
# built to stand in for a missing always-on reviewer must not itself be satisfiable by a
# review of code that no longer exists.
#
# So: **a red `claude-review` on a PR nobody has asked to review yet is the expected state,
# not a fault.** It is not something to fix by editing this script. It is fixed by dispatching
# a review once the PR is actually ready. See docs/ci.md.
#
# There is deliberately NO skip word, label, or "trivial change" escape. Any such escape is
# asserted by the same person who wants to use it, which makes it exactly as strong as not
# having the gate; and the case it would serve - a one-line fix after a review - is a
# two-minute re-review, so it saves little and costs the guarantee. The honest escape already
# exists and is loud: merge with the required check red, which leaves a permanent record that
# somebody chose to merge unreviewed. A silent escape would leave none.
#
# HOW THE STREAM IS SEGMENTED
#
# Every rule is about a SINGLE comment - a stale tracker with unticked boxes sitting next to a
# real review must not lend it its boxes, and a fresh comment from a human must not lend the
# bot its timestamp. So the workflow hands over one marker line per comment carrying that
# comment's created-at and author, and everything up to the next marker is that comment's
# body:
#
#     <!-- check-review-posted <token> <created-at> <login> -->
#
# The token is an UNGUESSABLE random value generated by the workflow for each run, and that
# property is load-bearing rather than incidental. A comment whose body carries a valid marker
# opens a comment of its own choosing in the stream - with an author field and a timestamp of
# its own choosing - so anyone who can predict the token can have their own comment counted as
# a finished review by the bot. It was the run id at first, which is published the instant the
# run starts and so guaranteed exactly nothing against a deliberate forgery; it now comes from
# `openssl rand`, generated inside the step so it never reaches the public run log.
#
# The weaker property still matters too: a comment innocently QUOTING this marker - this file's
# own documentation, a doc describing the protocol, a review of the PR that introduced it -
# cannot open a comment either.
#
# Keep the marker's shape in step in three places: here, the `--jq` in
# .github/workflows/claude-code-review.yml, and bin/test-check-review-posted.sh.
#
# WHICH TIMESTAMP COUNTS AS "THE HEAD APPEARED"
#
# Two are passed in, and the SERVER-SIDE one wins whenever it is present:
#
#   * the creation time of the first check suite GitHub raised for that SHA - server-side, and
#     effectively when GitHub first saw the commit. PREFERRED.
#   * the head commit's committer date - a FALLBACK, used only when the check-suite time is
#     absent (a brand new SHA, or an API that has not caught up).
#
# The server-side time is preferred rather than max()'d with the committer date, because the
# committer date is written by the author's own clock and is therefore contributor-controlled:
# under max(), a commit dated in the future would outrank every real review timestamp and hold
# the required check red until that date arrived, with no way to clear it. A stuck-red gate
# that no review can satisfy is worse than the window preferring the server time gives up -
# and it gives up almost nothing, because the check-suite time IS effectively the push time,
# so it already closes the "commit, then request a review, then push" window that the
# committer date was carried for.
#
# EQUALITY IS TREATED AS STALE. These timestamps are whole seconds, so a review created in the
# same second that a newer head arrived is genuinely ambiguous. The comparison is therefore
# strictly greater, not >=: the gate has to fail closed, so ambiguity resolves to "not
# reviewed". The cost is a rare extra review request; the benefit is that no false-green path
# survives on a one-second tie.
#
# LIMIT, STATED PLAINLY
#
# This proves the reviewer bot posted a finished comment after the current head appeared. It
# does not read the review, and it never will - a check that judges review quality is a check
# nobody can keep honest. Three specific things it does NOT do:
#
#   1. It cannot tell a review from any other answer the bot gave on the PR: `@claude what
#      does this file do?` produces a comment that satisfies all three rules and turns the
#      check green. Documented in docs/ci.md too, so nobody reads a green check as more than
#      it is.
#   2. The check-suite time is global to the SHA, not scoped to this PR's head transition. A
#      force-push onto a commit that already ran checks elsewhere carries that older
#      timestamp, so a review of a previous head can postdate it and pass. Narrow - it needs a
#      force-push to a previously-tested SHA - but real.
#   3. It runs from the PR's own checkout, like every other `pull_request` check in this repo,
#      so it polices a tree that can edit it.
#
# The remedy for 1 and 2 is the same and is recorded in docs/inflight/ci-review-agent.md: have
# the reviewer record the exact SHA it reviewed, as a check run on that head, and gate on that
# instead of on a timestamp. It is blocked until the dispatch route is proven after merge.
#
# All three are the same boundary the previous version drew, for the same reason - this is a
# guard against the action failing quietly, not against somebody who wants to get around it.
#
# Usage: gh api repos/OWNER/REPO/issues/N/comments --paginate \
#            --jq '.[] | "<!-- check-review-posted \(env.MARKER_TOKEN) \(.created_at) \(.user.login) -->", (.body // "")' \
#          | bin/check-review-posted.sh <marker-token> <head-sha> <head-committed-at> <head-first-seen-at>
#
#        <head-first-seen-at> may be empty; the other three may not.
#
# Exit: 0 a finished review covering the current head exists
#       1 no such review (never reviewed / only stale reviews / reviewer did not finish)
#       2 bad usage

set -euo pipefail

# The account the reviewer posts as. Its comments are identified by the API's author field,
# never by their contents.
REVIEWER_LOGIN='claude[bot]'

fail() { # <message>
    if [ -n "${GITHUB_ACTIONS:-}" ]; then
        echo "::error::$1"
    else
        echo "ERROR: $1" >&2
    fi
}

if [ "$#" -ne 4 ]; then
    echo "usage: $(basename "$0") <marker-token> <head-sha> <head-committed-at> <head-first-seen-at>" >&2
    echo "       (marked-up comment stream on stdin; <head-first-seen-at> may be empty)" >&2
    exit 2
fi

marker_token=$1
head_sha=$2
head_committed_at=$3
head_first_seen_at=$4

# The token becomes an awk field comparison, so a value with whitespace in it would silently
# never match any marker and collapse every comment into one.
case "$marker_token" in
    '' | *[![:alnum:]_-]*)
        echo "usage: marker-token must be non-empty and alphanumeric, got '$marker_token'" >&2
        exit 2
        ;;
esac

case "$head_sha" in
    *[![:xdigit:]]* | '')
        echo "usage: head-sha must be a hex commit id, got '$head_sha'" >&2
        exit 2
        ;;
esac

# Timestamps are compared as strings, which is only valid for a fixed-width UTC format - the
# one the GitHub API returns. Anything else is rejected rather than mis-compared silently.
iso_re='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'

if ! [[ $head_committed_at =~ $iso_re ]]; then
    echo "usage: head-committed-at must be an ISO-8601 UTC timestamp (YYYY-MM-DDTHH:MM:SSZ), got '$head_committed_at'" >&2
    exit 2
fi

if [ -n "$head_first_seen_at" ] && ! [[ $head_first_seen_at =~ $iso_re ]]; then
    echo "usage: head-first-seen-at must be empty or an ISO-8601 UTC timestamp, got '$head_first_seen_at'" >&2
    exit 2
fi

# Server-side time when we have one, the commit's own date only as a fallback. NOT max() of
# the two - see "WHICH TIMESTAMP COUNTS" above: the committer date is contributor-controlled,
# so max() lets a future-dated commit hold the check red forever.
head_time=$head_first_seen_at
if [ -z "$head_time" ]; then
    head_time=$head_committed_at
fi

comment_stream=$(cat)

# Herestring, not `printf | awk`: an early-exiting reader leaves the writer with EPIPE, and
# `pipefail` (above) promotes 141 to the pipeline's status - so FINDING the review would fail
# the check. That is not hypothetical, it shipped here once and misreported four PRs, and it
# only bites once >64 KiB of comments follow the match. bin/check-shell-sigpipe.sh enforces
# the rule across this directory.
#
# awk prints the newest reviewer comment's timestamp (empty if there is none) and reports its
# verdict as an exit code:
#   0 a finished reviewer comment strictly after head_time
#   3 a reviewer comment strictly after head_time, but it never ticked its own boxes
#   4 reviewer comments exist, none of them after head_time
#   5 the reviewer has never commented on this PR
verdict=0
latest_review_at=$(awk \
    -v token="$marker_token" \
    -v reviewer="$REVIEWER_LOGIN" \
    -v head_time="$head_time" '
    function settle() {
        if (open && login == reviewer) {
            saw_reviewer = 1
            # Strictly after, never equal: whole-second timestamps make a tie ambiguous, and
            # an ambiguous gate has to fail closed. See "EQUALITY IS TREATED AS STALE".
            if (created > head_time) {
                saw_fresh = 1
                if (!unticked) complete = 1
            }
            if (created > latest) latest = created
        }
        open = 0; unticked = 0; fenced = 0; fence_ch = ""; fence_len = 0; created = ""; login = ""
    }
    # A marker line, and only a marker line, starts a new comment. The token check is what
    # stops a comment that quotes the marker from opening one - see "HOW THE STREAM IS
    # SEGMENTED" in the header.
    NF == 6 && $1 == "<!--" && $2 == "check-review-posted" && $3 == token && $6 == "-->" {
        settle(); open = 1; created = $4; login = $5; next
    }
    # Track fenced code blocks, so nothing inside one is read as reviewer state.
    #
    # CommonMark gives a CLOSING fence three requirements, and all three are implemented here
    # rather than only the one that happened to bite. Two review rounds arrived at this block one
    # requirement at a time - first "same character and long enough", then "no info string" - so
    # the whole rule is written out to stop a third round finding the remainder. A closing fence
    # must:
    #   1. use the SAME character as its opener (backticks do not close tildes),
    #   2. be AT LEAST AS LONG as its opener, and
    #   3. be followed by nothing but whitespace - a fence carrying an info string is CONTENT.
    #
    # Each exists because dropping it fails the same way: a fenced example is closed early, a box
    # the review was only DISPLAYING comes back into scope, and `claude-review` goes red on a
    # completed review - permanently, because a posted comment never changes. Requirement 2 is a
    # four-backtick wrapper around a three-backtick example; requirement 3 is a ```markdown block
    # containing a displayed ```text line. Both are precisely the comment shape a review
    # DISCUSSING this rule produces, which is why they keep turning up here.
    #
    # Indentation is deliberately left lenient (`[ \t]*`, not CommonMark`s 0-3 spaces). Modelling
    # that properly needs the list-item context this line-wise scanner does not have, and erring
    # lenient only ever means treating an oddly indented fence AS a fence, which keeps content out
    # of scope. The strict reading could pull a displayed box back in - the direction that breaks
    # the gate.
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
    # A GitHub task list is `- [ ]` / `* [ ]` / `+ [ ]`, optionally indented. A ticked box
    # holds an x, so only a whitespace-filled box means "not done".
    #
    # Only an unticked box the reviewer wrote as ITS OWN state counts. One inside a fenced
    # code block is being DISPLAYED - an example, or feedback quoted back - not a progress
    # tracker. Counting those was a live hazard, not a theoretical one: a review of the PR
    # introducing this rule would very likely show an unticked box while discussing it, and
    # since a posted comment never changes, the check would then stay red with no way at all
    # to clear it.
    #
    # A `>`-quoted box needs no separate guard: the pattern anchors on the list bullet, so a
    # line starting with `>` cannot match it in the first place. Adding one would have been a
    # rule no test could ever make fail.
    open && !fenced && /^[ \t]*[-*+][ \t]+\[[ \t]\]([ \t]|$)/ { unticked = 1 }
    END {
        settle()
        print latest
        if (complete) exit 0
        if (saw_fresh) exit 3
        if (saw_reviewer) exit 4
        exit 5
    }
' <<<"$comment_stream") || verdict=$?

short_sha=${head_sha:0:8}

case "$verdict" in
    0)
        echo "Reviewed: ${REVIEWER_LOGIN} posted a finished review at ${latest_review_at}, after head ${short_sha} appeared (${head_time})."
        exit 0
        ;;
    3)
        fail "The reviewer commented on head ${short_sha} at ${latest_review_at} but left unticked task-list boxes - its own record that it stopped partway, so this head has not actually been reviewed. Dispatch another review (see docs/ci.md). See bin/check-review-posted.sh."
        exit 1
        ;;
    4)
        fail "This PR has been reviewed, but not since ${latest_review_at}, and head ${short_sha} is at least as new as that (${head_time}). A review of an earlier commit does not vouch for the commits after it. THIS IS THE EXPECTED STATE for a PR that has been pushed to since its last review, and it is not a defect to fix in CI: dispatch a review when the PR is ready for another look - see docs/ci.md."
        exit 1
        ;;
    5)
        fail "This PR has never been reviewed: ${REVIEWER_LOGIN} has not commented on it. THIS IS THE EXPECTED STATE for a new or in-progress PR - the review no longer runs automatically on push, precisely so that work in progress does not spend one. Dispatch a review when the PR is ready for review - see docs/ci.md."
        exit 1
        ;;
    *)
        fail "Could not scan the PR comments for a review of head ${short_sha}: the comment scan exited ${verdict}. Treating that as unreviewed rather than guessing."
        exit 1
        ;;
esac
