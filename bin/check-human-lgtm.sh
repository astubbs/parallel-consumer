#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Assert that the repository owner has stamped this PR with a human LGTM.
#
# The sibling half of bin/check-review-posted.sh. That one asks "has a machine reviewed this
# PR?"; this one asks "has the human who owns the repo said, in his own words, that he is happy
# for it to land?". Both run in the review gate
# (.github/workflows/claude-code-review.yml) and both must pass, and their failures are worded
# so you can tell at a glance WHICH half is missing without opening the job.
#
# WHY IT EXISTS
#
# Not to stop anybody. The owner can merge with a required check red at any time, and this
# check asserts a thing only he can assert, about himself - so as a security control it is
# worth nothing, and pretending otherwise would be the self-asserted escape docs/ci.md
# rejects by name. It is a MEMORY AID, and it is honest about that:
#
#   "I always need to do a review myself and add a LGTM ... sometimes I forget or get confused.
#    It would be much easier if there was just an action that highlighted that."
#
# The distinction that makes it worth having, against docs/ci.md's rule that a self-asserted
# escape is exactly as strong as no gate at all: an ESCAPE removes an obligation, and is
# therefore worthless when the person asserting it is the person who wants the merge. A
# self-asserted REQUIREMENT adds one. It can only ever make the gate redder, never greener, so
# the argument against self-assertion does not reach it. What it buys is visibility - a red
# check that names the missing half, instead of a human trying to remember across a dozen open
# PRs which ones he has personally read.
#
# THE MATCHING RULE, IN FULL
#
# A PR satisfies this check when at least one SUBMITTED PULL-REQUEST REVIEW exists such that:
#
#   1. SOURCE     - it came from `repos/<repo>/pulls/<n>/reviews`. A review, not an issue
#                   comment: "LGTM" typed into the ordinary comment box does NOT count, and
#                   neither does an inline comment on a line of code (those are review
#                   *thread* comments and are not in this endpoint's bodies). The stamp is the
#                   review summary, which is the deliberate, ceremonial act - it is the one
#                   the GitHub UI makes you press "Submit review" for.
#   2. IDENTITY   - `.user.login` is the repository owner, compared case-INSENSITIVELY. The
#                   author comes from the API, never from anything inside a body. Case is
#                   folded because GitHub logins are unique case-insensitively, so folding
#                   cannot admit a second person - and a required check that could brick the
#                   repo over a capital letter is a worse failure than the one it prevents.
#   3. TOKEN      - the body contains `LGTM`:
#                     * CASE-INSENSITIVE. `lgtm`, `LGTM` and `Lgtm` all count, and this was
#                       decided by looking rather than by taste. Every owner LGTM this repo
#                       has ever received - 18 of them, across PRs astubbs#210 to
#                       astubbs#292 - is the lower-case bare word `lgtm`, seventeen of them
#                       alone on a line and one as "lgtm to try". A case-sensitive rule would
#                       have gone red on all eighteen and told the owner to retype his own
#                       habit in capitals, which is how a memory aid turns into a nuisance
#                       people route around. The cost is real and is stated under "WHAT IT
#                       DELIBERATELY DOES NOT PROVE": a review DISCUSSING an lgtm reads the
#                       same as one giving it.
#                     * as a WHOLE WORD - no letter or digit either side - so `ALGTM`,
#                       `LGTMx` and `LGTM2` do not match. Punctuation either side is fine:
#                       `(LGTM)`, `LGTM!`, `LGTM.` and `-- LGTM` all count.
#                     * NOT followed by `?` (optionally after spaces). `LGTM?` asks a
#                       question; it does not stamp one.
#                     * NOT immediately preceded by a negator - `not`, `no`, `never`, or a
#                       contraction ending `n't` - so `NOT LGTM`, `not LGTM` and `isn't LGTM`
#                       are refused.
#                     * on a line that is neither inside a fenced code block nor a blockquote
#                       (`>`). A review DISCUSSING this rule, or quoting somebody else's
#                       LGTM, must not stamp the PR by accident - and that is not a
#                       hypothetical, because the review of the PR that introduced this check
#                       necessarily quotes it.
#   4. STATE      - is NOT consulted, with one exception: a `PENDING` review (started, never
#                   submitted) is ignored, because it has not been submitted. APPROVED,
#                   COMMENTED, CHANGES_REQUESTED and DISMISSED are all treated alike; the
#                   words in the body decide.
#
# WHAT THE STATE DECISION COSTS AND BUYS, since it is the least obvious of the four:
#
#   * AN `APPROVED` REVIEW WITH AN EMPTY BODY DOES NOT COUNT. That is deliberate. The whole
#     request was for a visible, deliberate stamp, and pressing Approve with no words is
#     exactly the reflex the check exists to interrupt.
#   * A `DISMISSED` REVIEW STILL COUNTS, and this one is load-bearing rather than pedantic.
#     The master ruleset sets `dismiss_stale_reviews_on_push: true`, so an approval this repo
#     receives is DISMISSED by the next push. Had this check consulted state, it would have
#     been head-sensitive through the back door - green, then silently red again after an
#     unrelated typo fix - which is precisely the rule this gate deliberately abandoned. Read
#     the body, ignore the state, and head-insensitivity holds by construction.
#   * A `CHANGES_REQUESTED` REVIEW SAYING `LGTM` COUNTS. A contradiction the human wrote about
#     his own PR is not a thing to arbitrate: unresolved review threads block the merge
#     anyway, by a separate rule in the same ruleset.
#
# IT IS NOT HEAD-SENSITIVE, AND THAT IS THE POINT
#
# An LGTM on ANY commit of the PR counts, permanently, however many commits land afterwards.
# Two independent reasons, either of which would be sufficient:
#
#   1. It matches the gate's existing design. The automated half stopped asking about head
#      freshness deliberately; the archived strict implementation is at the tag
#      `archive/review-gate-strict-head-freshness` and the reasoning is in
#      docs/inflight/parked-strict-review-gate-freshness.md. Two halves of one check
#      disagreeing about what "reviewed" means would be worse than either rule alone.
#   2. The owner asked for it: "it doesn't need to be on the latest head because I won't say
#      LGTM until I'm confident that we're near the merge point anyway". A stamp that
#      evaporates on the next push trains you to stop typing it.
#
# So this is not an oversight to be tightened later by anyone who notices that commit N+1 was
# never LGTM'd. It is the requested behaviour. Tighten it and you get a check that goes red
# after every rebase, which ends with the check being ignored.
#
# WHAT IT DELIBERATELY DOES NOT PROVE
#
# That the owner READ anything. He can type LGTM without opening the diff. Nothing can test
# that, and a check that implied it would be lying. What it proves is that the word was typed
# on this PR by that account, which is the fact the request asked to be made visible.
#
# And, because the match is case-insensitive, IT CANNOT TELL A REVIEW GIVING AN LGTM FROM ONE
# TALKING ABOUT LGTMs. Fences and blockquotes are excluded, which covers quoting and worked
# examples, but a sentence like "I would want an lgtm from someone else first" reads to this
# check as a stamp. That is the price of matching the owner's actual habit rather than a habit
# invented for the checker, and it is the right way round: this half exists to catch a
# forgotten review, not a lying one.
#
# It also cannot tell a THOROUGH review from a two-second one, cannot notice that the LGTM was
# aimed at an earlier version of the change (see head-insensitivity above, which is a choice,
# not a bug), and refuses to attempt sentiment analysis beyond the one negator rule above. The
# rule is a token match with four documented exclusions. Anything cleverer would fail in ways
# nobody could predict from the failure message, and the failure message IS the feature.
#
# HOW THE STREAM IS SEGMENTED
#
# Same protocol as bin/check-review-posted.sh, and for the same reasons. The workflow emits one
# marker line per review, carrying that review's metadata, and everything up to the next marker
# is that review's body:
#
#     <!-- check-human-lgtm <token> <submitted-at> <login> <state> -->
#
# The token is an UNGUESSABLE value the workflow generates per run with `openssl rand`, inside
# the step so it never reaches the public run log. That property is load-bearing: a body
# carrying a valid marker opens a segment of its own choosing, with an AUTHOR FIELD of its own
# choosing, so a predictable token would let anybody who can comment on a PR mint an owner
# LGTM. The weaker property matters too - a review innocently quoting this documentation
# cannot open a segment either.
#
# Keep the marker's shape in step in three places: here, the `--jq` in
# .github/workflows/claude-code-review.yml, and bin/test-check-human-lgtm.sh. The self-test
# reads the workflow file and asserts the coupling, so the three cannot drift silently.
#
# Usage: bin/check-human-lgtm.sh <marker-token>     (marked-up review stream on stdin)
# Exit codes: 0 = an owner LGTM exists, 1 = it does not (or the scan could not be trusted),
#             2 = usage error.

set -euo pipefail

# The repository owner, and the only account whose LGTM this check accepts. Hardcoded rather
# than taken from an environment variable on purpose: an env-var override would be a skip word
# wearing a hat, settable by the same PR that wants to skip the check.
OWNER_LOGIN='astubbs'

fail() { # <message>
    if [ -n "${GITHUB_ACTIONS:-}" ]; then
        echo "::error::$1"
    else
        echo "ERROR: $1" >&2
    fi
}

if [ "$#" -ne 1 ]; then
    echo "usage: $(basename "$0") <marker-token>" >&2
    echo "       (marked-up review stream on stdin)" >&2
    exit 2
fi

marker_token=$1

case "$marker_token" in
    '' | *[![:alnum:]_-]*)
        echo "usage: marker-token must be non-empty and alphanumeric, got '$marker_token'" >&2
        exit 2
        ;;
esac

review_stream=$(cat)

# Character classes are spelled out rather than written `[[:alnum:]]` because CI runs mawk,
# whose POSIX-class support has historically been the thing that differs from gawk. ASCII is
# the whole alphabet this rule cares about.
verdict=0
scan_result=$(awk \
    -v token="$marker_token" \
    -v owner="$(printf '%s' "$OWNER_LOGIN" | tr '[:upper:]' '[:lower:]')" '
    # Is the text immediately before an LGTM a negation? Only the word touching it is
    # considered - "not LGTM" and "never LGTM" are refused, "I am not sure this is
    # LGTM" is not. A general negation detector is unbounded, and half of one that fires unpredictably
    # is worse than a rule you can state in a sentence.
    function negated(pre) {
        return (pre ~ /(^|[^A-Za-z0-9])([Nn][Oo][Tt]|[Nn][Oo]|[Nn][Ee][Vv][Ee][Rr]|[A-Za-z]+[^A-Za-z0-9 \t][Tt])[^A-Za-z0-9]*$/)
    }

    # Records the strongest thing seen on one line of the CURRENT review body. A rejected
    # near-miss is remembered (first one wins) so the failure message can say what to fix
    # rather than just repeating the rule.
    function note_near(reason) {
        if (seg_near == "") seg_near = reason
    }

    # Walks every LGTM-shaped token on a line, applying the whole-word, question-mark and
    # negation clauses of rule 3 in order.
    function scan(line,   rest, start, pre, prevch, post, nextch) {
        rest = line
        while (match(rest, /[Ll][Gg][Tt][Mm]/)) {
            start = RSTART
            pre = substr(rest, 1, start - 1)
            prevch = (start > 1) ? substr(rest, start - 1, 1) : ""
            post = substr(rest, start + 4)
            nextch = substr(post, 1, 1)
            if (prevch !~ /[A-Za-z0-9]/ && nextch !~ /[A-Za-z0-9]/) {
                if (post ~ /^[ \t]*\?/) note_near("question")
                else if (negated(pre)) note_near("negated")
                else { seg_ok = 1; return }
            }
            rest = post
        }
    }

    function reset_segment() {
        open = 0; fenced = 0; fence_ch = ""; fence_len = 0
        submitted = ""; login = ""; state = ""; seg_ok = 0; seg_near = ""
    }

    # Charges the finished segment to the verdict. PENDING is the one state consulted: it
    # means the review was started and never submitted, so there is nothing to have said.
    function settle() {
        if (open && tolower(login) == owner && state != "PENDING") {
            saw_owner = 1
            if (submitted > latest) latest = submitted
            if (seg_ok) {
                ok = 1
                if (submitted > latest_ok) latest_ok = submitted
            } else if (seg_near != "" && near == "") {
                near = seg_near
            }
        }
        reset_segment()
    }

    NF == 7 && $1 == "<!--" && $2 == "check-human-lgtm" && $3 == token && $7 == "-->" {
        settle(); open = 1; submitted = $4; login = $5; state = $6; next
    }

    # CommonMark fence tracking, the same rule bin/check-review-posted.sh applies to task-list
    # boxes: a closing fence must use the same character, be at least as long, and carry no
    # info string.
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

    # Quoted or fenced text is discussion, not a stamp - but an LGTM in there is worth
    # reporting back, because "you wrote it inside a code block" is a fix nobody would guess.
    open && (fenced || /^[ \t]*>/) {
        if ($0 ~ /[Ll][Gg][Tt][Mm]/) note_near("quoted")
        next
    }

    open { scan($0) }

    END {
        settle()
        print latest_ok "|" latest "|" near
        if (ok) exit 0
        if (near != "") exit 3
        if (saw_owner) exit 4
        exit 5
    }
' <<<"$review_stream") || verdict=$?

latest_lgtm_at=${scan_result%%|*}
rest=${scan_result#*|}
latest_review_at=${rest%%|*}
near_miss=${rest#*|}

# Every failure names the human half explicitly. The gate's other half fails with "this PR has
# never been reviewed: claude[bot] ...", so the two reds are never confusable at a glance -
# which is the entire point of the request this check answers.
case "$verdict" in
    0)
        echo "Human LGTM: ${OWNER_LOGIN} submitted a review containing LGTM (latest at ${latest_lgtm_at})."
        echo "This counts for every later commit on this PR, permanently - see bin/check-human-lgtm.sh."
        exit 0
        ;;
    3)
        case "$near_miss" in
            question)
                detail="the closest was followed by a question mark. 'LGTM?' asks; it does not stamp." ;;
            negated)
                detail="the closest was negated (something like 'not LGTM')." ;;
            quoted)
                detail="the closest was inside a code fence or a blockquote, which is discussion rather than a stamp." ;;
            *)
                detail="the closest did not match the rule." ;;
        esac
        fail "NO HUMAN LGTM ON THIS PR (the automated review is a separate half of this check). ${OWNER_LOGIN} has reviewed it, but ${detail} Submit a review on this PR whose body contains LGTM. See docs/ci.md."
        exit 1
        ;;
    4)
        fail "NO HUMAN LGTM ON THIS PR (the automated review is a separate half of this check). ${OWNER_LOGIN} has submitted a review (latest at ${latest_review_at}) but none of them says LGTM. Submit a review whose body says LGTM (any case) once you are happy for this to land. See docs/ci.md."
        exit 1
        ;;
    5)
        fail "NO HUMAN LGTM ON THIS PR (the automated review is a separate half of this check). ${OWNER_LOGIN} has not submitted a review on it at all. THIS IS THE EXPECTED STATE until you have read the change yourself: submit a review whose body says LGTM (any case) when you are near the merge point. It then counts for every later commit, so you never have to re-stamp. See docs/ci.md."
        exit 1
        ;;
    *)
        fail "Could not scan this PR's reviews for a human LGTM: the scan exited ${verdict}. Treating that as unstamped rather than guessing."
        exit 1
        ;;
esac
