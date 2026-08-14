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
# THE MATCHING RULE, IN FULL - AND THIS IS THE ONLY COPY OF IT
#
# docs/ci.md owns the GATE contract ("both halves must pass, and here is what each half is").
# This file owns the MATCHING RULE - the clause-by-clause "what counts as an LGTM" below - and
# docs/ci.md links here rather than restating it. Those are two different facts at two
# different altitudes, and each has exactly one home. The rule is not restated in docs, in
# AGENTS.md, or in the workflow, because a duplicate of it drifted from its original inside a
# single PR: the survey under TOKEN below was restated in four files and was wrong in all four.
# The executable version of this rule is the awk program at the bottom of this file, which is
# why the prose lives beside it - prose and code in one file cannot drift apart unnoticed.
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
#                       decided by looking rather than by taste. THE SURVEY IS STATED HERE AND
#                       NOWHERE ELSE, because the first version of it was stated in four places
#                       and was wrong in all four. Every owner LGTM this repo has ever
#                       received, re-derived from
#                       `repos/astubbs/parallel-consumer/pulls/<n>/reviews` over all 181 PRs:
#                       50 submitted reviews across 38 PRs, from astubbs#63 to astubbs#292.
#                       Of those 50, forty-nine spell it `lgtm` and ONE - astubbs#84 - is
#                       `Lgtm`, so case-insensitivity is not a courtesy: it is the difference
#                       between accepting and rejecting a stamp the owner has already given.
#                       Forty-six are the bare word alone on a line; the other four carry a
#                       trailing clause ("lgtm to try" on astubbs#280, and three of the form
#                       "lgtm, @claude can you take a look please"). One of those three,
#                       astubbs#73's "lgtm, @claude how about you?", ENDS IN A QUESTION MARK
#                       and must still pass - which is why the `?` clause below rejects only a
#                       `?` touching the token rather than one anywhere in the body. And all
#                       50 are `COMMENTED` reviews, not approvals. A rule wanting capitals, or
#                       wanting APPROVED, would have gone red on every stamp this repo has ever
#                       been given, which is how a memory aid turns into a nuisance people
#                       route around. The cost is real and is stated under "WHAT IT
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
#                     * THE TWO CLAUSES ABOVE SEE ACROSS ONE SOFT LINE BREAK, because Markdown
#                       does. A newline inside a paragraph renders as a space, so a body
#                       reading "not\nLGTM" or "LGTM\n?" is displayed to every human as "not
#                       LGTM" and "LGTM ?" - and a rule that scanned each physical line alone
#                       accepted both. Hard-wrapped prose is the ordinary way to hit that, not
#                       a contrivance. The lookaround is exactly one line and only when this
#                       side of the token is otherwise empty, so a BLANK LINE (a paragraph
#                       break, where the two words are not adjacent in the rendered text)
#                       still separates them, and the rule stays statable in a sentence rather
#                       than becoming the unbounded sentiment analysis refused below. The
#                       reach is TO the token, not THROUGH punctuation on its own line:
#                       `not\n(LGTM)` still counts, because the negator is no longer the word
#                       touching the token once a bracket is between them. That is the same
#                       place the single-line rule stops (see `not LGTM/LGTM` below), and it
#                       is a bound rather than a claim to be right about that sentence.
#                     * on a line that is neither inside a fenced code block nor a blockquote
#                       (`>`). A review DISCUSSING this rule, or quoting somebody else's
#                       LGTM, must not stamp the PR by accident - and that is not a
#                       hypothetical, because the review of the PR that introduced this check
#                       necessarily quotes it. Three shapes of that rule are worth naming,
#                       because a line-at-a-time reading missed all three and each renders as
#                       quoted or fenced text to every human who opens the PR:
#                         - LIST NESTING. CommonMark lets a block start after a list marker, so
#                           `- > LGTM` is a blockquote and ``- ```py `` opens a fence just as
#                           surely as the unindented forms. Continuation lines of a list item
#                           are plain indentation and always worked; the marker line did not.
#                         - LAZINESS. A paragraph inside a blockquote continues onto following
#                           lines that DROP the `>`, so `> Bob said\nLGTM` is one quoted
#                           paragraph. The continuation ends at a blank line, a fence, or a line
#                           starting a block a paragraph cannot lazily continue into - and it
#                           only STARTS from a paragraph, because a heading, a fence, a list or
#                           an empty `>` inside the quote cannot be continued lazily either. So
#                           `> # context\nLGTM` DOES stamp: that LGTM is outside the quote.
#                         - THE CLOSING-FENCE INDENT. A closing fence may carry at most three
#                           spaces; the fourth makes it code and the block stays OPEN. Treating
#                           it as a close ended the block early and put the rest into prose.
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
# AND THE FENCE/QUOTE EXCLUSIONS ARE A BOUNDED HEURISTIC, NOT A COMMONMARK PARSER. This is
# stated because three successive review rounds each found a real CommonMark shape the previous
# rule missed - list nesting, laziness, the closing-fence indent - and the supply of such shapes
# is not exhausted. Known and DELIBERATELY not excluded, as of this writing:
#
#   * an INDENTED CODE BLOCK (four spaces after a blank line) - `    LGTM` is code and does not
#     render as prose, and this checker will still count it;
#   * an HTML block, a setext heading, a link reference definition, and the several other block
#     starts a real parser knows about.
#
# Closing that list properly means a CommonMark implementation, and this file cannot have one:
# it runs as a step of a REQUIRED check ahead of the gate it protects, so a dependency that is
# present on today's runner image and absent from tomorrow's can brick every open PR - the same
# argument that keeps bin/test-check-human-lgtm.sh on awk rather than PyYAML.
#
# What makes that acceptable rather than merely convenient is the DIRECTION OF THE ERROR, and
# the size of the hole this is a corner of. Above, in plain terms: this check already cannot
# tell a review GIVING an LGTM from one TALKING about one. "I would want an lgtm from someone
# else first" stamps the PR today. Against a hole that wide, an indented code block is a corner
# of a corner - and every one of these exclusions narrows a memory aid, not a security control.
# So each fix here is worth its cost only while it is cheap and exact. IF FALSE POSITIVES EVER
# MATTER MORE THAN THAT, the fix is not more awk in this file: it is to tighten the TOKEN rule -
# for instance to require the bare word alone on a line, which 46 of the 50 real stamps already
# are. That is a change to what the owner has to type, so it is his call and not this file's.
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

    # Drops leading whitespace and any leading CommonMark list markers, so the blockquote and
    # fence tests below see what the renderer sees. A block can start straight after a list
    # marker, which means `- > LGTM` IS quoted and ``- ```py `` DOES open a fence; matching only
    # `^[ \t]*` missed exactly those marker lines and let a leading `- ` carry an LGTM past both
    # exclusions. A marker needs whitespace (or end of line) after it, which is what keeps `->`
    # ordinary prose rather than a bullet, as CommonMark also has it. Bounded at eight levels:
    # deeper nesting is not a shape any review body has, and an unbounded loop here would be a
    # denial of service written into a required check.
    function strip_markers(line,   s, n) {
        s = line
        sub(/^[ \t]+/, "", s)
        for (n = 0; n < 8; n++) {
            if (s !~ /^([-*+]|[0-9]+[.)])([ \t]|$)/) break
            sub(/^([-*+]|[0-9]+[.)])[ \t]*/, "", s)
        }
        return s
    }

    # Walks every LGTM-shaped token on a line, applying the whole-word, question-mark and
    # negation clauses of rule 3 in order. `prevl` and `nextl` are the adjacent lines of the
    # same paragraph, or "" where there is no such line - see the soft-line-break clause.
    function scan(line, prevl, nextl,   rest, start, pre, prevch, post, nextch, epre, epost) {
        rest = line
        while (match(rest, /[Ll][Gg][Tt][Mm]/)) {
            start = RSTART
            pre = substr(rest, 1, start - 1)
            prevch = (start > 1) ? substr(rest, start - 1, 1) : ""
            post = substr(rest, start + 4)
            nextch = substr(post, 1, 1)
            if (prevch !~ /[A-Za-z0-9]/ && nextch !~ /[A-Za-z0-9]/) {
                # A soft line break renders as a space, so "not\nLGTM" and "LGTM\n?" are shown
                # to a human as "not LGTM" and "LGTM ?" and must be refused as those. The
                # adjacent line is spliced in ONLY when this side of the token is otherwise
                # empty, which is what keeps the reach to one line and lets a blank line - a
                # paragraph break, where the words are not adjacent in the rendered text -
                # separate them. The whole-word test above deliberately does not look across:
                # a break is a space, so it cannot glue two tokens together.
                epost = (post ~ /^[ \t]*$/) ? post " " nextl : post
                epre = (pre ~ /^[ \t]*$/) ? prevl " " pre : pre
                if (epost ~ /^[ \t]*\?/) note_near("question")
                else if (negated(epre)) note_near("negated")
                else { seg_ok = 1; return }
            }
            # Advance to the LAST CHARACTER of the token just examined, not past it. Advancing
            # past it dropped the character the next candidate needs to see, so glued repeats
            # walked straight through the whole-word guard: in `LGTMLGTM` the second token
            # became the start of a fresh string, where there is no preceding character to
            # reject it, and `xLGTMLGTM` passed a rule written to refuse `xLGTM`.
            rest = substr(rest, start + 3)
        }
    }

    # A fence may OPEN behind a list marker but may only CLOSE in the plain form, because the
    # lines between the two are literal code where a marker is just text - and a closing fence
    # may carry AT MOST THREE SPACES of indentation, because the fourth makes it code. An
    # unrestricted `^[ \t]*` here ended the block early on a line CommonMark keeps inside it,
    # which put every following LGTM into open prose.
    function fence_line(line) {
        return fenced ? (line ~ /^ {0,3}(`{3,}|~{3,})/) \
                      : (strip_markers(line) ~ /^(`{3,}|~{3,})/)
    }

    # Does this line START a block that a paragraph cannot lazily continue into? Only the two
    # that matter here; a fence is handled by the rule above, before laziness is consulted.
    function starts_block(line,   s) {
        s = line
        sub(/^[ \t]+/, "", s)
        return (s ~ /^([-*+]|[0-9]+[.)])([ \t]|$)/ || s ~ /^#{1,6}([ \t]|$)/)
    }

    # Laziness carries from a PARAGRAPH inside the quote, and from nothing else. CommonMark
    # does not let a heading, a fence, a list item or an empty `>` line be continued lazily, so
    # `> # context` followed by a bare LGTM leaves that LGTM OUTSIDE the quote - and calling it
    # quoted refused a stamp the owner really gave. That direction of error is the one this
    # whole check exists to avoid: a memory aid that rejects the habit it is meant to remind
    # you of is a nuisance people route around.
    function quote_is_paragraph(line,   s, n) {
        s = strip_markers(line)
        for (n = 0; n < 8 && s ~ /^>/; n++) {
            sub(/^>[ \t]?/, "", s)
            sub(/^[ \t]+/, "", s)
        }
        if (s !~ /[^ \t]/) return 0
        if (starts_block(s)) return 0
        if (s ~ /^(`{3,}|~{3,})/) return 0
        return 1
    }

    # Scanning runs ONE LINE BEHIND the reader, which is what makes the line after a candidate
    # available to the clause above. Nothing else needs the delay, so it is confined here: every
    # rule below either hands the current line to flush() as the next line, or ends the
    # paragraph with break_para().
    function flush(nextl) {
        if (have_pending) {
            scan(pending, prev_line, nextl)
            prev_line = pending
            pending = ""
            have_pending = 0
        }
    }

    # A marker, a fence line and a blockquote line all end the paragraph they follow (in
    # CommonMark a fence and a blockquote may interrupt one), so the pending line has no next
    # line and the line after them has no previous one.
    function break_para() {
        flush("")
        prev_line = ""
    }

    function reset_segment() {
        open = 0; fenced = 0; fence_ch = ""; fence_len = 0
        submitted = ""; login = ""; state = ""; seg_ok = 0; seg_near = ""
        pending = ""; have_pending = 0; prev_line = ""; in_quote = 0
    }

    # Charges the finished segment to the verdict. PENDING is the one state consulted: it
    # means the review was started and never submitted, so there is nothing to have said.
    function settle() {
        # The last line of the segment is still one line behind; charge it before judging.
        flush("")
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

    # GitHub stores review bodies as they were typed, so a body can arrive with CRLF endings.
    # Strip the carriage return before any rule looks at the line: it is invisible in every
    # failure message, and it defeats the fence-close test below, which requires nothing but
    # whitespace after the backticks. A CRLF body would therefore never close a fence and would
    # silently swallow every LGTM after it. No real body has done this yet - 0 of the 365 owner
    # review bodies in this repo carry a CR - which is precisely why it needs a test rather than
    # a wait for the first person to paste from an editor that does.
    { sub(/\r$/, "") }

    NF == 7 && $1 == "<!--" && $2 == "check-human-lgtm" && $3 == token && $7 == "-->" {
        settle(); open = 1; submitted = $4; login = $5; state = $6; next
    }

    # A line carrying the token for THIS RUN, and the name of THIS checker, but not the full
    # seven-field shape, is a marker we emitted and cannot parse. Close the open segment rather
    # than reading it as more of the previous body. Without this, a marker that lost a field -
    # an innocent --jq edit, or a `state` that came back empty - would leave the previous
    # segment open and merge the NEXT review into it, so a stranger LGTM would be reported as
    # "astubbs submitted a review containing LGTM". Fail closed instead.
    #
    # Deliberately keyed on the token, which is unguessable and never reaches the run log, so a
    # body cannot trigger this: a review quoting the marker shape out of this documentation
    # carries no token and still reads as ordinary prose, which is the property case 21 pins.
    $1 == "<!--" && $2 == "check-human-lgtm" && $3 == token {
        settle(); next
    }

    # CommonMark fence tracking, the same rule bin/check-review-posted.sh applies to task-list
    # boxes: a closing fence must use the same character, be at least as long, and carry no
    # info string. Read off the marker-stripped line, so a fence opened inside a list item is
    # still a fence.
    open && fence_line($0) {
        break_para()
        # Inside an open fence every line is literal, so only the plain form can CLOSE one: a
        # `- ` in front of the backticks there is code, not a list marker.
        norm = fenced ? $0 : strip_markers($0)
        sub(/^[ \t]+/, "", norm)
        match(norm, /^(`{3,}|~{3,})/)
        fence = substr(norm, RSTART, RLENGTH)
        ch = substr(fence, 1, 1)
        len = length(fence)
        after = substr(norm, RSTART + RLENGTH)
        if (!fenced) { fenced = 1; fence_ch = ch; fence_len = len }
        else if (ch == fence_ch && len >= fence_len && after ~ /^[ \t]*$/) { fenced = 0 }
        in_quote = 0
        next
    }

    # Quoted or fenced text is discussion, not a stamp - but an LGTM in there is worth
    # reporting back, because "you wrote it inside a code block" is a fix nobody would guess.
    #
    # The third clause is CommonMark LAZINESS: a paragraph inside a blockquote continues onto
    # the following lines even when they drop the `>`, so `> Bob said\nLGTM` is ONE quoted
    # paragraph and reads to a human as quoted text. Judging each line alone accepted that as a
    # stamp. The continuation ends at a blank line, at a fence (the rule above), or at a line
    # starting a block a paragraph cannot lazily continue into.
    open && (fenced || strip_markers($0) ~ /^>/ \
             || (in_quote && $0 ~ /[^ \t]/ && !starts_block($0))) {
        break_para()
        if (!fenced) in_quote = quote_is_paragraph($0)
        if ($0 ~ /[Ll][Gg][Tt][Mm]/) note_near("quoted")
        next
    }

    open { in_quote = 0; flush($0); pending = $0; have_pending = 1 }

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
