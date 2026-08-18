# The `@claude` trigger fires on prose about it, and the fix is a semantics change

`.github/workflows/claude.yml` decides whether to start on
`contains(github.event.comment.body, '@claude')` - a plain substring test with no awareness of
backticks, code fences or quotation. So any comment *discussing* the mechanism starts a billed job.

**Observed, not theorised.** While replying to review feedback on astubbs#286, two replies explained
the trust model and quoted the trigger string inside backticks. Exactly two `Claude Code` runs fired
on `pull_request_review_comment` and ran to completion - a one-to-one match with the two replies that
contained it. Nothing asked for a review; prose about the feature invoked the feature. The same
applies to quoting *this note* into a PR comment: the trigger does not care that you are describing
it.

**Why it now costs something.** It was harmless while that route could execute nothing. It is not
now: the comment route carries the curated execution allowlist, so an unasked-for start spends a
runner and a token-bearing job.

## Why it is not just fixed

The fix is cheap - `startsWith` on the trimmed body, so the trigger has to open the comment the way a
slash-command does. But it changes **user-facing semantics**: a mention part-way through a sentence
("hey, could you take a look") stops working. That is the repository owner's call rather than a
silent tightening, which is why this is recorded instead of applied.

## Testing it

Subject to the default-branch rule - a comment-triggered workflow runs `master`'s copy of its file,
so a trigger change cannot be exercised on the pull request that makes it. [`docs/ci.md`](../ci.md)
→ "Editing the reviewer" owns that rule; `docs/inflight/ci-review-agent.md` records the reviewer's
other open gaps.

## Delete when

`claude.yml` no longer decides on a bare substring match, or the owner decides the current behaviour
is wanted and says so there.
