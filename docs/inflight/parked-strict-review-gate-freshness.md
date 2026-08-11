# Parked: the strict, head-fresh review gate

The `claude-review` gate used to require a `claude[bot]` review **newer than the current head**,
so a push after a review turned it red. It now asks only whether a finished review exists on the
PR. The current contract is in [`docs/ci.md`](../ci.md) - read it there, not here.

**Archived at the tag `archive/review-gate-strict-head-freshness`, which points at commit
`38ccf88057428daefcdddd9dd05d1b217da58509`.** That is the complete strict implementation, reviewed
and signed off.

```bash
git fetch origin --tags                                          # shallow clones have no tags
git tag -n99 -l archive/review-gate-strict-head-freshness        # why it was archived
git show archive/review-gate-strict-head-freshness               # or the SHA above
```

The `git fetch --tags` is not boilerplate: a CI checkout is usually shallow and tagless, so the tag
name alone resolves to nothing there and the archive looks lost. The commit SHA is written out
above so it can be found either way.

## Why it was parked

Not because strict is wrong. It is the stronger guarantee, and a review of commit 1 genuinely
does not vouch for commit 20. It was parked because of **what enforcing it cost**, weighed
against a guarantee that turned out to be arriving from somewhere else anyway:

- Deciding "newer than the head" needed a timestamp the contributor does not control, which
  meant preferring the server-side check-suite time over the committer date, handling
  same-second ties, and paginating an endpoint whose ordering is undocumented. Three rounds of
  review findings, none of them about reviewing.
- Worse, it needed the gate to be **re-runnable after a review**, which put an `actions: write`
  token inside the review system - a token that can dispatch any workflow in the repo,
  `release.yml` included. Containing it safely is most of what the surrounding design does.

## The assumption this rests on - and the trigger to restore

**Per-push coverage now comes from the auto-reviewer, not from this gate.** Codex reviews every
push; the gate's job is to assert that somebody deliberately asked for a Claude review of this
PR and that it finished.

**If that auto-review ever stops - disabled, unsubscribed, rate-limited, or quietly changed to
run on fewer events - per-commit coverage stops coming from anywhere at all,** and nothing will
announce it. The gate will keep passing, because "a review exists on this PR" is still true.
That is the trigger to restore strictness, and it is the reason this entry exists rather than
just the tag.

## One correction to the tag's reasoning

The tag says leniency leaves "nothing to re-run", so the `refresh-gate` jobs go with it. That
holds for a push, but not for the flow that matters: the gate fires on `pull_request` only, so a
review posted **after** the last push raises no event, and the check keeps the red it recorded
before the review existed. Observed on astubbs/parallel-consumer#288. So both `refresh-gate`
jobs survived, and `actions: write` with them - the write scope is a cost of the *check being
produced by a `pull_request` workflow*, not of strictness.

Retiring it needs a different mechanism, not a different rule: have the reviewer raise a check
run on the reviewed SHA directly, which is tracked in
[`ci-review-agent.md`](ci-review-agent.md).
