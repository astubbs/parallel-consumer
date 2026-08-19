# Parked: the strict, head-fresh review gate

<!-- inflight-type: feature -->
<!-- inflight-state: parked - deferred -->


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
- It also needed the **reviewed SHA carried across job boundaries**, so the head-moved refusal
  could compare against the commit the reviewer actually checked out rather than one re-derived
  later.

That is the whole bill, and it is smaller than it first looked. An earlier draft of this entry
also charged freshness for the `actions: write` scope on the `refresh-gate` jobs. That was wrong -
see the correction below - and it matters here, because it inflates the apparent cost of going
back: restoring strictness buys the guarantee for the price of the timestamp machinery alone, not
for a new privilege-escalation surface.

## The trigger fired on 2026-08-19, and strictness stays parked anyway

This entry used to end by naming a trigger: parking rested on Codex auto-reviewing every push,
so if that auto-review ever stopped, per-commit coverage would stop coming from anywhere at all
and nothing would announce it - the gate would keep passing, because "a review exists on this
PR" is still true.

**It stopped.** Automatic review on every push was turned off in the Codex settings on
2026-08-19, because reviewing every push spends more than the coverage is worth at current
prices. Codex still reviews **on request**, by commenting `@codex review` on the PR.

**The decision that follows is to leave strictness parked, and to accept the gap knowingly.**
Restoring it would make every push want a fresh Claude review, which moves the per-push spend
onto the more expensive reviewer - the exact cost that both this parking and the reviewer's move
off `pull_request` were made to remove. The gap is therefore real and stated rather than
covered: a PR reviewed at commit 1 can merge at commit 20 with commits 2-20 unread.

**What would change the decision is price, not principle.** Strict is still the better guarantee;
it is unaffordable per push, and nothing else about the trade has moved. If a review ever costs
around two orders of magnitude less, restore it - the archive above is the implementation, and the
bill is the timestamp machinery alone. Rediscovering that the gap exists is not a trigger; it is
written down here on purpose so it does not read as an oversight to the next person who finds it.

Until then the coverage is a person's judgement: ask for a review when the PR is ready, and ask
again after a push that changes what the reviewer already looked at.

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
