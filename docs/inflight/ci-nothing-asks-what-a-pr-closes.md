# Nothing asks a PR which issues it closes

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->

Push and merge both have hooks, and neither asks the question. `remind-inflight-on-push.sh` surfaces
what the branch's own note still has open; `check-merge-outstanding-work.sh` refuses a merge while
background work is live. Both are about work the agent started. **Whether the PR resolves something
already on the tracker is asked by nobody**, and an agent that never learns an issue exists cannot
remember to close it.

Two halves, and the second is worse because it looks like success:

- **The PR closes an issue and says nothing.** The issue stays open with its fix merged, so the next
  reader scanning the tracker draws the obvious wrong conclusion, and the reasoning survives only
  inside a merged PR body. [`pr-mirror-fixes-and-what-they-close.md`](pr-mirror-fixes-and-what-they-close.md)
  records both shapes - three PRs that carry a qualified closing reference, and one that deliberately
  does not with the explanation stranded where no reader of the issue meets it.
- **The agent writes the closing keyword and it does nothing.** `Fixes astubbs#167` closes nothing;
  only the fully qualified `astubbs/parallel-consumer#167` does. The short form passes the issue-ref
  gate, reads as correct, and silently fails - so diligence is not what separates the two outcomes.

## Why this is the first slice, not the last

It needs no new data. [`issue-index.md`](issue-index.md) is already in the tree, already carries
labels, and is already injected at session start - so a candidate sweep is a local grep with no
network, no token and no rate limit. The graph work in
[`ci-issue-index-has-no-edges.md`](ci-issue-index-has-no-edges.md) makes this better; it does not gate
it.

What a hook should emit is the **ready-to-paste qualified line**, not a reminder to think about it.
The failure above is not that the rule is unknown - it is written in
[`docs/issue-references.md`](../issue-references.md) with a gate behind it - but that the wrong form
is the one that comes to hand.

## The decisions

- **Title-keyword sweep, or label-aware.** The recorded miss says label-aware:
  `issue-index.md`'s own post-merge block describes a keyword sweep of the live tracker missing
  astubbs#177, which one grep of the index would have surfaced, because its labels carried what its
  title did not.
- **Push, merge, or both.** `remind-inflight-on-push.sh`'s header owns the reasoning for that choice
  and should be followed rather than re-derived: push informs while it can still change what gets
  built, merge is the backstop where the honest outcome is often acknowledge-and-override.
- **What confirms it worked is not the body text.** `gh pr view <n> -R astubbs/parallel-consumer
  --json closingIssuesReferences` is what GitHub actually resolved, which is the check
  `pr-mirror-fixes-and-what-they-close.md` already prescribes.

## Delete when

Something asks the question at push or merge, with a self-test that has been red on purpose.
