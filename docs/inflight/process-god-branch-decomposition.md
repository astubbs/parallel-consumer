# God-branch decomposition campaign: astubbs#293 and the Streams forest become staggered PR stacks

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

The plan is [`docs/plans/2026-08-31-001-process-god-branch-decomposition-plan.md`](../plans/2026-08-31-001-process-god-branch-decomposition-plan.md);
this note exists so a session picking up ANY proxy- or streams-adjacent work knows a campaign owns
the cutting order, before it invents its own.

What no command will tell you:

- **Extractions land on `master` first; the god PRs shrink by merging `master` forward.** Never
  rewrite `feats/proxy-requirements` or the `ks-streams-*` forest - the forest is additionally
  bound by its own "merge, never rebase" settled decision.
- **The verdict-free return (closed astubbs#295) is resurrected by cherry-picking `4b4ff1968`
  from `feats/proxy-requirements`**, not by reopening its original branch, which predates the
  package rename.
- **`feats/native-image-sidecar` contains the whole demo chain** (sibling of astubbs#340's
  branch, on top of `feats/polyglot-demos`) - it is a stack rung today, not a `master` candidate.
- The four independent starters are the plan's A1-A4, plus B1 (Streams fork/build machinery).
  Check `gh pr list -R astubbs/parallel-consumer` for which have opened before starting one.

## Delete when

Both wagons are decomposed: the extractions in the plan have merged or been deliberately dropped,
and astubbs#293 and astubbs#271 describe only their residue.
