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
- **The verdict-free return is astubbs#295, reopened and re-cut onto current `master`** - the
  Wagon A rungs that need the verdict-free path carry `depends on` it. The re-cut was not a clean
  cherry-pick: the source commit predates the package rename and the atomic execution-state work
  reshaped the engine under it, so review it as a port, not a replay.
- **`feats/native-image-sidecar` is superseded by astubbs#385 and retained as evidence** -
  [`branch-native-image-sidecar.md`](branch-native-image-sidecar.md) owns its status, including
  when it may be deleted.
- **The extractions are stacks, not independent branches** - cut by partitioning the god tip's
  tree, each rung on its parent with a `depends on` line. Only the astubbs#295 resurrection and
  the hygiene audit are `master`-based singletons. Check
  `gh pr list -R astubbs/parallel-consumer` for which rungs have opened before starting one.

## Delete when

Both wagons are decomposed: the extractions in the plan have merged or been deliberately dropped,
and astubbs#293 and astubbs#271 describe only their residue.
