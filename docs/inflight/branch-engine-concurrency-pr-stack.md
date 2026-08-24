# The PR stack that lands perf/engine-concurrency

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

Agreed with the owner 2026-08-24. The integration branch holds four separable bodies of work -
engine code, the bench harness plus its results, the measurement docs, and (contained wholesale)
the `feats/proxy-requirements` tree that the astubbs#293 chain already delivers. This is the plan
for landing the rest; the owner merges serially, so a stacked chain is fine and preferred over
independent PRs that would conflict in the same files.

## The stack, in merge order

1. **astubbs#335 - fix(core): atomic work claim.** OPENED, cut fresh from master (adapted
   cherry-pick of `2e83185040`). The correctness prerequisite: it is what makes a second work
   selector safe. The adaptation is disclosed in its body - the delivery-abandonment mechanism
   stays on this branch because its `WorkManager` side is not on master, and the javadoc describes
   master, where the control loop is still the only selector.
2. **feat(core): virtual threads** (`ExecutionMode`, the virtual worker pool, the pool-queue gate -
   the pressure system reading queue depth rather than executor internals). Independent of the
   claim fix in principle; stacked on astubbs#335 because merging is serial anyway. Source commit:
   `b6083f5ef`.
3. **perf(core): direct pull + ShardOccupancy + DispatchScanMeter.** Genuinely depends on both
   parents: the claim fix for correctness (concurrent selectors), virtual threads for the dpvt
   composition (`DirectPullWorkerPool#start` runs pullers on whatever executor the option built).
   Source commits: `416f03dec`, `b73f8b97e`, `3c617f4f7`, plus the residence-time metric
   `5a0321282` if it does not earn its own step. Carries the delivery-abandonment `WorkManager`
   side and the dp sighting record for `test-untracked-ci-flakes.md` that astubbs#335 left behind.
4. **perf(bench): harness + results + measurement-method notes.** No build dependency on the
   engine PRs (the harness runs any released version by design - system properties old versions
   ignore, reflection for new APIs) but an evidential one: its results measure arms that must
   exist on master, and a `core-vt` arm against a master without the option would silently run
   platform threads. Includes the arrival-matrix driver and the claims-decision material.
5. **astubbs#333 (adaptive concurrency)** retargets from this branch onto the stack tip once its
   base exists on master.

Each PR carries its own inflight notes - notes travel with the code they describe, no omnibus docs
PR. Each stacked PR body carries `depends on astubbs/parallel-consumer#N`, one line per parent.

## Rules for the re-cuts (owner: "don't do any recutting yet" - astubbs#335 was ordered explicitly)

- Cut each from master in its own worktree; keep the branch text where possible (identical text
  minimises the eventual conflicts when this branch's residue merges).
- History order on this branch was direct pull BEFORE virtual threads; the stack inverts that, so
  step 2's re-cut resolves the inversion rather than replaying commits verbatim.
- The engine commits are interleaved with doc commits in this branch's history - re-cuts are
  restagings, not clean cherry-picks. Expect and resolve, never `checkout --ours`.

## Cleanup that travels with the plan

- `perf/unordered-available-queue` and `fix/close-shuts-down-worker-pool`: delete - both verdicts
  recorded (marginal; superseded by the owner's close-path work).
- Remote `rename/master-packages`: verified fully contained in master; deletion awaits the owner
  confirming the history-rewrite gate (local branch already deleted, main checkout moved to
  master).
- The merged `agent-*` worktrees, including `agent-aa327f59a1ca039f3` (the realistic-workload
  re-take agent - its results are committed as `bench/results/realistic-*.csv`).

## Why not one PR

The residue after the astubbs#293 chain lands is still tens of thousands of lines spanning engine
correctness, engine features, and measurement - one review could not hold it, and the claim fix in
particular needed a review where the reviewer looks at nothing else.
