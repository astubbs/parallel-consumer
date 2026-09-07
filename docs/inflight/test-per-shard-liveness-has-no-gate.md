# Demoting the Class 2 bound left one wedged shard with no gating detector

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-state: deferred - needs a red control before a new gate can be trusted; the demotion it follows is the right call on its own evidence -->

`CLASS2_STALL/LAG_STAGNATION` became a non-gating observation on 2026-08-25, on evidence that it
measures elapsed time and fires on runs that complete
([`a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`](../solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md)
owns that reasoning, having succeeded the critique note that was deleted once it was settled;
[`bug-857-family.md`](bug-857-family.md)'s 2026-08-25 entry owns the replays).
That is not in dispute here. What this note records is the **cost** of it, which the change's own
write-up initially understated.

## The uncovered case

The demotion's argument was that `INSTANCE_STALL/NO_WORK_COMPLETED` carries the liveness property the
lag bound only approximated. It does - **at instance granularity**. It is re-armed by any SUCCESSFUL
work result (`ProgressProbe#INSTANCE_STALL_BOUND` owns the exact wording, and it is narrower than the
"any returned work result" this note used to say - a failure and a revoked-partition drop both return
a result and re-arm nothing), so an instance whose other shards keep completing never fires it.

So this shape is now covered by nothing that gates:

> One partition's committed offset freezes because of a real commit-path or offset-management defect,
> while the owning instance's other shards keep completing work normally.

`assertScenarioSlos` asserts only `violations`, and observations are not violations. The correctness
ledger does not close the gap either: it counts records **processed**, not offsets **durably
committed**, so a run can balance while leaving exactly the un-committed offsets a restart would
redeliver.

**`ProgressProbe` had already written this down** - `INSTANCE_STALL_BOUND`'s javadoc says *"What per
instance cannot see is one wedged shard on an instance whose other shards keep completing; that case
remains `CLASS2_STALL`'s, false positives and all."* The demotion removed the "remains" without
removing the sentence's premise.
<!-- post-merge: checked-begin -->
It was found by the cross-model adversarial reviewer on astubbs#354, the PR that demoted the bound,
which attacked the claim rather than the code; three in-process reviewers on that same diff did not
raise it.
<!-- post-merge: checked-end -->

## What would close it: gate on the correlation, not the timer

A watermark that is stagnant **while completions advance and real backlog exists** is the wedge; a
watermark stagnant because one heavy record is still running is the false positive the bound could
never separate. Correlating the two signals separates them - and both 2026-08-25 replay seeds would
stay green under it, because their pinned partitions were pinned by an in-flight record, not by a
commit that never landed.

**It must not land on reasoning alone.** The bound being replaced was itself green-calibrated and
argued for, and was wrong for three months. Before this gates anything:

- **Build a red control** - inject a fault that freezes one partition's commits while its siblings
  keep completing, and show the new detector fires on it. `docs/investigating.md` is explicit that a
  detector which has never fired is not a detector; the same rule that closed the Class 2 RED hunt
  applies to its replacement.
- **Then re-run the two replay seeds** (`6825864417772979246`, `4044221734199516240`) and show they
  stay green. A gate that fires on those is the old false positive wearing a new name.
- **A Lincheck harness may reach this far more cheaply than a chaos scenario.**
  astubbs/parallel-consumer#347 adds a Lincheck lane calibrated by refinding four real races
  unaided - including one nobody had listed. The bar above asks for a red control that fires on an
  injected commit-freeze; building that as a chaos scenario means arranging a fleet, a broker and a
  fault injector, while the same property may be expressible as a concurrency harness over the
  commit path in seconds. Try that route first. Note that PR also records replay non-determinism on
  the commit path (micrometer, and `parallelStream()` in two `PartitionState` accessors), which is
  the obstacle a model-checking approach would hit here.
- Prefer a `ShardManager`-level signal if one becomes reachable without adding main-code accessors
  for a probe - the per-instance granularity is a reachability compromise, recorded as such in
  `INSTANCE_STALL_BOUND`'s javadoc, not the ideal.

## Why this is deferred rather than blocking

The demotion is correct on its own evidence and removes a measured false-positive class; holding it
hostage to its successor would keep an uninformative red firing for however long the successor takes.
The gap is real but narrow, has never been observed (no `INSTANCE_STALL` has ever fired either), and
is now stated at every surface that used to claim coverage - the probe's javadoc, the runtime
interpretation text a triager reads, `docs/testing.md`, and the ledger.

## Delete when

A correlated gate lands with a red control proving it fires, and both replay seeds proving it does
not fire on the old false positive. If instead the decision is that this case does not warrant a
gate, delete this note and say so in `docs/testing.md` - what must not happen is the gap quietly
becoming folklore.
