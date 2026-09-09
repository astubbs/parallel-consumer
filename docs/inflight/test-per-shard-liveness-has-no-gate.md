# Demoting the Class 2 bound left one wedged shard with no gating detector

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-state: open - the COMMIT half gated 2026-09-09 with a red control and both replay seeds green; the SHARD-DISPATCH half is still uncovered and has no red control -->

## 2026-09-09: half of this is closed, and the half that is not is now named precisely

**Closed: a commit that never lands.** `UncommittedCompletionDetector`
(`UNCOMMITTED_COMPLETIONS/COMMIT_NOT_LANDING`) gates on the DIFFERENCE between a member's own
next-offset-to-commit for a partition (`PartitionState#getOffsetHighestSequentialSucceeded` plus one,
which is exactly what PC would commit) and what the group has actually committed - held across
`COMMIT_NOT_LANDING_SAMPLES` consecutive samples with the committed offset not moving and the group
STABLE. It reads only public API, so the main-code accessor `INSTANCE_STALL_BOUND`'s granularity note
declines to add for a probe was not needed.

**The red control this note demanded exists**: `WedgedPartitionRedControlIT`. A real engine over
three partitions with a `LongPollingMockConsumer` that ANSWERS every commit and APPLIES only the
partitions that are not black-holed - a coordinator that acks and drops, the same defect class
astubbs#470 fixed inside PC. Measured on it, broker-free, in one JVM:

| Arm | broker committed for the watched partition | PC's local next-offset-to-commit | Class 2 | new detector |
|---|---|---|---|---|
| commits black-holed (RED) | 0 | 60 | observes (does not gate) | **fires** |
| healthy | 60 | 60 | silent | silent |
| pinned by an incomplete record | 9 | 9 | observes (does not gate) | silent |

Every existing gating detector is shown green on the red arm mechanically, not by argument: all 180
records reach the user function so `NO_PROGRESS` cannot fire and the correctness ledger balances; the
instance completes work throughout and holds nothing at the end, so `INSTANCE_STALL` re-arms - shown
across twenty times its bound, **with an armed control that fires at the same spacing** so the silence
is not vacuous. Sabotaged (the reporter never assigned in the detector), the red arm fails and the two
green arms do not.

**Why the third row is the result that matters.** It is the false positive the Class 2 bound could
never separate, and the new detector separates it *structurally* rather than by calibration: an
incomplete record pins `offsetHighestSequentialSucceeded` at precisely the offset the broker holds, so
the difference is zero however long the stagnation runs. That is also why the two replay seeds this
note nominated (`6825864417772979246`, `4044221734199516240`) cannot fire it - their pinned partitions
were pinned by an in-flight record, which is row three.

**Both seeds were then replayed, and they confirm it.** Full chaos suite per seed, this tree, a
loaded workstation (load average 19-25, other agents building alongside - which biases towards
crossing a timing bound, not away from it):

| Seed | Suite result | Gating violations | `COMMIT_NOT_LANDING` | Class 2 observations | peak `lagStagnation` | peak `uncommittedCompletions` |
|---|---|---|---|---|---|---|
| `6825864417772979246` | 10 tests, 0 failures | 0 | **0** | 30 | 151500ms - **over the bound** | 3009 records |
| `4044221734199516240` | 10 tests, 0 failures | 0 | **0** | 36 | 152020ms - **over the bound** | 2757 records |

The `lagStagnation` peaks matter: on both seeds the Class 2 bound was genuinely CROSSED, so these are
not runs where the false positive failed to occur. The new detector was silent through every one of
them. And the `uncommittedCompletions` peaks matter for the opposite reason - a healthy chaos run
carries thousands of records of finished-but-not-yet-committed work at its widest, which is why the
detector must gate on whether the committed offset MOVES rather than on how large the gap gets.
Predictions stated before the runs, all three held: Class 2 observes and the suite drains; the new
detector fires on neither; the peak is non-zero but never gates.

**One instrument trap this created, for whoever replays next.** The Class 2 interpretation text now
names `UNCOMMITTED_COMPLETIONS/COMMIT_NOT_LANDING` (so a triager meeting a frozen watermark is told
what the absence of that violation means). A log grep for the bare string therefore matches every
Class 2 observation and reports one apparent finding per observation - it read 33 and 39 on these two
runs, both of which are zero. Count `COMMIT_NOT_LANDING: partition`, or count `VIOLATION:`.

**Refuted along the way, and it changed the design.** This note's own prescription - "a watermark
stagnant *while completions advance and real backlog exists* is the wedge" - does not discriminate as
written. Completions advancing is an INSTANCE-wide fact, and row three has completions advancing, real
backlog, and a stagnant watermark; a gate built on that wording would have fired on both seeds it was
promised to spare. The term that had to change was the granularity of the second signal, from the
instance's completions to *that partition's own* local watermark.

**Still open: a shard that will never be dispatched again.** The signal above reads a PARTITION, and
any incomplete offset in it pins the local watermark, so a wedged key-order shard inside a partition
still gates nothing - the difference reads zero exactly as it does for a slow record. Separating those
two needs per-shard in-flight state, which is the `ShardManager#processingShards` accessor this suite
still declines to add. **No red control exists for that half**, and astubbs#483 found the nearest
candidate mechanism (the shard-displacement retry-queue orphan) unreachable - so it is not yet
established that the shape occurs at all. That is the next question, and it is a reachability question
before it is a detector one.

## The original note, unchanged below

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

**Amended 2026-09-09**: both clauses are met for the commit half - the red control fires, and both
replay seeds crossed the Class 2 bound while the new gate stayed silent. This note now closes when
either the shard-dispatch half is shown unreachable - the astubbs#483 style verdict, recorded where
the reachability argument lives - or a per-shard signal gates it with its own red control.
`docs/testing.md` already carries the split, so the gap is not folklore either way.
