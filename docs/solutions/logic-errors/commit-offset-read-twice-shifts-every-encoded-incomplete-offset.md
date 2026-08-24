---
title: "The offset to commit was read twice, so the payload was filed under a base it was not encoded against - offset reset, and silent record loss beside it (confluentinc#894)"
date: 2026-08-24
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: internal / offset-commit
symptoms:
  - "`Fetch position ... is out of range ... resetting offset` after a rebalance, for a position the partition never produced"
  - "Offset metadata that, decoded against the offset it was committed with, names incomplete offsets above the highest offset ever produced"
  - "Records delivered by the broker that are never processed and never retried, with no exception, no reset and no lag anomaly"
  - "The committed offset keeps pace with the log end offset while real records are dismissed as already-completed - healthy from the outside"
root_cause: two_reads_of_a_derived_offset_around_an_encode_step_let_a_completion_land_between_them
resolution_type: code_fix
severity: high
status: "Fixed by the confluentinc#893 carry on astubbs#121, guarded by PartitionStateCommittedOffsetTest's call-count assertion plus the two behavioural classes, PartitionStateCommitEncodeShift894Test and PartitionStateCommitShiftCompounding894Test. Unmerged upstream as of this writing. One question left open and recorded below - whether the compounding bound holds for all payload widths."
tags:
  - offset-encoding
  - commit-offset
  - rebalance
  - race-condition
  - silent-data-loss
  - upstream-cherry-pick
---

# Committing an offset the encoded payload was not written against (confluentinc#894 / astubbs#121)

## Problem

Every commit the Parallel Consumer makes can carry an encoded note in the offset metadata listing
which offsets *above* the committed one are still outstanding. That note is written **relative to**
the offset being committed - the committed offset is the note's decode base. On the unfixed code
`PartitionState.createOffsetAndMetadata()` derived those two numbers from two separate reads:

```java
Optional<String> payloadOpt = tryToEncodeOffsets();   // internally calls getOffsetToCommit()  <- read 1
long nextOffset = getOffsetToCommit();                //                                       <- read 2
```

The payload was encoded against read 1; the offset committed came from read 2. Nothing held the two
together, so a completion landing between them made the commit describe a partition state that never
existed. Restored after a rebalance, every offset in that note decodes too high.

The user-visible consequence has two forms. The reported one is loud: the consumer eventually asks
the broker for a position past the end of the log, and `auto.offset.reset` fires. The unreported one
is quiet: at traffic rates where real records keep overtaking the fabricated ones, the committed
offset tracks the log end exactly - no overshoot, no reset - while real records are dismissed as
already-done and are never processed and never retried. That second form is data loss, and it is the
more serious of the two, because the first is at least visible.

**Not to be confused with** the `committedOffsetRemoved[latest]` investigation recorded in
[`docs/solutions/test-flakiness/latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md`](../test-flakiness/latest-reset-nudge-race-committedoffsetremoved-2026-07-30.md),
which is the first thing a search for "offset reset" in this repository returns and which concluded
*test harness, not a product bug*. That conclusion stands for that investigation; it is a different
mechanism and says nothing about this one.

## Symptoms

- `Fetch position ... is out of range ... resetting offset` after a rebalance, on a partition where
  no offset in that range was ever produced. This is the reported symptom on
  [`confluentinc#894`](https://github.com/confluentinc/parallel-consumer/issues/894), under frequent
  rebalancing.
- Offset metadata that, decoded against the offset it was committed with, names incomplete offsets
  above the highest offset the partition ever produced.
- Records delivered by the broker that are never processed and never retried, with **no** error, no
  reset and no lag anomaly - the committed offset keeps pace with the log end offset while
  `isRecordPreviouslyCompleted` dismisses them against an `offsetHighestSucceeded` that was never
  actually reached.
- The reviewer's observation on the upstream PR: "after multiple rebalances it ends up committing not
  offset 10 - but offset 11". The shift magnitude varies with the state at commit time; it is not a
  constant of the defect.

Four things all have to hold, which is why this went unreproduced for so long:

1. there are incompletes at commit time (the empty case returns before read 1, so both reads only
   happen on the encoding path);
2. a completion **of the lowest incomplete** lands inside the encode window - microseconds -
   because `getOffsetToCommit()` returns the lowest incomplete when there is one and
   `offsetHighestSucceeded + 1` when there is not, so only that particular completion moves it;
3. a rebalance reads that commit back before a later clean commit overwrites it;
4. for the silent variant, traffic keeps arriving into the fabricated range.

Rare per commit cycle, then. Not rare over time in a deployment that rebalances often and keeps a
straggler outstanding - which is exactly the low, unbalanced traffic the original report describes,
and why the reporter saw it every few days.

## What Didn't Work

**Reading the shift magnitude off one fixture.** The first reproduction shifted the commit by +2 and
looked like a fixed property of the bug. It is not: completing the only incomplete empties the set,
so the second read falls through to `offsetHighestSucceeded + 1` and the shift is two; with a second
incomplete above the one that completes, the second read returns *that* offset and the shift is
exactly one. Both are the same defect at different fixtures.

**Sweeping the traffic rate while holding the payload width fixed.** This mistake was made and
corrected during this investigation, and it is the one worth recording. The first growing-partition
sweep varied only how many records the partition produced per cycle, and concluded the overshoot was
a single event that could not compound. That was a statement about the fixture, which polls offsets 0
to 2 and so encodes a payload two offsets wide. The committed offset advances by the payload width
`L` per cycle and the partition's end by the traffic rate `K`, so overshoot grows by `L - K` and the
run halts once overshoot reaches `K`. At `L = 2` the arithmetic permits exactly one growth cycle and
no more. Re-running with `L = 10` against `K = 9` produced overshoot climbing +0, +1, ... +9 over ten
consecutive rebalances - a measurement from that run, reported by the test rather than asserted by
it, and consistent with the arithmetic above rather than independent of it. Same class of error as
the shift-magnitude one, one level up: a sweep that holds the deciding parameter fixed answers a
question about its own fixture.

**Looking for the gap in the round-trip tests.** It is not there. Round-tripping *was* covered -
`WorkManagerOffsetMapCodecManagerTest` and `OffsetEncodingBackPressureTest` both decode committed
metadata, and `PartitionStateCommittedOffsetTest` rebuilds partition state from committed offset
data (constructed directly, rather than decoded from a payload). What
no test did was **perturb the state between the two reads**. Without that perturbation both reads
return the same number, the payload and the committed offset agree, and every assertion passes
honestly. The coverage was real; it simply could not see this.

## Solution

Sample the offset once and carry it with the payload it describes, so the two cannot disagree.
`tryToEncodeOffsets()` now takes its single read up front - before the empty-set early return, not
after it - and returns both values together:

```java
private ParallelConsumer.Tuple<Optional<String>, Long> tryToEncodeOffsets() {
    long offsetOfNextExpectedMessage = getOffsetToCommit();
    ...
    return ParallelConsumer.Tuple.pairOf(of(offsetMapPayload), offsetOfNextExpectedMessage);
}
```

and the caller reads both out of the tuple rather than re-deriving one of them:

```java
ParallelConsumer.Tuple<Optional<String>, Long> tuple = tryToEncodeOffsets();
Optional<String> payloadOpt = tuple.getLeft();
long nextOffset = tuple.getRight();
```

This is a cherry-pick of [`confluentinc#893`](https://github.com/confluentinc/parallel-consumer/pull/893),
carried here as [`astubbs#121`](https://github.com/astubbs/parallel-consumer/issues/121). Upstream
never merged it, and shipped no test with it.

Three tests guard it, at three altitudes:

- `PartitionStateCommittedOffsetTest` pins the shape - `getOffsetToCommit()` is invoked exactly once
  per commit cycle.
- `PartitionStateCommitEncodeShift894Test` reproduces the single hop behaviourally: the committed
  offset must equal the base its payload was encoded against, the restored state must not name
  offsets that were never produced, and the shifted commit must not drive the next commit past the
  log end offset.
- `PartitionStateCommitShiftCompounding894Test` runs the full encode-commit-decode-reassign cycle
  repeatedly, sweeping payload width against traffic rate, and counts records skipped-having-run
  separately from records dropped-without-ever-running so the second cannot hide inside the first.

The race is injected at a seam that can only move the thing under test: `encodeOffsetsCompressed`
calls `getIncompleteOffsetsBelowHighestSucceeded()` exactly once to snapshot its input, and that
snapshot is a fresh copy, so a completion landing immediately after it changes neither the payload
nor `offsetHighestSucceeded` - only a *subsequent* read of the offset to commit can see it.

Controls were run in both directions: every assertion flips against the fix, and the same tests with
the injected completion disarmed pass on unfixed code - so the failures come from the race and not
from the fixtures. Across the control-arm cycles with the fix in place, zero records were dropped.

## Why This Works

`getOffsetToCommit()` is `getOffsetHighestSequentialSucceeded() + 1`, which returns the lowest
incomplete when the set is non-empty and `offsetHighestSucceeded + 1` when it is empty. It is
therefore a *derived* value that moves whenever the lowest incomplete completes. Reading it twice
around an encode step means reading a moving value twice and treating the two results as one number.
Threading it through a tuple makes payload and committed offset the same number **by construction** -
there is no longer a second read that could observe a different state.

The residual race is in the safe direction. Work completing between the sample and the encode only
makes the commit more conservative: a lower offset, at-least-once replay, and the next commit cycle
catches up. There is no remaining path that commits ahead of what the payload describes.

The silent variant follows from the same arithmetic. When the fabricated state carries
`offsetHighestSucceeded` above what the partition ever produced, real records arriving into that
range hit the `recOffset <= offsetHighestSucceeded` branch of `isRecordPreviouslyCompleted` and are
dismissed as already done. They are not queued, so they cannot be retried.

**Which regime you land in depends on the same arithmetic as the overshoot, and the two are mutually
exclusive.** When the traffic rate `K` is below the payload width `L`, overshoot grows and the run
walks itself off the end of the log - the loud failure, which stops there. When `K` is at or above
`L`, overshoot stays at zero, the committed offset tracks the log end exactly, and the cycle never
halts - so the dismissals continue for as long as that regime holds. That is the sense in which the
loss is unbounded: not that a single run drops forever in all conditions, but that nothing in this
regime terminates it or announces it. This session measured one record per cycle at `L = 2` and nine
to ten at `L = 10`; those figures are reported by the test rather than asserted by it, so treat them
as observations from that run rather than as fixed properties.

**Blast radius.** The two-read shape is present in every released 0.5.x line - verified by reading
`createOffsetAndMetadata()` at each release tag from 0.5.0.0 through 0.5.3.3 (in releases before
0.5.2.4 the second read is spelled `getNextExpectedPolledOffset()`; it is the same defect under an
earlier name). Retrospective detectability is essentially nil for the silent variant: the evidence it
leaves behind is committed offsets that tracked the log end correctly.

**Still open.** That the compounding is bounded by the payload width is a regularity measured across
two widths and nine traffic rates with a stated mechanism, not a proof over the parameter. It does not
affect whether to take the fix - the second read is gone, so every path above is closed by
construction - but it is the sentence to revisit rather than cite.

## Prevention

- **When a value is derived from mutable state, sample it once and pass it along.** Two calls to the
  same accessor in one logical operation are two different values unless something guarantees
  otherwise. Where a payload is written *relative to* a number, that number is part of the payload -
  return them together rather than letting a caller re-derive one of them.
- **Test the window, not just the round trip.** Round-tripping a payload proves the codec; it says
  nothing about whether the two ends of the operation saw the same state. A concurrency defect needs a
  test that perturbs state *between* the steps. The seam used here - overriding a method the encoder
  calls exactly once, at the point where the snapshot has already been taken - is deterministic and
  reusable for the same shape elsewhere.
- **Before generalising from a sweep, ask which parameter the answer actually turns on, and vary that
  one.** Two conclusions in this investigation ("the shift is +2", "the overshoot cannot compound")
  were properties of the fixture rather than of the defect, and both survived a sweep that varied
  something else.
- **Count "correctly skipped" and "never ran" separately.** In any harness that measures record loss,
  a record legitimately recognised as already-processed and a record dismissed against fabricated
  state look identical at the call site. Counting them together lets real loss hide inside correct
  behaviour - it did here, until a control arm on fixed code appeared to "lose" records it had in
  fact processed.
- **A fault that leaves no trace deserves a test even when no one has reported it.** The reported
  symptom here was the recoverable one. The unreported symptom sharing its root cause was the data
  loss, and it was found only by asking what happens when the same race runs at a different traffic
  rate.

## Related Issues

- [`confluentinc#894`](https://github.com/confluentinc/parallel-consumer/issues/894) - "Offset reset
  when frequent rebalancing", the original report (October 2025). A reproducible example was
  never produced - though the request was made on the PR below, on 2025-10-26, not on the issue, so
  a reader looking for it here will not find it. The attachment on that PR turned out to contain both
  the recipe and the production logs, unread for nine months.
- [`confluentinc#893`](https://github.com/confluentinc/parallel-consumer/pull/893) - the upstream fix,
  never merged there, shipped with no test.
- [`astubbs#121`](https://github.com/astubbs/parallel-consumer/issues/121) - this fork's issue,
  carrying the fix and the reproduction.
- [`stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md`](stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md) -
  the nearest neighbour: a different mechanism reached through the same door, a rebalance leaving
  partition state describing something that is not true. Both are silent; that one wedges an offset,
  this one fabricates them.
