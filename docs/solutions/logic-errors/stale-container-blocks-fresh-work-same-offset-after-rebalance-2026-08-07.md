---
title: "A stale WorkContainer at the same offset silently drops the fresh one after a rebalance, wedging that offset forever (confluentinc#909)"
date: 2026-08-07
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: internal / work-state
symptoms:
  - "`pc.partition.latest.committed.offset` freezes for one partition while `pc.partition.highest.seen.offset` keeps climbing"
  - "`pc.partition.incomplete.offsets` never returns to zero for that partition; consumer-group lag grows on that partition alone while siblings are healthy"
  - "No exception, no retry, no timeout - the partition simply stops advancing and stays that way until the next rebalance or restart"
  - "At DEBUG on io.confluent.parallelconsumer.state.ProcessingShard: 'Entry for {} already exists in shard queue, dropping record' for an offset that was never worked"
  - "`pc.waiting.records` can UNDER-report after the fix's replacement path runs, so a low reading does not rule this out"
root_cause: stale_add_lands_after_the_point_in_time_sweep_and_shadows_the_fresh_container
resolution_type: code_fix
severity: high
status: "Fixed in PR astubbs#31 (carries confluentinc#909), guarded by ProcessingShardStaleReplacement909Test. Two follow-ups recorded below and not actioned: an availableWorkContainerCnt accounting gap, and a stale mechanism description in the test's own javadoc."
tags:
  - rebalance
  - work-state
  - stale-epoch
  - shard
  - silent-stall
  - offset-wedge
  - logic-error
---

# A stale WorkContainer at the same offset silently drops the fresh one after a rebalance

## Problem

After a rebalance, a `WorkContainer` carrying a revoked epoch could occupy an offset in a
`ProcessingShard`. The fresh container for that same offset was then discarded as a duplicate - while
the offset was still recorded as incomplete. Nothing can ever complete it, so the commit high-water
mark for that partition stops advancing.

## Symptoms

The failure is silent by construction: no exception, no retry, no timeout.

- `pc.partition.latest.committed.offset` **freezes** for one partition while
  `pc.partition.highest.seen.offset` keeps climbing.
- `pc.partition.incomplete.offsets` never returns to zero for that partition, and consumer-group lag
  grows on that partition alone while its siblings behave normally.
- `Entry for {} already exists in shard queue, dropping record` - real, but at **DEBUG** on
  `io.confluent.parallelconsumer.state.ProcessingShard`, so off by default. You will not see it
  unless you already suspect this.
- Counter-intuitively, `pc.waiting.records` can **under**-report once the fix's replacement path is
  in play (see the accounting gap below), so a low reading is not evidence against this bug.

## What Didn't Work

The system already had a defense, and on paper it covers this. `PartitionStateManager` sweeps stale
containers on both sides of a rebalance - `PartitionStateManager.java:132` on assignment, `:192` on
revocation - via:

```java
// ShardManager.java:307-313
public long removeStaleContainers() {
    return processingShards.values().stream()
            .map(ProcessingShard::removeStaleWorkContainersFromShard)
            ...
```

It cannot work, because it is a **point-in-time sweep**. It removes what is in the shards at the
instant it runs, and nothing orders it against a concurrent `ShardManager.addWorkContainer()` on the
control thread. Note it also empties shards without removing them, so the shard is typically
*present and empty* afterwards - shard absence is not the mechanism.

There is a second, earlier guard, and understanding why it also fails is the crux:

```java
// PartitionState.java:291-296
public void maybeRegisterNewPollBatchAsWork(...) {
    if (epochIsStale(recordsAndEpoch)) {
        log.debug("Inbound record of work has epoch ({}) not matching currently assigned epoch ...");
        return;
    }
```

`epochIsStale` compares the poll-time epoch against `getPartitionsAssignmentEpoch()` - but that is a
**final field snapshotted at construction** (`PartitionState.java:164`, assigned at `:196`), not the
live map in `PartitionStateManager`. A control thread that resolved its `PartitionState` reference
*before* the rebalance is holding an orphaned pre-rebalance object whose snapshot epoch matches the
batch's epoch. The guard passes, and the whole batch is registered stale.

That pre-revoke reference is the only hole: read the map after revoke and you get
`RemovedPartitionState`, whose `maybeRegisterNewPollBatchAsWork` is a no-op; read it after assign and
the guard fires correctly.

## Solution

Make the add path defend itself, since it is the only point that sees both containers.
`ProcessingShard.addWorkContainer()`, `ProcessingShard.java:61-78`:

```java
public void addWorkContainer(WorkContainer<K, V> wc) {
    long key = wc.offset();
    WorkContainer<K, V> existing = entries.get(key);
    if (existing != null) {
        // Check if the existing entry is stale and should be replaced
        if (isWorkContainerStale(existing)) {
            log.debug("Replacing stale entry (epoch {}) for offset {} with fresh one (epoch {})",
                    existing.getEpoch(), key, wc.getEpoch());
            entries.put(key, wc);
            // availableWorkContainerCnt stays the same since we're replacing, not adding
        } else {
            log.debug("Entry for {} already exists in shard queue, dropping record", wc);
        }
    } else { ... }
}
```

Before the fix this was an unconditional drop keyed on offset alone, with no epoch check. The
non-stale duplicate is still dropped, deliberately, and has its own test case so the fix cannot decay
into "always replace".

An alternative fix existed - have `epochIsStale` consult the live epoch map instead of the orphaned
snapshot. The add-path fix was preferred because it converges no matter which upstream guard is
raced.

## Why This Works

The race needs only two things:

1. **The epoch changes.** `incrementPartitionAssignmentEpoch` is called from `onPartitionsRemoved`
   (`PartitionStateManager.java:185`) and `onPartitionsAssigned` (`:121`), so under **eager**
   rebalancing a revoke+assign cycle moves it by two. Under **cooperative** rebalancing a newly
   gained partition sees +1 and a retained partition sees no change at all. The bug does not depend
   on the size of the jump, only on the epoch changing.
2. **A stale add lands after the sweep.** The control thread, still inside
   `maybeRegisterNewPollBatchAsWork`'s per-record loop (`PartitionState.java:304-311`) on an orphaned
   `PartitionState`, calls `addWorkContainer` with the old epoch. It lands in whatever shard exists,
   or in one `computeIfAbsent` creates (`ShardManager.java:185-187`) - either way, after the sweep
   has passed.

Then the next poll's fresh container for that offset is dropped, and here is the part that makes it
permanent. Registration does two things per record, unconditionally:

```java
// PartitionState.java:308-309
getShardManager().addWorkContainer(epochOfInboundRecords, aRecord);
addNewIncompleteRecord(aRecord);
```

So the offset is recorded **incomplete on the live partition state** even though its work container
was thrown away.

The stale occupant itself does *not* linger - `getWorkIfAvailable()` evicts stale containers as it
scans (`ProcessingShard.java:173-177`), reached because `couldBeTakenAsWork` returns false on
staleness as its very first branch (`PartitionState.java:640-643`). Evicting it changes nothing. The
fresh container is already gone, the offset is still marked incomplete, the consumer position has
moved past it so Kafka will not redeliver it, and no work container exists that could ever complete
it. The commit high-water mark cannot advance past that offset until the next rebalance or restart
re-polls it.

Ordering cannot fix this. The rebalance callbacks run on the `BrokerPollSystem` thread inside
`consumer.poll()` while `registerWork` is drained on the control thread, and there is no mutual
exclusion between the sweep and a concurrent add. (`processingShards` is a `ConcurrentHashMap` and
`entries` a `ConcurrentSkipListMap`, so each individual access is safe - the missing thing is
ordering between the two operations, not memory visibility.) Any "sweep again afterwards" is the same
race one step later.

**Severity note.** This sits at `high` while its neighbour `confluentinc#857` is documented at
`medium`, and the difference is real rather than an inconsistency: the 857 drain-path zombie is
time-bounded and self-heals within `max.poll.interval.ms`, whereas a wedged offset here has no
timeout, no retry and no self-heal. It persists until some incidental event - a deploy, an autoscale,
a restart - happens to rebalance that partition.

## Prevention

- **The regression test is `ProcessingShardStaleReplacement909Test`**, reproducing the timeline
  directly: work at epoch 0, a rebalance, a late add at offset 300 carrying the *old* epoch, then a
  fresh add at offset 300 with the new epoch; assert the fresh one is available and carries the new
  epoch. Its sibling case asserts a same-epoch duplicate is still dropped.
- **Verified from the red side**: reverting the `ProcessingShard` change sends
  `staleContainerAtSameOffsetShouldBeReplacedByFreshOne` red while
  `nonStaleDuplicateAtSameOffsetShouldStillBeDropped` stays green.
- **The generalisable shape**: a point-in-time sweep over a concurrent collection cannot protect
  against a writer that is already in flight. If the cleanup and the write are on different threads
  with no mutual exclusion, the write path has to be self-defending. Sweeping harder, or sweeping
  again, only moves the window.
- **A second shape worth naming**: a guard that reads a value snapshotted into a long-lived object
  (`PartitionState.partitionsAssignmentEpoch`, a `final long` set at construction) will not notice
  that the world has changed. Guards against "am I stale?" must consult the live source of truth, or
  they only detect staleness that predates the object.

### Follow-up 1: availableWorkContainerCnt on the replacement path

The comment "`availableWorkContainerCnt` stays the same since we're replacing, not adding"
(`ProcessingShard.java:70`) asserts an invariant the code does not have. It holds only if the stale
occupant was still counted as available. It is not, if it was taken as work while fresh and then went
stale - `getWorkIfAvailable` decrements at `ProcessingShard.java:195` but leaves the container in
`entries` until success. That is exactly the in-flight-across-a-rebalance case this bug is about.

Replacing such an entry yields a genuinely available fresh container that is never counted, so
`getNumberOfWorkQueuedInShardsAwaitingSelection()` and `workIsWaitingToBeProcessed()` undercount -
feeding the same silent-stall symptom family as `confluentinc#857`. Not a correctness break, since
the counter clamps at zero in `dcrAvailableWorkContainerCntByDelta()`, but the comment should either
become true (`if (!existing.isInFlight())`) or be corrected.

The worse version of this does **not** bite: a stale in-flight container completing would call
`entries.remove(offset)` and delete the fresh replacement by offset, but `WorkManager.handleFutureResult`
short-circuits stale results before reaching `sm.onSuccess`.

### Follow-up 2: the test's javadoc describes the wrong mechanism

`ProcessingShardStaleReplacement909Test.java:24-27` says the sweep "can't clean shards that don't
exist yet". That framing is wrong, and it is the version a future reader meets first. The test itself
reproduces the bug with the shard **present** - `removeStaleContainers()` empties shards without
removing them, and the test's direct `sm.addWorkContainer(...)` calls bypass `addNewIncompleteRecord`,
so the revocation path finds nothing to clean up either. Shard absence is a corollary, not the cause.

Also in that test: `assertThat(epoch2).isGreaterThan(epoch0)` is weaker than the code supports. The
test drives `onPartitionsRevoked` then `onPartitionsAssigned` explicitly, so the value is
deterministic and should be `isEqualTo(epoch0 + 2)`.

## Related Issues

- Upstream: `confluentinc/parallel-consumer#909`. It is an upstream **PR**, not an issue, so it has
  no mirrored issue number in this fork by convention.
- `docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md` - the neighbouring
  post-rebalance stall family (`confluentinc#857`). Same symptom class, different mechanism: a
  drain-path zombie/busy-spin there, a wedged offset here. Its "VERIFIED: no overlap" claim against
  astubbs#31 was re-checked against the live PR diff and still holds - the diff touches only
  `ProcessingShard.java` and its test.
- `docs/solutions/test-issues/dormant-regression-test-uncollected-by-surefire-2026-08-07.md` - why
  this fix's regression test did not run for its first 104 days.
