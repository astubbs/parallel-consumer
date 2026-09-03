# astubbs#431's queue-first sweep needs a second queue removal, or it reopens the re-queue orphan

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->

**Cross-branch coordination between two changes that do not know about each other.** Neither PR is
wrong on its own; they are incompatible at the point they meet, and nothing fails until both are on
one branch.

- **astubbs/parallel-consumer#431** makes the rebalance callbacks decline the retry queue's write
  lock. To do that safely it asks the queue **first** (`tryRemove`, non-blocking) so a refusal
  abandons the paired shard removal and the pair never splits.
- **`ShardManager.onFailure`'s residency confirmation** closes the orphan window between
  `WorkManager.onFailureResult`'s live epoch check and the re-queue it guards, by adding to the queue
  and **then** confirming shard residency.

The confirmation relies on the sweep removing from the **shard before the queue**, which is what
master does. Reverse it and the confirmation is defeated: the sweep's queue removal passes over an
empty queue, the controller adds and reads residency while the container is still resident, and the
shard removal happens after - so neither party removes the entry.

## What astubbs#431 has to do

**Repeat the queue removal after the shard removal**, in `removeWorkFromShardFor` - anchor
`ignoredRemovedWC`. That closes the half where the controller's add lands inside the sweep; the
residency confirmation closes the half where the add arrives after the sweep has finished. Neither
half is redundant and neither closes the other.

The mechanism, the four interleavings and the reasoning are owned by
[`docs/solutions/runtime-errors/retry-queue-orphan-window-between-the-requeue-check-and-the-add.md`](../solutions/runtime-errors/retry-queue-orphan-window-between-the-requeue-check-and-the-add.md)
and restated at `ShardManager.onFailure`. Do not re-derive them here.

## What makes it fail loudly rather than silently

`RetryQueueRequeueWindowTest.aQueueFirstSweepDefeatsTheOneShotConfirmation` models the queue-first
ordering and **asserts the orphan appears** - green while master is shard-first, and the thing that
has to be consciously inverted when astubbs#431 lands with the paired removal. Its matched control
`aShardFirstSweepIsCaughtByTheConfirmation` runs identical steps with only the order changed.

**Whichever of the two lands second owns this.** Retire this note then: invert the assertion, drop
the ordering caveat from `ShardManager.onFailure`'s javadoc, and correct the solutions write-up's
"astubbs#431 is an open draft" framing.

## A correction worth carrying

The in-flight note that first recorded the re-queue orphan describes astubbs#431's declining sweep as
shipped. It is not - astubbs#431 is an open draft and master is still shard-first. The defect was
reproduced on master regardless, so nothing about its reachability depended on the wrong version.
