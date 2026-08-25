# Bug: the shard's available-work counter is still an approximation

<!-- inflight-type: bug -->
<!-- inflight-impact: correctness -->
<!-- inflight-labels: needs-measurement -->

What is left of `bug-available-work-counter-needs-a-clamp.md` after the load gate stopped depending on
`ProcessingShard.availableWorkContainerCnt`. **The clamp is gone and the gate is now conservation-derived**
(`ShardManager.getNumberOfRecordsInShards`, `RecordPopulation`); two conditional-decrement defects that caused
the drift were fixed at the same time. What follows is what that change deliberately did *not* do.

## The counter still describes a predicate it does not own

`availableWorkContainerCnt` still claims to be "how many of this shard's entries are selectable", and it still
is not, by design: `markAvailableAgain()` counts a failed record back in *before* its retry delay has passed,
and `ShardManager.getNumberOfWorkQueuedInShardsAwaitingSelection()` nets that out against the retry queue
rather than the shard doing so. So the shard's own number is only meaningful in that aggregate.

That aggregate still feeds three things - the `WAITING_RECORDS` metric, the under-served-retrieval diagnostic
in `ShardManager.getWorkIfAvailable`, and `AbstractParallelEoSStreamProcessor.drain()` via
`WorkManager.isRecordsAwaitingProcessing()`. **The drain one is the one that can still hurt**: drift high and
`drain()` may not transition to closing; drift low and it may close early. It is `||`-ed with
`areMyThreadsDone()`, which is what has been covering for it.

**The candidate fix is to derive it too**, from the same population minus an authoritative in-flight count. The
blocker is that in-flight is toggled by `WorkContainer.onQueueingForExecution()` / `endFlight()`, which the
container does without telling its shard, and `endFlight()` runs *before* shard removal on the success path -
so no local predicate at a removal site recovers "was this record counted". `retireAndDeductIfStillCounted`
uses `isNotInFlight()` and is correct only because the one case it gets wrong (a stale result handed back for
a record still in a shard) cannot occur: revocation removes the entry before the result returns.

## And the second counter in the same calculation is untouched

`WorkManager.numberRecordsOutForProcessing` - the `confluentinc#857` counter-drift signature named in
`isSufficientlyLoaded()`'s comment - is a plain `int` mutated from the control thread on five paths. It no
longer gates record intake, but it still drives `hasWorkInFlight()`, `isWorkInFlightMeetingTarget()` and the
`INFLIGHT_RECORDS` metric. **Two drifting counters feeding one throttle decision was the parallel-state shape;
one of them has been dissolved, the other has not.**

## What to do first

1. **Measure whether the remaining approximation ever bites.** An invariant check in a chaos or stress run -
   `sumOfShardAvailableCounters()` versus a scan of selectable entries - would say whether the aggregate
   netting is enough in practice. `ShardManager.sumOfShardAvailableCounters()` and `countRecordsInShardsByScan()`
   exist for exactly this and are already used by `WorkManagerTest`.
2. **Only then decide whether to derive it.** Replacing state that turns out never to disagree is a refactor;
   replacing state that does is a fix, and the two deserve different amounts of risk.
