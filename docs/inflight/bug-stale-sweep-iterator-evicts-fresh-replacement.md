# The poller's stale sweep removes by KEY, so it can evict a fresh replacement

<!-- inflight-type: bug -->
<!-- inflight-impact: data-loss -->
<!-- inflight-labels: concurrency -->

`ProcessingShard.removeStaleWorkContainersFromShard` walks `entries` with an entry-set iterator and
calls `iterator.remove()` on each stale occupant. **`ConcurrentSkipListMap`'s iterator removes by
key, unconditionally** - it is not a remove-if-still-mapped-to-this-value - so whatever is at that
offset when `remove()` lands is what leaves the shard, not necessarily what `next()` returned.

The two sides genuinely run on different threads: the sweep is reached from
`PartitionStateManager.onPartitionsRemoved` / `onPartitionsAssigned`, i.e. inside the rebalance
callback on the **broker-poll** thread, while the racing writer - `addWorkContainer`'s
stale-replacement branch - runs on the **controller**.

    poller     : entry = iterator.next()            -> (100, staleWc); isWorkContainerStale -> true
    controller : addWorkContainer(freshWc)          -> entries.put(100, freshWc), accounting settled
    poller     : iterator.remove()                  -> evicts freshWc, by key
    poller     : uncount(entry.getValue())          -> releases staleWc, which holds nothing

**The primary harm is the lost record**, not the counter: `freshWc` is gone from the shard while
`PartitionState` still carries its offset as incomplete, so nothing selects it again until the
partition is re-polled. That is the same fresh-replacement-at-the-same-offset class as
[`docs/solutions/logic-errors/stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md`](../solutions/logic-errors/stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md),
reached from the other side.

<!-- post-merge: checked-begin -->
**The secondary harm changed direction on astubbs#373, which is why this is written down now.**
Before that PR the branch decremented unconditionally and the replacement branch incremented not at
all, so this interleave left the shard counter one *low* - and the floor-at-zero clamp resynced it
whenever the shard drained. astubbs#373 deleted the clamp and made the replacement branch count its
fresh entry, so the same interleave now leaves the counter one *high*, permanently, for a container
that is no longer resident. High is the worse direction: `ShardManager.workIsWaitingToBeProcessed()`
stays true, so `AbstractParallelEoSStreamProcessor.drain()` never reaches `transitionToClosing()`.
<!-- post-merge: checked-end -->

**Why it was not fixed there.** There is no identity-keyed removal on the map to reach for.
`entries.remove(key, value)` and `entrySet().remove(entry)` both compare with `equals`, and
`WorkContainer.equals` is topic/partition/offset only - so the fresh container compares *equal* to
the stale one it replaced, which is the same reason `ProcessingShard.countAsSelectable` compares
with `!=`. A get-then-remove guarded by reference identity is still a check-then-act. Closing this
properly means deciding how the sweep and the replacement branch coordinate at all, which is a
<!-- post-merge: checked -->
redesign rather than a review edit - the same call that produced astubbs#373 itself.

**Decision needed:** whether the sweep should hold the shard against concurrent replacement, or
whether `addWorkContainer` should refuse to replace an occupant the sweep has already claimed.
Either way it wants a deterministic white-box test first: take the entry-set iterator, call
`next()`, run `addWorkContainer` for a fresh container at that offset, then call `iterator.remove()`
and assert both what is resident and that `getCountOfWorkAwaitingSelection()` agrees with the units
actually held.

Adjacent and NOT this: `docs/inflight/bug-retry-queue-orphaned-by-inline-stale-removal.md` - an
unpaired removal between two structures, same neighbourhood, different defect.
