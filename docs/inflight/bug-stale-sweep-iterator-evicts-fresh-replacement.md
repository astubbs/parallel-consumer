# The poller's stale sweep removes by KEY, so it can evict a fresh replacement

<!-- inflight-type: bug -->
<!-- inflight-impact: data-loss -->
<!-- inflight-labels: concurrency -->
<!-- inflight-vetted: 2026-09-08 - applied: shrunk to the lost-record half; the mechanism sentence now names the code as it stands and the astubbs#373 counter/stall block is removed as closed by astubbs#336 (3e668a448), which the note's own last paragraph already contradicted; checked: `ProcessingShard.removeStaleWorkContainersFromShard` walks `workMap.entrySet()` and calls `removeWorkAtOffset(entry.getKey())`, accounting for the object actually evicted, and its comment names this note as the owner of what is left -->

`ProcessingShard.removeStaleWorkContainersFromShard` walks `workMap.entrySet()` and, for each stale
occupant, calls `removeWorkAtOffset(entry.getKey())`. **That removes by key** - it is not a
remove-if-still-mapped-to-this-value - so whatever is at that offset when the removal lands is what
leaves the shard, not necessarily what the iterator returned.

The two sides genuinely run on different threads: the sweep is reached from
`PartitionStateManager.onPartitionsRemoved` / `onPartitionsAssigned`, i.e. inside the rebalance
callback on the **broker-poll** thread, while the racing writer - `addWorkContainer`'s
stale-replacement branch - runs on the **controller**.

    poller     : entry = next()                      -> (100, staleWc); isWorkContainerStale -> true
    controller : addWorkContainer(freshWc)           -> workMap.put(100, freshWc), accounting settled
    poller     : removeWorkAtOffset(100)             -> evicts freshWc, by key

**The harm is the lost record.** `freshWc` is gone from the shard while `PartitionState` still
carries its offset as incomplete, so nothing selects it again until the partition is re-polled. That
is the same fresh-replacement-at-the-same-offset class as
[`docs/solutions/logic-errors/stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md`](../solutions/logic-errors/stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md),
reached from the other side.

**The accounting half is closed and is not this note's subject.** The sweep no longer calls
`iterator.remove()`: since astubbs#336 (`3e668a448`) it removes through the map's own return value,
so the population retirement and the claim release both follow the object that actually left, and the
caller is handed that object rather than the one the sweep inspected. What is left open is the
eviction of the fresh record itself.

**Why it was not fixed there.** There is no identity-keyed removal on the map to reach for.
`workMap.remove(key, value)` and `entrySet().remove(entry)` both compare with `equals`, and
`WorkContainer.equals` is topic/partition/offset only - so the fresh container compares *equal* to
the stale one it replaced, which is the same reason `ProcessingShard.includeInSelection` compares
with `!=`. A get-then-remove guarded by reference identity is still a check-then-act. Closing this
properly means deciding how the sweep and the replacement branch coordinate at all, which is a
<!-- post-merge: checked -->
redesign rather than a review edit - the same call that produced astubbs#373 itself.

**Decision needed:** whether the sweep should hold the shard against concurrent replacement, or
whether `addWorkContainer` should refuse to replace an occupant the sweep has already claimed.
Either way it wants a deterministic white-box test first: take the entry-set iterator, call
`next()`, run `addWorkContainer` for a fresh container at that offset, then let the sweep remove by
key and assert which container is resident afterwards.

Adjacent and NOT this: the inline stale eviction that removed a container from its shard while
leaving its retry-queue entry behind - an unpaired removal between two structures, same
neighbourhood, different defect. It is fixed and its note retired; the trace is at
`git show a80f2bbd1:docs/inflight/bug-retry-queue-orphaned-by-inline-stale-removal.md`.
