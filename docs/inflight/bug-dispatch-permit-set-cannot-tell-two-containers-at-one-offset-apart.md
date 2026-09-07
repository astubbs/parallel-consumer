# `ExternalEngine`'s dispatch-permit set cannot tell two containers at one offset apart, so it leaks permits

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->

<!-- post-merge: checked-begin -->
Found by the same-defect-class sweep on astubbs/parallel-consumer#468, which fixed the shard sweep's
by-key removal. **Not fixed there** - it is a different structure, on the async engines
(vertx/reactor/mutiny), and it wants its own reproduction.
<!-- post-merge: checked-end -->

`ExternalEngine.holdingDispatchPermit` is a `ConcurrentHashMap.newKeySet()` of `WorkContainer`, and
`WorkContainer.equals` is topic, partition and offset only. The permit accounting is per **record**
and the set is per **offset**, and those stop agreeing the moment two containers exist for one
offset:

    takeDispatchCapacity : dispatchCeiling.tryAcquire(batch.size())   -- one permit PER RECORD
                           holdingDispatchPermit.addAll(batch)        -- a Set: an equal member is a no-op
    addToMailbox         : if (holdingDispatchPermit.remove(wc)) dispatchCeiling.release()

Two containers at one offset coexist exactly as they do in the shard: one is dispatched, its
partition is revoked and reassigned so it goes stale mid-flight, and the fresh container for that
offset is polled and dispatched while the first is still out at a worker. The second `addAll` finds
an equal member and adds nothing, so one permit is acquired with no set membership to return it. The
first completion removes the single entry and releases one; the second finds nothing and releases
none.

**The consequence is a monotonic leak of the dispatch ceiling** - one permit per collision, never
recovered - so `takeDispatchCapacity` eventually cannot acquire and the async engines stop
dispatching. It is the `confluentinc#857` symptom family (progress stops and stays stopped) reached
by yet another route.

<!-- post-merge: checked-begin -->
**Not reproduced.** The reachability argument above is read off the code, and the deliberate next
step is a deterministic reproduction before any fix - the shard defect this came from was talked
about for weeks and only became tractable once astubbs/parallel-consumer#468 pinned it with a seam.
`ShardSeamTestBase` is the shape to copy, not the seam itself.
<!-- post-merge: checked-end -->

**Two candidate fixes, and the second closes the class rather than the instance:**

<!-- post-merge: checked-begin -->
- Key the set by coordinates *and* identity - an identity-keyed set (`Collections.newSetFromMap(new
  IdentityHashMap<>())` is not concurrent; `ConcurrentHashMap.newKeySet()` keyed on a wrapper is),
  which is the same move astubbs/parallel-consumer#468 made in the shard with
  `ProcessingShard.Residency`.
<!-- post-merge: checked-end -->
- Give `WorkContainer` identity equality, which fixes this and the shard together and deletes both
  tokens. It is breaking (public class, `compareTo` orders by offset), so it is queued in
  [`docs/refactoring.md`](../refactoring.md) under the next major.

The class itself - *a collection of `WorkContainer` that has to distinguish two occupants of one
offset, and cannot* - is written up in
[`docs/solutions/logic-errors/a-by-key-removal-cannot-say-which-container-it-meant-2026-09-07.md`](../solutions/logic-errors/a-by-key-removal-cannot-say-which-container-it-meant-2026-09-07.md).
