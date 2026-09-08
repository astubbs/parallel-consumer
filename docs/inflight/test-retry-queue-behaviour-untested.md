# `RetryQueue`'s re-add and two-map invariant are unasserted - could be hiding bugs

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-vetted: 2026-09-08 - applied: shrunk to the two unasserted bullets (re-add with a different retry-due, and the unique/sorted invariant), title and opening rewritten so the stale "only coverage is three tests in ShardManagerTest" is gone; checked: RetryQueueTest, RetryQueueLincheckTest, RetryQueueRequeueWindowTest and RetryQueueIteratorConfinementTest all exist and none of their test methods touches either surviving bullet, and `ProcessingShard.getWorkIfAvailable`'s sweep now calls `removeWorkAtOffset` then `retryQueue.remove` -->

`RetryQueue` is no longer bare: `RetryQueueTest` pins the `removeAll` contract, `RetryQueueLincheckTest`
stresses its locking, `RetryQueueIteratorConfinementTest` holds the iterator to its owning thread, and
`RetryQueueRequeueWindowTest` covers the rebalance/revoke window - including the ordering
`removeWorkFromShardFor` depends on. `ShardManagerTest`'s three `retryQueueOrdering` tests remain, and
still test ordering only.

Two of the four originally-listed gaps survive all of that, and both are about the queue's own
internal consistency rather than its interaction with the shard:

- **`add` is last-write-wins**, replacing the sort key and re-inserting (`unique.put` then
  `sorted.remove`/`sorted.put`). Nothing asserts what happens when the same offset is added twice with
  *different* retry-due times, which is the normal shape after a retry is scheduled.
- **The two-map invariant itself.** `unique` (uniqueness by topic/partition/offset) and `sorted`
  (ordering by retry-due) must stay in step; only `add`/`remove` maintain that, and no test asserts
  they cannot diverge - e.g. after `clear`, after a removal of an absent element, or under the
  interleaving the shard's two removal paths create.

The other two are closed. The shard/queue consistency gap after a stale removal is gone with the
defect behind it: `ProcessingShard.getWorkIfAvailable`'s inline sweep now calls `removeWorkAtOffset`
and then `retryQueue.remove` on what it returned, rather than `iterator.remove()` alone. Behaviour
under revoke is what `RetryQueueRequeueWindowTest` was written for.

Why this is worth writing down rather than leaving to a code reader: the class carries its own
uniqueness/ordering invariant across two collections, it is on the retry path (so a defect shows up
as *records retried late or never*, not as an exception), and the counters it feeds gate the poller.
A wrong answer here is quiet.

Surfaced while reviewing astubbs/parallel-consumer#31; nothing left here is that PR's doing.
