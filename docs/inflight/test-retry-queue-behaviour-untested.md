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

## Update 2026-09-08 - two sentences above name a mechanism that has since gone

The two surviving bullets are unchanged and still open; this corrects only the reasons given for the
other two being closed, because both named code that no longer exists.

- The opening paragraph credits `RetryQueueRequeueWindowTest` with covering "the ordering
  `removeWorkFromShardFor` depends on". That method no longer touches the retry queue at all - it
  runs on the broker-poll thread inside a rebalance callback, and the queue's write lock is unbounded
  and fair, so it removes from the shards only. There is no ordering left for a test to depend on.
- "The shard/queue consistency gap after a stale removal is gone with the defect behind it" is still
  true, and now doubly so: `ShardManager.purgeDepartedRetryEntries()` collects, once per control-loop
  pass on the controller thread, any retry-queue entry whose container is resident in no shard. The
  inline sweep's paired `retryQueue.remove` is kept, but it is no longer the only thing standing
  between a departed container and a permanent orphan.

`RetryQueueRebalancePathTest` is the class that asserts both. The design, and the one it superseded:
[`../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md`](../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md).
