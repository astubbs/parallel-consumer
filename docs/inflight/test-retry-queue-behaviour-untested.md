# `RetryQueue` behaviour is essentially untested - could be hiding bugs

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->


The only coverage is three tests in `parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/state/ShardManagerTest.java`:
`retryQueueOrdering`, `testRetryQueueOrdering`, `testRetryQueueOrderingMultipleTries`. All three test
**ordering only**. Nothing tests the queue's consistency with the shard it mirrors.

Untested behaviour, in rough priority:

- **Shard/queue consistency after a stale removal.** `ShardManager.removeStaleContainers` cleans both
  (`// remove stale containers from both processingShards and retryQueue`, mapping `retryQueue::remove`),
  but `ProcessingShard.getWorkIfAvailable`'s inline stale removal calls `iterator.remove()` alone. A
  known orphan follows from that asymmetry - see `docs/refactoring.md`, `state/ProcessingShard.java`.
  Not record loss; it inflates `getQueueSizeAndNumberReadyToBeRetried` and so the awaiting-selection
  count that gates the poller throttle.
- **`add` is last-write-wins**, replacing the sort key and re-inserting (`unique.put` then
  `sorted.remove`/`sorted.put`). Nothing asserts what happens when the same offset is added twice with
  *different* retry-due times, which is the normal shape after a retry is scheduled.
- **The two-map invariant itself.** `unique` (uniqueness by topic/partition/offset) and `sorted`
  (ordering by retry-due) must stay in step; only `add`/`remove` maintain that, and no test asserts
  they cannot diverge - e.g. after `clear`, after a removal of an absent element, or under the
  interleaving the shard's two removal paths create.
- **Behaviour under revoke**, where `removeWorkFromShardFor` removes from the shard and then from the
  queue only `if (Objects.nonNull(removedWC))`.

Why this is worth writing down rather than leaving to a code reader: the class carries its own
uniqueness/ordering invariant across two collections, it is on the retry path (so a defect shows up
as *records retried late or never*, not as an exception), and the counters it feeds gate the poller.
A wrong answer here is quiet.

Surfaced while reviewing astubbs/parallel-consumer#31; the orphan is pre-existing and independent of
that PR.

## Update 2026-09-08 - two of the four items are now covered, beside the dated text above

The list above is left as written; what follows says which parts of it have stopped being true, and
by what.

- **"Shard/queue consistency after a stale removal"** is covered from a different direction than the
  one this note expected. `ShardManager.removeStaleContainers` no longer cleans the queue at all - it
  runs inside a rebalance callback on the broker-poll thread, and the queue's write lock is unbounded
  and fair - so consistency is now maintained by `ShardManager.purgeDepartedRetryEntries()` on the
  controller thread, which collects any entry whose container is resident in no shard. The asymmetry
  the bullet describes is therefore gone by removal rather than by symmetry, and
  `RetryQueueRebalancePathTest` is what asserts it. The design and both alternatives:
  [`../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md`](../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md).
- **"Behaviour under revoke"** is covered by that same test class, and the `if (Objects.nonNull(...))`
  the bullet quotes no longer guards a queue removal.
- **`add` is last-write-wins** and **the two-map invariant** are still uncovered by anything this
  note would recognise, except that `RetryQueueTest` and `RetryQueueLincheckTest` arrived in the
  meantime and cover the `removeAll` contract and the concurrent interleavings respectively. Read
  those before writing anything new here.
