# `RetryQueue` behaviour is essentially untested - could be hiding bugs

<!-- inflight-class: blind-spot -->


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
