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

## Update 2026-09-03 - what is closed, and what the closures moved

The text above is left as written; the corrections go here, beside it.

**The opening claim is out of date.** `RetryQueueTest` (the `removeAll` return contract, and now
`tryRemove`) and `RetryQueueLincheckTest` (the concurrent half) both exist, so "the only coverage is
three ordering tests" has not been true since they landed.

**Shard/queue consistency after a stale removal - closed, and its stated cause was already gone.**
The asymmetry the bullet names no longer exists: `ProcessingShard.getWorkIfAvailable`'s inline stale
branch calls `retryQueue.remove` and is pinned by
`ShardPopulationRaceTest.theInlineStaleSweepTakesTheRecordOutOfTheRetryQueueToo`. The
`docs/refactoring.md` entry it pointed at was stale on the same facts and has been retired. What was
still missing was consistency under *contention*, which is where the pair could actually split, and
`RetryQueueRebalancePathTest` now asserts it on both removal paths - the revoke sweep and the
epoch-change stale sweep.

**Behaviour under revoke - closed, and the `Objects.nonNull(removedWC)` guard the bullet names is
gone with it.** The revoke path now asks the queue first, keyed by the record's coordinates rather
than by a container, and abandons the shard removal too when refused. Reasoning and the reopen
conditions:
[`../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md`](../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md).

**Update later the same day - the retirement half of that consistency claim is now driven too.**
Asserting the pair stays whole under a refusal only covers half the contract; the other half is that
something later takes the whole pair away, and nothing observed that after a refusal. Three cases in
`RetryQueueRebalancePathTest` now do - refuse, release the read lock, make one controller-thread work
request, and assert the container has left the retry queue AND the shards - one per refusal site plus
the ordered-mode shape. Checked by deleting the retry-queue removal from
`ProcessingShard.getWorkIfAvailable`'s last-resort branch, which turns exactly those three red.

The ordered-mode one recorded a measurement rather than a defect: under KEY or PARTITION ordering the
shard scan breaks as soon as it takes a container, so a stale entry at a higher offset than a takeable
one waits for that head to leave the shard rather than being retired on the next tick. The pair stays
whole throughout, so it is a delay and not an orphan. `ShardManager.removeWorkFromShardFor` owns the
statement of it, under "what would reopen this"; do not restate it here.

**Still open, and the reason this note stays:**

- **`add` is last-write-wins with a *changed* retry-due time.** Untouched - and it is the normal shape
  after a retry is scheduled, so it is the highest-value gap left.
- **The two-map invariant on the paths Lincheck does not cover** - `clear` in particular, which has no
  production caller at all and so is exercised by nothing.
