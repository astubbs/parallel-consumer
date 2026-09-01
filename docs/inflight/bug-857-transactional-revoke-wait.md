# confluentinc#857 family: the unbounded revoke wait in transactional mode

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->

**Commit mode: `PERIODIC_TRANSACTIONAL_PRODUCER` only.** This is the discriminator - the defect below
<!-- post-merge: checked -->
and the AB-BA deadlock in astubbs#29 are in mutually exclusive modes and cannot be the same bug.

## The defect - TWO unbounded waits, not one

`AbstractParallelEoSStreamProcessor.onPartitionsRevoked` held the poll thread in **two** waits with
<!-- post-merge: checked -->
no deadline, on master, predating astubbs#29. Every earlier version of this note named only the
first, which is the correction that matters most here:

1. **The spin**, added by confluentinc#548:

   ```java
   while (isTransactionCommittingInProgress())
       Thread.sleep(100); //wait for the transaction to finish committing
   ```

   The predicate is `producerTransactionLock.isWriteLocked()`, gated on
   `options.isUsingTransactionCommitMode()`, so it runs in transactional mode only - and there it is
   the *common* case, not a rare race: the control thread takes that write lock in
   `maybeAcquireCommitLock()` before every commit.

2. **The commit beneath it.** `commitOffsetsThatAreReady()` reaches
   `ProducerManager#acquireCommitLock`, whose `writeLock.tryLock(commitLockAcquisitionTimeout)` waits
   **five minutes** by default. **This is the one confluentinc#803 actually threw from** - its stack
   trace is `TimeoutException: Timeout getting commit lock (which was set to PT5M)` raised *inside*
   `onPartitionsRevoked`, and `grep -rn "Timeout getting commit lock"` finds exactly one throw site.
   The reporter's timeline confirms it: rebalance at t=0, `max.poll.interval.ms` eviction at 180s,
   the throw at 300s = `commitLockAcquisitionTimeout`.

**Fixing only the spin would have left the user's report reproducible**, which is why the earlier
plan of record - "delete the `KNOWN_BLOCKING_VIOLATIONS` entry and the rule goes green" - was not a
sufficient acceptance criterion. See the ArchUnit gap below.

Both run on the poll thread inside `poll()`, so both are bounded by nothing but
`max.poll.interval.ms`. Overrunning it evicts the member.

## Why this is not astubbs#29's deadlock <!-- post-merge: checked -->

The AB-BA cycle's second edge lives in `ConsumerOffsetCommitter`, which `BrokerPollSystem` constructs
**only** for the consumer-commit modes (`switch (options.getCommitMode())`, the
`PERIODIC_CONSUMER_SYNC, PERIODIC_CONSUMER_ASYNCHRONOUS` arm). In transactional mode there is no
request queue, no response queue and no `commitAndWait()` - **the cycle cannot occur here**.
<!-- post-merge: checked -->
astubbs#29's `tryLock()` change does not touch `:418-419` and cannot fix this.

Two different locks are both called "commit lock", which is part of why this was conflated: the
`commitCommand` monitor guarding consumer commit execution, and the producer transaction lock behind
`maybeAcquireCommitLock()` / `commitLockAcquisitionTimeout` (5 min default). This defect is the
latter.

## Sighting: `RebalanceEoSDeadlockTest`, 1 failure in 20, 2026-07-30

Local fork16 stress hunt on astubbs#80's branch (master-like code). Recorded in the original family
ledger as *"Live confirmation the deadlock is still present"* - see
`test-load-tightness-flakes.md`, where it is explicitly *not* a member.

**That attribution was wrong, and the correction is the point of this file.**
`RebalanceEoSDeadlockTest` runs `PERIODIC_TRANSACTIONAL_PRODUCER`
(`.commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)`), the mode in
<!-- post-merge: checked -->
which the AB-BA cycle cannot close. So the failure is **not** evidence for astubbs#29.

It is, however, a **real** failure and it is evidence for the block above. The run was on
master-family code, where the test's latch was still reachable - the latch-unreachable defect
(the revoke path calling the private `tryCommitOffsetsOnRevoke()` instead of the overridden
<!-- post-merge: checked -->
`commitOffsetsThatAreReady()`) only voids runs on **astubbs#29's branch**. So this sighting survives
the correction; only its attribution moves.

No seed was captured.

## User-facing report

**astubbs#44 (confluentinc#803)** - *"Transactional Producer instance gets timeout getting commit lock
while second instance starts"* - matches this mechanism exactly: second instance joins, rebalance
fires, poll thread spins here, `max.poll.interval.ms` is breached, the group reports *"group is
already rebalancing"*, and the run ends on `commitLockAcquisitionTimeout`.

It is the **only** issue on the upstream tracker ever labelled *verified bug*. It was re-triaged off
<!-- post-merge: checked -->
astubbs#29 and onto this block on 2026-08-18; its `pr-available` label was removed, because no open
PR addresses it.

## Decision, settled 2026-09-01: decline, do not deadline

**The revoke path takes the producer write lock with a bare `tryLock()` and skips the commit when it
cannot have it.** Both waits above are gone: the spin is deleted, and the commit is only attempted
once the lock is already held, so `acquireCommitLock`'s five-minute wait is never reached with the
lock contended.

**Why not deadline the holder**, which was the standing candidate. Two independent reasons, one of
them measured rather than argued:

- **A per-transaction bound cannot work.** `Revoke857TransactionalWaitProbeIT`'s defect arm held the
  poll thread **79s** from a dwell of only **20s**. With a 1s commit interval the control thread
  re-takes the write lock as fast as it drops it, so the waiter is starved across *successive*
  transactions. Bounding any one transaction leaves that untouched; the deadline has to sit on the
  wait, and the shortest correct deadline is none.
- **It trades a stall for a crash.** Deadlining the holder means aborting a transaction it owns, and
  `ProducerFencedException` is still fatal - see
  [`core-recoverable-producer-fencing.md`](core-recoverable-producer-fencing.md) and astubbs#225.
  **Declining does not touch that**: when it declines, no transaction operation happens at all, so
  astubbs#225 is not a blocker for this design. That asymmetry is the deciding practical difference.

**What declining costs.** The offsets were never marked committed, so they stay dirty and travel to
the partitions' new assignee, which reprocesses them. That is at-least-once doing its ordinary job -
already the published contract for the consumer lane in the rebalance-behaviour feature record
(*"declines rather than blocks ... and declining is what keeps the callback inside
max.poll.interval.ms"*), and the same disposition astubbs#317's `tryCommitOffsetsOnRevoke` takes
there. That record lives on astubbs#29's branch, not master, so it is named rather than cited -
and **its transactional boundary needs updating when this lands**: it currently says the
transactional revoke wait "is currently unbounded". This change applies the identical rule to the other lock the transactional mode uses.

**It is also what Kafka does.** Kafka Streams commits inline in `onPartitionsRevoked`
(`TaskManager.handleRevocation` -> `taskExecutor.commitOffsetsOrTransaction`, whose comment says it
must not skip the commit because a rebalance is in progress) - but it can only do that because one
`StreamThread` owns the consumer *and* the producer, so there is no lock to wait on. When its own
bound (`max.block.ms`) expires it falls back to `closeDirtyAndRevive`: abandon and replay. PC cannot
have the first half without the ownership change, so it takes the fallback as its normal path.

**Ruled out, do not re-propose:** the poll thread aborting the transaction itself. `ProducerManager`
enforces single-writer from the control thread and throws `ConcurrentModificationException`
(grep `is not safe for multi-threaded access`).

**The successor, not superseded by this:** unify who owns the consumer, so the callback can commit
inline as Streams does - confluentinc#200 / astubbs#142, still open. The architecture write-up's
verdict stands: *"mode-conditional thread topology is the root hazard ... a fix should unify who
commits across modes, or unify who owns the consumer."* Declining removes the stall; it does not
remove the split that produced it.

## The ArchUnit acceptance criterion is not sufficient as written

`ArchitectureTest.rebalanceCallbacksMustNotBlock` exempts this callback in
`KNOWN_BLOCKING_VIOLATIONS` with *"remove this entry when that lands"*. **Removing it is necessary
but does not prove the fix**, for two independent reasons, and the rule lives on astubbs#29's branch
so neither can be repaired from here:

- **`Lock.tryLock(long, TimeUnit)` is not in `BLOCKING_CALLS`.** The rule's own message says
  *"Decline (tryLock) rather than wait"*, treating tryLock as the cure - but a five-minute timed
  acquire is waiting, and it is the call confluentinc#803 threw from. Timed acquires need adding.
- **The transitive walk stops at the interface.** It resolves `call.getTarget().resolveMember()`,
  and `committer` is declared as `OffsetCommitter`, so the walk never descends into
  `ProducerManager#acquireCommitLock` at all.

Until both are fixed, **the probe is this defect's acceptance test**, not the ArchUnit rule.
