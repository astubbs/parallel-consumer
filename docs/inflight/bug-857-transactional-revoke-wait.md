# confluentinc#857 family: the transactional revoke wait, and the decision behind its fix

<!-- post-merge: checked-begin - written to read the same once astubbs/parallel-consumer#408 is a
     merged PR rather than an open one; no "delete this when it merges" marker, which
     docs/inflight/AGENTS.md forbids -->
> **The wait is no longer unbounded.** astubbs/parallel-consumer#408 made the revoke path decline
> both locks rather than wait on either, and the two-sites correction below is why bounding only the
> spin would not have been enough. What is still open is named in
> [`core-revoke-commit-skips-the-work-mailbox-drain.md`](core-revoke-commit-skips-the-work-mailbox-drain.md)
> and in the successor this does not remove - unifying consumer ownership, confluentinc#200 /
> astubbs#142. The decision record below outlives this note and belongs in `docs/solutions/`.
<!-- post-merge: checked-end -->

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

It carries upstream's *verified bug* label. **Earlier versions of this note called it the ONLY such
issue, which is false** - a couple of dozen upstream issues carry that label, and the claim
propagated from here into a roadmap entry, a plan, several notes and a PR body before anyone ran
`gh issue list -R confluentinc/parallel-consumer --state all --label "verified bug"`. The label
still matters - a maintainer confirmed the report rather than merely triaging it - but it does not
make this issue unique. It was re-triaged off
<!-- post-merge: checked -->
astubbs#29 and onto this block on 2026-08-18; its `pr-available` label was removed, because no open
PR addresses it.

## Re-settled 2026-09-02: decline stands, and astubbs#225 is what settled it

<!-- post-merge: checked - records a decision between two pieces of work and how it was reached, and
     reads the same once both have landed -->
**Earlier the same day this section put the decision on hold**, because "decline, do not deadline"
rested on two legs and astubbs#225 was about to remove one: a per-transaction bound cannot fix
starvation across successive transactions (measured, still true), *and* deadlining the holder meant
aborting a transaction while `ProducerFencedException` was fatal. With fencing recoverable, a bounded
wait in the revoke path looked like the better end state - it is Kafka Streams' shape (inline commit
bounded by `max.block.ms`, `closeDirtyAndRevive` on expiry), and it is what `RebalanceEoSDeadlockTest`
asserted.

**astubbs#225's plan, on astubbs#410, took the question up by name and answered it.** Its KTD11
makes the revoke commit a third detection site for a recoverable condition - recorded and declined on
the poll thread, never rethrown as fatal and never waited out - and states that once recovery bounds
the write-locked region to abort, close, drain and replay, **neither a bounded wait nor deadlining the
holder is needed for liveness**. Both stay viable and both are deliberately not taken. So the fencing
objection is gone, and so is the reason to want a wait. What is left is a trade: a bounded wait buys
fewer redeliveries on a transactional rebalance, at the price of an ArchUnit exemption with a stated
bound, and with the starvation measurement still standing against it. Decline stays.

**What that costs, stated once more so nobody rediscovers it as a regression:** with the overlap the
deadlock test forces - control holding the producer write lock when the revoke lands - the revoke
path declines, `wm.onPartitionsRevoked` truncates the revoked partitions, and their completed but
uncommitted work is redelivered to the next owner. That is the at-least-once contract doing its job,
and it is the "always declines on a busy instance" consequence `docs/features/rebalance-behaviour.yaml`
records. It is not a loss of data; it is a replay.

**`RebalanceEoSDeadlockTest` is amended accordingly, not weakened.** It used to assert that the
group's committed offsets for the revoked partitions had advanced by the time the callback returned,
which on master held *because of* the unbounded spin - the callback waited the control commit out.
It now asserts what the confluentinc#541 guard actually needs plus the new contract: the callback
returns well inside the forced dwell (so it neither deadlocked nor waited the commit out), and the
window was resolved without blocking - either the revocation declined (observed by counting, the same
instrument the probe uses) or, if the dwell had already ended, it committed inline and the offsets
moved. The vacuity guard on the forced overlap is unchanged. Survival and liveness are unchanged.

<!-- post-merge: checked-begin - names the PR that widened the rule, which reads the same once merged -->
**A trap for whoever builds the bounded version, should the trade ever be re-argued.** The ArchUnit
widening on astubbs#408 adds `Lock.tryLock(long, TimeUnit)` and siblings to `BLOCKING_CALLS`, so a
bounded wait will trip the rule. It cannot distinguish a 2s bound from the 5-minute one that caused
confluentinc#803 - a duration is invisible to a static walk - so the site needs an explicit exemption
with a "bounded well under max.poll.interval.ms" justification, or the rule needs a way to see the
bound.
<!-- post-merge: checked-end -->

<!-- post-merge: checked-begin - records a reconciliation already made; it reads the same once both
     PRs have landed -->
**Reconciled with astubbs#410, 2026-09-02, on this branch.** The `ProducerFencedException |
InvalidProducerEpochException` rethrow in `tryCommitOffsetsOnRevoke` is gone: on the PC-built path
the commit path converts those into `ProducerInvalidatedException`, which the generic catch logs and
the control thread recovers from on its next pass, and on the deprecated producer-instance path
nothing is recorded and the raw condition is logged the same way. The rethrow had only ever fired for
a raw fence from `commitTransaction`, and nothing pinned it. **A correction to what this branch said
about it earlier:** the rethrow was added on this branch on the claim that master failed the instance
loudly at this site. It did not - master's revoke catch was already the generic WARN, and master's
`commitTransaction` threw raw into it - so deleting the rethrow restores master exactly, and the
instance path's "keeps its pre-recovery behaviour" is master's behaviour, which astubbs#410 defers by
design (its R19). Found by the code review's validator on 2026-09-02.

**The reconciliation was half true until a test was written for it.** Record-and-decline held. "The
control thread recovers on its next pass" did not: the revoke path produces no mailbox event, so
nothing woke the control loop, which sat out the rest of its commit-interval wait with every worker
parked on the produce lock - invisible at the 100ms transactional default, an hour at an hour. The
revoke path now calls `notifySomethingToDo()` after releasing both locks when the manager reports it
is replacing. Pinned by
`ProducerRecoveryTest.fencedDuringTheRevokePathCommitIsRecordedAndDeclinedThenRecoveredByTheControlThread`,
which failed on exactly that wait before the call existed. The other side of the reconciliation is
recorded in [`core-recoverable-producer-fencing.md`](core-recoverable-producer-fencing.md).
<!-- post-merge: checked-end -->

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

## The ArchUnit acceptance criterion - repaired, and now actually met

`ArchitectureTest.rebalanceCallbacksMustNotBlock` exempted this callback in
`KNOWN_BLOCKING_VIOLATIONS` with *"remove this entry when that lands"*. **That entry is now deleted
and the rule is green** - but removing it alone would have been a false pass, because the rule could
not see the call that actually threw. Two defects were found and one is fixed:

- **FIXED: timed acquires were invisible.** `BLOCKING_CALLS` listed only unbounded primitives, and
  the rule's own message - *"Decline (tryLock) rather than wait"* - reads as if `tryLock` were the
  cure. A five-minute `tryLock(commitLockAcquisitionTimeout)` is waiting, and it is the call
  confluentinc#803's stack trace threw from. The timed overloads of `Lock`, `ReentrantLock`, both
  `ReentrantReadWriteLock` locks, `CountDownLatch.await`, `Future.get` and `BlockingQueue.poll` are
  now denied; the no-arg `tryLock()`, which *is* the cure, is deliberately not.
  **Verified by negative control**: restoring the five-minute acquire turns the rule red naming
  `ProducerManager.tryAcquireCommitLockForRevocation()` as the path.
- **STILL OPEN, and narrower than first thought: the walk stops at an interface.** It resolves
  `call.getTarget().resolveMember()`, which yields the *declared* member - so a call through an
  interface-typed field never descends into the implementation. `committer` is declared as
  `OffsetCommitter`, so the walk cannot follow `committer.retrieveOffsetsAndCommit()` into
  `ProducerManager`. This did **not** hide the defect above, because the revoke path now reaches
  `ProducerManager` through a concrete reference and the negative control proves the rule sees it -
  but the blind spot is real for any blocking call reachable only through an interface hop, and
  nothing currently measures how much that hides.
