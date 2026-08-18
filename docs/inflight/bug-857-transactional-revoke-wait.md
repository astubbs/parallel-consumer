# confluentinc#857 family: the unbounded revoke wait in transactional mode

**Commit mode: `PERIODIC_TRANSACTIONAL_PRODUCER` only.** This is the discriminator - the defect below
and the AB-BA deadlock in astubbs#29 are in mutually exclusive modes and cannot be the same bug.

## The defect

`AbstractParallelEoSStreamProcessor.onPartitionsRevoked` waits with **no deadline** for an in-flight
transaction, on master, predating astubbs#29:

```java
// AbstractParallelEoSStreamProcessor.java:418-419 (master)
while (isTransactionCommittingInProgress())
    Thread.sleep(100); //wait for the transaction to finish committing
```

`isTransactionCommittingInProgress()` (`:1494-1496`) is gated on
`options.isUsingTransactionCommitMode()`, so this loop only runs in transactional mode - and there it
is the *common* case, not a rare race: the control thread takes the producer write lock in
`maybeAcquireCommitLock()` before committing.

The callback runs on the poll thread inside `poll()`, so it is bounded by `max.poll.interval.ms`.
Overrunning it evicts the member.

## Why this is not astubbs#29's deadlock

The AB-BA cycle's second edge lives in `ConsumerOffsetCommitter`, which `BrokerPollSystem` constructs
**only** for the consumer-commit modes (`switch (options.getCommitMode())`, the
`PERIODIC_CONSUMER_SYNC, PERIODIC_CONSUMER_ASYNCHRONOUS` arm). In transactional mode there is no
request queue, no response queue and no `commitAndWait()` - **the cycle cannot occur here**.
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
which the AB-BA cycle cannot close. So the failure is **not** evidence for astubbs#29.

It is, however, a **real** failure and it is evidence for the block above. The run was on
master-family code, where the test's latch was still reachable - the latch-unreachable defect
(the revoke path calling the private `tryCommitOffsetsOnRevoke()` instead of the overridden
`commitOffsetsThatAreReady()`) only voids runs on **astubbs#29's branch**. So this sighting survives
the correction; only its attribution moves.

No seed was captured.

## User-facing report

**astubbs#44 (confluentinc#803)** - *"Transactional Producer instance gets timeout getting commit lock
while second instance starts"* - matches this mechanism exactly: second instance joins, rebalance
fires, poll thread spins here, `max.poll.interval.ms` is breached, the group reports *"group is
already rebalancing"*, and the run ends on `commitLockAcquisitionTimeout`.

It is the **only** issue on the upstream tracker ever labelled *verified bug*. It was re-triaged off
astubbs#29 and onto this block on 2026-08-18; its `pr-available` label was removed, because no open
PR addresses it.

## Open decision - do not write code before settling it

The wait needs a deadline, and the obvious design is ruled out: the poll thread **cannot** abort the
transaction, because `ProducerManager` enforces single-writer from the control thread and throws
`ConcurrentModificationException` otherwise.

The candidate is to deadline the **holder** instead - bound the control thread, which owns the
transaction and can abort itself - rather than the revoke callback that merely notices the overrun.
Not agreed with the user.

Proceeding past the wait is separately unsafe until producer fencing is recoverable:
`ProducerFencedException` is wrapped in `InternalRuntimeException` and kills the instance. See
`next-recoverable-producer-fencing.md` and astubbs#225.

Branch `fix/bound-revoke-transaction-wait` exists with no code on it.
