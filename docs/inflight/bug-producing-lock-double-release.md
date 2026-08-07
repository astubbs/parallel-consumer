# Confirmed defect: the produce lock is acquired per context and released per record

**Confirmed 2026-08-07 at `batchSize >= 2`** - see "Confirmed, and its consequence is a stall" below.
The measurement that follows in "What is measured" was taken at `batchSize = 1`, where the defect
cannot fire, which is why it read 1:1 and closed the wrong half of the question.

The original framing is kept below because the prediction it made was wrong in a way worth keeping:
it expected `IllegalMonitorStateException` from `ReadLock.unlock()`, and the exception actually thrown
arrives one frame earlier, from `ensureProduceStarted`.

## The question

In transactional poll-and-produce, the produce read lock is released from **two** places, on the same
worker thread, against the same `ProducingLock` instance held in the context:

- `WorkContainer.onPostAddToMailBox` (`WorkContainer.java:271-276`) → `finishProducing` →
  `ProducerManager.releaseProduceLock` → `ProducingLock.unlock()`
- `AbstractParallelEoSStreamProcessor.cleanUpContext` (`AbstractParallelEoSStreamProcessor.java:1418-1419`),
  in the `finally` of `runUserFunction`

Nothing clears `PollContextInternal#producingLock` between them, so both see a present `Optional`.
`ProducingLock.unlock()` calls `ReadLock.unlock()` unconditionally
(`ProducerManager.java:456-459`). A thread holding zero read locks that calls `unlock()` on a
`ReentrantReadWriteLock.ReadLock` throws `IllegalMonitorStateException`. So either both paths do not
in fact both fire, or something is swallowing it.

## What is measured (2026-08-07)

Counted from a transactional integration run with
`allowEagerProcessingDuringTransactionCommit=false` (`TransactionTimeoutsTest#commitTimeout`), with
`io.confluent.parallelconsumer.internal.ProducerManager` at DEBUG:

| Acquires | Releases | `IllegalMonitorStateException` |
|---|---|---|
| 340 | 340 | 0 |

Exactly 1:1. So **no double release actually occurs** - only one of the two paths fires per context.
That closes the "is it silently throwing" half of the question and leaves the other half open:

- **which** path is skipped, and **why**;
- whether that is guaranteed by construction or holds only for the paths exercised so far -
  the error, retry and stale-work branches (`handleStaleWork`, and the failure branch that calls
  `addToMailbox` before `finally { cleanUpContext(context); }`) were not separately measured.

## Confirmed, and its consequence is a stall (2026-08-07, U13)

`TransactionalCrashReplayIT#outputHoldsEachResultExactlyOnceAcrossTheReplayWhenBatching` reproduces it
at `batchSize = 3`, 200 payload records, transactional poll-and-produce, `ordering=UNORDERED`,
`maxConcurrency=4`. **5/5 runs failed** (2 ad hoc + `bin/soak-test.sh` 3/3, `SOAK_FREE_CORES=99`, i.e.
no added load). The same scenario at `batchSize = 1`, same volume and same machine, **passed 4/4**
(1 in-suite + soak 3/3). One term, two positions.

**The exception is not the predicted one.** `finishProducing` calls `ensureProduceStarted()` *before*
`releaseProduceLock`, and that guard trips first, so the second release never reaches
`ReadLock.unlock()`:

```
io.confluent.parallelconsumer.internal.InternalRuntimeException: Need to call #beginProducing first
    at ProducerManager.ensureProduceStarted(ProducerManager.java:442)
    at ProducerManager.finishProducing(ProducerManager.java:433)
    at WorkContainer.onPostAddToMailBox(...)
    at AbstractParallelEoSStreamProcessor.addToMailbox(...)
```

It is invisible without deliberate capture: `runUserFunction` logs it and rethrows into
`WorkContainer#future`, which nothing in main reads. The IT attaches an appender to
`AbstractParallelEoSStreamProcessor` to see it at all.

**The user-visible consequence is a liveness failure, not a duplicate.** The chain:

1. the lock is acquired once per `PollContextInternal` but released once per `WorkContainer`, so with
   more than one record in the context the second release throws;
2. `runUserFunction`'s catch-all marks the **whole batch** failed;
3. only a success sets a partition dirty - `PartitionState#onFailure` is a no-op - and the commit gate
   ANDs `wm.isDirty()`, so a partition whose every batch fails is never dirty and **no commit is ever
   attempted**.

Observed, replacement instance, commit interval 200ms:

```
frozen partitions (committed stagnant >= 10s with lag >= 1):
  - ...-input-808863704-0: committed=3 end=201 lag=198 stagnant=116s
```

84 batch failures on that instance and **no commit-path error at all** - so commits were not
attempted-and-failing, they were never attempted. The offset had moved to 3 before steady-state
batching set in and then never moved again.

Ruled out: load-tightness (3/3 unloaded; the `batchSize=1` arm completes the same replay in ~44s
*under* 4-way parallel test load); a rebalance artefact (the probe's 15.6s `PreparingRebalance` dwell
is the abandoned instance being fenced, and the stall outlasts it by ~100s); the transaction timeout
(set to 5 minutes, longer than the run). Note the probe's *top-line* verdict reads "none crossed the
chaos-calibrated bounds" only because `LAG_STAGNATION_BOUND` is 150s and the await is 120s - the
detector could not fire. The frozen-partition detail (10s threshold) is the reading that matters.

This is more severe than "a record is produced twice": at `batchSize >= 2` a transactional
poll-and-produce pipeline stops committing entirely. It refutes C14 (`RESULTS_EXACTLY_ONCE_UNDER_FAILURE`)
by liveness rather than by duplication - the results never come to exist.

The fix is `d95a21d4` on the local branch `fix/produce-lock-double-release`. Until it lands, the IT
arm ships `@Disabled` naming that commit, because the class is untagged and runs in the gating lane.

## Why it is worth someone's time

The invariant this lock enforces is the EOS one: the controller must not collect offsets while a
produce is in flight. `WorkContainer#onPostAddToMailBox` states it outright in its own comment. Two
problems have already come out of this exact lock lifecycle - the resolved harness bug in
`docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md`, and the trigger-margin flake in
`test-transaction-commit-timeout-trigger-margin.md`. A release path that is correct by accident rather
than by construction is how the next one arrives.

## Provenance and a trap

Raised, and deliberately not chased, in
[`docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md`](../plans/2026-08-03-001-investigate-transactional-commit-flake.md)
§11 ("Open question, deliberately not chased"). That investigation tried to count acquire/release
pairs by turning on debug logging and got an invalid result: **`surefire:test` alone does not
reprocess test resources**, so the edited `logback-test.xml` never reached `target/test-classes` and
the run logged nothing new. The counts above were taken with `./mvnw -pl parallel-consumer-core -am
verify` (as `bin/soak-test.sh` uses), which does reprocess them. Confirm `BUILD SUCCESS` on the
compile step before believing any instrumented result here.

Related, but a different bug: production commit-lock timeouts from a rebalance-vs-normal commit race,
confluentinc#803 / astubbs#44 (maintainer-confirmed upstream).
