# Open question: two paths release the same `ProducingLock`, and nothing stops the second

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->


**Not confirmed a defect.** It is an unresolved invariant in the transactional poll-and-produce lock
lifecycle. Recorded because the reasoning is cheap to lose and the code path is one that has already
produced two flakes.

## The question

In transactional poll-and-produce, the produce read lock is released from **two** places, on the same
worker thread, against the same `ProducingLock` instance held in the context:

- `WorkContainer.onPostAddToMailBox` (`WorkContainer.java`) → `finishProducing` →
  `ProducerManager.releaseProduceLock` → `ProducingLock.unlock()`
- `AbstractParallelEoSStreamProcessor.cleanUpContext` (`AbstractParallelEoSStreamProcessor.java`),
  in the `finally` of `runUserFunction`

Nothing clears `PollContextInternal#producingLock` between them, so both see a present `Optional`.
`ProducingLock.unlock()` calls `ReadLock.unlock()` unconditionally
(`produceLock.unlock()` in `ProducerManager.java`). A thread holding zero read locks that calls `unlock()` on a
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
