---
title: "Transactional batching stalled the consumer entirely: the produce lock is acquired per poll context but released per record, and a partition whose every batch fails is never dirty, so no commit is ever attempted"
date: 2026-08-08
category: test-issues
module: parallel-consumer-core
problem_type: logic_error
component: transactional-commit
symptoms:
  - "At batchSize >= 2 in transactional poll-and-produce, the source offset freezes and never advances - committed=3, end=201, lag=198, stagnant=116s"
  - "No commit-path error at all: commits are not attempted-and-failing, they are never attempted"
  - "84 batch failures on the instance, each logged as a user-function failure, blaming code that did not fail"
  - "Invisible without deliberate capture - the exception is rethrown into WorkContainer#future, which nothing in main reads"
  - "batchSize = 1 completes the identical scenario in ~44s, even under 4-way parallel test load"
root_cause: produce_lock_acquired_per_context_released_per_record
resolution_type: main_code_fix_single_release_point
severity: high
status: "Fixed by astubbs#257 (fix/produce-lock-double-release). Confirmed by this suite: applying only that PR's three src/main files takes the reproducing test from 1 error in 178s to 5/5 passing in 72s."
last_updated: 2026-08-08
related_prs:
  - "astubbs#257 - the fix. Written for the DUPLICATE symptom before the stall was known"
  - "astubbs#220 - the sibling investigation into this same producerTransactionLock; source of the control-arm method"
---

# The defect is a stall, not a duplicate

The produce lock is acquired **once per `PollContextInternal`** and released **once per
`WorkContainer`**. With more than one record in the context, the second release has no lock to
release. The prediction on record was that this throws `IllegalMonitorStateException` from
`ReadLock.unlock()`; it does not. `finishProducing` calls `ensureProduceStarted()` first, and that
guard trips one frame earlier:

```
io.confluent.parallelconsumer.internal.InternalRuntimeException: Need to call #beginProducing first
    at ProducerManager.ensureProduceStarted(ProducerManager.java:442)
    at ProducerManager.finishProducing(ProducerManager.java:433)
    at WorkContainer.onPostAddToMailBox(...)
```

What makes it severe is the third step, and it is the part that was missed for months:

1. the second release throws;
2. `runUserFunction`'s catch-all marks the **whole batch** failed;
3. **only a success marks a partition dirty** - `PartitionState#onSuccess` is the sole `setDirty`
   caller, `onFailure` is a no-op - and the commit gate ANDs on `wm.isDirty()`, which
   `requestCommitAsap()` cannot override.

So a partition whose every batch fails is never dirty, no commit is ever *attempted*, and the offset
cannot advance. The pipeline does not produce duplicates. It stops.

## Why the earlier measurement said the opposite

A previous investigation instrumented this lock and counted **340 acquires, 340 releases, 0
exceptions**, and concluded "no double release actually occurs". That measurement was taken at
`batchSize = 1`, where the defect cannot fire by construction. It answered a question the run could
not reach, and the 1:1 result read as reassurance.

The lesson generalises: an instrumented count is only evidence within the configuration that can
exercise the defect. Record the configuration next to the number, or the number outlives its scope.

## How it was settled

Same magnitude, different position - the house method:

| Arm | Result |
|---|---|
| `batchSize = 3`, 200 records, unloaded | **5/5 fail** (2 ad hoc + soak 3/3 at `SOAK_FREE_CORES=99`) |
| `batchSize = 1`, same volume, same machine | 4/4 pass |
| `batchSize = 3` with astubbs#257's `src/main` applied | **5/5 pass, 72s** (was 1 error, 178s) |

Alternatives were ruled out rather than assumed: not load-tightness (the failing arm is unloaded and
the passing arm survives 4-way load), not an unforceable trigger (the awaited event is the ordinary
completion path), not rebalance (the 15.6s dwell is the abandoned instance being fenced; the stall
outlasts it by ~100s), not the transaction timeout (5 minutes, longer than the run).

**The ambient probe could not have helped here.** Its `LAG_STAGNATION_BOUND` is 150s and the await is
120s, so the top-line "no violations" line is vacuous for this test - the detector had no opportunity
to fire. The frozen-partition detail, threshold 10s, is the reading that mattered. Check a probe's
thresholds against the run's own dimensions before citing its silence.

## What it refutes

The README promises that under transactional mode "even under failure, the results will exist exactly
once in the Kafka output topic". At `batchSize >= 2` on the affected code, they never come to exist at
all - refuted by liveness rather than by duplication. Recorded in the claim register as
`RESULTS_EXACTLY_ONCE_UNDER_FAILURE`.

## Prevention

`TransactionalCrashReplayIT#outputHoldsEachResultExactlyOnceAcrossTheReplayWhenBatching` is the
regression test. It runs a crash and replay at `batchSize = 3` and asserts the source offset advances
and each result exists exactly once on the output topic. It cannot be satisfied by a pipeline that has
stopped committing, which is what the earlier duplicate-shaped assertions could not distinguish.
