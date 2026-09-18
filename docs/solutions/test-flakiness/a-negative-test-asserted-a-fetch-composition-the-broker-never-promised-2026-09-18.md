---
title: A negative test asserted a fetch composition the broker never promised - the mechanism it wanted is a unit test
date: 2026-09-18
category: test-flakiness
module: parallel-consumer-core
problem_type: flaky_test
component: testing
symptoms:
  - "PartitionOrderProcessingTest.allPartitionsAreNotProcessedInParallel red on a head that differed from a passing one by three javadoc lines; the re-run of the same head passed"
  - "Failure message: 'Expect some processing thread starving and not all partition counts to have some messages processed, actual partitionCounts:{0=109, 1=109, 2=109, 3=108, 4=109}' - every partition served evenly"
  - "Local matched pair on an 8-core box, both arms against their own broker, green on every run - the failing shape was only reached on the hosted runner"
root_cause: test_design_bug
resolution_type: test_rewrite
severity: low
tags:
  - flaky-tests
  - backpressure
  - load-factor
  - partition-ordering
  - negative-test
  - fetch-composition
---

# A negative test asserted a fetch composition the broker never promised - the mechanism it wanted is a unit test

## Problem

`PartitionOrderProcessingTest.allPartitionsAreNotProcessedInParallel` was the NEGATIVE half of a pair
that arrived with upstream PR confluentinc/parallel-consumer#682 (configurable message buffer), written
to demonstrate the README passage the same change added - the one beginning "As default buffer size
is calculated as `maxConcurrency * batchSize * loadFactor`". It asserted that under PARTITION ordering
with the default buffer, by the time 500 records had been processed, at least one of five pre-loaded
partitions had been served nothing.

It went red on astubbs#517 at a head that differed from a passing head by three javadoc lines, and the
re-run of the same head passed. A comment-only change cannot turn a contract red; so the question the
owner asked was what the pair actually proved.

## The mechanism, which is not the one the test named

- `pc.subscribe` preceded the 10000-record produce, so the consumer's first fetch raced the producer.
  If the fetch landed after every partition held 500 or more records, one poll returned one partition's
  batch, the 10-record buffer filled with it, and the intake gate (`WorkManager#isSufficientlyLoaded`,
  read by `BrokerPollSystem`'s throttle) paused the poller with one partition resident - starvation,
  test green. If the fetch landed while partitions held ~100 records each, the first polls carried all
  five - no starvation, test red. The failing run's `{0=109, 1=109, 2=109, 3=108, 4=109}` is the
  second shape. Which shape a run got was producer-versus-fetch timing on the runner. Same code, either
  verdict.
- The README states starvation as a possibility, not a promise: threads *can* be starved *if* the
  incoming rate outruns processing *as* the small buffer fills from a subset of partitions. The test
  turned each conditional into a fact it expected the broker to produce on cue.
- Neither half of the pair exercised the shard scan's choice. What decides which partitions get served
  is which partitions have records IN THE BUFFER, and that is decided upstream of `ShardManager` by the
  fetch composition and the intake gate. So a change to `ProcessingShard#getWorkIfAvailable`'s ordered
  loop - astubbs#178's per-candidate departure gate, which is where the sighting happened - was neither
  caught nor exonerated by it. The scan under PARTITION is pinned by `WorkManagerTest.resumesFromNextShard`
  (every populated shard drawn from, in turn), `WorkManagerTest.basic` under every ordering, the
  confluentinc#236 `starvation` test in the same class, and `KeyOrderAcrossRebalanceTest`'s
  departure-gate cases.

The same narrowness is what a perf note on another branch measured at scale: PARTITION ordering at
`maxConcurrency` 24 over 24 partitions ran 2 to 6 in flight on the default buffer and 24 with the
buffer forced wide, because the prefetch target is counted in records while the Java client answers a
poll largely from one partition at a time (`bug-partition-ordering-starves-on-a-narrow-buffer.md`,
branch-only - `node bin/inflight.mjs docs show` it). The integration test was a demonstration of that
tuning problem, written as an assertion.

## Options considered

1. **Assert the mechanism at unit level, then delete the demonstration.** Chosen.
2. **Keep the integration test but produce before subscribing and wait for the acks.** Narrows the
   window without closing it: the Fetcher's completed-fetch order and `max.poll.records` still decide
   whether the first poll is one partition or several, and that is broker and client behaviour, not a
   contract PC makes.
3. **Delete it outright as a demonstration that never belonged in a suite.** What option 1 does after
   the unit test exists; on its own it would have lost the only coverage of the gate under this shape.

## Solution

Two unit tests in `WorkManagerTest`, with fixed inputs and no broker, next to the existing
confluentinc#236 `starvation` test:

- `aDefaultBufferFilledByOnePartitionsPollPausesIntakeAndStarvesTheOtherPartitions` - PARTITION,
  `maxConcurrency(5)`, the default factor, five partitions assigned, one partition's 500-record poll
  registered. Asserts the README's formula gives a threshold of 10; that `WorkManager#shouldThrottle`
  (the poller's route into the gate) reads loaded on that one poll; that a pool of five is handed one
  record, from that partition, on every draw while the gate is closed; and that the gate reopens only
  when the resident partition has drained TO the threshold - 490 of 500 retired - so the other four
  partitions are starved for the whole of that drain, not just the first pass.
- `aBufferSizedForEveryPartitionsPollKeepsIntakeOpenAndEveryPartitionIsDrawnFrom` - the README's
  remedy at the same inputs: `messageBufferSize(2 * partitions * 500)` becomes a static factor of 1000,
  five partitions' polls are each admitted with the gate still open, and the pool is handed one record
  from each.

To make the second one honest, `PCModuleTestEnv#dynamicExtraLoadFactor` now defers to the production
computation when `messageBufferSize` is set - the env's fixed factor stands in for the DYNAMIC one, and
an explicit buffer is already static in production - rather than silently answering with the stand-in.

The integration negative test is deleted. Its positive twin, `allPartitionsAreProcessedInParallel`,
stays: it is a bounded liveness claim (every partition served within 120s with a tuned buffer) that
can only go red if some partition is never served, and it proves the one thing the unit pair cannot -
that a real broker and fetcher deliver every partition's poll while the gate is open. The gate-to-pause
wiring is pinned separately by `BrokerPollerBackpressureTest.brokerPollPausedWhenBlockedInFlightFillsBuffer`.

## Sabotage, per `docs/testing-at-write-time.md`

One mutant at a time in main code, the two new tests run against each, restored after:

| Mutant | Killed by |
|---|---|
| `LoadGateReading#isLoaded` returns `false` - the gate never pauses | the default-buffer test, at "the poller is told to pause" |
| `isLoaded` compares `>=` instead of `>` - the strict gate off by one | the default-buffer test, at the reopen point: expected 490, was 491 |
| `PCModule#initDynamicLoadFactor` ignores `messageBufferSize` | the tuned-buffer test, at the threshold: expected 5000, was 10 |
| `ProcessingShard`'s ordered break removed - a shard hands out more than one | both: the default-buffer test reads 5 handed out instead of 1, the tuned-buffer test sees duplicate partitions |

## What deleting the integration test loses

A broker-backed demonstration that the untuned default DOES starve on a pre-loaded topic - true, and
documented with numbers in the README, but only reachable when the fetch composition cooperates. The
unit test asserts the same property where it is decided. Nothing else read the integration test:
`docs/test-hardening/inactive-tests-audit-2026-08-08.md` counts the class as coverage for
partition-offset ordering, which the positive half still is.

## Prevention

- **A negative test that asserts an OUTCOME of external behaviour is a demonstration, not a contract.**
  Ask what PC decides and what the broker decides; assert PC's part where it is decided, with fixed
  inputs. The other test-design cases in this directory are the same move in different clothes:
  [`back-pressure-freezes-the-frontier-the-test-asserted-2026-08-24.md`](back-pressure-freezes-the-frontier-the-test-asserted-2026-08-24.md),
  [`a-randomised-key-draw-decided-a-batch-count-the-test-computed-from-the-record-count-2026-09-08.md`](a-randomised-key-draw-decided-a-batch-count-the-test-computed-from-the-record-count-2026-09-08.md).
- **A test that goes red on a comment-only change has told you its subject is not the code.** Read
  the mechanism before reading the diff; the diff had nothing to say.
- **When a shared test module stands in for a production component, check which options it silently
  ignores.** `PCModuleTestEnv`'s fixed factor ignored `messageBufferSize` for its whole life; nothing
  had asserted a buffer-derived threshold through it before, so nothing noticed.
