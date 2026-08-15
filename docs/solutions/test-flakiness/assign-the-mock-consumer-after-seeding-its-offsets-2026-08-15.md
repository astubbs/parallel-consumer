---
title: "A mock consumer assigned before its beginning offsets were recorded: the Kotlin CI row's 30-second commit await"
date: 2026-08-15
category: test-flakiness
module: parallel-consumer-core
problem_type: flaky_test
component: testing
symptoms:
  - "SpikeConformanceTest.awaitCommittedOffset: ConditionTimeout, `expected: OptionalLong[2] but was: OptionalLong.empty within 30 seconds`"
  - "Earlier in the same log, ignored by the failure report: `IllegalStateException: MockConsumer didn't have beginning offset specified, but tried to seek to beginning` out of BrokerPollSystem.pollBrokerForRecords"
  - "PC then closes with `Error from poll control thread, will attempt controlled shutdown`, so nothing can ever commit"
  - "Only under load - green in isolation, and green in a sibling CI row of the same run driving the identical test classes"
root_cause: fixture_assigned_partitions_before_recording_their_beginning_offsets
resolution_type: test_fix_reorder_fixture_setup
severity: medium
status: "SOLVED - test-side fix, no product defect. The engine did exactly what a consumer throwing from poll requires."
last_updated: 2026-08-15
related:
  - "docs/inflight/test-untracked-ci-flakes.md - the 2026-08-15 'under concurrent agent load' group, for which this is a candidate explanation with a one-grep check"
  - "docs/inflight/test-load-tightness-flakes.md - the family this is NOT a member of: the await was not tight, it was awaiting something that had already become impossible"
  - "docs/investigating.md - the control-arm method this was settled with"
---

# The failure names a deadline; the cause is four seconds earlier in the log

On PR astubbs#293 the Kotlin client's `clients.yml` row failed twice over, and neither failure was in
the Kotlin module: `Build and test` failed in `parallel-consumer-proxy-client-java-harness` and
`Conformance suite (kotlin)` in `parallel-consumer-proxy-client-java-direct` - both dragged in by
`-am`. Every Kotlin test passed. The Scala row of the same run drove the identical two test classes
green in 3.6s and 1.9s, which is what identified this as a shared fixture problem rather than
anything a client wave owns.

What the report said was `awaitCommittedOffset` timing out on
`expected: OptionalLong[2] but was: OptionalLong.empty within 30 seconds`. What had actually happened,
seconds into the test:

```
IllegalStateException: MockConsumer didn't have beginning offset specified, but tried to seek to beginning
  at MockConsumer.resetOffsetPosition / updateFetchPosition / poll
  at LongPollingMockConsumer.poll
  at BrokerPollSystem.pollBrokerForRecords
```

That kills the broker-poll thread; PC closes with `Error from poll control thread`; the offset the
test waits for can never be committed. **The test then waits out its full budget for something that
became impossible in its first second** - the same shape as the load-tightness family's lesson that a
test awaiting a consequence it cannot force is unsound rather than tight.

## The window

`LongPollingMockConsumer#subscribeWithRebalanceAndAssignment` assigned the partitions and *then*
recorded their beginning offsets. Between those two calls the partitions are assigned with nothing
for `MockConsumer#resetOffsetPosition` to seek to, and any poll landing there throws.

The window is not theoretical. `MockConsumer#rebalance` both assigns the partitions and - since
kafka-clients 3.7 - fires the registered rebalance listener from inside the call, so PC is already
polling before the method returns. `MockConsumerTestBase` had the same shape with a whole extra call
inside the window.

## Why seeding first is the required order, not a lucky one

Read off the kafka-clients 3.9.2 bytecode rather than inferred:

- `updateBeginningOffsets` is `beginningOffsets.putAll(newOffsets)` and nothing else. It needs no
  assignment, so it can always be called first.
- `rebalance` clears `records` and reassigns `subscriptions`. It never touches `beginningOffsets`, so
  seeding first is never undone.
- `resetOffsetPosition` throws exactly when `beginningOffsets.get(tp)` is null under `EARLIEST`.

So there is no interleaving in which the old order was correct, and none in which the new order
leaves a partition assigned without an offset to reset to. That is what stops the next reader
reordering it back.

## The control arm - the baseline proves nothing

The baseline was green 24 seconds at a time, so the fix had to be tested against a widened window
rather than against a clean run:

| Window between the two calls | Order | Result |
|---|---|---|
| none (baseline) | assign, then seed | green |
| 500ms | assign, then seed | **green** - PC's poll thread was inside its simulated long poll for the whole window |
| 5s | assign, then seed | red on every test, with exactly the CI exception |
| 5s | **seed, then assign** | green, zero occurrences of the exception anywhere in the run |
| none | seed, then assign | green |

The 500ms row is the useful one: it is why the race needs a loaded runner to appear at all, and why a
short soak on a quiet box would have reported the flake as unreproducible.

## Other instances of the class

All call sites of `rebalance`/`assign` near `updateBeginningOffsets` in the tree were checked:
`ReactorAppTest` and `VertxAppTest` record the offsets when they construct the consumer, before any
assignment; `ParallelEoSSStreamProcessorRebalancedTest#rebalanceWithoutAssignment` re-assigns
partitions whose offsets are already recorded, and `updateBeginningOffsets` never removes;
`KafkaTestUtils#assignConsumerToTopic` has the shape but is dead code, marked as such, and reaches
an `assign` override that fires the listener without assigning anything. Two were fixed:
`LongPollingMockConsumer#subscribeWithRebalanceAndAssignment` and `MockConsumerTestBase`.
