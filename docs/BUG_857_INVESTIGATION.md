# Bug #857 Investigation: Paused Consumption After Rebalance

Upstream issue: https://github.com/confluentinc/parallel-consumer/issues/857

## Summary

Multiple users report that after Kafka rebalances (especially with cooperative sticky assignor under heavy load), Parallel Consumer stops processing messages on certain partitions. Lag accumulates indefinitely; only restart fixes it.

## Reproduction

**Test:** `MultiInstanceRebalanceTest.largeNumberOfInstances` (was `@Disabled` since 2022, re-enabled for this investigation)

- 80 partitions, 12 PC instances, 500k messages, chaos monkey toggling instances
- **Failure rate: ~80% (4/5 runs)** with original code
- **Failure rate: 100% (3/3 runs)** after fixing the restart logic
- Stalls at varying progress points (17%-74%), confirming timing-dependent race

## Root Cause Found

```
ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access
  currentThread(name: pc-broker-poll-PC-4, id: 1466)
  otherThread(id: 1465)
```

**Call stack:**
```
ConsumerManager.updateCache()
  → ConsumerManager.poll()
    → BrokerPollSystem.pollBrokerForRecords()
```

When `close()` is called from an external thread (chaos monkey, shutdown signal, rebalance handler) while the broker poll thread is mid-`consumer.poll()` or `consumer.groupMetadata()`, the Kafka client detects multi-threaded access and throws `ConcurrentModificationException`. This crashes the PC instance via the control loop's error handler (`AbstractParallelEoSStreamProcessor:854`), setting `failureReason` and closing the PC.

### Why this causes "paused consumption"

In production, the sequence is:
1. Rebalance starts → `onPartitionsRevoked` callback fires on the poll thread
2. Meanwhile, `close()` or another operation touches the consumer from a different thread
3. `ConcurrentModificationException` → PC crashes internally
4. The consumer group coordinator sees the member as failed → partitions redistributed
5. But the PC's work containers, shard state, and epoch tracking are left in an inconsistent state
6. If the same JVM process creates a new PC instance (e.g., supervisor restart), it starts fresh — but the consumer group's committed offsets may not reflect all in-flight work, leading to a gap

In the test:
1. Chaos monkey calls `stop()` → `close()` from the chaos thread
2. Poll thread is mid-`consumer.poll()` or `consumer.groupMetadata()`
3. `ConcurrentModificationException` crashes the PC
4. `failFast` detects the dead PC → test fails with "Terminal failure"

### What sangreal's PR #882 fix addressed

PR #882 fixed stale work container cleanup in `ProcessingShard.getWorkIfAvailable()`. That fix is correct and necessary, but it addresses a different symptom: stale containers blocking new work after a clean rebalance. It does NOT address the concurrent access crash.

### What the deterministic unit tests showed

The `ShardManagerStaleContainerTest` tests (3 tests, all pass) prove that the stale container logic works correctly in single-threaded scenarios. The epoch tracking, stale detection, and mid-iteration removal all function as designed. The bug is purely a concurrency issue.

## Bug 2: Silent Stall (the real #857)

After fixing the restart logic in tests, we still see 100% failure rate — but with a different pattern: NO exceptions, NO crashes, consumers alive and running, but consumption stops making progress. This is exactly what production users describe.

### Root Cause: `numberRecordsOutForProcessing` counter drift

**File:** `WorkManager.java:65` — `private int numberRecordsOutForProcessing = 0`

This counter tracks how many work items have been dispatched to the worker pool but not yet completed. It's used by `calculateQuantityToRequest()` to determine how many new work items to fetch from shards:

```
delta = target - numberRecordsOutForProcessing
```

**The bug:** When partitions are revoked (`onPartitionsRevoked`), work is removed from shards and partition state — but `numberRecordsOutForProcessing` is NOT adjusted. In-flight work for revoked partitions is expected to complete through the mailbox, where `handleFutureResult()` detects them as stale and decrements the counter. But if the work items don't make it back through the mailbox (e.g., the worker pool was shut down during close, interrupting in-flight tasks), the counter stays permanently inflated.

**The consequence:** `calculateQuantityToRequest()` computes `target - inflated_counter = 0` (or negative). No new work is requested. No records are polled. Consumption stalls silently.

**Evidence:** In the failing test runs:
- All partitions are correctly assigned after rebalances
- No exceptions, no errors, no crashes
- But the overall progress count stops incrementing
- The `ProgressTracker` detects "No progress after 11 rounds"

### Proposed Fix

In `WorkManager.onPartitionsRevoked()`, count the number of in-flight work containers for the revoked partitions and subtract them from `numberRecordsOutForProcessing`. This ensures the counter accurately reflects the actual amount of outstanding work after a rebalance.

## Fix for Bug 1 (CME)

The `close()` path needs to safely interrupt the poll thread via `consumer.wakeup()` instead of directly touching the consumer from another thread. Partial fix committed: moved `updateCache()` after `pollingBroker=false` in `ConsumerManager.poll()`. May need additional work.

## Test Infrastructure Improvements

As part of this investigation, we also:
1. Extracted `ManagedPCInstance` from `MultiInstanceRebalanceTest`'s inner class into a shared test utility
2. Added whitelist-based exception classification for restart: expected close exceptions (InterruptedException, WakeupException, etc.) are logged, unexpected errors fail the test
3. Added a CooperativeStickyAssignor test variant
4. Added deterministic unit tests for stale container handling
5. Added DEBUG-level logging config for integration tests
