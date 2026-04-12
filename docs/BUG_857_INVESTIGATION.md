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

## Current Status

Under moderate rebalance stress, PC handles multi-instance rebalancing correctly. Under extreme stress (12 instances toggling every 500ms), consumption stalls.

### What we observed

Diagnostic logging in the poll loop during the aggressive test stall:
```
#857-poll: runState=RUNNING, pausedForThrottling=false, assignment=0
```
All PC instances were running, not paused, but the Kafka consumer reported zero assigned partitions. The control loop was requesting work (`delta=41`) but shards were empty because no records were being polled.

### What we don't yet know

The `assignment=0` observation has multiple possible explanations:
- The Kafka group coordinator can't keep up with rapid membership changes (12 instances toggling every 500ms)
- PC's `close()` doesn't cleanly deregister from the consumer group, delaying rebalance completion
- The lifecycle wait in `ManagedPCInstance` (10s) isn't long enough for the old consumer to fully leave
- There's a PC bug that only manifests under high concurrency/churn, and the gentle test doesn't hit the race window
- The `consumerManager.assignment()` cache is stale or reported incorrectly

Further investigation is needed to determine whether this is a Kafka group protocol limitation under extreme churn, a PC issue with consumer group cleanup during close, or something else entirely.

### Test Matrix

| Test | Assignor | Chaos | Result |
|------|----------|-------|--------|
| No chaos, 2 instances | Range | None | **6/6 PASS** |
| Gentle chaos, 6 instances | Range | 3s intervals | **3/3 PASS** |
| Gentle chaos, 4 instances | Cooperative Sticky | 3s intervals | **3/3 PASS** |
| Aggressive chaos, 12 instances | Range | 500ms intervals | **~20% PASS** |

### Defensive fixes applied

These are all correct improvements regardless of the root cause:

1. **CME prevention**: moved `updateCache()` after `pollingBroker=false` in `ConsumerManager.poll()`
2. **Counter adjustment**: `adjustOutForProcessingOnRevoke()` in `WorkManager` before shard cleanup
3. **Throttle reset**: `pausedForThrottling=false` on partition assignment in `BrokerPollSystem`
4. **Lifecycle wait**: `ManagedPCInstance.run()` waits for previous PC to fully close before creating a new one

### Regarding production #857

The production reports describe consumers that are stable (not being rapidly toggled). The aggressive chaos test may not reproduce the exact production scenario. With gentle chaos (which better simulates production rebalances from deployments), PC handles rebalances correctly with both Range and Cooperative Sticky assignors.

## Bug 3: commitCommand Lock Contention — THE ROOT CAUSE

### Discovery

With 12 instances + gentle chaos (3s intervals): 0/3 pass. The instance count matters, not just chaos frequency. Analysis of the failed run showed:

1. All 80 partitions revoked at `20:05` via `onPartitionsRevoked` callbacks
2. Zero `onPartitionsAssigned` callbacks fire after that — ever
3. All threads go silent — no poll, no control loop, no chaos monkey
4. System is completely dead for the remaining 15 minutes until timeout

### Root Cause: `synchronized(commitCommand)` deadlock

**File:** `AbstractParallelEoSStreamProcessor.java:1314`

`commitOffsetsThatAreReady()` takes `synchronized(commitCommand)`. This method is called from both:
- **Poll thread** (line 424): inside `onPartitionsRevoked`, which runs during a Kafka rebalance callback
- **Control thread** (line 894): inside `controlLoop`, as part of normal offset commit cycle

When the control thread holds the `commitCommand` lock (mid-commit), and a rebalance fires on the poll thread, `onPartitionsRevoked` tries to acquire the same lock. The poll thread blocks. But the control thread's `consumer.commitSync()` (called through `committer.retrieveOffsetsAndCommit()`) needs the poll thread to be responsive for the Kafka protocol to work. **Deadlock.**

With more instances:
- More consumers in the group = more frequent rebalances
- More rebalances = higher probability of hitting the window where the control thread is mid-commit
- This explains why 6 instances passes and 12 fails: the collision probability scales with group size

### This IS a PC bug

The `commitCommand` lock creates a cross-thread dependency between the poll thread (which must remain responsive during rebalance) and the control thread (which holds the lock during potentially slow broker commits). This violates Kafka's threading model: rebalance callbacks must complete quickly, and the poll thread must not be blocked by operations on other threads.

### Fix direction

The fix should ensure `onPartitionsRevoked` never blocks on the `commitCommand` lock. Options:
1. Skip the commit attempt in `onPartitionsRevoked` if the lock is already held (use `tryLock()` semantics)
2. Move to a single-thread model where the control loop IS the poll thread
3. Use a non-blocking commit in the revocation handler

Open questions:
- Could a single rebalance (not a storm) trigger the stall under specific timing conditions we haven't hit in the gentle test?
- Is there a relationship between the number of in-flight records at rebalance time and the likelihood of the lock collision?

## Test Infrastructure Improvements

As part of this investigation, we also:
1. Extracted `ManagedPCInstance` from `MultiInstanceRebalanceTest`'s inner class into a shared test utility
2. Added whitelist-based exception classification for restart: expected close exceptions (InterruptedException, WakeupException, etc.) are logged, unexpected errors fail the test
3. Added a CooperativeStickyAssignor test variant
4. Added deterministic unit tests for stale container handling
5. Added DEBUG-level logging config for integration tests
