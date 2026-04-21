# `LargeVolumeInMemoryTests` - OOM at 1M messages

**Date observed:** 2026-04-22
**Branch:** `fix/ci-kafka-matrix` (PR #49) at that time
**Context:** Restoring `quantityOfMessagesToProduce` from 500 to 1,000,000 messages caused an `OutOfMemoryError: Java heap space` on GitHub Actions `ubuntu-latest` runners.

## Symptom

Test hangs/crashes with OOM while closing `ParallelEoSStreamProcessor` after processing 1M messages. The OOM surfaces in the `BrokerPollSystem` close path.

## Stack Trace

```
23:09.107  WARN  [pc-control]  (AbstractParallelEoSStreamProcessor.java:733)#innerDoClose failed to close brokerPollSubsystem during close sequence
java.util.concurrent.ExecutionException: java.lang.OutOfMemoryError: Java heap space
    at java.base/java.util.concurrent.FutureTask.report(FutureTask.java:122) ~[na:na]
    at java.base/java.util.concurrent.FutureTask.get(FutureTask.java:205) ~[na:na]
    at io.confluent.parallelconsumer.internal.BrokerPollSystem.closeAndWait(BrokerPollSystem.java:278) ~[parallel-consumer-core-0.6.0.0-SNAPSHOT.jar:na]
    at io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.innerDoClose(AbstractParallelEoSStreamProcessor.java:731) ~[parallel-consumer-core-0.6.0.0-SNAPSHOT.jar:na]
    at io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.doClose(AbstractParallelEoSStreamProcessor.java:658) ~[parallel-consumer-core-0.6.0.0-SNAPSHOT.jar:na]
    at io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.lambda$supervisorLoop$9(AbstractParallelEoSStreamProcessor.java:855) ~[parallel-consumer-core-0.6.0.0-SNAPSHOT.jar:na]
    at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[na:na]
    at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136) ~[na:na]
    at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635) ~[na:na]
    at java.base/java.lang.Thread.run(Thread.java:840) ~[na:na]
Caused by: java.lang.OutOfMemoryError: Java heap space

23:15.099  WARN  [pc-control]  (PCMetrics.java:206)#close Trying to close PCMetrics instance that is already closed.
23:15.099  ERROR [pc-control]  (AbstractParallelEoSStreamProcessor.java:668)#doClose PC closed due to error: java.lang.RuntimeException: Error from poll control thread: Error in BrokerPollSystem system.
```

## Location

`parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/LargeVolumeInMemoryTests.java:62`

```java
int quantityOfMessagesToProduce = 1_000_000;  // OOMs on ubuntu-latest with default heap
```

(Originally this line was `int quantityOfMessagesToProduce = 500;` with the `1_000_000` version commented out. Restoring to 1M caused this failure.)

## What the Test Actually Does

- Creates a `MockConsumer` (in-memory - hence "InMemory" in the test name)
- Calls `ktu.generateRecords(1_000_000)` to create 1M `ConsumerRecord<String, String>` objects in a List
- Sends all of them to `consumerSpy` via `ktu.send(consumerSpy, records)`
- Runs `parallelConsumer.pollAndProduceMany` with 1M `CountDownLatch` counts
- Waits for all to be consumed
- Asserts `producerSpy.history().hasSize(quantityOfMessagesToProduce)` - i.e., all 1M producer records are retained

## Why It Runs Out of Memory

The test accumulates everything in memory:

1. **1M `ConsumerRecord` objects** - each with key + value strings, topic, partition, offset, headers, timestamp. Rough estimate: ~200-400 bytes each = **200-400 MB**.
2. **1M mocked `ProducerRecord` objects** - accumulated in `producerSpy.history()` for the final size assertion. Similar footprint = **200-400 MB**.
3. **1M latch decrements** through `allMessagesConsumedLatch` + bookkeeping in `ParallelEoSStreamProcessor`.
4. Plus all the internal state PC tracks per in-flight message (WorkContainer, offset encoding, etc.)

GitHub Actions `ubuntu-latest` runners have ~7GB RAM but surefire default heap is typically 256-512MB. The test likely needs 2-4 GB.

The OOM manifests during `close()` because that's when PC tries to drain the remaining work mailbox, which is still holding references to everything.

## Suggested Fixes (for the triage agent)

### Option A: Increase heap for the test

Simplest. Add JVM args to the CI profile or test-specific config:

```xml
<argLine>-Xmx3g</argLine>
```

**Risk:** Hides resource issues; test may still be tight on 2GB runners. Check what JVM heap is allocated to other runners for parity.

### Option B: Stream, don't accumulate

The test currently asserts on `producerSpy.history().size()`. That forces retention of every produced record. Change to a counter or running hash:

```java
// Replace: List<ProducerRecord<...>> history (1M entries)
// With: AtomicLong producedCount + small sampling buffer for ordering checks
```

**Risk:** Changes test semantics - need to decide what invariants must be checked and reduce to just those.

### Option C: Reduce volume

Pick a smaller number that still exercises the same code paths. Probably 100k-500k.

**Risk:** Reduces coverage; conflicts with the intent of "Large Volume" in the test name.

### Option D: Break into streaming batches

Run the test in chunks of e.g. 100k and assert between batches, releasing references. More test complexity but preserves volume.

**Risk:** More maintenance; test structure becomes harder to read.

## Recommendation

Start with **Option A** (bump heap to 2-3GB just for this test class via a failsafe `<argLine>`). If it still OOMs or is too slow, fall back to **Option B** (switch size assertion to a counter) which is probably the "correct" fix.

Do not silently drop back to 500 messages - that was the original kneecap and the whole point of restoring it is to have a real volume test.

## Related

- Commit that restored the volume: the cherry-picked `test: restore LargeVolumeInMemoryTests to 1M messages` on this branch
- Original kneecap predates the audit - see `disabled-and-weakened-tests-audit-2026-04-22.md` section 2.1
- Test class has `@Tag("performance")` so this only runs in the performance suite job, not regular PR builds

## Verification When Fixed

1. Test passes on `ubuntu-latest` GitHub runner (standard hosted, ~7GB RAM) in the PR performance suite job
2. Test passes 3 runs in a row (no flake)
3. Runtime documented - under 10 minutes is ideal for PR builds
4. If Option A was chosen: the heap setting is documented with a comment explaining why
