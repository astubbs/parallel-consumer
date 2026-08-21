# Bug: worker threads contend on the thread pool's single work-queue lock

<!-- inflight-type: bug -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

Found 2026-08-21 by profiling, after three separate attempts to make the shard dispatch scan cheaper
all returned zero end-to-end. **This is where the time actually goes.**

## The measurement

Java Flight Recorder, `core` engine, `UNORDERED`, 500,000 records, 10 partitions, maxConcurrency
1,000, zero-cost handler, real broker. **Five seconds of recording:**

| Parks | Thread | Lock class |
|---:|---|---|
| **31,112** | `pc-pool-1-thread-N` | `ReentrantLock$NonfairSync` |
| 7,615 | `pc-pool-1-thread-N` | `AbstractQueuedSynchronizer$ConditionObject` |
| 5 | `bench-*` | `ReentrantLock$FairSync` |
| 4 | `pool-3-thread-N`, `main` | condition / future |

**~39,000 park events in five seconds, essentially all of them PC's own worker threads**, blocking on
a non-fair `ReentrantLock` and awaiting a `Condition`.

## What that lock is

`AbstractParallelEoSStreamProcessor.setupWorkerPool` builds

```java
new ThreadPoolExecutor(poolSize, poolSize, 0L, MILLISECONDS, new LinkedBlockingQueue<>(), ...)
```

A `LinkedBlockingQueue` has **one `takeLock`** - a non-fair `ReentrantLock` - and **one `notEmpty`
Condition**. Every worker thread calls `take()` on it. At maxConcurrency 1,000 that is a thousand
threads serialising through a single lock to collect their next record, which is exactly the
`NonfairSync` + `ConditionObject` pair the profile shows, on exactly those threads.

**Caveat, stated plainly:** JFR truncated the stacks below the `lockInterruptibly` frame even with
`stackdepth` raised, so the `LinkedBlockingQueue` attribution is inferred from the lock class, the
thread family and the pool's construction rather than read off a stack. It is strong, and it is not
yet direct. **YourKit** - credited in the README, open-source licence - does lock-contention
attribution properly and would settle it in one run.

## Why this is the important finding

**It explains why every attempt to speed up dispatch failed.** The shard scan was measured at 97ms per
20,000 records and made ten times cheaper, and end-to-end nothing moved, at 0ms, 2ms or 100ms. The scan
was never the cost - **the handoff was**. See
[`parked-resume-shard-dispatch-scan.md`](parked-resume-shard-dispatch-scan.md) for all three attempts.

**It gives the in-flight ceiling a mechanism.** In-flight plateaus near 2,750 regardless of setting,
and a bare `KafkaConsumer` with a thread pool plateaus at 2,848 - the same wall
([`bug-in-flight-ceiling-above-2000-concurrency.md`](bug-in-flight-ceiling-above-2000-concurrency.md)).
Both arms share a fixed pool fed by one locked queue. That is a shared cause where previously there was
only a shared number.

**And it rules out the other suspect.** The `RetryQueue` uses a **fair** `ReentrantReadWriteLock`, which
is materially more expensive than a non-fair one and is read on the broker-poll path - a good suspect on
inspection. **It accounts for 5 parks out of 39,000.** Not the problem.

## What to do

1. **Virtual threads, and this changes their status.** `Executors.newVirtualThreadPerTaskExecutor()`
   has no shared work queue and no pool of threads competing for one lock - each task gets its own
   carrier-scheduled continuation. That removes precisely the mechanism measured here.
   **[PR #51](https://github.com/astubbs/parallel-consumer/pull/51) already implements the option**, and
   reaches the Java 21 API reflectively so it compiles under this project's Java 8 target. It was
   previously listed as "a measurement worth taking"; **it is now a candidate fix with a named
   mechanism.** Needs the package-rename rebase and a JDK 21 CI lane, since JDK 17 silently skips its
   tests.
2. **Confirm with YourKit** before building anything, so the attribution is read rather than inferred.
3. **If virtual threads are not viable**, the conventional mitigation is to stop having one queue: a
   `ForkJoinPool` with work stealing, or per-worker queues. Both are larger changes than swapping the
   executor, which is why 1 comes first.

**What NOT to do:** optimise the shard scan. Three attempts, three zeros, and now a reason.

## Reproducing

`BENCH_JFR=<dir>` on `bench/run-bisect.sh` records a flight recording per measured run. **Profiled and
unprofiled runs are not comparable** - recording is not free, and every figure in `bench/results/` was
taken without it, so never put them in the same table.
