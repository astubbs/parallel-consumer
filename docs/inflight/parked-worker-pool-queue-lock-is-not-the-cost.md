# Not a bug: worker threads park on the pool's queue lock, and it costs nothing

<!-- inflight-type: parked -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

Opened 2026-08-21 by profiling, **and immediately falsified by testing it.** Kept as a parked note
rather than deleted, because the negative result is worth more than the note ever was and because the
reasoning error in it is one this session made four times.

**THE HEADLINE WAS WRONG.** This began as *"this is where the time actually goes"*. It is not. Replacing
the lock with a lock-free queue made throughput **69% worse**, not better - see the test below.

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

## The test that falsified it

`LinkedTransferQueue` is lock-free, has the same `BlockingQueue` contract and the same unbounded
behaviour, so swapping it for `LinkedBlockingQueue` in `setupWorkerPool` is a **one-line test** of
whether that `takeLock` costs throughput.

| 1,000 concurrent | Baseline | Lock-free queue | |
|---|---:|---:|---|
| 0ms handler | 109,709 | **33,743** | **-69%** |
| 100ms handler | 8,800 | 8,841 | +0.5% |

**And the parks did not go away - they multiplied**, from 31,112 on the lock to **332,631** on the
transfer queue.

**Likely cause of the loss**, and it is the cost the change was expected to carry:
`LinkedTransferQueue.size()` is **O(n)** where `LinkedBlockingQueue` keeps a counter, and
`getNumberOfUserFunctionsQueued()` reads it **every control loop**. At a zero-cost handler the queue
holds on the order of a thousand entries, so every loop pays a linear scan. That it is invisible at
100ms fits: the loop runs far less often relative to the work.

## The reasoning error, which is the reusable part

**Thirty-one thousand parks in five seconds is not evidence of a problem.** A park is where a thread
**waits**, and a worker waiting for its next record is the pool working exactly as designed. A profile
showing heavy parking on a lock says *threads queue here*; it does not say *time is lost here*.

**Contention visible in a profile is not contention costing throughput.** Distinguishing the two needs
a control arm - remove the lock and see whether the number moves - and here it moved the wrong way.

This is the **fourth** time in one session that a signal read from inspection or a profile was
overturned by a controlled run: the load-factor buffer, the shard-count scan, the in-shard rescan, and
now this. The pattern is consistent enough to be a rule: **an explanation that has not been removed and
re-measured is a hypothesis, however well it reads.**

## What survives

**The negative result on `RetryQueue`, which is genuinely useful.** It uses a **fair**
`ReentrantReadWriteLock` - materially more expensive than a non-fair one - and it is read on the
broker-poll path via `getQueueSizeAndNumberReadyToBeRetried()`. On inspection that is a strong suspect.
**It accounts for 5 parks out of 39,000.** Ruled out, cheaply, and nobody needs to look again.

**And `BENCH_JFR=<dir>` on `bench/run-bisect.sh`**, which records a flight recording per measured run.
Profiled and unprofiled runs are not comparable and must never share a table.

## What to do

**Nothing here, on this mechanism.** The items below were written when the finding looked real; they
are kept because virtual threads remain worth doing, but **not for the reason given here** - the
executor handoff is not the bottleneck. Treat them as they were before this note: a measurement worth
taking, ranked behind the key-distribution sweep.

1. **Virtual threads.** `Executors.newVirtualThreadPerTaskExecutor()`
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
