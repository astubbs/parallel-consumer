# The in-flight ceiling is platform threads. Proven with no Kafka and no Parallel Consumer.

<!-- inflight-type: perf -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: release-note, needs-measurement -->

Settled 2026-08-21. **The owner proposed this early, I argued against it, and it is correct.**

## The control

[`bench/threads/ThreadCeiling.java`](../../bench/threads/ThreadCeiling.java) - about forty lines.
**No Kafka. No Parallel Consumer. No queueing decisions.** A fixed thread pool, an infinite supply of
free synthetic work capped by a semaphore, and `Thread.sleep` - which is exactly what both Java
benchmark arms' handlers do. Nothing upstream can starve it.

| concurrency 5,000, 100ms sleep | Peak in flight | msg/s |
|---|---:|---:|
| **Platform threads** | **2,756** of 5,000 | 6,481 |
| **Virtual threads** | **5,000** of 5,000 | **46,083** |
| Virtual threads, concurrency 20,000 | **20,000** of 20,000 | **159,681** |

**7.1x, and the only difference is the thread type.** Theoretical at 5,000 and 100ms is 50,000 msg/s:
virtual threads reach **92%** of it, platform threads **13%**.

At concurrency 1,000 both reach 1,000 exactly - the ceiling appears between 2,000 and 5,000, which is
precisely where every measured arm stopped.

## What this explains, all at once

- **Why both Java arms plateau near 2,750-2,850** - PC at 2,751, a bare `KafkaConsumer` with a thread
  pool at 2,848. The pure control lands at 2,756 with no Kafka at all.
- **Why both Go arms reach 5,000 exactly.** Goroutines and virtual threads are the same class of
  answer: a blocked unit of work does not hold an OS thread.
- **Why every internal fix returned zero.** Ten hypotheses, nine refuted - shard scans, queue locks,
  the mailbox, the load factor. **None of them could have worked**, because the constraint was never
  in the queueing.
- **Why the 2022 rework came out ~1/3 as fast.** It replaced the executor and kept the thread model,
  so it added machinery to the wrong side of the constraint.
- **Why "poller throttling" was suspected in 2022 and never fixed anything.** The poller looked slow
  because the consumer could not absorb records, not because it could not deliver them.

## The reasoning error that delayed it, stated so it is reusable

**The owner's argument was the correct one and I dismissed it on a technicality.** It was: *it cannot
be the client's fault if we cannot even reach our own thread limit - the Kafka client will pull down as
many records as you want.*

**That is simply right, and the arithmetic was available all along:** at 0ms the same pipeline moves
105,000+ records/sec, while holding 5,000 in flight at 100ms needs only 50,000/sec of supply. **The
required supply was half the demonstrated supply.** Supply was never a candidate.

My counter-argument had been that in-flight rose to 3,889 when the delay doubled, so "a limit that
scales with latency is a rate limit, not a capacity limit". That ruled out a fixed *cap* on thread
count and said nothing about threads being *expensive* - and I presented it as though it settled the
question.

**The general lesson: when a subsystem is accused, check whether the accusation is arithmetically
possible before designing experiments around it.**

## What follows

**[PR #51](https://github.com/astubbs/parallel-consumer/pull/51) is now the highest-value performance
change available to this project**, by a distance, and its status changes from "a measurement worth
taking" to "the fix". It adds a `useVirtualThreads` option, generalises `setupWorkerPool` to
`ExecutorService`, and replaces `synchronized` with `ReentrantLock` to avoid pinning. It reaches the
Java 21 API reflectively, so it compiles under this project's Java 8 target and fails loudly on an
older JVM.

**Before it can land:**

1. **Rebase across the `io.confluent` -> `bz.stub` package rename.**
2. **A JDK 21 CI lane.** The PR's own tests skip on JDK 17, which is what CI runs - a green check that
   verified nothing, the failure mode this repo has shipped before.
3. **Settle what the pressure system observes.** `isPoolQueueLow()` reads
   `workerThreadPool.getQueue().size()` and `getActiveCount()` off the `ThreadPoolExecutor`. A
   virtual-thread executor exposes neither, so the pressure system must move onto PC's own accounting -
   which is [the counter with a drift clamp](bug-available-work-counter-needs-a-clamp.md). **That
   dependency is the real work**, and it is why this is not a one-line change.

**And the honest boundary:** this was measured on a 12-core laptop with `Thread.sleep` as the handler.
A real handler blocks on I/O rather than sleeping, and a server has more cores. The *mechanism* -
blocked work holding an OS thread - is the same, but the **numbers should be re-taken against PC
itself** with PR #51 before any of them are published.
