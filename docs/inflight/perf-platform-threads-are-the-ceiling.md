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

## Virtual threads are one way to lift it. They are not the only way.

[`bench/threads/AsyncCeiling.java`](../../bench/threads/AsyncCeiling.java) - the same control with one
change: **the work does not hold a thread while it waits.** Each unit registers a completion on a small
scheduler instead of a pool thread sleeping, which is what a non-blocking engine does with an async
call. **Concurrency decoupled from threading**, the owner's framing.

**On JDK 17, with four scheduler threads:**

| concurrency, 100ms | Peak in flight | msg/s |
|---:|---:|---:|
| 5,000 | **5,000** of 5,000 | 46,802 |
| 20,000 | **20,000** of 20,000 | 173,661 |
| 50,000 | **50,000** of 50,000 | **405,954** |

**Four threads hold fifty thousand records in flight.** And at 5,000 it matches virtual threads almost
exactly - 46,802 against 46,083 - **on a JDK that has no virtual threads at all.**

**What that establishes:** the ceiling is not "Java" and not "the JVM". It is *blocking work holding an
OS thread*. Anything that breaks that link lifts it - virtual threads by making the thread cheap, async
completion by not needing one.

**Which is why `ExternalEngine` matters here.** The Vert.x, Reactor and Mutiny engines already have
this shape: the worker dispatches an async call and returns rather than blocking. **They should not
have the platform-thread ceiling at all.** That is a strong, cheap prediction and it is currently
**untestable with this harness** - the Vert.x arm's stub server sleeps on *its own* container threads
(`startStub(delayMs, concurrency * 2)`), so it reproduces the ceiling server-side. **Testing it needs a
stub that completes asynchronously**, which is a harness change worth making.

## Independent review, and the corrections it forced

An independent analysis was commissioned on the full evidence set and **found four real errors**. Its
own headline hypothesis was then tested and is *not* confirmed, but the corrections stand and matter
more than the hypothesis did.

### Corrections that stand

1. **The measurement window includes the consumer-group join, for every arm.** Derivable from the
   committed data: both Go arms report ~37.6k at 100ms/5,000, and 500,000 at the theoretical 50,000/s
   is exactly 10.0s of steady state - so both carry ~3.3s of join and ramp. **This was never deducted**,
   and it is why "mean in-flight 1,958 against a peak of 2,751" looked like thousands of idle threads.
   Corrected, the mean sits at the plateau: **the in-flight population is pinned, not fluctuating.**
2. **The two Go arms at 100ms/5,000 are clipped by their own semaphore and measure nothing about
   franz-go's speed.** They are pinned at exactly theoretical, which is why two unrelated Go programs
   agree to 0.26%. **So "the Java floor reaches 53% of the Go floor at 100ms/5,000" is wrong** - that
   row compares a saturated arm against a limited one. **The honest client-versus-client number is the
   0ms row: 96k against 143k, about 67%.**
3. **"Roughly 7,250 records are sitting in the shards awaiting selection" was inferred from the throttle
   formula, never observed.** Those records could equally be in the executor's own queue, already
   selected. PC already logs the discriminating pair - `pool active: {} queued: {}` - and nobody read
   it. The dispatch-defect framing built on that inference is unsupported as written.
4. **`maxConcurrency` is not "silently not honoured".** The cap never binds; the system equilibrates
   below it because throughput times latency is smaller than the cap. **That inverts the user-facing
   advice**: not "your setting is being ignored" but **"achieved concurrency is throughput-limited, and
   raising `maxConcurrency` cannot help."**

### The hypothesis that did not survive

The analysis proposed a **constant ~21-22ms per-record latency tax** above ~2,000 sleeping threads,
derived by solving Little's law on the corrected numbers, and predicted a sleep-overshoot p50 of
20-25ms at 5,000 against ~3ms at 1,000.

**Measured, by instrumenting the control's actual sleep duration: overshoot p50 is 0-10ms and does not
step with concurrency** - 7ms at 1,000, 7ms at 2,000, 8ms at 5,000 in one run; 3ms at 5,000 in another.
It tracks machine load, not sleeper count. **A 3ms overshoot on a 100ms sleep cannot produce a 9.7x
throughput difference or a 47% in-flight shortfall.**

Its second proposed experiment - replace the sleeping handler with a shared scheduler completing after
100ms - **had already been run** as `AsyncCeiling`, and its prediction under the thread hypothesis
(45-50k/s, pinned at 5,000) matched exactly: **47,022 msg/s, 5,000 of 5,000.** So the analysis and the
control agree on the conclusion while disagreeing on the mechanism.

### What is robust

Re-run with all three arms back to back at load average ~102-110 on twelve cores:

| concurrency 5,000, 100ms | msg/s | Peak in flight | Sleep overshoot p50 |
|---|---:|---:|---:|
| Platform threads | 4,648 | **2,673** of 5,000 | 3ms |
| Virtual threads | **44,994** | **5,000** of 5,000 | **0ms** |
| Async, 4 threads | **47,022** | **5,000** of 5,000 | - |

**9.7x under heavy load, against 7.1x on a quiet machine** - so load makes the case stronger, not
weaker. And the platform ceiling is stable across every run at every load level: **2,438 · 2,531 ·
2,673 · 2,697 · 2,756.**

**The threads are created.** `getPoolSize()` reaches 5,000. They exist and simply cannot all be in
their sleep window at once.

### The mechanism is still not pinned down, and this note should not claim it is

Sleep overshoot is ruled out. Thread *creation* is ruled out - the pool reaches 5,000. The submitter is
ruled out: the async arm uses the **same single-threaded submit loop** and reaches 47k/s. What remains
is the cost of the handoff itself once thousands of platform threads are attached to the executor, and
**that pathway is not identified.** Scheduler dispatch, wake-herd behaviour, timer coalescing and
safepoint coordination are all candidates and none has been separated from the others.

**What is established is the fix, not the cause** - and the fix is the same under every candidate
mechanism, which is why it is safe to act on while the cause is open.

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

**A clarification, because it is easy to misread the control above: no virtual-thread support has been
implemented in Parallel Consumer.** `ThreadCeiling.java` is a standalone JDK program with **zero**
references to any PC class. PC's own `setupWorkerPool` is unchanged and contains no
`useVirtualThreads`. The control measures the ceiling; it does not remove it.

**And it sidesteps the hard part entirely**, which is worth being explicit about. The control has **no
pressure system** - a `Semaphore` is its whole backpressure mechanism. It never asks a question that a
virtual-thread executor cannot answer, which is precisely the obstacle PR #51's author hit. **That
obstacle is real and my control says nothing about it.**

**Before it can land:**

1. **Rebase across the `io.confluent` -> `bz.stub` package rename.**
2. **A JDK 21 CI lane.** The PR's own tests skip on JDK 17, which is what CI runs - a green check that
   verified nothing, the failure mode this repo has shipped before.
3. **Settle what the pressure system observes - and note that direct pull would dissolve this.** `isPoolQueueLow()` reads
   `workerThreadPool.getQueue().size()` and `getActiveCount()` off the `ThreadPoolExecutor`. A
   virtual-thread executor exposes neither, so the pressure system must move onto PC's own accounting -
   which is [the counter with a drift clamp](bug-available-work-counter-needs-a-clamp.md). **That
   dependency is the real work**, and it is why this is not a one-line change.

**And the honest boundary:** this was measured on a 12-core laptop with `Thread.sleep` as the handler.
A real handler blocks on I/O rather than sleeping, and a server has more cores. The *mechanism* -
blocked work holding an OS thread - is the same, but the **numbers should be re-taken against PC
itself** with PR #51 before any of them are published.
