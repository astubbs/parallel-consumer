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

## The law: `peak in-flight = min(maxConcurrency, r x handler_latency)`

**Settled by two one-line experiments after a second round of independent review.** The ceiling is not
a count and not a cap. It is a **rate** times a residence time.

| Prediction | Test | Result |
|---|---|---|
| At 400ms the ceiling lifts, because `r x delay` exceeds the cap | run the control at 400ms | **CONFIRMED - peak 5,000 of 5,000** |
| `prestartAllCoreThreads()` lifts it, if lazy thread *creation* sets it | prestart, then run at 100ms | **REFUTED - peak 2,722 of 5,000** |

**The first kills every fixed-limit explanation.** There is no OS thread cap, no timer-slot limit and no
wait-queue ceiling at ~2,700: the same JVM on the same machine held **5,000 threads inside a 400ms
sleep simultaneously**. Whatever binds at 100ms is gone at 400ms.

**The second kills thread creation as the mechanism**, which was the reviewer's leading candidate.
With all 5,000 threads created and parked in `take()` *before the first submit*, the ceiling is
unchanged. Creation timing is also arithmetically wrong for the job: `prestartAllCoreThreads()` took
**3,235ms for 5,000 threads - about 1,546/s**, twenty times too slow to produce a 2,700 ceiling at
100ms.

**So `r` is a thread ACTIVATION rate - park to running - not a creation rate.** Solving `peak / delay`
across every run gives a consistent machine constant:

| Run | Peak | Delay | Implied `r` |
|---|---:|---:|---:|
| 100ms, prestarted | 2,722 | 0.1s | **27,220/s** |
| 100ms, lazy | 2,673 | 0.1s | 26,730/s |
| 200ms, lazy (bench arm) | 3,889 | 0.2s | 19,445/s |
| 400ms | 5,000 (capped) | 0.4s | >= 12,500/s |

**And it is load-stable where throughput is not.** The ceiling holds at 2,438-2,756 across a 10x range
of machine load; throughput over the same range moves 4,648 to 22,844. Two quantities, opposite load
behaviour - which is why no single-mechanism story fitted both.

**Sleep overshoot is now reported as mean and tail, not just p50**, at the reviewer's request, and does
not hide a heavy tail: at 100ms it is **mean 3.8ms, p99 30ms, max 47ms**. Too small to matter, which
rules it out properly rather than by median alone.

### The shared queue is refuted too - and the constant is remarkably stubborn

A second review proposed that the gate is **serialized admission through the executor's single
`LinkedBlockingQueue` takeLock**, at roughly 40us per admission, and named a discriminator that no
competing theory could rationalise: **shard the executor.** Ten pools of 500 threads instead of one
pool of 5,000 - same total threads, same semaphore, same sleeps, round-robin submission. A shared-queue
gate predicts the ceiling lifts to ~5,000 because each sub-gate now carries a tenth of the admissions
and the gates run in parallel. Every process-wide explanation predicts no change.

| | Peak in flight |
|---|---:|
| One pool of 5,000 | 2,717 |
| **Ten pools of 500** | **2,840** |

**No change. Refuted.** Which also refutes the queue lock a third time, after replacing it with a
lock-free queue made things 69% worse and counting its size changed nothing.

**And the constant is now stable across an ~80x range of machine load:**

| Load average (12 cores) | Ceiling |
|---:|---:|
| ~8 | 2,756 |
| ~10 (prestarted) | 2,722 |
| ~102 | 2,673 |
| ~667 | 2,717 / 2,840 |

**A quantity that does not move when the machine is 80x busier is not a queueing or contention
effect.** Contention gets worse under load. This does not move at all.

### Where that leaves the mechanism: rate-shaped, process-wide, and unidentified

Everything that has been tested is out:

| Candidate | Refuted by |
|---|---|
| An OS thread cap or count limit | 5,000 concurrent sleepers at 400ms |
| Timer slots or wait-queue entries | same - and a limit that doubles when the sleep doubles is a rate |
| Lazy thread creation | `prestartAllCoreThreads` - ceiling unchanged |
| The shared executor queue's lock | sharding into ten pools - ceiling unchanged |
| The queue lock's cost | lock-free replacement, 69% worse; counted, no change |
| Sleep overshoot | measured: mean 3.8ms, p99 30ms - far too small |
| The submitter | the async arm uses the same submit loop and reaches 50,000 |

**What survives is a process-wide or system-wide rate of platform-thread activation**, around
**20,000-27,000 per second**, invariant to load, invariant to how the threads are pooled, and invariant
to whether they already exist. **That is a property of the JVM-on-macOS thread wake path, and this
investigation has not localised it further.**

**Which is the point at which to stop.** The formula is established and predictive, the fix is
established and does not depend on the cause, and further attribution needs kernel-level tooling
(`dtrace`, syscall counts, a `jstack` park census) rather than another Java experiment. **Recorded as
open, not as solved.**

### Why this matters more than the raw comparison

**It turns the ceiling into a formula a user can apply.** `min(maxConcurrency, r x handler_latency)`
predicts the whole curve: exact at 250 and 1,000, a knee somewhere between 2,000 and 5,000, and the
knee moving with the handler's duration. **The 200ms row, which the bug note recorded as unexplained,
sits on the line.**

**And it corrects the user-facing advice again.** Not "your `maxConcurrency` is ignored", nor even
"achieved concurrency is throughput-limited", but: **the reachable concurrency is your handler's
latency times how fast this machine can activate a thread. Raising `maxConcurrency` past that does
nothing; making the work not hold a thread removes the term entirely.**

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
   virtual-thread executor exposes neither, so the pressure system must move onto PC's own accounting.
   The load gate's half of that accounting is now conservation-derived (`ShardManager.getNumberOfRecordsInShards`);
   the shard's available-work counter is [still an approximation](bug-available-work-counter-is-still-an-approximation.md).
   **That dependency is the real work**, and it is why this is not a one-line change.

**And the honest boundary:** this was measured on a 12-core laptop with `Thread.sleep` as the handler.
A real handler blocks on I/O rather than sleeping, and a server has more cores. The *mechanism* -
blocked work holding an OS thread - is the same, but the **numbers should be re-taken against PC
itself** with PR #51 before any of them are published.
