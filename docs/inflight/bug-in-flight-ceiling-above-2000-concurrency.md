# Bug: in-flight plateaus near 2,750 records - and it is the Java Kafka stack, not the engine

<!-- inflight-type: bug -->
<!-- inflight-impact: performance -->

Found 2026-08-21 while building an unordered comparison at high concurrency. **No prior art**:
searched `docs/inflight/` for concurrency/in-flight/load-factor entries, `docs/solutions/`,
`docs/refactoring.md`, and the fork and upstream trackers for issues about maxConcurrency not being
reached. Nothing describes this. The nearest neighbour is the `ExternalEngine` throughput regression
in [`perf-throughput-regression-since-0-3.md`](perf-throughput-regression-since-0-3.md), which is a
different defect in the same subsystem - that one is about the *load factor buffer*, this one persists
with the load factor free to grow.

## The finding changed once a control arm existed - read this first

**Originally written as a PC defect. A control arm overturned that, and the note has been corrected
rather than quietly reworded.**

The `pool` arm is a plain `KafkaConsumer` with a fixed thread pool, a semaphore capping in-flight, a
sleep and a counter. **No engine, no shards, no control loop, no offset tracking.** At 100ms and a
concurrency setting of 5,000 it plateaus at **2,848 records in flight**. PC plateaus at **2,751**.

**The ceiling is essentially identical with the engine and without it.** Both Go arms - llingr and a
bare franz-go consumer - reach 5,000 exactly under the same conditions on the same broker and dataset.

**So this is not PC's dispatch path.** Everything below about shard traversal and allocation stands as
a description of real costs in that path, but those costs are not what sets this ceiling, and the
earlier draft that pinned it there was wrong. What follows keeps the refuted reasoning visible on
purpose: the controlled runs that ruled out four other suspects are still the most useful thing here,
and so is the record of a plausible source-level explanation that a control arm demolished.

**What PC does still owe.** Measured against the Java floor rather than against Go:

| Delay / concurrency | Java floor (`pool`) | PC (`core`) | PC's own cost |
|---|---:|---:|---:|
| 0ms / 250 | 97,201 | 96,618 | -0.6% |
| 0ms / 1,000 | 100,725 | 105,865 | **+5.1%** |
| 0ms / 5,000 | 96,246 | 93,162 | -3.2% |
| 2ms / 250 | 53,209 | 57,425 | **+7.9%** |
| 2ms / 1,000 | 68,250 | 67,259 | -1.5% |
| 2ms / 5,000 | 43,482 | 34,016 | **-21.8%** |
| 100ms / 250 | 2,384 | 2,241 | -6.0% |
| 100ms / 1,000 | 9,095 | 7,318 | **-19.5%** |
| 100ms / 5,000 | 20,102 | 19,577 | -2.6% |

**PC's engine is close to free at most points and sometimes faster than a naive pool** - which is a
genuinely good result and was not visible before this arm existed. **Two cells are not**: 2ms at 5,000
and 100ms at 1,000, both around -20%. Those two are PC's to answer for, and they are a much smaller
and much better-defined target than "PC is half the speed of the competition".

**And the large term is the client.** The Java floor against the Go floor: 67% of it at 0ms, 53% at
100ms/5,000, and 31% at 2ms/5,000. That gap is `kafka-clients` versus franz-go with no engine on
either side, and it dwarfs everything the engine contributes.

## The symptom

Core engine, `UNORDERED`, all keys distinct, handler is a plain `sleep`, real broker, one consumer
group per run.

| maxConcurrency | Delay | Peak in flight | Reached the setting? | msg/s |
|---:|---:|---:|:--|---:|
| 250 | 100ms | **250** | yes, exactly | 2,241 |
| 1,000 | 100ms | **1,000** | yes, exactly | 7,318 |
| 5,000 | 100ms | **2,751** | **no - 55% of it** | 19,577 |
| 5,000 | 200ms | **3,889** | **no - 78% of it** | 15,594 |

Raw data: [`bench/results/high-concurrency-unordered.csv`](../../bench/results/high-concurrency-unordered.csv).

At 250 and 1,000 the engine hits its configured concurrency **exactly**, which is what makes the
5,000 row a finding rather than noise.

## What it is not - three suspects ruled out by controlled runs

Each changed one term and left everything else identical.

| Suspect | Control | Result | Verdict |
|---|---|---|---|
| **Measurement window too short** (ramp, not ceiling) | 100,000 vs 500,000 records | Throughput rose 12,429 -> 19,385 (+56%); plateau moved 2,708 -> 2,752 (+1.6%) | **Partly guilty for throughput, innocent for the plateau.** The short window did understate the rate; it did not create the ceiling |
| **Single partition cannot supply records fast enough** | 1 vs 10 partitions | 19,385 -> 19,635 msg/s; plateau 2,752 -> 2,710 | **Refuted.** Within 8%, and the plateau moved the *wrong way* |
| **`max.poll.records` bounds intake** | 500 vs 5,000 | 19,635 -> 19,577 msg/s; plateau 2,710 -> 2,751 | **Refuted.** Within 0.3% |
| **The pressure / loading system is starving it** | Dynamic load factor vs a **static 25,000-record buffer** - 2.5x the threshold the dynamic system starts at | 19,577 -> 19,776 msg/s; plateau 2,751 -> 2,654 | **Refuted.** Within 1% on rate, and the plateau moved *down* |

**So it is not the broker, not the client's fetch pipeline, not the harness, and not the buffer.** It
is inside PC, downstream of every knob that governs how much work is allowed into the pipeline.

### Why the pressure system was the obvious suspect, and why it is not the answer

It was the right thing to check - it is the same subsystem that caused the `ExternalEngine`
regression, where the load-factor buffer *was* the cause. Here it is not, and the source says why as
clearly as the experiment does.

`WorkManager.isSufficientlyLoaded()` throttles the broker poller when
`awaitingSelection + outForProcessing > targetAmountOfRecordsInFlight x loadingFactor`. At
maxConcurrency 5,000 with the load factor at its initial 2, that threshold is **10,000 records**. The
measured `outForProcessing` is about 2,750 - so roughly **7,250 records are already sitting in the
shards awaiting selection** while only 2,750 are dispatched.

**The work is there. It is not being selected fast enough.** That is what makes this a dispatch defect
rather than a supply or admission one, and it is why raising the buffer to 25,000 changed nothing:
the pipeline was never short of records to choose from.

## What it appears to be, and the experiment that supports it

**Hypothesis: the ceiling is a dispatch *rate*, not a dispatch *count*.** If the control loop can only
hand the pool some fixed number of records per second, then by Little's law the in-flight count
settles at `dispatch_rate x delay` - and the way to test that is to change the delay, because a rate
ceiling predicts in-flight scales with it while a count ceiling predicts it does not.

**Prediction, stated before the run:** at 200ms, in-flight should roughly double to ~3,900.

**Result: 3,889.** The prediction held to within 0.3%, which is a stronger confirmation than the
experiment deserved and should be re-run before too much is built on it.

**But throughput fell from 19,577 to 15,594 (-20%)**, which a *fixed* rate ceiling does not predict.
So the dispatch rate is not constant - it **degrades as in-flight grows**, which is the signature of
per-record work whose cost rises with the size of the tracked set.

## What it is not caused by - the obvious candidate, checked

**Not the in-flight target.** `getTargetOutForProcessing()` returns
`targetAmountOfRecordsInFlight x currentFactor`; with maxConcurrency 5,000 and the load factor
starting at 2, the target is at least 10,000 - roughly four times the observed plateau. The control
loop is asking for far more work than it manages to place, so the constraint is downstream of the
target calculation, in the path that actually moves work from the shards into the pool.

Next place to look, and now the only place left: **`ShardManager.getWorkIfAvailable(int)` and the
per-iteration shard traversal.** It builds a `LoopingResumingIterator` over the shard map and walks
shards until it has collected `requestedMaxWorkToRetrieve` containers. With all keys distinct there is
**one shard per record**, so a request for 7,000 records means visiting 7,000 map entries, allocating
a fresh iterator each control-loop pass, and taking one container from each shard before moving on.
That cost rises with the number of live shards - which is exactly the "degrades as in-flight grows"
signature the 200ms run produced.

**The prediction that would confirm it** (not yet run): repeat the 5,000-concurrency measurement with
a *bounded* key set - say 10,000 distinct keys instead of one per record - so the shard map stays
small while everything else is identical. If the plateau lifts, the traversal is the cause. If it does
not, the limit is the control loop's iteration rate rather than its per-iteration cost. This is the
single cheapest next experiment and it needs the key-distribution axis from
[`next-performance-regression-testing.md`](next-performance-regression-testing.md) to exist first.

## What "theoretical" means here, and why 5,000 concurrency implies 50,000 msg/s

Worth stating plainly, because the two numbers have different units and it is easy to read the table
as claiming a throughput setting.

**`maxConcurrency` is a count of records being processed at once, not a rate.** The rate follows from
it by Little's law:

```
throughput = concurrency / latency
```

With 5,000 records each taking 100ms, 5,000 complete every 100ms, so **5,000 / 0.1s = 50,000
msg/s**. Change the delay and the same setting implies a different rate: at 200ms it is 25,000/s, at
2ms it is 2,500,000/s. **The "theoretical" column is therefore a property of the setting *and* the
handler, never of the engine** - which is why a single delay says nothing and the delay has to be an
axis.

Read the other way, the same law is the diagnostic: `in-flight = throughput x latency`. PC's measured
19,577 msg/s at 100ms implies a *mean* in-flight of 1,958, and we observed a peak of 2,751 - a
peak-to-mean ratio of 1.4, the same ratio the 200ms run produced. **Consistent, and it is what
identifies the constraint as a dispatch rate rather than a dispatch count.**

## The goal that follows: reach the setting at a zero-cost handler

**Owner's direction, 2026-08-21:** *PC should be able to reach its maximum theoretical concurrency
with a zero-millisecond user function.*

**The goal is right and needs restating in the units above, because at a literally zero-cost handler
"reach the concurrency" is unmeasurable, not merely hard.** By Little's law, in-flight is
`throughput x latency`; if latency is zero then in-flight is zero at any throughput. Nothing is ever
concurrently in progress because nothing is ever in progress. So the target at 0ms is not an in-flight
number - it is a **throughput** number, and what is being asked is: *with the handler costing nothing,
how fast can the engine move records?* That figure is pure framework overhead and it is the honest
measure of the engine.

**Two goals, then, and both belong on the v6 gate:**

1. **At 0ms: maximise records/second**, and state it as the engine's overhead figure. This is the
   number the `vanilla`, `franz` and `core` arms exist to make comparable.
2. **At a realistic delay (2ms and up): reach the configured `maxConcurrency` exactly**, as the engine
   already does at 250 and 1,000. That is a promise the API implicitly makes and currently breaks
   above roughly 2,000.

The second is the defect in this note. The first is a separate measurement that this note does not
address, and the low-delay sweep exists to establish it.

## Is it the cost of 5,000 platform threads? Probably not, and the existing data argues against it

**A good hypothesis** - Java platform threads are expensive next to goroutines or virtual threads, and
[the roadmap already carries virtual-thread support](../data/roadmap.yaml). But two things point away
from it:

- **The pool is built for it.** `setupWorkerPool` returns
  `new ThreadPoolExecutor(poolSize, poolSize, 0L, MILLISECONDS, new LinkedBlockingQueue<>(), ...)`
  with core and max both `maxConcurrency`. With an unbounded queue, a `ThreadPoolExecutor` adds a
  thread on every submission until core size is reached, so across 500,000 submissions the pool does
  grow to 5,000 threads. The threads exist; they are idle.
- **More threads were successfully kept busy when the delay was longer.** At 200ms, in-flight reached
  **3,889** - well above the 2,751 seen at 100ms. If the ceiling were a thread-count or
  thread-scheduling limit it would not move with the handler's duration. **A limit that scales with
  latency is a rate limit, not a capacity limit.**

**What would settle it**, and is not yet run: a virtual-thread executor behind the same engine, so
thread cost changes and nothing else does. If the plateau is unchanged, threads are exonerated
outright. This is a strong argument for bringing the virtual-threads roadmap item forward, if only as
a measurement.

## Is it lock contention? On the evidence, no - and that is worth stating precisely

**The question is the right one to ask**, and it is the standing hypothesis for the *other* defect in
this subsystem - the `ExternalEngine` regression, where the suspicion has been that more defensive
concurrent code and more thread-safe collections cost throughput between 0.3 and 0.5. That question is
still open. **This defect is not that.**

**There are no locks on the dispatch path.** A search for `synchronized`, `ReentrantLock` and
`.lock()` across `parallel-consumer-core/src/main/java` returns nothing in `WorkManager`,
`ShardManager` or `ProcessingShard` - the three classes that select and hand out work. The locks that
do exist are in `RetryQueue` (a `ReadWriteLock`), the `commitCommand` monitor in the control loop,
`ProducerManager`'s transaction begin, `DynamicLoadFactor.doStep()`, and `PCMetrics`. **None of them
sits between "a record is available" and "the record is submitted".**

**What the dispatch path does do per shard visit is allocate.** `ProcessingShard.getWorkIfAvailable`
opens with

```java
var slowWork  = new HashSet<WorkContainer<?, ?>>();
var workTaken = new ArrayList<WorkContainer<K, V>>();
var iterator  = entries.entrySet().iterator();
```

and `ShardManager.getWorkIfAvailable` builds a fresh `LoopingResumingIterator` over the shard map on
every control-loop pass. **With one shard per record - which is what a distinct-key workload produces -
a request for 7,000 records means visiting thousands of shards and allocating a `HashSet`, an
`ArrayList` and an iterator at each one, to take a single container from most of them.** That is a
per-record cost borne entirely by a single thread, and it grows with the number of live shards, which
is precisely the "degrades as in-flight grows" signature the 200ms run produced.

**REFUTED as the cause of this ceiling, by the `pool` arm.** The reasoning above describes real costs -
they are in the source and they are worth fixing - but a bare `KafkaConsumer` with a thread pool has
none of them and hits the same wall. Keeping this here as a record of a plausible source-level
explanation that a control arm demolished: **reading the source told a coherent story, and the story
was wrong.**

**Two factual corrections inside it, both mine:**

- **"One shard per record" is false for this workload.** `ShardKey.of` maps `KEY` ordering to the
  record key but maps **`PARTITION` and `UNORDERED` to the topic-partition**. The benchmark runs
  `UNORDERED` over 10 partitions, so there are **10 shards**, not 10,000. The allocation-per-shard-visit
  cost is therefore ten allocations per pass, not thousands.
- **The real per-pass cost inside a shard is a rescan, and it is still worth knowing about.**
  `ProcessingShard.getWorkIfAvailable` opens a **fresh iterator at the head of the shard's
  `ConcurrentSkipListMap` on every call**, and records that are already out for processing stay in that
  map until `onSuccess` removes them. `isOrderRestricted()` is false for `UNORDERED`, so the loop does
  not stop early. Every dispatch pass therefore walks past every in-flight container in the shard
  before reaching selectable work - O(in-flight) per pass, over a skip list, which is pointer-chasing
  with poor cache locality. **That is a genuine inefficiency that scales the wrong way. It is simply
  not what produced the 2,750 plateau**, because the plateau appears without it.

**One contention hypothesis does remain untested**, and it is on the other side of the pipeline: at
20,000 records/second, thousands of worker threads are calling back into shared completion state
concurrently. That is a genuine contention candidate, it is invisible to the dispatch-path reading
above, and it would show up in the same measurement. **Do not treat "no locks on dispatch" as "no
contention anywhere".**

**What the control arm points at instead: platform threads.** The one thing PC and the `pool` arm
share, and that neither Go arm has, is **thousands of live JVM platform threads**. At 100ms and 5,000
concurrent, both Java arms are trying to keep roughly 2,800 threads cycling through sleep and wake;
both Go arms keep 5,000 goroutines doing it without difficulty. **An earlier draft of this note
dismissed the thread hypothesis too quickly** - on the grounds that in-flight rose to 3,889 when the
delay doubled, which shows the ceiling is not a hard cap on thread count. That argument is still
correct and it does not rule out threads being expensive; a per-thread cost produces a rate limit, and
a rate limit is exactly what was measured.

**The decisive experiment is virtual threads, and it is now cheap and well-motivated.** Run the same
grid with a virtual-thread executor behind both the `core` and `pool` arms: thread cost changes and
nothing else does. If the plateau lifts, the cause is platform threads and
[the virtual-threads roadmap item](../data/roadmap.yaml) stops being a convenience feature and becomes
a throughput fix. If it does not, the remaining candidate is the Java client's fetch pipeline, and the
next step after that is a profiler over the 5,000-concurrency case to separate allocation, loop CPU
and contention in one pass.

## Why it matters

- **It is a silent misconfiguration.** A user who sets `maxConcurrency(5000)` gets 2,750 and nothing
  says so. There is no warning, no metric that names it, and no documentation of a practical upper
  bound. The setting is accepted and quietly not honoured.
- **It bounds the headline number** in any high-concurrency comparison, and it is the reason PC
  reached 39% of the theoretical rate in the unordered comparison where a competing engine reached
  its full configured concurrency.
- **It is directly relevant to [`next-auto-scaling.md`](next-auto-scaling.md)** - and is, in fact, an
  argument *for* it. A feedback-driven loop would discover this ceiling automatically and stop
  raising concurrency past it, without anyone having to know the number or why it exists. It also
  means the adaptive work must measure *achieved* in-flight, not the configured setting, or it will
  believe a number the engine is not delivering.
- **It is a v6 release-gate candidate**, alongside the `ExternalEngine` regression, since v6's stated
  purpose is being able to trust the library.

## Cheapest useful next steps

1. **Re-run on a quiet machine.** These were taken under normal desktop load; the ratios are sound,
   the absolutes are approximate, and the 200ms prediction landing to 0.3% wants confirming.
2. **Sweep concurrency between 1,000 and 5,000** to find where the plateau starts, rather than knowing
   only that it is somewhere in that gap.
3. **Instrument the control loop**: records submitted per iteration and iterations per second. That
   distinguishes "the loop runs too rarely" from "each iteration places too few".
4. **Emit a warning when achieved in-flight stays far below the configured maximum.** Independently of
   the root cause, silently not honouring a setting is the part users cannot debug.
