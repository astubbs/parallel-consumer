---
title: "Kafka Streams polls and processes on one thread, so anything you make asynchronous is throttled by the poll wait"
date: 2026-08-10
category: integration-issues
module: parallel-consumer-streams
problem_type: integration_issue
component: background_job
symptoms:
  - "Background workers complete during the consumer poll wait, but their completions are not drained until the StreamThread returns from poll"
  - "Throughput tracks the poll cadence rather than the work, so adding workers to the pool changes nothing"
  - "A no-concurrency control arm measures the asynchronous path SLOWER than stock Kafka Streams (0.69x) instead of tying with it"
  - "Roughly 74ms of per-record overhead that no profiler attributes to the user function"
root_cause: async_timing
resolution_type: config_change
severity: high
related_components:
  - kafka-streams
  - PcTaskDispatcher
  - HeadOfLineBlockingBenchmarkTest
tags:
  - kafka-streams
  - streamthread
  - poll-ms
  - threading-model
  - async-dispatch
  - framework-integration
  - performance
---

# Kafka Streams polls and processes on one thread, so anything you make asynchronous is throttled by the poll wait

## Context

`StreamThread` is a single thread that both polls the consumer and runs the topology. That is not an
implementation detail you can route around: it is the assumption that prices every other cost in the
loop. Under stock Kafka Streams, blocking in `Consumer#poll()` for up to `poll.ms` is free, because
while the thread is parked in poll there is by definition no processing it could be doing instead.
The same thread would have been doing it.

Introduce a background worker pool anywhere in the topology and that arithmetic inverts. Records are
being *completed* by the pool while the thread sits in poll, but the completions cannot be drained
and the next records cannot be dispatched until poll returns. The blocking call stops being idle
time and becomes dispatch latency, charged on every record.

This was found on `astubbs/parallel-consumer#271` (tracking issue `astubbs/parallel-consumer#255`)
while building a seam that hands `StreamTask.process()` records to a Parallel Consumer worker pool.
It surfaced not as a bug report but as a negative control that went the wrong way: with every record
forced onto one key, so no concurrency was available at all, the asynchronous path measured **0.69x**
against stock. Absence of concurrency explains a missing gain; it never explains a penalty. The
penalty was the poll wait.

The write-up is filed as domain knowledge about **Kafka Streams**, not about this project, because
nothing about it is specific to Parallel Consumer. Any integration that makes topology work
asynchronous inherits it: a background thread pool, an async HTTP client whose callback completes off
the StreamThread, an external task executor, a reactive bridge.

## Guidance

**1. Read the framework's run loop before you add a thread to it.** Kafka 3.9.2,
`org/apache/kafka/streams/processor/internals/StreamThread.java` (extract it with
`unzip -p ~/.m2/repository/org/apache/kafka/kafka-streams/3.9.2/kafka-streams-3.9.2-sources.jar org/apache/kafka/streams/processor/internals/StreamThread.java`).
The loop is `runLoop()` at line 690, which calls `runOnceWithoutProcessingThreads()` at line 713. Its
own javadoc states the coupling outright (`StreamThread.java:933-943`):

```
 * One iteration of a thread includes the following steps:
 *
 * 1. poll records from main consumer and add to buffer;
 * 2. restore from restore consumer and update standby tasks if necessary;
 * 3. process active tasks from the buffers;
 * 4. punctuate active tasks if necessary;
 * 5. commit all tasks if necessary;
```

Poll is step 1 and process is step 3, on one thread, in one iteration.

**2. Find the loop's blocking call and ask who else can now make work available.** The wait is
`pollPhase()` (`StreamThread.java:1213`), which in the `RUNNING` state passes the configured budget
down to the consumer (`StreamThread.java:1225-1228`, `1274-1286`):

```java
} else if (state == State.RUNNING || state == State.STARTING || ...) {
    // try to fetch some records with normal poll time
    // in order to get long polling
    records = pollRequests(pollTime);
```

`pollTime` is `poll.ms` (`StreamThread.java:635`), whose default is **100ms**
(`StreamsConfig.java:1146-1150`). The consumer is the only thing that can end that wait early, and
the consumer cannot see your worker completions. That is the whole defect in one sentence.

**3. Expect the exit condition to make it worse, not better.** The inner processing loop breaks back
out to poll the moment a pass dispatches nothing (`StreamThread.java:1049-1051`):

```java
if (processed == 0) {
    // if there are no records to be processed, exit after punctuate / commit
    break;
}
```

Under stock, "processed nothing" means the buffers are empty and blocking is correct. Under an
asynchronous dispatcher, "dispatched nothing" also means *the pool is full* or *every available key
is already in flight*, which are exactly the states that resolve on a worker completion rather than
on a broker fetch. So the loop reliably chooses to block at the precise moments a completion is
imminent. (The loop also breaks when it is halfway to the consumer's poll deadline,
`StreamThread.java:1052`, for the same reason: it is protecting a poll it assumes nobody else is
waiting on.)

**4. Confirm the cost with a one-term control, then report the term, not the workaround.** Change
`poll.ms` alone, hold the workload, arms, pool size and thread count identical, and show the outcome
flips. See `docs/solutions/best-practices/control-arms-vary-exactly-one-term.md` for why "one term"
has to mean one term and not one *parameter* that silently derives others.

**5. Treat lowering `poll.ms` as a mitigation with its own bill, never as the fix.** A flat low value
busy-spins an idle consumer, which is what the 100ms default exists to prevent. The framework's
one-thread model and an asynchronous model want opposite values from that one setting, and no
constant satisfies both. The fix is to **wake the loop when a completion arrives** rather than waiting
out the poll: poll with a short timeout to collect broker records, then block on your own condition
for the remainder of the configured budget, signalled by a worker completion or a retry timer. That
work is open here, ranked as item 3 in `docs/inflight/pr-ks-spike-next-work.md:49-61`, with the design
and its trap in `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:1345-1382`.

**6. Do not reach for `Consumer#wakeup()` as the signal.** It is the obvious mechanism and it is the
wrong one. `wakeup()` throws `WakeupException`, and Kafka Streams already uses it to mean *shutdown*.
A wake delivered while the thread is not polling arms the *next* poll instead, so a stray completion
signal can swallow the shutdown one. That is a failure that appears once in a thousand shutdowns and
will never be reproduced on demand.

**7. Check whether the framework already offers a supported decoupling.** Kafka has a second loop,
`runOnceWithProcessingThreads()` (`StreamThread.java:1102`), selected by `processingThreadsEnabled`
(`StreamThread.java:646, 710-714`). In 3.9.2 it is gated on the internal config
`__processing.threads.enabled__`, default **false** (`StreamsConfig.java:1310-1313`), so it is not
something to build on yet. It is worth knowing about anyway: its existence is Kafka's own
acknowledgement that poll-and-process on one thread is a constraint rather than a design goal, and
its shape (poll on the StreamThread, execute on task executors that pull their own work) is the shape
any asynchronous integration ends up needing.

## Why This Matters

**The cost is invisible from inside the original design.** `poll.ms` is documented as "The amount of
time in milliseconds to block waiting for input" (`StreamsConfig.java:695-696`) and marked
`Importance.LOW`. Both are true under stock Streams. Neither warns you that the parameter converts
into a throughput ceiling the moment something other than the consumer can make work available.
Nothing in the name, the documentation, the importance level, or any Kafka Streams benchmark will
tell you, because none of them were written for a topology with a second thread in it.

**It is charged on every workload, and concurrency masks it.** In the head-of-line-blocking
experiment the penalty was hidden in the positive arm because the concurrency win was larger than it.
Only the control, where concurrency was forbidden and could not pay the bill, exposed it. Once
removed, the experiment's own numbers more than doubled: the measured p50 improvement went from 8.0x
to 19.1x. A cost that only shows up when the benefit is absent is a cost that survives every
optimistic benchmark you run.

**It falsifies the claim people actually care about.** "No penalty when your workload cannot be
parallelised" is the safety property that makes an integration adoptable, and it is false at 0.69x.
That is why this is not filed as an optimisation.

**The general lesson: a dependency's performance characteristics encode its threading model's
assumptions.** A parameter that is free under the original design can become the dominant cost under
a modified one, and the parameter's name and documentation will not warn you, because they were
written by someone for whom it was free. When integrating with any framework, ask explicitly: *which
of this framework's costs are free only because of its threading model?* Those are exactly the ones
your change starts paying. Blocking waits, "cheap" polling intervals, batch flush timers, lock hold
times across a call you are about to make asynchronous, and any "we can afford to sleep here, there
is nothing else to do" comment in the source are all the same shape.

## When to Apply

- Dispatching Kafka Streams records to a thread pool, an executor, an async client, or any component
  that completes work off the `StreamThread`.
- Any framework integration where you introduce a thread the framework's main loop does not know
  about, and the loop contains a blocking wait.
- Reading a benchmark whose no-concurrency control came out *slower* than the baseline rather than
  tied. The absence of concurrency explains a missing gain, never a penalty, so a penalty there is a
  live finding.
- Tuning a timeout to recover throughput. Before writing it down as the answer, check whether you are
  compensating for a wait that should be interrupted by an event you already have in hand.
- Reviewing a change that adds a config recommendation to an integration guide. "Set `poll.ms` low"
  is the shape of a mitigation being quietly promoted to a solution.

## Examples

**The measurement (one-term control, only `poll.ms` changed).** From
`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:1333-1343`, using
`parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/HeadOfLineBlockingBenchmarkTest.java`
(one blocking 1500ms record at the head of a partition, 24 fast 25ms records behind it, pool of 4,
`NUM_STREAM_THREADS_CONFIG` pinned to 1 at line 275 so the only concurrency is the one under test):

| | Async-path overhead vs stock, single key | Experiment A p50 | Experiment A p99 |
|---|---|---|---|
| `poll.ms` = 100 (the default) | ~1695ms | 8.0x | 3.5x |
| `poll.ms` = 1 | ~24ms | **19.1x** | **11.8x** |

About 98% of the measured penalty was poll wait. With it gone, the asynchronous arm became limited by
pool size, which is the only thing that should limit it. Note that the committed benchmark does
**not** set `poll.ms` (`HeadOfLineBlockingBenchmarkTest.java:268-275`), so its published figures are
the pessimistic, default-configuration ones.

**The mitigation, and how to label it.** One line, and it must carry the reason it is not the fix:

```java
// MITIGATION, not a fix. StreamThread polls and processes on one thread, so a blocked poll stalls
// dispatch while workers are completing in the background. The fix is to wake the loop on a
// completion; a flat low value busy-spins an idle consumer, which is what the 100ms default prevents.
props.put(StreamsConfig.POLL_MS_CONFIG, 1L);
```

**The same trap, one layer down, already fixed in this repo.** A record consumed synchronously during
preparation (corrupted or dropped) has no worker to complete it, so its key-mate only became
available on the *next* pump. Under KEY ordering that stalled the whole key for a full poll cycle:
one poison pill, `~poll.ms` of head-of-line blocking. The fix was to feed synchronous outcomes back
within the same pass, and the comment recording it is at
`parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcTaskDispatcher.java:264-268`:

```java
// The pump loops while preparation consumes records SYNCHRONOUSLY (corrupted or dropped records:
// completed on this thread, no worker involved). Under KEY ordering a synchronously-consumed
// record's key-mate only becomes available once that completion is fed back, and deferring the
// feed-back to the next pump would stall the key by a full poll cycle - a poison pill would hold up
// its whole key for ~poll.ms. Stock consumes such records inline, so this loop restores parity.
```

The general form is worth extracting: **anything your integration defers to "the next time the loop
runs" is priced at the framework's blocking wait, not at your code's latency.** Once you know the
loop blocks for 100ms by default, every deferral in your own design gets re-read at that price.

**What to grep for in a new framework.** Not the symptom, the mechanism. Find the run loop, find its
blocking call, and read the exit conditions of the work loop inside it:

```
runLoop / runOnce / mainLoop      -> who owns the thread
poll( / take( / await( / sleep(   -> where it blocks, and for how long
if (processed == 0) break         -> when it decides there is nothing to do
```

The third line is the one that matters. "Nothing to do" is a judgement the framework makes on behalf
of a thread it believes it owns exclusively, and your integration has just made that belief false.

## Related

- `docs/solutions/best-practices/control-arms-vary-exactly-one-term.md` - the one-term control
  discipline that confirmed this, and the co-variation failure that nearly voided the control which
  exposed it.
- `docs/solutions/best-practices/chase-refuted-predictions.md` - the control arm here went the wrong
  way, and chasing that anomaly rather than filing it is what found this.
- `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:1315-1386` - the full finding, the
  measurements, the wake-on-work design, and the `wakeup()` trap.
- `docs/inflight/pr-ks-spike-next-work.md:49-61` - the open item for the proper fix, ranked.
- `astubbs/parallel-consumer#271` - the PR the finding came out of.
- `astubbs/parallel-consumer#255` - the tracking issue for the Kafka Streams dispatch spike.
