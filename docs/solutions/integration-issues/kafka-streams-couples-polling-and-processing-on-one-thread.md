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
  - "A paused topology pins one core - the StreamThread stays RUNNING and healthy while the poll phase spins at roughly 1kHz, clearing only at the commit interval"
root_cause: async_timing
resolution_type: code_fix
severity: high
last_updated: 2026-08-11
related_components:
  - kafka-streams
  - PcTaskDispatcher
  - HeadOfLineBlockingBenchmarkTest
  - PcWorkSignal
  - PcWorkSignalTest
tags:
  - kafka-streams
  - streamthread
  - poll-ms
  - threading-model
  - async-dispatch
  - framework-integration
  - performance
  - busy-spin
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
for the remainder of the configured budget, signalled by a worker completion or a retry timer.

The contrast is real but it is not clean, and the rest of this section is about the part that is not:
the fix reproduces the mitigation's own busy-spin, from a different cause, unless the wait releases
once per raise rather than once per pass.

**This has now been built, and the numbers are in.** `parallel-consumer-streams` patches
`StreamThread.pollPhase()` to do exactly that, gated so an idle dispatcher keeps the stock full-budget
poll (`PcWorkSignal`, `-Dpc.streams.wakeOnWork.enabled`). At the **default** `poll.ms`, three runs each,
varying only that switch: the single-key control went from **0.70x to 0.99x** - the penalty is gone
rather than reduced - and the head-of-line experiment's p50 went from 5.5x to 17.1x, p99 3.0x to 9.2x.
The finding that prompted it is in `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md`, section
"The StreamThread's poll wait throttles dispatch"; the design as built, including its observability
counters, is in `docs/plans/2026-08-10-001-feat-ks-wake-on-work-plan.md`. Cited by section rather than
by line range on purpose - those plans are still being edited, and every line-range pointer into them
in this file had drifted by the time this section was added.

Two implementation notes worth carrying to any other framework, because both are ways to build this and
still lose:

- **Make the wake predicate a level-triggered read of live state, then AND it with a release-generation
  guard.** Level-triggered first, because an edge-triggered "a completion happened" flag has to be armed
  before the wait, and the arming point is necessarily *after* the previous dispatch pass, so a
  completion landing in that gap is discarded and the thread waits out the full budget with work in
  hand. Reading the completions queue directly cannot lose a wake, because only the parked thread drains
  it and it is not draining while parked. But that last clause says who *can* drain, and it is read as
  saying what *will* happen: a level-triggered read alone assumes the woken thread goes on to clear what
  it read, and a framework is free to skip the drain while keeping the thread alive and polling. Then
  the predicate is permanently true and the wait degenerates into a busy-spin. So release a wait only
  for raises it has not already been let out on. See **A level-triggered predicate assumes the woken
  thread drains** below, which is the half of this rule that was missing when it was first written here.
- **Signal after the in-flight count is decremented, not with the completion.** Signal first and the
  woken thread drains the completion, computes its free capacity against a count not yet decremented,
  dispatches nothing, and parks again with an empty queue and no further signal coming. That is a
  full-budget stall, microseconds wide, that will never reproduce on demand.

**A level-triggered predicate assumes the woken thread drains, and a framework that can legitimately
skip the drain turns the wait into a spin.** The predicate here is "an outcome is pending"
(`PcTaskDispatcher.hasPendingCompletions`,
`parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcTaskDispatcher.java:610-612`),
and that queue is emptied by exactly one method, `drainCompletions()` (`PcTaskDispatcher.java:490`).
What matters is not how many callers it has - it has several, all on owner-thread paths - but which of
them runs on a normal pass. That is the one at the head of `dispatchAvailable`
(`PcTaskDispatcher.java:344-345`), reached through the patched `StreamTask.process()`. The others are
commit (`collectCommitData()`, `PcTaskDispatcher.java:526-528`) and the two teardown paths, quiescence
and close - none of which a topology sitting paused in its poll phase reaches. So the question the
predicate turns on is whether Kafka gets to `process()`, and Kafka does not always get there. `KafkaStreams.pause()`
(`KafkaStreams.java:1827-1834`) adds the topology to `TopologyMetadata`'s paused set
(`TopologyMetadata.java:267-268`); `TaskExecutionMetadata.canProcessTask` reads that set and answers
`false` (`TaskExecutionMetadata.java:64-77`); and `TaskExecutor.process` skips `processTask` entirely
for any task that answers no (`TaskExecutor.java:69-89`, reached from `TaskManager.process`,
`TaskManager.java:2017-2018`). Kafka states the consequence in `pause()`'s own javadoc, which is the
strongest citation in the set because the framework is describing the trap itself
(`KafkaStreams.java:1821-1822`, Kafka 3.9.2):

```
 *  <p>Paused topologies will only skip over a) processing, b) punctuation, and c) standby tasks.
 *  Notably, paused topologies will still poll Kafka consumers, and commit offsets.
```

The thread stays `RUNNING`, keeps entering `pollPhase()`, and never reaches the drain. With one outcome
sitting undrained, a purely level-triggered wait returns instantly on every pass and the poll phase
becomes a loop of 1ms polls: one core pinned, and a thousand fetches a second where the default
`poll.ms` asks for ten. The rate is arithmetic from the short poll rather than a measurement, because
this was caught in review before anything ran it. It clears only when the commit path reaches the other
`drainCompletions()` call, and `commit.interval.ms` defaults to 30s outside EOS
(`StreamsConfig.java:160-161`), so "self-clearing" is worth very little. That it ends on a timer at all
is why it would have presented as an unexplained CPU cost rather than as a hang.

**The obvious reading of that failure is wrong, and acting on it re-opens the defect the design was
built to avoid.** "Level-triggering was the mistake, use an edge-triggered flag" restores the arming gap
and the lost wakeup with it. Both halves are load-bearing. The fix keeps the level-triggered read and
adds a *release generation* beside it: `workSignals`, bumped by every raise, against
`workSignalsReleasedOn`, the raise count this waiter has already left a wait on
(`PcWorkSignal.java:146-167`). A wait leaves early only for a raise it has not already been released
for:

```java
// before: the level-triggered form, which returns instantly on every pass once nobody drains
if (hasPendingCompletions()) {
    workArrived = true;
    break;
}
```

```java
// after (PcWorkSignal.java:339-345)
// Both halves are load-bearing. The pending outcome is what makes leaving USEFUL; the
// unreleased signal is what stops a caller that never drains from turning this into a spin.
if (workSignals != workSignalsReleasedOn && hasPendingCompletions()) {
    workSignalsReleasedOn = workSignals;
    workArrived = true;
    break;
}
```

A completion that landed *before* the wait began still ends it, because raising it bumped the counter
(`signalWorkAvailable`, `PcWorkSignal.java:278-283`), so the lost-wakeup property survives untouched. A
second pass with nothing new takes the full budget, which is exactly stock behaviour and the right
answer when nobody is draining.

**What the guard is not.** It is not a shadow of "is there work": nothing consults it to decide whether
to dispatch, and it cannot answer whether an outcome exists, which is still read live off the queue. It
records one thing only, which raises this waiter has already been let out on. It is also distinct from
`wakeRequests` (`PcWorkSignal.java:133-144`), the counter that abandons the wait outright for shutdown
and for the last dispatcher going away, and which deliberately does not count as a wake. Two counters
with two meanings is the minimum here, not duplication: one says "leave, work arrived", the other says
"leave, regardless".

**Which waits are immune, which is how to sweep for the rest.** A wait whose *departure is itself the
drain* cannot have this defect: `BlockingQueue.poll(timeout)` removes the element as the condition of
returning, so there is no separate path for a framework to skip. That is why this repository's core
control loop is not exposed - it waits on `workMailBox.poll(timeToBlockFor.toMillis(), MILLISECONDS)`
(`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java:1192`),
a consuming take. The exposure begins the moment the predicate reads state that some *other* code path
clears. Sweeping the tree on that criterion turns up one live sibling and one latent one, and both are
worth carrying:

- The sibling, and the first instance in this repository: the drain-path zombie, where a shadow copy of
  the shutdown state meant `consumer.poll()` was never called and the loop's intended long poll silently
  stopped happening, spinning at roughly 10kHz. Different term, identical shape and identical bill -
  `docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md`. `PcWorkSignal`'s class
  javadoc already cites it as the same shape (`PcWorkSignal.java:53-55`).
- The latent one: `ConsumerManager.commitRequested`
  (`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/ConsumerManager.java:75`)
  is a non-volatile boolean set by `onCommitRequested()` (`:292`), which has zero callers in the tree.
  The recorded recommendation is to wire it up level-triggered by reading `commitRequestQueue` directly
  (`docs/inflight/next-candidates.md`). That recommendation is right and it is now incomplete in exactly
  the way the bullet above was: whoever implements it has to name what clears `commitRequestQueue` and
  ask whether that path can be skipped while the waiter stays alive.

**Found in code review, and negative-controlled rather than merely fixed.** No test failed and nothing
was observed in production - the spin needs a paused or otherwise non-draining topology, which no
benchmark arm exercised. It was introduced and removed inside `astubbs/parallel-consumer#271`, before
the module was ever merged. Reverting the predicate to the level-triggered form fails one test, alone,
on its own assertion; see the negative control under Examples.

**6. Do not reach for `Consumer#wakeup()` as the signal.** It is the obvious mechanism and it is the
wrong one. `wakeup()` throws `WakeupException`, and it is the framework's word for *shutdown*. A wake
delivered while the thread is not polling arms the *next* poll instead, so a stray completion signal
can swallow the shutdown one. That is a failure that appears once in a thousand shutdowns and will
never be reproduced on demand.

**Correction of a detail, recorded so nobody "discovers" it and throws out the rule.** An earlier
revision of this note said Kafka Streams *already* calls `wakeup()` on the shutdown path. It does not,
in 3.9.2: `grep -rn "wakeup" ` over the sources jar returns only `TopologyMetadata.wakeupThreads()`,
which is a `Condition.signalAll` for the empty-topology park, and `StreamThread.shutdown()` merely sets
`PENDING_SHUTDOWN` and lets the run loop notice. So the collision is **latent rather than live**. The
rule is unchanged and the reasoning is stronger, not weaker: `wakeup()`'s meaning belongs to the
framework, which is free to start using it in any patch release, and a signal you own costs one small
class. Building on the absence of a call would be building on the framework's current implementation
rather than on its contract.

An incidental benefit of owning the condition: because you own it, shutdown can *end* the wait. That
needs a wake path distinct from "work arrived" - a `notifyAll` against a work predicate that is still
false will not release the waiter, so the shutdown wake has to be a state change the waiter also
checks. Confirm the shutdown path with a test that closes mid-dispatch, and assert the thread was
provably parked on your condition first, or a green result proves nothing. The built code went one step
further than this paragraph asked: the shutdown path is a *counter* captured on entry
(`wakeRequests`, `PcWorkSignal.java:133-144`) rather than a flag, for the same reason the work path
needed a release generation - a flag can be armed and cleared around a waiter, a monotonic count read
at entry cannot.

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

**The second general lesson: a level-triggered predicate carries a hidden assumption, that whoever you
wake goes on to clear the state you read.** It is never stated, it is invisible while it holds, and it
is precisely what makes level-triggering the safe choice in the first place - the state cannot be
missed because the waiter is the only one who consumes it. The moment the framework can legitimately
decline to run the consuming path while keeping the thread alive, the predicate is permanently true and
the wait becomes a spin: not a hang, not an error, just a pinned core and a self-inflicted request storm
that clears whenever some unrelated timer happens to drain the queue. Every framework has at least one
such state and they are all documented as features - paused, throttled, quiesced, circuit-broken,
backpressured, in backoff, mid-rebalance, standby. The check is mechanical: **for every level-triggered
predicate, name the single code path that clears it, then ask what makes that path skippable while the
waiting thread stays alive.** If you cannot name one drain site, the predicate is already wrong for a
different reason. If you can name it and the framework can skip it, the wait needs a generation guard
so it releases once per raise instead of once per pass. Note where the cost lands: this defect is paid
in CPU rather than in correctness, so no assertion fails, no record is lost, and nothing but a profiler
or a power bill will report it.

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
- Writing or reviewing a `wait`/`await` whose predicate reads live state rather than a flag. Name the
  one code path that clears that state and check it is on the path the wake is supposed to unblock. A
  predicate cleared by someone other than the woken thread, or by a path the framework can skip, spins
  instead of waiting. A wait whose departure is itself the drain, such as a blocking-queue take, is
  immune and needs no guard.
- Integrating with a framework that has a pause, throttle, quiesce, drain-stop, standby or
  circuit-breaker mode. Those all mean "keep the thread alive but do not run the work", which is exactly
  the state that starves a level-triggered predicate and converts your wait into a busy loop.

## Examples

**The measurement (one-term control, only `poll.ms` changed).** From
`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md`, section "The StreamThread's poll wait
throttles dispatch", using
`parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/HeadOfLineBlockingBenchmarkTest.java`
(one blocking 1500ms record at the head of a partition, 24 fast 25ms records behind it, pool of 4,
`NUM_STREAM_THREADS_CONFIG` pinned to 1 at `HeadOfLineBlockingBenchmarkTest.java:282` so the only
concurrency is the one under test):

| | Async-path overhead vs stock, single key | Experiment A p50 | Experiment A p99 |
|---|---|---|---|
| `poll.ms` = 100 (the default) | ~1695ms | 8.0x | 3.5x |
| `poll.ms` = 1 | ~24ms | **19.1x** | **11.8x** |

About 98% of the measured penalty was poll wait. With it gone, the asynchronous arm became limited by
pool size, which is the only thing that should limit it. Note that the committed benchmark does
**not** set `poll.ms` anywhere in `HeadOfLineBlockingBenchmarkTest.java`, so its published figures are
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

**The negative control for the anti-spin guard, and why it has to assert on elapsed time.**
`parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/PcWorkSignalTest.java:343`
dispatches one record, waits until an outcome is genuinely sitting in the mailbox, takes a first wait
and asserts it returns well inside its budget, then asserts the outcome is *still* undrained - "which
is exactly what a paused topology does to this thread" - and takes a second wait against the same
budget:

```java
long second = timeMillis(() -> PcWorkSignal.awaitWorkForRemainderOf(SHORT_BUDGET));
assertThat(second)
        .as("nothing new arrived and nobody drained, so this wait must take its budget. Returning "
                + "instantly here is a busy-spin on the StreamThread, not a fast path.")
        .isGreaterThanOrEqualTo(SHORT_BUDGET.toMillis() - PcWorkSignal.SHORT_POLL.toMillis() - 50);
```

The assertion is on elapsed time and not on a counter, because a wait counter reads the same whether
the wait blocked or returned instantly - which is the whole defect. Reverting the predicate to the
level-triggered `if (hasPendingCompletions())` fails this test and no other, on that last assertion:
the deliberately undrained outcome is the one fixture in the suite that reproduces a non-draining
caller, and every other case reaches the wait with either no signal at all or real work still in
flight. A fix that reproduces the symptom when removed, on a named assertion, is the difference between
a fix that works and a fix you can show is the cause.

## Related

- `docs/solutions/best-practices/control-arms-vary-exactly-one-term.md` - the one-term control
  discipline that confirmed this, and the co-variation failure that nearly voided the control which
  exposed it.
- `docs/solutions/best-practices/chase-refuted-predictions.md` - the control arm here went the wrong
  way, and chasing that anomaly rather than filing it is what found this.
- `docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md` - the first instance in
  this repository of the same spin shape: a wait that was meant to block silently stopped blocking, at
  roughly 10kHz. Different mechanism, identical bill.
- `docs/solutions/integration-issues/kafka-streams-task-lifecycle-callbacks-do-not-mean-what-they-are-named.md`
  - the same governing rule from the other side, that the framework reaches or skips a path contrary to
  what the integration assumes. That doc says two instances is a class rather than a coincidence; the
  paused topology skipping `process()` is a third.
- `docs/solutions/architecture-patterns/a-progress-signal-must-count-work-consumed-not-work-accepted.md`
  - the same structural fact underneath, that the only drain on a normal pass is inside
  `dispatchAvailable` and the StreamThread reaches it only through `process()`.
- `docs/solutions/best-practices/fresh-work-needs-independent-review.md` - the spin was found by a
  review pass over same-session work, not by a test or by production.
- `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md`, section "The StreamThread's poll wait
  throttles dispatch" - the full finding, the measurements, the wake-on-work design, and the
  `wakeup()` trap.
- `docs/inflight/pr-ks-spike-next-work.md` item 3 - the fix, built and measured on the branch. The
  module itself is unmerged.
- `parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcWorkSignal.java` - the
  condition itself, and the reasoning for every choice in it.
- `astubbs/parallel-consumer#271` - the PR the finding came out of.
- `astubbs/parallel-consumer#255` - the tracking issue for the Kafka Streams dispatch spike.
