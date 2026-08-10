# `parallel-consumer-streams` - Kafka Streams, driven by Parallel Consumer

> ## ALPHA. EXPERIMENTAL. NOT PRODUCTION READY.
> This is published so people can try it, not because it is finished. Large parts of Kafka Streams do
> not work here, **and they do not say so** - they run to completion and emit wrong answers. This
> README says which. **Field testers wanted** -
> [astubbs#255](https://github.com/astubbs/parallel-consumer/issues/255).

Stock Kafka Streams processes one partition strictly one record at a time. If a record takes a second
of blocking IO, every record behind it in that partition waits a second, whatever key it has and
however cheap it is. Your only lever is more partitions.

This module removes that constraint from *inside* the topology. Records on **different keys in the
same partition** run concurrently, in your unmodified processor chain, with per-key order preserved.
In this module's own benchmark, the typical record behind a 1.5 second blocker went from **1872ms to
366ms**, and the quickest from 1554ms to 27ms.

Two questions follow immediately, and they are the whole of this document. There is a third answer you
should not have to take from us at all: it is a **switch**, so you can
[run your own topology both ways](#do-not-take-any-of-this-on-trust-run-your-own-topology-both-ways)
and check both the speed and the correctness claims on your own code.

---

## 1. Why is this even possible?

There is one point inside a Kafka Streams task where the task takes the next record off the partition
and hands it into the processor chain. That handover is the only thing this module replaces.

| | Stock Kafka Streams | With this module |
|---|---|---|
| Who picks the next record | the task, from the head of the partition | Parallel Consumer's work manager |
| How many at once, per partition | exactly one | as many as the pool allows, one per key |
| Ordering guarantee | per partition | **per key** - weaker than per partition, and usually the one a topology actually relies on |
| The processor chain itself | unmodified | **unmodified** |
| Concurrency ceiling | your partition count | your key count, up to the worker pool size |

Parallel Consumer already knows how to select the next runnable record while respecting key order,
track what is in flight, and commit only what is genuinely finished. It has done that for a plain
consumer for years. All this module does is let a Kafka Streams task ask it the question that the task
was previously answering with "the next one, then wait".

That substitution is the whole idea, and it needed **no new Parallel Consumer API**. What it did need
is a patch to Apache Kafka, because the handover point is not extensible - see
[How it is built](#how-it-is-built).

**What it is not.** This is not "Kafka Streams, but faster". It is *within one partition* and it is
for *blocking* work. Stock Streams already parallelises across partitions, and that is not what is
being compared anywhere in this document. CPU-bound topologies will not behave like the numbers below.

---

## 2. Why do we believe it works?

Because of what fails, and does not fail, when you run it. Every figure below has a companion figure
that qualifies it, and they are printed together on purpose: on their own, each one misleads.

### Apache Kafka's own test suite, run against the patched classes

With the seam **off**, Kafka's own tests pass unmodified against our patched classes:

| Apache Kafka test class | Run | Failures |
|---|---|---|
| `StreamTaskTest` | 101 | 0 |
| `RecordCollectorTest` | 59 | 0 |
| `ProcessorContextImplTest` | 28 | 0 |
| **Total** | **188** | **0** |

Zero skipped as well as zero failed. Not rewritten, not recompiled, not excluded, no assertion
relaxed - Kafka's own compiled test classes, taken from the `kafka-streams` test jar on Maven Central.
This runs on every build of this module, no profile and no flag.

**And here is what that number does not mean.** With the seam **on**, the same `StreamTaskTest` is
**67 of 101**. All 34 failures are in that one class: `RecordCollectorTest` and `ProcessorContextImplTest`
still pass 59 and 28 with the seam on, because neither of them constructs a `StreamTask`. Those 34 are
the semantic gap between this module and stock Kafka Streams, measured rather than estimated, and they
are what
[Current Shortcomings](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md#current-shortcomings)
is drawn from.

So: 188 is a claim that **the patch does not break Kafka when it is not being used**. It is not a claim
of equivalence. Quote it without the 67 and it says something untrue.

> **If you re-derive these counts yourself, do not scope the run with `-Dtest=`.** It silently
> overrides this execution's `<includes>`, so Kafka's suite does not run at all - and the build still
> goes green, with the number you were checking never computed. Run the module's whole `test` phase and
> read the per-class counts out of `target/surefire-reports-kafka-upstream/`.

### Head-of-line blocking, and the control that costs us the headline

One partition. One record costing 1500ms at the head. Twenty-four records costing 25ms each behind it,
on other keys. Same JVM, same patched classes, same broker; the *only* thing that changes between the
two arms is whether the seam is on.

| Fast-record latency, n=24 | Stock dispatch | PC dispatch | |
|---|---|---|---|
| quickest | 1554ms | 27ms | *57.6x - read the note* |
| **median** | **1872ms** | **366ms** | **5.1x** |
| slowest | 2226ms | 814ms | 2.7x |
| *control: identical run, all records on **one** key (median)* | *1934ms* | *2715ms* | ***0.71x*** |

**The median is the speedup: 5.1x here, and 5.1x to 8.0x across three runs.**

**The minimum states the claim, and is not a speed multiplier.** "A fast record does not have to wait
for a slow one" is falsified if even the luckiest fast record waited, and demonstrated if a single one
did not - so the quickest is the statistic that states it, and under stock dispatch the quickest still
paid the full 1.5 seconds, because the partition is handed over one record at a time. But 57.6x is
bounded by the fixture's own construction: the workload is 1500ms over 25ms, so the quickest record
can never beat 60x, and 57.6x is that ratio minus the handoff. It is a figure we designed rather than
discovered, and the first competent reader will divide 1500 by 25. Quote it as evidence that
head-of-line blocking is gone. Never as a speed multiplier.

**The last row is against us, and it is the most important row in the table.** Put every record on a
single key and the seam does not merely stop helping - it **loses**, at 0.71x. Key ordering permits
one in-flight record per key, so with one key there is nothing to run concurrently and the honest
expectation is 1.00x. No absence of concurrency can produce a number below it. Something is being
*paid* here, on every workload, and concurrency is merely hiding it in the rows above.

**The cause is known, and it is not key ordering.** `StreamThread` is a single thread that both polls
and processes, so blocking for up to `poll.ms` - **100ms by default** - costs stock Kafka Streams
nothing: while that thread is parked there is by definition no processing it could be doing instead.
Under the seam that assumption is false. Workers finish *during* the poll wait, and neither their
completions nor the records they unblock can move until poll returns. With one key, Parallel Consumer
releases one record at a time, so the thread dispatches a single record and goes straight back into a
100ms block.

The size of it is visible in the control's own numbers. Ideal serial time for that arm is
`1500 + 24 x 25 = 2100ms`. Stock drained the batch in 2270ms, overshooting by 170ms; the seam took
3949ms, overshooting by 1849ms. That is roughly **67ms per record of cost that bought nothing** - the
same order as the 100ms poll it is waiting on. A separate one-term experiment on `poll.ms` alone,
recorded in
[Current Shortcomings](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md#current-shortcomings),
attributes nearly all of it to that wait.

**So set `poll.ms` low yourself while the seam is on** - it recovers most of the gap today. That is a
workaround and not the fix; the fix is to wake the blocked poll the moment a worker completes, and it
is on the same worklist as the largest single win available. Until it lands, the honest statement of
this module's converging case is "worse", not "no better".

That row stays in the table for two reasons. Dropping the unflattering row of your own benchmark is how
benchmarks become marketing. And it is what *licenses* the rows above it: had the seam still won with
one key, the 5.1x would be coming from a faster harness or a warm-up artefact rather than from key
concurrency, and both results would have to be withdrawn rather than published.

**The third row is the weakest.** At n=24 the slowest sample is the last record queued through a pool
of four, so it measures pool depth rather than blocking, and we do not lean on it.

Two caveats attach to every figure here, and to any figure quoted from it: the comparison is **within
one partition** (stock Kafka Streams parallelises across partitions, and that is not what is being
measured), and the workload is **blocking IO**, which is the case Parallel Consumer exists for.
CPU-bound work will not behave like this.

The table is one run of three taken on the same machine, and deliberately the one with the *lowest*
median gain. Across all three the median ratio was 5.1x to 8.0x, the quickest-record ratio 51.5x to
57.6x, and the single-key control 0.67x to 0.71x - so the control reproduces, and the median is the
figure with real spread in it. Quote it as a range if you quote it at all. Absolute latencies move with
machine load, which is why the test asserts a floor of 3x on the median rather than the measured
figure, and a ceiling of 1.5x on the control. Reproduce with `HeadOfLineBlockingBenchmarkTest`
([source](src/test/java/io/confluent/parallelconsumer/streams/integrationTests/HeadOfLineBlockingBenchmarkTest.java)) -
it is an integration test, so `./mvnw -pl .,parallel-consumer-streams verify` - or better,
[run it against your own topology](#do-not-take-any-of-this-on-trust-run-your-own-topology-both-ways).

### Crash safety, proven rather than argued

The hard part of running records out of order is knowing what you are allowed to commit. Commit too
eagerly and a crash silently swallows work that was still running.

Parallel Consumer's answer is the one it already uses everywhere else, and this module inherits it
whole. It commits the **frontier**: the offset below which *everything* is contiguously finished. Work
that finished *above* the frontier is not thrown away - it is recorded in the commit's metadata as a
list of exceptions, so a restart resumes at the frontier without redoing what is already done. It is
the same shape as TCP's cumulative ACK plus its SACK blocks. Both terms are defined in
[`CONCEPTS.md`](../CONCEPTS.md).

The test is written red-first, and it is an integration test against a real broker rather than an
argument:

| What is checked | Result |
|---|---|
| A commit lands while the head record is still parked inside the chain | committed offset is **0**, the parked record's own offset - never higher |
| Kill the process at that moment, with no drain and no final commit | restart replays the in-flight record; **11 of 11 outputs present, nothing lost** |
| Restart the same consumer group with the seam **off** | stock Kafka Streams reads our commit metadata and continues, no crash |

Against the pre-fix mechanism the first row committed offset 11 - the consumer position - which is the
defect the test exists to demonstrate. Source:
[`CommitFrontierCrashRestartTest`](src/test/java/io/confluent/parallelconsumer/streams/integrationTests/CommitFrontierCrashRestartTest.java).

### Do not take any of this on trust: run your own topology both ways

Every benchmark invites the same reply, and it is a fair one: *you chose that workload*. So the
seam is a switch, and the switch is the point.

**One term changes.** Same JVM, same broker, same patched classes, same topology, same data - dispatch
on, then dispatch off. Every measurement in this document was produced that way, and the same control
is available to you on code we have never seen. Two uses, and the second is the stronger:

| | What you do | What it tells you |
|---|---|---|
| **Performance** | Run *your* topology both ways: your key distribution, your processing costs, your partition count | Whether this helps *you*. It is evidence we cannot manufacture and cannot be accused of rigging |
| **Correctness** | Point an existing Kafka Streams test suite at your app with dispatch **on**, and see whether it still passes | Whether the parallel path preserves the behaviour *your own tests already encode* |

The correctness use is what this module's own proof tests do against a stock fixture, handed to each
adopter for the code they actually care about. It turns "trust our alpha" into "verify it on your own
code", which for an alpha is a far better offer than any number we could publish. It also matters more
here than it would elsewhere, because nothing in this module will *tell* you when it is wrong - see
[what does not work](#what-does-not-work---and-it-will-not-tell-you). Your own assertions are the
detector.

**What makes the A/B meaningful is what happens when it is off.** With dispatch disabled the topology
behaves exactly as stock Kafka Streams. The choice is made once, when each task is constructed, and a
task that chose the stock path never takes the parallel one: no worker pool, no work manager, nothing
to bypass at runtime. That is why Kafka's own 188 still pass. So a suite that goes red with dispatch on
and green with it off has isolated the seam rather than something else in your build.

**Two honest limits on the correctness use**, because it is easy to over-read:

- A green suite with dispatch on is strong evidence about **your topology**. It is not a general
  equivalence proof, and it does not retire the 34 `StreamTaskTest` failures above.
- Every known gap still applies. Stream-time behaviour, caching on state stores, retries and EOS do not
  become safe because your tests pass, and a suite that never exercises them proves nothing about them.
  Read
  [Current Shortcomings](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md#current-shortcomings)
  first, and treat a green run as evidence for what you tested.

Today the switch is a JVM-wide system property, described under [Trying it](#trying-it) - fine for a
benchmark harness or a test suite, awkward if you want two differently-configured instances in one
process.

### Output equality

For a stateless topology and for a non-windowed aggregation over a state store, output is identical to
a stock Kafka Streams baseline generated in a **different Maven module** that has no dependency on this
one - with a test asserting the patched classes are absent from that JVM, so "stock" means stock.

Two things make that a claim about the *seam* rather than about the harness, and both are asserted
rather than assumed: every record must have reached the chain through the worker pool (a counter, since
output equality on its own would be satisfied by the stock path doing all the work), and a pool of four
must have had **at least three records inside the chain at once**. The stateless arm is a
`@RepeatedTest(3)`, because one green run of a concurrency experiment is a coin toss with the schedule.
Full write-up in the [result document](../docs/plans/2026-08-08-002-ks-on-pc-spike-result.md).

---

## What does not work - and it will not tell you

**This is the sharpest edge on the module, and it is worth reading before anything else here.**
Everything known to be broken is **silently** broken. There is no exception, no warning and no log
line. Build a topology that uses one of the constructs below, run it with the seam on, and it will
start, run to completion, and emit plausible wrong answers.

That is not an oversight in the reporting; it is the shape of the defect. These constructs read a
stream-time counter that never advances on this path, and several of them mutate a non-volatile `long`
from every worker thread. Nothing about either failure mode is detectable from inside the operator, so
nothing throws.

**The practical consequence: your own assertions are the only detector you have.** Run the switch both
ways and compare outputs, on every topology you care about. A run that merely completes tells you
nothing.

The list itself is deliberately **not** enumerated in full here. Implementation has not stopped, so a
list in this README goes stale the week it is written. The living list, with the mechanism behind each
item and an assessment of what closing it would cost, is
**[Current Shortcomings](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md#current-shortcomings)**.

Read it before relying on this anywhere that matters. In outline:

- **Stream time does not advance**, because it advanced at the selection step this module replaced.
  `STREAM_TIME` punctuation never fires. Wall-clock punctuation is unaffected and works normally.
- **Windows, joins and suppression are downstream of that**, and are worse than merely
  stream-time-blind: window close, join emission and suppression all read fields that are plain
  non-volatile `long`s doing read-modify-write, mutated from every worker. Under concurrency they are
  corrupted, not just reordered. Results change, not only their timing.
- **Caching must be disabled on state stores**, which changes what your topology emits - with caching
  on the DSL emits roughly one record per key per commit interval, with it off it emits every update.
- **Retries are off**, and a failure surfaces one pump cycle later than it would in stock, rather than
  synchronously at the throw.
- **Exactly-once is out of scope.** This module is at-least-once. The obstacle is on the Streams side:
  the transaction is per-`StreamThread`, so a worker's send joins a transaction covering every task on
  that thread.
- **Consumer backpressure never fires.** `StreamTask`'s buffer is what pauses a partition, and the PC
  path never fills it, so PC's own concurrency limits are the only inflow control there is.
- **Rebalance is unexercised.** Not known broken; not tested either, which for an alpha is the same
  thing in practice. Every test here runs one `StreamThread`, one partition, one task.

---

## Does this change anything if you only use plain Parallel Consumer?

**No, and that is checkable rather than a reassurance.**

The sentence "Parallel Consumer now patches Kafka Streams internals" is alarming if you read it as a
change to the library you already use. It is not one:

| Claim | How to check it |
|---|---|
| This module is a **leaf**. It depends on `parallel-consumer-core`; nothing depends on it | grep the reactor for `parallel-consumer-streams` - the only hits are its own pom and the `<module>` line in the root pom |
| Adding it **changed no shipped code in any existing module** | diff this branch against `master`: outside this module and the docs, nothing under any `src/main` changed at all. The edits are the root pom's `<module>` line, a `NOTICE` addition attributing the modified Apache classes, a skip in the copyright-header script for generated Apache source, and new test-only files in the examples module |
| The patched Kafka classes exist **only inside this module's own jar** | they are compiled from a patch at build time into this module's `target/classes` |
| The seam is **unreachable** from core, vert.x and reactor | no source in those modules names `PcDispatchSwitch` or the `io.confluent.parallelconsumer.streams` package. The stock-baseline fixture in the examples module asserts *at runtime* that the patched classes are absent from a JVM that does not depend on this module, and fails the build if one appears |

So **taking the dependency is the entire opt-in, and not taking it is a complete opt-out** that
requires no configuration, no flag, and no knowledge that this module exists.

---

## Trying it

Take the dependency, and the seam is on. Nobody puts an alpha module called
`parallel-consumer-streams` on their classpath by accident, so there is no second switch to find.

```xml
<dependency>
    <groupId>bz.stub.parallelconsumer</groupId>
    <artifactId>parallel-consumer-streams</artifactId>
    <version>${parallel-consumer.version}</version>
</dependency>
```

Same version as the rest of Parallel Consumer; this module is released in lockstep with core.

> **Read the classpath hazard below before you do this in an application.** It is the single biggest
> reason this is alpha, and it is a packaging problem rather than a code one.

| Property | Default | Effect |
|---|---|---|
| `pc.streams.dispatch.enabled` | `true` | `false` gives stock, serial Kafka Streams dispatch back. This is the A/B control described [above](#do-not-take-any-of-this-on-trust-run-your-own-topology-both-ways) |
| `pc.streams.dispatch.poolSize` | `4` | Worker threads per task; also Parallel Consumer's max concurrency |

A value for the first that is neither `true` nor `false` fails loudly rather than being read as "off" -
a typo in the property whose whole job is to disable the seam would otherwise leave a control arm
silently uncontrolled.

Also worth setting while the seam is on: a **low `poll.ms`**. The default 100ms throttles dispatch on
this branch, which is what the single-key control measures. See
[the control row](#head-of-line-blocking-and-the-control-that-costs-us-the-headline).

From a test, so that each arm states which path it wants rather than inheriting a default:

```java
PcDispatchSwitch.enable(4);            // worker threads per task
// PcDispatchSwitch.disable();         // ... or the stock path, said out loud
try {
    // ... build and run the topology ...
} finally {
    PcDispatchSwitch.resetToDefault(); // hand the JVM back as you found it
}
```

Two things to know before you get a surprising result:

- **The switch is process-wide static state**, decided once when the task is constructed, so a task
  never changes record paths halfway through a run. Tests that touch it must be `@Isolated`.
- **There is no example application yet.** The working demonstrations on this branch are the
  integration tests under
  [`src/test/.../integrationTests`](src/test/java/io/confluent/parallelconsumer/streams/integrationTests) -
  `PcDrivenStreamsProofTest` and `PcDrivenStatefulProofTest` are the two to read first. The existing
  `parallel-consumer-example-streams` module shows the *old* pattern (a topology handing slow work to a
  separate Parallel Consumer downstream) and does not use this module.

### The classpath hazard

**Do not combine this module with a different `kafka-streams` version than it was built against**
(currently **3.9.2**).

This module ships a handful of compiled classes in Apache Kafka's *own* packages, and depends on
`kafka-streams` for the other thousand. The mechanism is that ours precede the real jar on the
classpath and win. Inside this module's build that is controlled and asserted. As a published
dependency it is not defensible, in three distinct ways:

1. **Classpath order is a convention, not a guarantee.** Maven, Gradle, IDEs, shaded uber-jars and
   Spring Boot's loader may order entries differently. When ours lose you silently get pure stock Kafka
   Streams, with no error - the worst shape a failure can take.
2. **Class loading is per class, so the result is always a mixture.** That works only while both halves
   are the same version, and nothing checks that they are. Bump `kafka-streams` to 3.10 without
   bumping this module and you run our 3.9.2-derived internals against their 3.10 internals. That is
   the outcome of a routine dependency bump, not an exotic misconfiguration.
3. **It is illegal on the module path.** JPMS forbids split packages outright.

The options for fixing it properly, and why the leading one turns on Maven *coordinates* rather than on
forking, are weighed in
[Current Shortcomings](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md#current-shortcomings)
(the parked packaging entry) and in
[`docs/inflight/next-fork-packaging-docs-and-licensing.md`](../docs/inflight/next-fork-packaging-docs-and-licensing.md).

---

## How it is built

**No Apache Kafka source is committed to this repository.** Four classes are unpacked from the
published `kafka-streams` sources jar at `generate-sources`, patched with the tracked
[`src/main/patch/pc-streams.patch`](src/main/patch/pc-streams.patch), and compiled into
`target/classes`, which precedes the `kafka-streams` jar on the classpath.

The patch is **657 lines across those four classes** - `StreamTask`, `AbstractProcessorContext`,
`ProcessorContextImpl` and `RecordCollectorImpl`. A second, smaller patch does the same to one of
Kafka's own *test* fixtures, `InternalMockProcessorContext`, which reads the fields the main patch
thread-confines; without it Kafka's `RecordCollectorTest` cannot construct its subject at all, and the
188 above would not be measurable.

Those four were named deliberately rather than discovered by the compiler - `RecordCollectorImpl` is
constructed outside `StreamTask` and nothing forces it into the set, but its non-concurrent maps are
written from every worker thread through the `to()` sink.

### Kafka version pinning, and re-deriving the patch

The module is pinned to the reactor's `${kafka.version}`, currently **3.9.2**, and the patch is derived
against exactly those sources. `org.apache.kafka.streams.processor.internals` is package-private,
unsupported and explicitly not an API, so those classes may change shape in any patch release.

**On a Kafka bump the patch will need re-deriving.** The build fails *loudly* when it stops applying -
`bin/apply-patch.sh` dry-runs first and fails on any rejected hunk - rather than drifting into a runtime
`NoSuchMethodError`, which is what a vendored copy would do. That is a real improvement, and it is
still a recurring maintenance obligation with a 188-test regression run behind each one.

On Kafka trunk and 4.x the target classes have already diverged materially: `ProcessorContextImpl` is
`final` and the record context is mutated in place. A green result on 3.9 does **not** transfer
unexamined.

```bash
./mvnw -pl parallel-consumer-streams -am process-sources   # patched tree into target/kafka-patched
# edit target/kafka-patched/... - RUN NO MAVEN in between, `unpack` silently reverts your edits
parallel-consumer-streams/bin/regen-patch.sh               # re-derives pc-streams.patch
```

`regen-patch.sh` warns when the hunk count drops, which is the tripwire for a silently lost edit.

---

## Reporting what you find

Field reports are the point of publishing this. Please report on
**[astubbs#255](https://github.com/astubbs/parallel-consumer/issues/255)**, and include:

1. **Your topology's shape** - stateless or stateful, and whether caching is disabled. **A construct
   that produced wrong output without complaining is the single most useful report we can get**,
   because nothing in this module detects that today and your run is the only detector there was.
2. **The switch state** - `pc.streams.dispatch.enabled` and `poolSize`, and whether the same run is
   correct with the seam off. *A result with no seam-off control arm cannot be attributed to this
   module.*
3. **Kafka version**, and whether you re-derived the patch.
4. **What you compared against** - output equality against stock, per-key ordering, aggregate values.
   **The most valuable report of all is your own test suite run both ways**: what passed with dispatch
   on, what did not, and what the failures looked like.
5. **Reproduction rate** - "3 of 20 runs" is worth far more than "sometimes".
6. **Your `poll.ms`**, if you are reporting a performance figure. The default throttles dispatch here,
   so a slow result at 100ms may be measuring that rather than your topology.

Anything already in
[Current Shortcomings](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md#current-shortcomings)
is known. The valuable reports are the ones *outside* that list, and any evidence that something on it
is worse than stated.

---

## Further reading

- [Result: can PC's work-shard manager drive a Kafka Streams processor chain?](../docs/plans/2026-08-08-002-ks-on-pc-spike-result.md) - the full write-up, the evidence, and the reproduction rates
- [The plan](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md) - requirements, key technical decisions, and the live Current Shortcomings list
- [The origin analysis](../docs/plans/2026-08-07-002-investigate-kafka-streams-on-pc-report.md) - why the seam is where it is, and the routes that were rejected
- [`CONCEPTS.md`](../CONCEPTS.md) - the project's vocabulary, including *frontier* and *frontier semantics*
