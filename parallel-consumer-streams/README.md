# `parallel-consumer-streams` - Kafka Streams, driven by Parallel Consumer

> ## ALPHA. EXPERIMENTAL. NOT PRODUCTION READY.
> This is published so people can try it, not because it is finished. Large parts of Kafka Streams do
> not work here, and this README says which. **Field testers wanted** -
> [astubbs#255](https://github.com/astubbs/parallel-consumer/issues/255).

Stock Kafka Streams processes one partition strictly one record at a time. If a record takes a second
of blocking IO, every record behind it in that partition waits a second, whatever key it has and
however cheap it is. Your only lever is more partitions.

This module removes that constraint from *inside* the topology. Records on **different keys in the
same partition** run concurrently, in your unmodified processor chain, with per-key order preserved.
In this module's own benchmark, the quickest record behind a 1.5 second blocker went from **1545ms to
27ms**.

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
| `StreamThreadTest` | 231 (21 skipped) | 0 |
| `RecordCollectorTest` | 59 | 0 |
| `ProcessorContextImplTest` | 28 | 0 |
| **Total** | **419** | **0** |

Not rewritten, not recompiled, not excluded, no assertion relaxed - Kafka's own compiled test classes,
taken from the `kafka-streams` test jar on Maven Central. The 21 skips are Kafka's own annotations, not
ours; a control run against the *unpatched* `StreamThread` skips the same 21 (recorded under
astubbs#255). This runs on every build of this module, no profile and no flag.

**And here is what that number does not mean.** With the seam **on**, the same `StreamTaskTest` is
**65 of 101**: 30 assertion failures, plus 6 cases that now stop at an explicit refusal because they
configure exactly-once. Those 36 are the semantic gap between this module and stock Kafka Streams,
measured rather than estimated, and they are the worklist that
[Current Shortcomings](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md#current-shortcomings)
is drawn from.

So: 419 is a claim that **the patch does not break Kafka when it is not being used**. It is not a claim
of equivalence. Quote it without the 65 and it says something untrue.

### Head-of-line blocking, with its control

One partition. One record costing 1500ms at the head. Twenty-four records costing 25ms each behind it,
on other keys. Same JVM, same patched classes, same broker; the *only* thing that changes between the
two arms is whether the seam is on.

| Fast-record latency, n=24 | Stock dispatch | PC dispatch | |
|---|---|---|---|
| **quickest** | 1545ms | **27ms** | **57x** |
| median | 1887ms | 136ms | 14x |
| slowest | 2270ms | 265ms | 8.6x |
| *control: identical run, all records on **one** key (median)* | *1900ms* | *1897ms* | ***1.00x*** |

**The first row is the claim.** "A fast record does not have to wait for a slow one" is falsified if
even the luckiest fast record waited, and demonstrated if a single one did not - so the *quickest* is
the statistic that states it, and under stock dispatch the quickest still paid the full 1.5 seconds,
because the partition is handed over one record at a time. The median is a summary of the claim; the
minimum is the claim.

**The last row is what makes the first one believable.** Put every record on a single key and the gain
vanishes completely. It has to: per-key ordering permits one in-flight record per key, so with one key
there is nothing to run concurrently. Had the seam still won there, the 57x would be coming from a
faster harness or a warm-up artefact rather than from key concurrency, and both results would have to
be withdrawn rather than published.

That control is a live regression test rather than a formality. It measured **0.69x** - the parallel
path *slower* than the serial one - until the poll-wait behaviour was fixed under astubbs#255. No
absence of concurrency can produce a number below 1.00x, which is exactly why the control is worth
running: it catches the class of defect that headline throughput hides.

**The third row is the weakest.** At n=24 the slowest sample is the last record queued through a pool
of four, so it measures pool depth rather than blocking, and we do not lean on it. It is in the table
because dropping the unflattering row of your own benchmark is how benchmarks become marketing.

Two caveats attach to every figure here, and to any figure quoted from it: the comparison is **within
one partition** (stock Kafka Streams parallelises across partitions, and that is not what is being
measured), and the workload is **blocking IO**, which is the case Parallel Consumer exists for. CPU-bound
work will not behave like this.

The table is one run of three taken on the same machine; across all three the quickest-record ratio was
57.0x to 59.3x, the median 13.9x to 14.1x, and the single-key control stayed between 1.00x and 1.01x.
Absolute latencies move with machine load, so the test asserts a floor of 3x rather than the measured
figure, and a ceiling of 1.5x on the control. Reproduce with
`HeadOfLineBlockingBenchmarkTest`
([source](src/test/java/io/confluent/parallelconsumer/streams/integrationTests/HeadOfLineBlockingBenchmarkTest.java)),
or better, [run it against your own topology](#do-not-take-any-of-this-on-trust-run-your-own-topology-both-ways).

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
code", which for an alpha is a far better offer than any number we could publish.

**What makes the A/B meaningful is what happens when it is off.** With dispatch disabled the topology
behaves exactly as stock Kafka Streams. The choice is made once, when each task is constructed, and a
task that chose the stock path never takes the parallel one: no worker pool, no work manager, nothing
to bypass at runtime. That is why Kafka's own 419 still pass, and it is why the refusals above are
refusals only *on the dispatch path* - a construct refused with dispatch on builds and runs normally
with it off. So a suite that goes red with dispatch on and green with it off has isolated the seam
rather than something else in your build.

**Two honest limits on the correctness use**, because it is easy to over-read:

- A green suite with dispatch on is strong evidence about **your topology**. It is not a general
  equivalence proof, and it does not retire the 36 `StreamTaskTest` failures above.
- Every known gap still applies. Stream-time behaviour, caching on state stores, retries and EOS do not
  become safe because your tests pass - most of them are refused rather than silently wrong, but a
  suite that never exercises them proves nothing about them. Read
  [Current Shortcomings](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md#current-shortcomings)
  first, and treat a green run as evidence for what you tested.

Today the switch is a JVM-wide system property, described under [Trying it](#trying-it) - fine for a
benchmark harness or a test suite, awkward if you want two differently-configured instances in one
process. Per-instance configuration through `StreamsConfig`, alongside your other Streams settings, is
landing shortly; this section will name the property when it does.

### Output equality

For a stateless topology and for a non-windowed aggregation over a state store, output is identical to
a stock Kafka Streams baseline generated in a **different Maven module** that has no dependency on this
one - with a test asserting the patched classes are absent from that JVM, so "stock" means stock. Four
worker threads, with four records demonstrably inside the chain at once. Full write-up in the
[result document](../docs/plans/2026-08-08-002-ks-on-pc-spike-result.md).

---

## What does not work

### First, the good news: it refuses instead of lying

Everything known to be broken here is **physically refused**. Build a topology that uses one, with the
seam on, and you get an `UnsupportedOperationException` that names the construct, says why, and tells
you the flag that turns the seam off. You do not get a plausible wrong answer.

```
PC dispatch (astubbs#255): windowed aggregation (windowedBy) is not supported on the Parallel
Consumer dispatch path, because windowCloseTime is derived from observedStreamTime, which never
advances on the PC path ...

This is refused rather than allowed to produce silently wrong results. Run with
-Dpc.streams.dispatch.enabled=false for stock Kafka Streams dispatch, which supports it.
```

This matters more than it might sound. These constructs do not throw in stock Kafka Streams and they
would not have thrown here either: they read a stream-time counter that never advances on this path,
and several of them mutate a non-volatile `long` from every worker. Left reachable, they run to
completion and emit the wrong numbers. Refusal is the only honest behaviour available until the
semantics are fixed.

| You get | When |
|---|---|
| A **compile error** (`@DoNotCall`), or a deprecation warning without ErrorProne | you write `join`, `leftJoin`, `outerJoin`, `windowedBy` or `suppress` against `KStream`, `KTable`, `KGroupedStream` or `CogroupedKStream` |
| An `UnsupportedOperationException` naming the construct | you build that topology with the seam on |
| An `UnsupportedOperationException` at task construction | your topology reaches a `WindowStore`, `SessionStore`, versioned key-value store or suppression buffer through the Processor API, or sets `processing.guarantee` to exactly-once |

The two runtime rows are conditional on the seam: with `-Dpc.streams.dispatch.enabled=false`, all of
this builds and runs exactly as stock Kafka Streams does. That is both the escape hatch and the reason
Kafka's own 419 still pass. The compile-time row is not conditional and cannot be - an annotation in a
class file cannot read a system property - so if you have deliberately turned the seam off and want the
call anyway, suppress the ErrorProne check at that call site.

Each refused method also carries a javadoc `@deprecated` tag naming this module as the thing refusing.
Without it an IDE strikes `stream.join(...)` through with no reason attached, and the obvious inference
- that Apache Kafka deprecated `join` - is false and alarming.

Nothing was deleted. The signatures are all still there, because Kafka's own test suite calls them
heavily and deleting them would forfeit the evidence above.

**Reinstatement is evidence-gated.** A construct comes off the refused list when Kafka's own suite
exercises it with the seam **on** and passes, not when someone reads the code and concludes it looks
fine.

### The shape of the gap

Deliberately *not* enumerated here. Implementation has not stopped, so a list in this README goes stale
the week it is written. The living list, with the mechanism behind each item and an assessment of what
closing it would cost, is
**[Current Shortcomings](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md#current-shortcomings)**.

Read it before relying on this anywhere that matters. In outline:

- **Stream time does not advance.** Everything downstream of that is affected: `STREAM_TIME`
  punctuation, windows, joins, suppression. These are the refused list above.
- **Caching must be disabled on state stores**, which changes what your topology emits.
- **Retries are off**, and a failure surfaces one pump cycle later than it would in stock.
- **Exactly-once is out of scope.** This module is at-least-once; EOS is refused rather than
  approximated.
- **Rebalance is unexercised.** Not known broken; not tested either, which for an alpha is the same
  thing in practice.

---

## Does this change anything if you only use plain Parallel Consumer?

**No, and that is checkable rather than a reassurance.**

The sentence "Parallel Consumer now patches Kafka Streams internals" is alarming if you read it as a
change to the library you already use. It is not one:

| Claim | How to check it |
|---|---|
| This module is a **leaf**. It depends on `parallel-consumer-core`; nothing depends on it | grep the reactor for `parallel-consumer-streams` - the only hits are its own pom and the `<module>` line in the root pom |
| Adding it **changed no shipped code in any existing module** | diff this branch against `master`: outside this module and the docs, nothing under any `src/main` changed at all. The only edits are the root pom's `<module>` line, a `NOTICE` addition attributing the modified Apache classes, and new test-only files in the examples module |
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
| `pc.streams.dispatch.enabled` | `true` | `false` gives stock, serial Kafka Streams dispatch back. This is the A/B control described [above](#do-not-take-any-of-this-on-trust-run-your-own-topology-both-ways), and what every refusal message points at |
| `pc.streams.dispatch.poolSize` | `4` | Worker threads per task; also Parallel Consumer's max concurrency |
| `pc.streams.wakeOnWork.enabled` | `true` | `false` restores one full-budget consumer poll. See [How it is built](#how-it-is-built) |

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

This module ships about a dozen compiled classes in Apache Kafka's *own* packages, and depends on
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

**No Apache Kafka source is committed to this repository.** Thirteen classes are unpacked from the
published `kafka-streams` sources jar at `generate-sources`, patched with the tracked
[`src/main/patch/pc-streams.patch`](src/main/patch/pc-streams.patch), and compiled into
`target/classes`.

The patch is 1774 lines across those thirteen classes. Five carry the dispatch seam itself
(`StreamTask`, `AbstractProcessorContext`, `ProcessorContextImpl`, `RecordCollectorImpl`,
`StreamThread`); the other eight are the `kstream` interfaces and implementations that carry the
refusals.

### Wake on work

Worth knowing because it explains the control row above. Kafka Streams polls the consumer and runs the
topology on one thread, so blocking in `poll()` for up to `poll.ms` costs nothing - while that thread
is parked there is by definition no processing it could be doing instead.

Hand records to a worker pool and that inverts. Workers finish *during* the poll wait, and neither
their completions nor the records they unblock can move until poll returns, so throughput starts
tracking poll cadence instead of the work. That is charged on every workload, and concurrency merely
hides it - which is why the single-key control measured 0.69x, the parallel path slower than the serial
one, a result no absence of concurrency can explain.

So the patched poll phase splits the wait: poll briefly for whatever the broker already has, then block
on Parallel Consumer's own condition for the rest of the budget, woken the instant a worker completes.
It does this only while work is actually outstanding. When idle, the stock full-budget poll is exactly
right, and shortening it would delay broker records for nothing.

The trade it makes is real and not a free win: while workers are busy and no completion arrives, a
record arriving from the broker mid-wait can wait out the remainder of `poll.ms`. Under load it barely
arises, because every completion ends the wait. Turn it off with
`-Dpc.streams.wakeOnWork.enabled=false`, which is how the before-and-after was measured as a one-term
control.

Deliberately **not** `KafkaConsumer#wakeup()`, which is the obvious mechanism and the wrong one: it
throws `WakeupException`, it is Kafka Streams' own word for *shutdown*, and a wake delivered while the
thread is not polling arms the *next* poll instead - so a stray completion could swallow a shutdown.

### Kafka version pinning, and re-deriving the patch

The module is pinned to the reactor's `${kafka.version}`, currently **3.9.2**, and the patch is derived
against exactly those sources. `org.apache.kafka.streams.processor.internals` is package-private,
unsupported and explicitly not an API, so those classes may change shape in any patch release; the
`kstream` types are public API, but the patch tracks their bodies line by line and is just as exposed.

**On a Kafka bump the patch will need re-deriving.** The build fails *loudly* when it stops applying -
`bin/apply-patch.sh` dry-runs first and fails on any rejected hunk - rather than drifting into a runtime
`NoSuchMethodError`, which is what a vendored copy would do. That is a real improvement, and it is
still a recurring maintenance obligation with a 419-test regression run behind each one.

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

1. **Your topology's shape** - stateless or stateful, and whether caching is disabled. If it refused,
   say which construct it named. **A refusal you believe is wrong is the single most useful report we
   can get**, because the refused list is what the supported envelope is made of.
2. **The switch state** - `pc.streams.dispatch.enabled` and `poolSize`, and whether the same run is
   correct with the seam off. *A result with no seam-off control arm cannot be attributed to this
   module.*
3. **Kafka version**, and whether you re-derived the patch.
4. **What you compared against** - output equality against stock, per-key ordering, aggregate values.
   **The most valuable report of all is your own test suite run both ways**: what passed with dispatch
   on, what did not, and what the failures looked like.
5. **Reproduction rate** - "3 of 20 runs" is worth far more than "sometimes".

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
