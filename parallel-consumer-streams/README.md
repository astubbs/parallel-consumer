# `parallel-consumer-streams` - Kafka Streams on Parallel Consumer

> **ALPHA. EXPERIMENTAL. NOT PRODUCTION READY.**
> It is published so people can try it, not because it is finished. **Field testers wanted** - see
> [How to report what you find](#how-to-report-what-you-find).

## What it is

Kafka Streams processes one partition serially: `PartitionGroup.nextRecord()` hands `StreamTask.process()`
one record at a time, on the `StreamThread`. This module replaces that selection step with Parallel
Consumer's, so records are selected by PC's `WorkManager` and executed on a worker pool - through the
**unmodified** `ProcessorNode` chain:

```
consumer poll
  -> StreamTask.addRecords          --[switched]-->  PcTaskDispatcher.registerRecords
                                                       -> WorkManager.registerWork(EpochAndRecordsMap)
  -> StreamTask.process(wallClock)  --[switched]-->  PcTaskDispatcher.dispatchAvailable
                                                       -> WorkManager.getWorkIfAvailable(n)
                                                       -> worker pool x N
                                                            -> StreamTask.doProcess(record, recordInfo)
                                                            -> the unmodified ProcessorNode chain
                                                       -> WorkManager.handleFutureResult
```

Under PC's KEY ordering, at most one record per key is in flight, so records on distinct keys of the same
partition run concurrently while per-key order is preserved.

The Kafka side of the change is a **716-line patch across 5 `processor.internals` classes**
(`StreamTask`, `AbstractProcessorContext`, `ProcessorContextImpl`, `RecordCollectorImpl`, `StreamThread`),
and it needed **no new Parallel Consumer API**.

**No Apache Kafka source is committed to this repository.** The five classes are unpacked from the
published `kafka-streams` sources jar at `generate-sources`, patched with the tracked
`src/main/patch/pc-streams.patch`, and compiled into `target/classes`, which precedes the `kafka-streams` jar
on the classpath.

### Wake on work

`StreamThread` polls the consumer and runs the topology on **one thread**. Under stock Kafka Streams,
blocking in `Consumer#poll()` for up to `poll.ms` (default **100ms**) is free - while the thread is parked
there is by definition no processing it could be doing instead, because it is the same thread.

Hand records to a worker pool and that inverts. Workers complete *during* the poll wait, and neither their
completions nor the records they unblock can move until poll returns, so throughput starts tracking poll
cadence rather than the work. It is charged on every workload and concurrency merely hides it: with every
record forced onto one key, so no concurrency was available to pay the bill, the seam measured **0.69x
against stock** - the asynchronous path *slower* than the synchronous one, which absence of concurrency can
never explain.

So the patched `pollPhase()` splits the wait: poll briefly to collect whatever the broker already has, then
block on **Parallel Consumer's own condition** for the rest of the budget, woken the instant a worker
completes. Only while PC actually has work outstanding - idle, the stock full-budget poll is exactly right,
and shortening it would delay broker records for nothing.

Measured at the **default** `poll.ms`, three runs each (`HeadOfLineBlockingBenchmarkTest`):

| | Single key - no concurrency available (p50) | Head-of-line blocking (p50) | (p99) |
|---|---|---|---|
| without wake-on-work | 0.70x | 5.5x | 3.0x |
| **with wake-on-work** | **0.99x** | **17.1x** | **9.2x** |

The first column is the point: **no penalty when PC cannot parallelise**. The other two are what the poll
wait was costing every other workload while concurrency masked it.

Deliberately **not** `KafkaConsumer#wakeup()`, which is the obvious mechanism and the wrong one - it throws
`WakeupException` and is Kafka Streams' own word for *shutdown*, and a wake delivered while the thread is
not polling arms the *next* poll instead, so a stray completion could swallow a shutdown. Owning the
condition costs one small class and removes that failure mode entirely.

**To turn it off**, without turning the seam off: `-Dpc.streams.wakeOnWork.enabled=false`. That restores one
full-budget poll, and it is how the before/after above was measured as a one-term control.

## Status

**Alpha / experimental, seeking field testers.** The evidence behind it is real and is written up in full
in [`docs/plans/2026-08-08-002-ks-on-pc-spike-result.md`](../docs/plans/2026-08-08-002-ks-on-pc-spike-result.md):

- Output is identical to a provably-external stock Kafka Streams baseline, for a stateless topology and
  for a non-windowed aggregation over a state store, under 4 worker threads with 4 records demonstrably
  inside the chain at once.
- **419 of Apache Kafka's own Streams tests pass unmodified against the patched classes, zero failures**
  (see [The 419-test claim](#the-419-test-claim)).
- **No penalty when Parallel Consumer cannot parallelise**: with every record on one key, so the seam can
  offer nothing, it ties with stock at the default configuration (see [Wake on work](#wake-on-work)).
- The thread-confinement that makes it work is *proven* load-bearing by a controlled experiment, not
  merely undisturbed.

What that does **not** mean: that this is finished, crash-safe, or semantically equal to stock Kafka
Streams. It is not. Read [Known gaps](#known-gaps) before relying on it anywhere that matters.

## Using it

**The seam is on by default. Depending on this artifact is the opt-in.** Nobody puts an alpha module
called `parallel-consumer-streams` on their classpath by accident, and having done so, they wanted
the PC seam - so there is no second switch to find.

**To turn it off** and get stock, serial Kafka Streams dispatch back - which is exactly what an A/B
comparison needs:

```
-Dpc.streams.dispatch.enabled=false
```

Other settings:

```
-Dpc.streams.dispatch.poolSize=4      # worker threads per task; also PC's maxConcurrency. Default 4.
-Dpc.streams.wakeOnWork.enabled=false # back to one full-budget poll - see Wake on work. Default on.
```

From a test, so each arm states which path it wants rather than inheriting a default:

```java
PcDispatchSwitch.enable(4);            // worker threads per task
// PcDispatchSwitch.disable();         // ... or the stock path, said out loud
try {
    // ... build and run the topology ...
} finally {
    PcDispatchSwitch.resetToDefault(); // hand the JVM back as you found it
}
```

The decision is taken **once, in the `StreamTask` constructor**, so a task never changes record paths
halfway through a run. Tasks created while the switch was off keep the stock path.

Two things to know before you get a surprising result:

- **The switch is process-wide static state.** `StreamTask` is constructed several layers inside
  `KafkaStreams` with no seam to inject through. Tests that use it must be `@Isolated`.
- **The patched classes only win where `target/classes` precedes the `kafka-streams` jar** - which is true
  inside this module's own build and is *not* a distribution mechanism you can rely on in your own
  application. See §7.3 of the result document: there is no shipped distribution shape yet. This is the
  single biggest reason this is alpha.

## Known gaps

**This is an alpha, and it has real, known shortcomings.** They are not enumerated here on purpose:
implementation has not stopped, so any list in this README would be out of date by the time you read it.
The living list, with the mechanism behind each item and an assessment of what it would take to close it,
is **[Current Shortcomings in the plan](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md#current-shortcomings)**.

**Read that section before relying on this anywhere that matters.** In summary, and without the detail:
stream-time-driven behaviour (punctuation, windows, joins, suppression) does not work; caching must be
disabled on state stores, which changes what your topology emits; retries are off and failures surface a
pump cycle late; and EOS is out of scope. The size of the gap is measured, not estimated - **33 of Apache
Kafka's own `StreamTaskTest` cases fail with the seam on** (68/101, against 101/101 with it off), and the
shortcomings list maps onto those failures.

**Crash safety is not on that list.** Offsets are committed from Parallel Consumer's own completion
tracking, so a commit covers only work that is genuinely finished and a crash cannot skip a record that
was still in flight. Note that 14 of the 33 failures above are Kafka's offset and commit tests, which
still fail - not because anything can be lost, but because they assert Kafka's *encoding* of the commit
metadata, and this module deliberately owns that field. A failing test there is the divergence being
detected, not data being lost.

## The 419-test claim

**"419 of Apache Kafka's own Streams tests pass unmodified against the patched classes, zero failures."**

This is a substantiated claim, available for release notes and other promotional use. Its provenance:

- **The tests:** `org.apache.kafka.streams.processor.internals.StreamTaskTest` (101),
  `RecordCollectorTest` (59), `ProcessorContextImplTest` (28), `StreamThreadTest` (231) - Apache Kafka's
  own, taken as compiled classes from the `kafka-streams` `test` jar published to Maven Central. Not
  re-written, not re-compiled, not excluded, no assertion relaxed.
- **What they run against:** our patched `StreamTask`, `AbstractProcessorContext`, `ProcessorContextImpl`,
  `RecordCollectorImpl` and `StreamThread`, which precede the `kafka-streams` jar on the classpath - proven
  separately by `ShadowedClassLoadingTest`, and cross-checked by the fact that turning the dispatch flag on
  changes the result (the released classes have no such flag).
- **The skips:** 21 of `StreamThreadTest`'s cases are skipped, by Kafka's own annotations and not by
  anything here. That is stated rather than hidden, because the honest form of the claim has to survive
  someone reading the surefire output. A control run against the **unpatched** `StreamThread` skips exactly
  the same 21 and passes exactly the same 231 - so the skips predate this module, and the patched arm is no
  weaker than stock.
- **The condition:** dispatch switch **off** - set explicitly on that surefire execution, not inherited
  from a default. This is a *behaviour-preservation* claim about the patch, not a claim about the parallel
  path. The parallel path's number is 68/101 on `StreamTaskTest`, and is stated everywhere the 419 is.
- **How to reproduce:** it runs in this module's **normal** test run, no profile and no flag:

  ```
  ./mvnw -pl parallel-consumer-streams -am test
  ```

  Kafka's execution reports separately, under `target/surefire-reports-kafka-upstream/`.

The count lives in exactly three places - the surefire execution's comment in `pom.xml`, this section, and
§9 of the result document. If it changes, change all three.

## Kafka version pinning

The module is pinned to the reactor's `${kafka.version}` (currently **3.9.2**), and the patch is derived
against exactly those sources. `org.apache.kafka.streams.processor.internals` is package-private,
unsupported and explicitly not an API - the five classes are free to change shape in any patch release.

**On a Kafka bump the patch will need re-deriving.** The build fails *loudly* when it no longer applies -
`bin/apply-patch.sh` dry-runs first and fails on any rejected hunk - rather than drifting silently into a
runtime `NoSuchMethodError`, which is what a vendored copy would do. That is a real improvement and it is
still a recurring maintenance obligation, with a 419-test regression run behind each one.

On Kafka trunk/4.x the patched classes have already diverged materially: `ProcessorContextImpl` is `final`
and the record context is mutated in place. A green result on 3.9 does **not** transfer unexamined.

To re-derive:

```bash
./mvnw -pl parallel-consumer-streams -am process-sources   # patched tree into target/kafka-patched
# edit target/kafka-patched/... - RUN NO MAVEN in between, `unpack` silently reverts your edits
parallel-consumer-streams/bin/regen-patch.sh                # re-derives pc-streams.patch
```

`regen-patch.sh` warns when the hunk count drops, which is the tripwire for a silently lost edit.

## How to report what you find

Field reports are the point of publishing this. Please report on
**[astubbs#255](https://github.com/astubbs/parallel-consumer/issues/255)**, and include:

1. **Your topology's shape** - stateless, stateful, windowed, joins; whether caching is disabled.
2. **The switch state** - `pc.streams.dispatch.enabled` and `poolSize`, and whether the same run is
   correct with the switch off. *A result with no switch-off control arm cannot be attributed to this
   module.*
3. **Kafka version**, and whether you re-derived the patch.
4. **What you compared against** - output equality against stock, per-key ordering, aggregate values.
5. **Reproduction rate** - "3 of 20 runs" is worth far more than "sometimes".

Anything in [Current Shortcomings](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md#current-shortcomings)
is already known; the valuable reports are the ones *outside* that list, and any evidence that one of
those items is worse than stated.

## Further reading

- [Result: can PC's work-shard manager drive a Kafka Streams processor chain?](../docs/plans/2026-08-08-002-ks-on-pc-spike-result.md) - the full write-up, evidence and reproduction rates
- [The plan](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md) - requirements and the key technical decisions
- [The origin analysis](../docs/plans/2026-08-07-002-investigate-kafka-streams-on-pc-report.md) - why the seam is where it is
