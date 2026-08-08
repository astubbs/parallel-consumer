# `parallel-consumer-streams-spike` - Kafka Streams on Parallel Consumer

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

The Kafka side of the change is a **530-line patch across 4 `processor.internals` classes**
(`StreamTask`, `AbstractProcessorContext`, `ProcessorContextImpl`, `RecordCollectorImpl`), and it needed
**no new Parallel Consumer API**.

**No Apache Kafka source is committed to this repository.** The four classes are unpacked from the
published `kafka-streams` sources jar at `generate-sources`, patched with the tracked
`src/main/patch/pcspike.patch`, and compiled into `target/classes`, which precedes the `kafka-streams` jar
on the classpath.

## Status

**Alpha / experimental, seeking field testers.** The evidence behind it is real and is written up in full
in [`docs/plans/2026-08-08-002-ks-on-pc-spike-result.md`](../docs/plans/2026-08-08-002-ks-on-pc-spike-result.md):

- Output is identical to a provably-external stock Kafka Streams baseline, for a stateless topology and
  for a non-windowed aggregation over a state store, under 4 worker threads with 4 records demonstrably
  inside the chain at once.
- **188 of Apache Kafka's own Streams tests pass unmodified against the patched classes, zero skipped**
  (see [The 188-test claim](#the-188-test-claim)).
- The thread-confinement that makes it work is *proven* load-bearing by a controlled experiment, not
  merely undisturbed.

What that does **not** mean: that this is finished, crash-safe, or semantically equal to stock Kafka
Streams. It is not. Read [What is known not to work](#what-is-known-not-to-work) before enabling it
anywhere that matters.

## How to turn it on

**The seam is off by default.** With the switch unset, the stock Kafka Streams path is the one that runs
and this artifact changes nothing about your topology's behaviour.

For a whole JVM (manual experimentation, running an app):

```
-Dpc.streams.spike.dispatch.enabled=true
-Dpc.streams.spike.dispatch.poolSize=4      # optional, defaults to 4
```

From a test, so it can be turned off again:

```java
PcDispatchSwitch.enable(4);   // worker threads per task; also PC's maxConcurrency
try {
    // ... build and run the topology ...
} finally {
    PcDispatchSwitch.disable();
}
```

The decision is taken **once, in the `StreamTask` constructor**, so a task never changes record paths
halfway through a run. Tasks created before `enable(...)` keep the stock path.

Two things to know before you get a surprising result:

- **The switch is process-wide static state.** `StreamTask` is constructed several layers inside
  `KafkaStreams` with no seam to inject through. Tests that use it must be `@Isolated`.
- **The patched classes only win where `target/classes` precedes the `kafka-streams` jar** - which is true
  inside this module's own build and is *not* a distribution mechanism you can rely on in your own
  application. See §7.3 of the result document: there is no shipped distribution shape yet. This is the
  single biggest reason this is alpha.

## What is known not to work

All of these are recorded decisions, not defects discovered late. The full list with reasoning is §8 of
the result document.

| Not working | Detail |
|---|---|
| **Stream-time punctuation** | Stream time advances at partition-group *selection*, and the PC path never selects from the partition group, so stream-time punctuators do not fire. Wall-clock punctuation is unaffected. Disqualifying for anything windowed. |
| **Consumer pausing** | Stock `addRecords` pauses a partition once its buffer exceeds `maxBufferedSize`. The PC path hands everything to `WorkManager`, so PC's own backpressure is the only inflow limit. |
| **Prompt failure reporting** | A worker's exception is stored and re-thrown on the `StreamThread` at the *next* `process()` call. Records dispatched in between will already have run. |
| **Crash safety - offsets are committed optimistically** | Offset commit stays on the stock Streams path. `consumedOffsets` is written by workers in *completion* order, so Streams may commit an offset while a lower one from the same partition is still in flight. **Do not run this where a crash must not lose or replay records.** |
| **Retries** | Disabled. PC's response to a failure is re-dispatch, which would re-run the whole chain including `forward()` calls that already emitted downstream - duplicates stock Streams never produces. |
| **Caching on stateful stores** | Caching must be **disabled** (`Materialized...withCachingDisabled()`). This is a user-visible semantics change: with caching on a KTable emits at flush (one record per key per commit interval); with it off, every update is forwarded. Your downstream volume and output-topic retention change. |
| **Windowed operators, joins, suppression** | Out of scope. They change semantics under out-of-order processing. |
| **Exactly-once (EOS)** | Out of scope - at-least-once only. This is what keeps `StreamsProducer` out of the patch entirely. |
| **Multiple tasks / rebalancing** | Every test runs one `StreamThread`, one partition, one task. Multi-task and rebalance behaviour under PC dispatch is untested. |
| **Kafka versions other than the pinned one** | See [Kafka version pinning](#kafka-version-pinning). |
| **Two read-modify-write races survive** | `commitNeeded` and `partitionsToResume` are `volatile`/concurrent, which fixes *corruption*, not *atomicity*. |

### The precise semantic gap: 33 of Kafka's own `StreamTaskTest` cases

With the switch **off**, Kafka's `StreamTaskTest` is 101/101. With it **on**, it is **68/101**. Those 33
failures are the honest measure of how far the PC path is from stock Kafka Streams - and they are a
worklist, written by Kafka's own authors:

| Cluster | Tests | Corresponds to |
|---|---|---|
| Offset / commit accounting | 11 | optimistic commit - the largest cluster, and the one blocking crash-safety |
| Buffering, pause/resume | 5 | no consumer pausing; `maxBufferedSize` is meaningless when nothing fills the partition group |
| Close / suspend | 5 | the drain-before-suspend path this module adds |
| EOS commit gates | 3 | EOS deliberately out of scope |
| Error wrapping | 3 | failures surface a pump cycle late; exception type and timing differ |
| Stream-time punctuation | 2 | stream time advances at selection, which the PC path skips |
| Ordering | 1 | global ordering across a partition, which parallel dispatch necessarily changes |

It doubles as a positive control: the released `StreamTask` has no dispatch flag, so a run whose behaviour
changes when the flag is set is *provably* executing the patched class.

## The 188-test claim

**"188 of Apache Kafka's own Streams tests pass unmodified against the patched classes, zero skipped."**

This is a substantiated claim, available for release notes and other promotional use. Its provenance:

- **The tests:** `org.apache.kafka.streams.processor.internals.StreamTaskTest` (101),
  `RecordCollectorTest` (59), `ProcessorContextImplTest` (28) - Apache Kafka's own, taken as compiled
  classes from the `kafka-streams` `test` jar published to Maven Central. Not re-written, not re-compiled,
  not excluded, no assertion relaxed.
- **What they run against:** our patched `StreamTask`, `AbstractProcessorContext`, `ProcessorContextImpl`
  and `RecordCollectorImpl`, which precede the `kafka-streams` jar on the classpath - proven separately by
  `ShadowedClassLoadingTest`, and cross-checked by the fact that turning the dispatch flag on changes the
  result (the released classes have no such flag).
- **The condition:** dispatch switch **off**. This is a *behaviour-preservation* claim about the patch,
  not a claim about the parallel path. The parallel path's number is 68/101 on `StreamTaskTest`, above,
  and is stated everywhere the 188 is.
- **How to reproduce:** it runs in this module's **normal** test run, no profile and no flag:

  ```
  ./mvnw -pl parallel-consumer-streams-spike -am test
  ```

  Kafka's execution reports separately, under `target/surefire-reports-kafka-upstream/`.

The count lives in exactly three places - the surefire execution's comment in `pom.xml`, this section, and
§9 of the result document. If it changes, change all three.

## Kafka version pinning

The module is pinned to the reactor's `${kafka.version}` (currently **3.9.2**), and the patch is derived
against exactly those sources. `org.apache.kafka.streams.processor.internals` is package-private,
unsupported and explicitly not an API - the four classes are free to change shape in any patch release.

**On a Kafka bump the patch will need re-deriving.** The build fails *loudly* when it no longer applies -
`bin/apply-patch.sh` dry-runs first and fails on any rejected hunk - rather than drifting silently into a
runtime `NoSuchMethodError`, which is what a vendored copy would do. That is a real improvement and it is
still a recurring maintenance obligation, with a 188-test regression run behind each one.

On Kafka trunk/4.x the four classes have already diverged materially: `ProcessorContextImpl` is `final`
and the record context is mutated in place. A green result on 3.9 does **not** transfer unexamined.

To re-derive:

```bash
./mvnw -pl parallel-consumer-streams-spike -am process-sources   # patched tree into target/kafka-patched
# edit target/kafka-patched/... - RUN NO MAVEN in between, `unpack` silently reverts your edits
parallel-consumer-streams-spike/bin/regen-patch.sh                # re-derives pcspike.patch
```

`regen-patch.sh` warns when the hunk count drops, which is the tripwire for a silently lost edit.

## How to report what you find

Field reports are the point of publishing this. Please report on
**[astubbs#255](https://github.com/astubbs/parallel-consumer/issues/255)**, and include:

1. **Your topology's shape** - stateless, stateful, windowed, joins; whether caching is disabled.
2. **The switch state** - `pc.streams.spike.dispatch.enabled` and `poolSize`, and whether the same run is
   correct with the switch off. *A result with no switch-off control arm cannot be attributed to this
   module.*
3. **Kafka version**, and whether you re-derived the patch.
4. **What you compared against** - output equality against stock, per-key ordering, aggregate values.
5. **Reproduction rate** - "3 of 20 runs" is worth far more than "sometimes".

Anything in [What is known not to work](#what-is-known-not-to-work) is already known; the valuable reports
are the ones *outside* that table, and any evidence that one of those items is worse than stated.

## Further reading

- [Result: can PC's work-shard manager drive a Kafka Streams processor chain?](../docs/plans/2026-08-08-002-ks-on-pc-spike-result.md) - the full write-up, evidence and reproduction rates
- [The plan](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md) - requirements and the key technical decisions
- [The origin analysis](../docs/plans/2026-08-07-002-investigate-kafka-streams-on-pc-report.md) - why the seam is where it is
