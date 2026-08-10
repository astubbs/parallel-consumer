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
Streams. It is not. Read [Known gaps](#known-gaps) before relying on it anywhere that matters.

## Using it

**The seam is on by default. Depending on this artifact is the opt-in.** Nobody puts an alpha module
called `parallel-consumer-streams-spike` on their classpath by accident, and having done so, they wanted
the PC seam - so there is no second switch to find.

**To turn it off** and get stock, serial Kafka Streams dispatch back - which is exactly what an A/B
comparison needs:

```
-Dpc.streams.spike.dispatch.enabled=false
```

Other settings:

```
-Dpc.streams.spike.dispatch.poolSize=4      # worker threads per task; also PC's maxConcurrency. Default 4.
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
stream-time-driven behaviour (punctuation, windows, joins, suppression) does not work; offsets are
committed optimistically, so a crash can lose records; caching must be disabled on state stores, which
changes what your topology emits; retries are off and failures surface a pump cycle late; and EOS is out
of scope. The size of the gap is measured, not estimated - **33 of Apache Kafka's own `StreamTaskTest`
cases fail with the seam on** (68/101, against 101/101 with it off), and the shortcomings list maps onto
those failures.

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
- **The condition:** dispatch switch **off** - set explicitly on that surefire execution, not inherited
  from a default. This is a *behaviour-preservation* claim about the patch, not a claim about the parallel
  path. The parallel path's number is 68/101 on `StreamTaskTest`, and is stated everywhere the 188 is.
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

Anything in [Current Shortcomings](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md#current-shortcomings)
is already known; the valuable reports are the ones *outside* that list, and any evidence that one of
those items is worse than stated.

## Further reading

- [Result: can PC's work-shard manager drive a Kafka Streams processor chain?](../docs/plans/2026-08-08-002-ks-on-pc-spike-result.md) - the full write-up, evidence and reproduction rates
- [The plan](../docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md) - requirements and the key technical decisions
- [The origin analysis](../docs/plans/2026-08-07-002-investigate-kafka-streams-on-pc-report.md) - why the seam is where it is
