# Throughput dropped ~30% since 0.3.0.2, and the first cliff is a correctness fix

<!-- inflight-type: task -->
<!-- inflight-impact: release-gate -->

**Release gate (owner, 2026-08-20):** the question to answer before v6 is whether this throughput can
be recovered legitimately - without going back to ignoring the concurrency the user asked for.

Found 2026-08-20 while rescuing the 2021 Vert.x demo behind the README's asciinema cast (see
`branch-classic-comparison-demo.md`). The rescued demo ran slower than the recording, which looked
like an environment difference until it was measured against a control.

**It is not the environment, and it is not the Kafka client.** The harness is
[`bench/run-bisect.sh`](../../bench/run-bisect.sh) and it is built to be re-run rather than trusted.

## The measurement

Same laptop, same JVM, same harness source - one file, compiled twice, with only the package prefix
substituted, because the fork renamed its packages. Same workload: 350,000 records, a 2ms simulated
per-record server delay, `maxConcurrency` 100, `UNORDERED`, the Vert.x engine. Published `0.3.0.2`
came from Central; nothing of the 2021 code was recompiled or patched.

| arm | mean | range | spread |
|---|---|---|---|
| PC 0.3.0.2 | **28,070 msg/s** | 26,519 - 28,959 | 8.7% |
| PC 0.6.0.0-SNAPSHOT | **19,281 msg/s** | 18,184 - 19,869 | 8.7% |
| plain KafkaConsumer, 0.3.0.2 classpath | 279.7 msg/s | - | 13.0% |
| plain KafkaConsumer, current classpath | 267.9 msg/s | - | 1.5% |

n=3 each. **PC is 31.3% slower. The two ranges do not overlap** - the slowest 0.3.0.2 run beat the
fastest current run by a third.

**The vanilla arm is the control that matters**, because it touches only `KafkaConsumer` and
`HttpURLConnection` and no Parallel Consumer code at all. It moved 4.2%, inside its own run-to-run
spread. So the transitive `kafka-clients` change across the range (2.5.1 on the old classpath, 3.9.2
on the current one) does not account for the PC delta. Normalising PC by its own vanilla arm leaves
28.3% unexplained.

For scale: 0.3.0.2 on this laptop reproduces the 2021 cast (26,519 against the recording's 27,201),
so the machine is not the variable either.

## What is not yet known

- **Where in the range it happened.** A sweep across every published version between 0.3.0.2 and
  0.5.3.2, at two `kafka-clients` pins, is what `bench/run-bisect.sh` exists to answer. Until it has
  run, "roughly 30% since 0.3.0.2" is the whole of the finding - there is no attributed commit,
  release or subsystem.
- **Whether it is Vert.x-specific.** Everything above measures `VertxParallelEoSStreamProcessor`.
  The core engine has not been measured this way, so the regression could be in the shared control
  loop or in the Vert.x seam, and those have very different consequences.
- **Whether it is a regression or a trade.** Five years of correctness work sits between these two
  versions - offset-encoding density, rebalance handling, commit-mode safety, the `astubbs#857`
  family. Some of that has a legitimate cost, and this note deliberately does not claim otherwise.
  It claims a number, and that the number was not known.

## Why it matters now

The clients are being prepared for an experimental v6 release, and the per-language demo
(`branch-classic-comparison-demo.md`) puts a throughput comparison in front of users as the
project's headline argument. Publishing that argument at 70% of the number the README's own cast
shows, without knowing why, is the thing to avoid.

## The harness, and why it looks like that

[`bench/run-bisect.sh`](../../bench/run-bisect.sh) with
[`bench/Bench.java.template`](../../bench/Bench.java.template). Two design points paid for in the
first attempt, recorded so the next person does not repeat them:

- **The broker and the dataset are prepared once and reused.** The first version started a
  Testcontainers broker and produced the records inside every measured run, which put container
  startup and hundreds of thousands of produces into each data point and made a sweep unaffordable.
  Worse, it meant no two arms read the same bytes. Now each run is a fresh consumer group re-reading
  one topic on one long-lived broker, so the only thing that varies between data points is the
  classpath.
- **`kafka-clients` is a separate dimension, not an assumption.** The first result had the client
  version confounded with the PC version. The sweep pins it so the two can be separated rather than
  normalised around.

Two traps it already encodes: Jabel ships as a transitive of older PC releases and javac auto-loads
it as a compiler plugin, where its 2021 ASM cannot read modern class files - so it is stripped from
the compile classpath. And a measurement window that is too short reads as "no difference": at
20,000 records both arms returned ~4,200 msg/s because consumer-group join dominated. 350,000 is
where the arms separate.

## Rebuilding the 2021 tree is a dead end - do not retry it

Attempted first, and abandoned for reasons that will not change: the 2021 build requires JDK 13,
which has **no Apple Silicon build**; an x86 JDK under Rosetta would skew a throughput measurement
worse than the toolchain difference it fixes; and Jabel's 2021 ASM cannot read JDK 17 class files,
so it fails with `Unsupported class file major version 61` regardless of how many module exports are
opened. Consuming the **published artifact** from Central sidesteps all of it and is more faithful,
since it is the jar users actually got.

## The curve, measured under a harness that pins logging

Everything below is `bench/run-bisect.sh` at 350,000 records, 2ms simulated delay, `UNORDERED`, over
Vert.x, two runs per point, logging pinned to WARN for every arm. **`peak` is observed at the stub** -
what the engine actually had outstanding, not what it was configured to allow.

| version | run 1 | run 2 | peak in flight |
|---|---|---|---|
| 0.3.0.2 | 22,178 | 22,025 | 100 |
| 0.3.2.0 | 21,987 | 20,936 | 100 |
| **0.4.0.0** | **11,878** | **11,662** | 98 / 99 |
| 0.5.0.0 | 9,999 | 9,644 | 100 |
| 0.5.2.0 | 9,610 | 8,581 | 100 |
| 0.5.2.4 | 15,267 | 14,978 | 100 |
| 0.5.2.8 | 16,657 | 14,947 | 100 |
| 0.5.3.2 | 16,094 | 16,224 | 100 |
| 0.6.0.0-SNAPSHOT | 16,139 | 16,276 | 100 |

**Net: about 22,100 then, about 16,200 now - a 27% regression at `maxConcurrency` 100**, with both
ends verified to be holding exactly 100 requests in flight.

Shape: one sharp cliff at **0.3.2.0 -> 0.4.0.0** (-45%), a slow decline to a trough at 0.5.2.0, a
**recovery between 0.5.2.0 and 0.5.2.4** (+65%), and flat since. The current build is not the worst
point in the range; the trough is.

### An earlier version of this curve was wrong, and the cause is worth keeping

The first sweep reported a second cliff - 0.5.2.8 at ~2,500 msg/s - and a dramatic recovery in the
current build. **Both were artefacts of logging configuration**, and the same figures under a pinned
harness go *up* rather than down:

| version | unpinned (logback default DEBUG) | pinned WARN |
|---|---|---|
| 0.3.2.0 | 22,451 | 21,987 |
| 0.5.2.8 | 2,583 | 16,657 |
| 0.6.0.0-SNAPSHOT | 2,595 | 16,231 |

With no logback config on the classpath, logback defaults to DEBUG and PC logs per record. Old
versions barely notice (0.3.2.0 moves 2%); modern ones are crushed (6.3x). So the confound does not
cancel between arms - **it manufactures a cliff exactly where per-record logging was added.** It hid
because the local arm resolved `parallel-consumer-core`'s tests jar, which ships a `logback-test.xml`,
while every arm from Central did not.

That is itself a finding worth acting on independently of the regression: **modern PC at DEBUG is
about six times slower than modern PC at WARN.** Anyone who turns on debug logging to investigate a
throughput problem will change the thing they are measuring.

### REFUTED: "the old build was over-dispatching"

An earlier revision claimed 0.3.2.0 ignored `maxConcurrency`, ran up to 10,000 requests in flight,
and that the first cliff was therefore the price of a correctness fix. Measured peak in flight kills
it: **0.3.2.0 peaks at exactly 100, the same as today**, at every point in the curve above. The
`maxConcurrency x loadFactor` figure governs the worker pool **queue depth** - records queued ahead
of the dispatch thread - not concurrent requests. The two were conflated.

The `ExternalEngine` change is still the boundary throughput drops at, and its overrides
(`calculateQuantityToRequest`, a no-op `checkPressure`) are still the most likely mechanism. But "it
used to cheat" is not the explanation, and the cost is real work per record rather than accounting.

### Matched concurrency: the gap is real at every ceiling

Both arms, same harness, same workload, only the ceiling varies:

| maxConcurrency | 0.3.2.0 | peak | 0.6.0.0-SNAPSHOT | peak | delta |
|---|---|---|---|---|---|
| 100 | 22,271 | 100 | 16,304 | 100 | **-26.8%** |
| 1,000 | 33,952 | 340 | 28,338 | 340 | **-16.5%** |
| 10,000 | 30,370 | 398 | 23,785 | 423 | **-21.7%** |

Three things follow.

1. **The regression is not an artefact of the ceiling.** It holds at every level, with observed
   in-flight matched between arms.
2. **It narrows as concurrency rises** (-27% at 100, -16% at 1,000). Whatever the per-record cost is,
   more work in flight partially hides it - which fits a fixed serialised cost per record rather than
   something that scales with load.
3. **Both versions saturate around 340-420 in flight and both are slower at 10,000 than at 1,000.**
   The knee is a property of the engine, not of the version.

## WHY it got slower: the missing pipeline buffer, proven by patch

`ExternalEngine` overrides two things, and has since 0.4.0.0. In today's names:

```java
protected int getTargetOutForProcessing() {
    return getOptions().getTargetAmountOfRecordsInFlight();   // maxConcurrency x batchSize
}
protected void checkPipelinePressure() { }                    // no-op: load factor never steps up
```

The core instead returns `getQueueTargetLoaded()` = `getPoolLoadTarget() * dynamicExtraLoadFactor`,
where the factor starts at 2, steps by 2, and is capped at 100.

**The consequence is a pipeline depth, not a concurrency limit.** The core keeps a deep buffer of
work queued behind the records currently in flight, so the dispatch thread always has something
ready. `ExternalEngine` asks only for the shortfall against in-flight, so once the ceiling is full
the request is approximately zero and every completion needs a control-loop iteration before the next
record can be dispatched. Same in flight; nothing behind it.

### Proven, not argued

The two overrides were patched out of the current build - `getTargetOutForProcessing()` delegating to
`getQueueTargetLoaded()`, `checkPipelinePressure()` calling `super` - and nothing else changed:

| build | msg/s at maxConcurrency 100 | peak in flight |
|---|---|---|
| 0.3.2.0 | 22,271 | 100 |
| 0.6.0.0-SNAPSHOT, as shipped | 16,304 | 100 |
| **0.6.0.0-SNAPSHOT, overrides removed** | **21,847 / 21,907 / 22,096** | **100** |

**The current engine matches the 2021 engine exactly once the buffer is restored.** Two consequences,
and the second is the important one:

1. **These two overrides account for the whole regression.** There is no residual per-record cost. An
   earlier revision of this note claimed a "16-27% per-record regression that survives every
   configuration" - that is now explained rather than outstanding, and it was never per-record.
2. **Peak in flight stays at exactly 100.** The recovery is *not* bought by over-dispatching. The
   buffer sits behind the ceiling, not through it, so `maxConcurrency` is still honoured. This is the
   control that matters, because the obvious worry about restoring old behaviour is that it restores
   old over-dispatching, and it measurably does not.

The patch was reverted and the clean snapshot reinstalled; it exists only as this measurement.

### It is the buffer, NOT the pressure system

The first patch changed both overrides at once, so it could not say which mattered. A second patch
restored **only** the target formula and left `checkPipelinePressure()` a no-op, pinning the load
factor at its initial value of 2:

| build | msg/s at maxConcurrency 100 | peak in flight |
|---|---|---|
| as shipped | 16,304 | 100 |
| target restored **and** pressure re-enabled | 21,950 | 100 |
| **target restored, pressure still disabled** | **21,777 / 22,059 / 22,059** | **100** |

**A 2x buffer is the whole effect.** `getPoolLoadTarget()` is `maxConcurrency x batchSize`, so at the
initial factor of 2 the target is 200 against 100 in flight - one record queued behind each one being
worked. The dynamic stepping contributes nothing measurable at this workload.

So the pressure system is **not the cause of the regression - it is inert**. All of the value sits in
a constant multiplier that happens to live inside it. That matters for the auto-scaling track below:
the adaptive mechanism that exists is not earning its complexity here, while the fixed constant next
to it is worth 35%.

### The core arm: core does NOT have the cliff

Every measurement above this section is Vert.x. That was the blind spot, and closing it changes the
conclusion's scope. The harness now has a `core` mode driving `ParallelEoSStreamProcessor` directly
(`MODE=core`), same broker, same dataset, same 100 in flight:

| version | core | Vert.x |
|---|---|---|
| 0.3.0.2 | 26,110 | 22,178 |
| 0.3.2.0 | 25,951 | 21,987 |
| **0.4.0.0** | **25,979** | **11,878** |
| 0.5.0.0 | 23,230 | 9,821 |
| 0.5.2.0 | 23,094 | 9,095 |
| 0.5.2.4 | 25,894 | 15,123 |
| 0.5.3.2 | 23,402 | 16,159 |
| 0.6.0.0-SNAPSHOT | 23,751 | 16,304 |

**Core has no cliff at 0.4.0.0.** It is flat across the exact boundary where Vert.x halves, which is
the control that settles it: the cliff is `ExternalEngine`, not the engine core, not the Kafka
client, not the machine.

Core's own decline is about **10% over five years** (26,110 -> 23,751), against Vert.x's 27%. That
10% is real but is a different question from the cliff, and it is not diagnosed.

### The precise statement of the defect

`ExternalEngine` is **core with the loading factor pinned at 1**:

```java
core:           getPoolLoadTarget() * loadFactor    ==  (maxConcurrency * batchSize) * factor
ExternalEngine: getTargetAmountOfRecordsInFlight()  ==  (maxConcurrency * batchSize)
```

Same formula, multiplier removed. Not a different algorithm. And 1 -> 2 is the whole 35%.

The rationale in the code is *"unlike core, we don't pipeline messages into the executor pool for
processing"* - true, but the dispatch thread still needs a queue to pull from, and with no buffer it
starves waiting on the control loop.

### Scope: which engines this affects

`ExternalEngine` is the base for **Vert.x, Reactor, Mutiny and the language proxy**:

```
ParallelEoSStreamProcessor       extends AbstractParallelEoSStreamProcessor   <- core, unaffected
VertxParallelEoSStreamProcessor  extends ExternalEngine
ReactorProcessor                 extends ExternalEngine
MutinyProcessor                  extends ExternalEngine
ProxyProcessor                   extends ExternalEngine
```

**`ProxyProcessor` is on this path**, so every foreign-language client heading for the v6 experimental
release runs on the throttled pipeline. Nobody has measured it. That ties this note directly to
`branch-classic-comparison-demo.md`: a per-language demo whose headline is a throughput comparison
would be publishing the throttled number.

**Core does not go through these overrides and is immune to this mechanism. That is not the same as
core being unaffected overall** - core has not been benchmarked at all here, and a separate core
regression would be invisible to every measurement in this note. The harness only has a Vert.x arm;
adding a core arm is the obvious next coverage gap.

### What this does NOT establish

Whether removing the overrides is *safe*. They were added deliberately, and the comment on
`checkPipelinePressure` says the pressure system does not apply to external engines. Reasons they
might be right that this measurement cannot see: a deep pipeline of records queued ahead of an
external engine may interact badly with rebalances (queued work whose partitions have been revoked),
with the `astubbs#857` family, or with shutdown draining. **That is the next question, and it is a
correctness question, not a performance one.**

### The release gate, answered

**The 2021 headline number is reachable on the current build**: `maxConcurrency` 1,000 gives ~28,300
msg/s against the cast's 27,201. Nothing needs reverting - it is a configuration change, and the
demo and README should state the concurrency they used.

**And the regression is fully recoverable in code, not merely worked around by configuration.**
Removing the two `ExternalEngine` overrides restores 0.3.2.0's throughput exactly, while still
honouring `maxConcurrency`. What is not yet known is whether that is safe - see above. The decision
before v6 is therefore about correctness risk, not about performance: either establish that a deep
pipeline is safe for external engines, or ship at today's number and record why.

## Open questions, and the owner's reading of them

Recorded as hypotheses, not findings. None of these is tested.

- **Core's 0.5.2.4 -> 0.5.3.2 drop (25,894 -> 23,402) is interesting in its own right**, and so is
  the earlier 0.4.0.0 -> 0.5.0.0 decline. Core has the same trough-and-recovery *shape* as Vert.x
  around 0.5.2.0 / 0.5.2.4, at about a third of the amplitude - so whatever causes it is in shared
  code, not in `ExternalEngine`.
- **Owner's hypothesis for those:** the collection changes in that window, **and more than
  collections - more defensive concurrent code generally**, plus possible changes to the pressure
  system itself. The candidates in the 0.5.2.x range remain `ShardKey`, `ShardManager`,
  `ProcessingShard`, `PartitionState`, and the two refactor commits `f06c26fc8` ("unify PS
  collections, change Set to List") and `b74314d0f` ("SortedSet's all the way down").
- **The load factor is independently reported as broken.** **astubbs#155** (`confluentinc#402`),
  open: *"Max loading factor steps reached: 100/100"* - the factor pegs at its maximum, which is the
  pressure system failing to be a control loop at all. Directly relevant: this note found the
  stepping to be inert on external engines (because it is disabled there) and worth nothing beyond
  the initial 2 on the workload measured. A mechanism that either does nothing or pegs at maximum is
  not regulating anything.
- **astubbs#311** - "Batching requests a full extra in-flight target of work, and batchSize is
  unvalidated" - touches `getTargetAmountOfRecordsInFlight()`, the exact expression at issue here.
- **astubbs#187** (`confluentinc#884`), open: *"Parallel Consumer is 30 times slower than Normal
  Consumer"*. A user report of exactly this class of problem, unresolved. Whether it is this defect
  is unknown, but it should be re-read against these findings before being answered.

## Compare against the other Java parallel-consumption project

Owner's note, 2026-08-20. **The name is not recorded because it could not be identified.** The repo's
own landscape notes were searched - `next-architecture-landscape-comparison.md` (Beam, Temporal, Ray,
Envoy `ext_proc`, Bytewax, Quix, Dask, Share Groups) and the distributed-throttling ideation (Karafka
Pro, Netflix `concurrency-limits`) - and none of them names a Java project matching this description.
Guessing a name would be worse than carrying the description, so here is the description:

- Another Java library doing parallel Kafka consumption.
- **It does not commit offsets** - which removes most of what makes this problem hard, so any
  comparison must say so prominently rather than present a like-for-like number.
- It appears to offer key-ordered parallelism, the same core claim as this project.
- Its material emphasises being *provable* / formally argued.
- It targets a current Java release and uses **virtual threads**.

**Why it is worth doing eventually:** it is the closest comparable, and knowing where the gap is - and
how much of it is explained by not committing offsets - is better than not knowing. The same
discipline as
[`parked-perf-against-native-kafka-clients.md`](parked-perf-against-native-kafka-clients.md) applies:
name the workloads where the comparison is unfair, and publish the case we expect to lose.

**Sequencing:** the virtual-threads comparison is only meaningful after this project has a virtual
threads implementation, and that is **deferred** - `release-0.6.0.0.md` lists virtual threads as
deliberately not in this release, and astubbs#51 is the open PR (cross-repository, on a contributor's
fork; see `branch-package-rename-sweep.md`). So: identify the project, measure the non-virtual-threads
gap first, and revisit the virtual-threads arm when there is something to put in it.

## Feeds the auto-scaling / dynamic concurrency work

The concurrency sweep produced the most directly useful data here, and it belongs to that track
rather than this one:

- **The engine saturates well below the configured ceiling.** At `maxConcurrency` 1,000 and 10,000
  the observed peak in flight was ~330 and ~385 respectively. Asking for 10,000 does not produce
  10,000 outstanding requests; it produces about 385 and a slower result.
- **Throughput is non-monotonic in the ceiling**, with a knee: 100 -> ~16,500, 1,000 -> ~31,350,
  10,000 -> ~23,400 on the same build and workload. A wrong `maxConcurrency` costs about as much as
  the regression this note is about, in either direction.
- **`messageBufferSize` is a public, documented option that sets a static load factor** - and
  `ExternalEngine` ignores the load factor entirely, so on Vert.x, Reactor, Mutiny and the proxy it
  silently does nothing. On core it also measured as no change (23,565 at default vs 23,471 at
  buffer 200), because the default factor of 2 already produces that target.
- **That is the argument for tuning it automatically**, and it is measured rather than asserted. A
  user cannot be expected to find a knee that moves with their workload, and the current failure mode
  is silent: too high looks like a reasonable setting and simply runs slower.

Where that work already lives:

- **[`next-auto-scaling.md`](next-auto-scaling.md)** is the live design note and the place these
  numbers belong. It already argues the case this data supports - an adaptive controller stepping
  concurrency up until performance degrades, TCP-congestion shaped, where *"the cause of the plateau
  never needs diagnosing"*. The measurements here are an instance of exactly that: the knee sits near
  340-420 in flight, and neither 100 nor 10,000 finds it.
- **[`next-distributed-throttling.md`](next-distributed-throttling.md)** - the sibling it was split
  from.
- **astubbs#227** (`confluentinc#21`) - "Dynamic concurrency control with flow control or tcp
  congestion control theory", open.
- **The differentiator claim is already recorded there**, and this note does not restate it:
  `next-auto-scaling.md` says *"no known competitor does runtime-discovered, per-instance adaptive
  concurrency"*, with the priority raised on 2026-08-18 to candidate killer feature. The nearest
  comparator that document's ideation names is **Karafka Pro** (Ruby), whose throttler is explicitly
  local-only and resets on rebalance, plus Netflix's `concurrency-limits` as adaptive prior art.
- **`origin/feature/auto-tuning-pressure`** (2020-12-01, "Wip! Experiments in self tuning") and
  **`origin/features/dynamic-concurrency-control`** (2020-11-05) - prior attempts, unmerged and
  unread.
- `DynamicLoadFactor` is the shipped mechanism, and it is **not** this: it sizes the buffer feeding a
  fixed pool rather than the pool itself, and `ExternalEngine.checkPressure()` no-ops it entirely for
  Vert.x and Reactor. So on the external engines there is currently no adaptive element at all.

## Related

- [`branch-classic-comparison-demo.md`](branch-classic-comparison-demo.md) - where this surfaced.
- [`next-perf-comparison-matrix.md`](next-perf-comparison-matrix.md) - owns measurement semantics and
  the blessed-numbers pipeline. A version-over-version regression check is a natural member of that
  matrix, and this harness is a candidate input to it.
- [`test-required-perf-lane-scope.md`](test-required-perf-lane-scope.md) - the existing perf lane is
  a required PR gate. It did not catch this, which is worth understanding on its own: whatever it
  measures, it is not this.
