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

## Connections to existing issues

What this investigation says about each open issue it touches. **None of these has been posted to the
issue tracker** - recorded here first, deliberately.

| Issue | What this work found |
|---|---|
| **astubbs#155** (`confluentinc#402`) *"Max loading factor steps reached: 100/100"* | Corroborated from a second direction, and worse than reported. The factor pegging at max is one failure; the other is that on **Vert.x, Reactor, Mutiny and the proxy the stepping is disabled outright** (`ExternalEngine.checkPipelinePressure()` is a no-op), so on four of five engines it never steps at all. Measured: re-enabling the stepping added **nothing** beyond the initial factor of 2. A controller that is inert on four engines and saturated on the fifth is not regulating anything. |
| **astubbs#187** (`confluentinc#884`) *"Parallel Consumer is 30 times slower than Normal Consumer"* | A plausible, unconfirmed explanation now exists. If the reporter is on Vert.x, Reactor, Mutiny or the proxy, they are running with the loading factor pinned at 1 and no buffer behind the in-flight ceiling - measured here as worth 35% on one workload, and the effect grows as per-record latency falls. **Read the issue against these findings before answering it**; the first question to ask is which engine they use, and the second is their `maxConcurrency`. Note this is a hypothesis about someone else's workload, not a diagnosis. |
| **astubbs#311** *"Batching requests a full extra in-flight target of work, and batchSize is unvalidated"* | Same expression, opposite direction. That issue is about `getTargetAmountOfRecordsInFlight()` (= `maxConcurrency * batchSize`) requesting **too much**; this note is about `ExternalEngine` using it raw and so requesting **too little**. Any change to that expression must satisfy both, so they should be designed together rather than fixed independently. |
| **astubbs#227** (`confluentinc#21`) *"Dynamic concurrency control..."* | Supplied with measured evidence rather than argument: the knee sits near 340-420 in flight, throughput is non-monotonic in the ceiling, and neither 100 nor 10,000 finds it. Recorded in [`next-auto-scaling.md`](next-auto-scaling.md), which is the design note that owns it. |
| **astubbs#242** (`confluentinc#154`) - the language proxy | `ProxyProcessor extends ExternalEngine`, so **every foreign-language client is on the throttled path**. Its per-language comparison demo would publish the throttled number. See [`branch-classic-comparison-demo.md`](branch-classic-comparison-demo.md). |

## Compare against the other Java parallel-consumption project

**Identified: llingr / llingr-demux** (<https://llingr.io/>), supplied by the owner 2026-08-20 after
a search of the repo's own landscape notes failed to name it. It has its own entry -
[`market-analysis-llingr.md`](market-analysis-llingr.md) - because it is closer to this project than
anything else recorded: same key-ordered-concurrency claim, a gRPC sidecar relay for other languages
(the astubbs#242 architecture, already shipped), formally verified, and commercial with a patent
pending.

**It is a Go project, not a Java one** - the JVM build is a separate native implementation of the
same design. See the note.

**Two further things in the original description turned out to be wrong, and both change the
comparison:**

- **It does commit offsets**, by reconstructing contiguous per-partition order before committing -
  the same hard problem PC's offset encoding solves. So there is no "they skipped the hard part"
  discount available.
- **Nothing supports virtual threads.** The JVM build requires JDK 21+ and lists **Kotlin
  coroutines** among its dependencies. JDK 21+ is a floor, not evidence of Loom.

The rest of the original description held:

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

## Concurrency as an axis, and the client control the llingr comparison was missing

Added 2026-08-21. Two harness changes and the sweep they made possible.
[`bench/run-bisect.sh`](../../bench/run-bisect.sh) grew a `CONCURRENCIES` list on the same contract
as `DELAYS`, and a `concurrency` results column, so one invocation sweeps concurrency x delay x
mode. And [`bench/franz`](../../bench/franz) is a new arm: **franz-go with no engine at all**, the
Go-side counterpart of `vanilla`, a fixed worker pool of `-concurrency` goroutines behind an
unbuffered channel, sleeping per record and counting. Data in
[`results/concurrency-sweep-0ms.csv`](../../bench/results/concurrency-sweep-0ms.csv) and
[`results/concurrency-sweep-2ms.csv`](../../bench/results/concurrency-sweep-2ms.csv). Same broker,
same 350,000-record topic, same bytes, fresh consumer group per run, logging pinned, two repeats
(four for the two core points measured in both sweeps). Ranges are inside 3% except llingr at
concurrency 1 and 5, which spread 6%.

### The hypothesis that prompted this is REFUTED

The delay sweep found llingr at 0ms beating PC while holding a peak of **two or three** records in
flight against PC's hundred, which read as PC paying to fan out work that had none in it. If that
were so, PC at a low ceiling would match or beat PC at 100.

It does the opposite, monotonically, with no knee anywhere in the range:

| `maxConcurrency` | core, delay 0 | peak in flight |
|---|---|---|
| 1 | 8,473 | 1 |
| 2 | 11,600 | 2 |
| 5 | 19,014 | 5 |
| 10 | 25,417 | 10 |
| 25 | 36,472 | 25 |
| 100 | 51,881 | 100 |
| 1,000 | **63,915** | 1,000 |

Dropping from 100 to 1 costs a factor of six. **PC is not paying for fan-out it does not need - the
fan-out is what pays for PC.** Concurrency 1 is the reading that matters: with no overlap available,
a record costs PC **118µs** of wall clock end to end, against franz-go's 8.9µs and llingr's 13.2µs
on the same records. Concurrency is how that 118µs gets hidden, and the more of it PC is given the
more of it hides. Nothing here locates PC's optimum: 1,000 is the largest ceiling measured at 0ms
and the curve is still climbing at it.

The mirror of that reading explains llingr's flat curve without any appeal to cleverness. Its peak
in flight is 2-3 whatever the dial says - 2 at a setting of 2, and still 3 at a setting of 1,000 -
because at 0ms the handler returns before the next record arrives, so no queue ever forms to fan out
from. Its throughput barely moves across the whole range (75,916 at 1, 88,641 at 100, 85,817 at
1,000). llingr is not out-scheduling PC at 0ms; it has so little per-record cost that one worker
nearly saturates the fetch path and extra concurrency has nothing left to buy.

**llingr's dispatch does respond to work, and PC's stops responding first.** At 2ms and a ceiling of
1,000, llingr reaches a peak of exactly 1,000 while **core reaches 438-456** - PC cannot supply
itself the concurrency it was configured for once the handler takes time, which is the in-flight
target and buffer ceiling this note already documents, seen from a third direction. It costs
directly: 83,269 against 51,865 at that setting. (Read the core peak of 1,000 at *0ms* carefully -
with a zero-length sleep, a thousand pool threads entering the handler together is most plausibly a
dispatch burst rather than sustained concurrency. That is a reading, not a measurement.)

### The franz control: none of llingr's advantage survives it

`franz`'s mean is above `llingr`'s at **every one of the ten concurrency-delay points measured**,
and the two arms' ranges are disjoint at nine of them. The exception is 2ms / ceiling 25, where
llingr ran 9,969-10,102 and franz 10,045-10,116 - overlapping, so read that row as a tie rather than
a win. There is no point in this data at which the llingr engine is faster than the bare client it
runs on, and one at which it is level.

| delay | ceiling | core | llingr | franz | llingr as % of franz | core as % of franz |
|---|---|---|---|---|---|---|
| 0ms | 1 | 8,473 | 75,916 | 112,181 | 68% | 8% |
| 0ms | 10 | 25,417 | 88,079 | 109,308 | 81% | 23% |
| 0ms | 100 | 51,881 | 88,641 | 106,562 | 83% | 49% |
| 0ms | 1,000 | 63,915 | 85,817 | 106,189 | 81% | 60% |
| 2ms | 25 | 8,667 | 10,035 | 10,081 | 100% | 86% |
| 2ms | 100 | 25,413 | 30,734 | 31,683 | 97% | 80% |
| 2ms | 1,000 | 51,865 | 83,269 | 92,385 | 90% | 56% |

At the point the original finding was taken - 0ms, ceiling 100 - llingr is **1.71x** core. The bare
client is **2.05x** core at the same point. **The whole of llingr's advantage there is franz-go, and
llingr then gives 17% of it back.** The engine-versus-engine question the delay sweep appeared to
answer was never asked by it; what it measured was mostly the client underneath.

The delay column is the more useful half. At 2ms - still a fast handler, but one that does something
- franz-go with no engine is **1.16x** core at a ceiling of 25 and **1.25x** at 100. A cross-runtime,
cross-client gap of a quarter is not the shape the 0ms numbers implied, and the 0ms row is the
synthetic one: no real handler returns in zero time.

### What this does NOT show, and one of these matters a lot

- **PC's own deficit cannot be split into client and engine.** This is the biggest gap and it is the
  direct next measurement. `franz` isolates franz-go from llingr; **there is no Java-side
  equivalent** - the `vanilla` arm drives real HTTP through WireMock rather than sleeping, so it is
  not a floor for the sleep-handler arms and returns a few hundred msg/s. So "core reaches 49% of
  franz-go at 0ms/100" bounds PC and the Java client *together*, and says nothing about how that 51%
  divides between them. A `Bench` mode that is a plain `KafkaConsumer` plus a fixed thread pool plus
  a sleep would settle it, and would make the 118µs-per-record figure attributable. Until it exists,
  every core-versus-franz ratio above is a joint result.
- **`franz` is a floor, not a competitor, and it is deliberately weaker than both engines.** It does
  not order by key - records go to whichever worker is free, so two records sharing a key run
  concurrently and in either order, which is the entire problem both engines exist to solve. And it
  commits what franz-go's default autocommit commits, which is what has been *polled*: a crash loses
  fetched-but-unrun records. Nothing here is evidence that anyone should use it.
- **It does not show llingr is slow.** It shows llingr's overhead is small enough to be visible only
  against its own client, which is a compliment paid by a sharper instrument. Its 3% cost at 2ms /
  ceiling 100 is close to unmeasurable at n=2.
- **The machine was not quiet, and this was discovered after the fact.** A second agent session was
  working in this same worktree throughout, and by the end of the window was running
  `bench/run-divergence.sh` against the same broker; 15-minute load average on a 12-core laptop read
  8.57 when the sweeps finished. Four things bound how much that can have moved the conclusion, and
  none of them makes the data clean:
  - **The two core points measured twice, ten minutes apart, agree.** Concurrency 25 gave
    36,904 / 36,592 in the first window and 35,773 / 36,619 in the second; concurrency 100 gave
    52,286 / 51,080 and 52,153 / 52,006. Spread across both windows is under 3%.
  - **Contention depresses; two of these numbers went up.** core at 2ms / ceiling 100 measured
    25,413 today against 23,752 in [`core-curve.csv`](../../bench/results/core-curve.csv) earlier,
    and llingr at 2ms / ceiling 100 measured 30,734 against 29,291 in
    [`delay-sweep-llingr.csv`](../../bench/results/delay-sweep-llingr.csv). Both are ~6% faster than
    the same points on a quieter machine, not slower.
  - **The load ran against the franz arm's favour, not for it.** Within every invocation `llingr`
    ran before `franz`, so rising background load penalises franz - and franz won every point
    anyway. The franz conclusion is therefore conservative.
  - **The core-versus-Go comparison is the exposed one.** core and the Go arms were measured in
    *separate invocations* minutes apart rather than interleaved, so any drift in machine state sits
    directly on the ratio that matters most. The size of the core-versus-franz gap should be treated
    as approximate; its direction is not in doubt at 2x, but a claim like "80% at 2ms / 100" should
    be re-taken on a quiet machine before it is relied on.

  **Interleave the arms and check the machine next time.** The harness runs mode as the outer loop,
  which is right for compiling each classpath once and wrong for isolating a slow drift, and it says
  nothing about what else is running.
- **One partition, one laptop, every record a distinct key, n=2.** Ranges are reported above; no
  statistic stronger than a range is claimed. All-distinct keys is llingr's best case on routing and
  is unchanged from the earlier sweep.
- **PC's optimum was not found.** The 0ms curve is still rising at 1,000, and the earlier
  matched-concurrency data has Vert.x turning over between 1,000 and 10,000. Where core turns over
  at 0ms is unmeasured.
- **llingr's demand-driven dispatch is inferred, not observed.** The peak column is consistent with
  it and nothing here reads llingr's scheduler.

### Where this lands

- **For the release gate:** unchanged in direction, sharpened in size. The gate is about recovering
  PC's own throughput without ignoring the concurrency the user asked for, and the concurrency curve
  says the recovery lever is real and large - **51,881 -> 63,915 at 0ms from the ceiling alone** -
  while the 2ms rows say the cross-engine gap being chased is closer to 25% than to 71%.
- **For [`next-auto-scaling.md`](next-auto-scaling.md):** three measured inputs. Throughput is
  monotonic in the ceiling at 0ms across three orders of magnitude with no knee, so a controller has
  a clean gradient to climb in the regime where overhead dominates. PC cannot reach a configured
  ceiling of 1,000 once the handler takes 2ms, topping out near 450, so the controller's actual
  actuator is the in-flight target, not `maxConcurrency`. And a competitor's dispatch demonstrably
  tracks demand rather than configuration - peak 3 at 0ms, peak 1,000 at 2ms, same dial.
- **For [`market-analysis-llingr.md`](market-analysis-llingr.md):** the throughput comparison is
  even less worth competing on than that note already argues, and now for a measured reason rather
  than a structural one. The gap at the headline point is the Kafka client, not the engine.
  **Private research; none of the llingr figures above may be published.**

*Provenance, two items. The `llingr` binary these runs used was built from a working tree carrying
another session's uncommitted edit to `bench/llingr/main.go`. It was checked rather than assumed:
the edit lifts `concurrentKeysMax` to package scope and adds an early return taken only when
`-scenario` is passed, which these runs did not pass, so the throughput path executed is the
committed one. And the `franz` rows in `concurrency-sweep-0ms.csv` were relabelled by hand from
`franz-go-github.com/twmb/franz-go` to `franz-go-v1.21.5`. `franz_version()` read the wrong go.mod
field on a single-line `require`; the runs are unaffected, the same binary from the same `go.mod`,
and the extractor was fixed rather than the label being left to mislead.*

## The other benchmark: what the offset encoding buys, measured

Added 2026-08-21, and it is a different question from everything above. Every measurement in this
note is steady-state throughput with all records succeeding - which is the one workload where PC's
central design decision is invisible, and also the workload that flatters a leaner engine. So the
throughput arms could never say whether PC's hardest subsystem earns its complexity.

Harness: [`bench/run-divergence.sh`](../../bench/run-divergence.sh),
[`bench/Divergence.java.template`](../../bench/Divergence.java.template) and the scenario runners in
[`bench/llingr/scenarios.go`](../../bench/llingr/scenarios.go). Format and controls are documented in
[`bench/README.md`](../../bench/README.md); raw data in
[`bench/results/divergence-500ms-commit.csv`](../../bench/results/divergence-500ms-commit.csv),
[`divergence-5s-commit-slow.csv`](../../bench/results/divergence-5s-commit-slow.csv),
[`divergence-5s-commit-too-short.csv`](../../bench/results/divergence-5s-commit-too-short.csv),
[`divergence-commit-metadata.csv`](../../bench/results/divergence-commit-metadata.csv) and the two
`divergence-series-stuck-*.csv` time series. **The llingr arm is internal only** - see
[`market-analysis-llingr.md`](market-analysis-llingr.md) and `bench/llingr/NOTICE.md`.

**This scenario is chosen because it favours PC.** That is stated first because it is the same
selection the pure-throughput benchmark makes in the other direction, and a benchmark that only
flatters us is not usable. `bench/README.md` lists where it is unfair to llingr, and the two findings
below that go against PC are in the same section as the ones for it.

### The thesis this set out to test was WRONG, in an instructive way

The premise was: *with a stuck record, PC's committed offset keeps advancing while a contiguous
frontier freezes.* **It does not.** Both engines' committed offsets freeze at exactly the same place.

200,000 records, one partition, `UNORDERED`, `maxConcurrency`/`ConcurrentKeys` 100, 2ms per record,
and the record at **offset 1** taking 25 seconds. Committed offset sampled from the **broker**
(`Admin.listConsumerGroupOffsets` / `kadm.FetchOffsets`) every 250ms - the same question a user asks
with `kafka-consumer-groups.sh`, so neither engine answers it about itself. n=3, agreeing to within
1%:

| | PC `core` | llingr |
|---|---|---|
| committed offset at end of run | **1** | **1** |
| longest the committed offset did not move | 24.8s | 24.3s |
| records completed above the committed offset | 199,998 | 199,998 |
| **commit metadata** | **12 bytes** | 40 bytes |

PC commits the **lowest incomplete offset**, exactly as a contiguous-commit design does. The
committed offset alone therefore cannot distinguish the two architectures, and any comparison that
quotes it - in either direction - is measuring the wrong thing. It also means the offset lag a user
sees in `kafka-consumer-groups.sh` overstates PC's exposure, which is worth knowing independently of
any competitor.

**The difference is entirely in the metadata**, and the harness now records it verbatim:

```
PC core :  ZQAAAAEAAw0+                              ->  0x65 'e' = RunLengthV2, runs [1, 199998]
llingr  :  kgo-fca5df11-8e2b-4182-accf-0463e966d73f  ->  a franz-go client instance UUID
```

Nine bytes describe "offset 1 incomplete, the next 199,998 complete". llingr's forty bytes are its
Kafka client's identity and carry no completion information at all. **That is the whole differentiator
in one line of evidence**, and it is stronger than the throughput-shaped argument it replaces.

### Restart: 6,412 wasted records against 100,008

The number a user actually pays. Same workload, the stuck record blocking indefinitely, the process
killed mid-flight with `Runtime.halt` / `os.Exit` - no drain, no shutdown hook, no final commit -
then restarted on the same group. Both engines' commit interval set to **500ms** (PC's
`commitInterval`, llingr's `AutoCommitInterval`; both default to 5s). n=3:

| | PC `core` | llingr |
|---|---|---|
| records completed before the crash | 100,426 | 100,009 |
| committed offset at the crash | 1 | 1 |
| **already-completed records redelivered** | **6,412** (6.4%), range 5,823-6,978 | **100,008** (100.0%), range 99,999-100,022 |
| total records the restart had to process | 105,986 | 199,999 |
| time to finish the topic after the restart | 2.8s | 4.7s |

llingr reprocessed **every single record it had already done**. PC reprocessed 6.4% of them, and
those 6,412 are not the encoding failing - they are the 500ms of completions that had not yet been
committed when the process died. The encoding did its job on the other 94,000, from 24 bytes of
metadata.

**This is measured, not inferred.** The crashed run writes out the offsets it finished and the resume
run counts how many of those come back. Deriving it from the committed offset would have got PC's
answer wrong in PC's favour, because for PC the offsets are in the metadata and not in the number.

### Against PC: the advantage is bounded below by the commit interval, and at the shipped default it can vanish

The first restart run left both engines at their shared 5s default and crashed 2.6s after the only
commit that had happened. Result: PC redelivered 149,983 of 150,250 - **no better than llingr**
([`divergence-5s-commit-too-short.csv`](../../bench/results/divergence-5s-commit-too-short.csv), kept
as the negative control). That run was measuring commit **lag**, not commit **strategy**, because it
was shorter than two commit cycles.

Re-run at that same 5s default but with a **20ms** handler, so the crashed run spans about five
commit cycles instead of one. 200,000 records, crash after 100,000 completions, n=3:

| | PC `core` | llingr |
|---|---|---|
| records completed before the crash | 100,052 | 100,002 |
| commit metadata at the crash | 35 bytes | 40 bytes |
| **already-completed records redelivered** | **15,693** (15.7%), range 15,306-16,411 | **100,001** (100.0%), range 99,999-100,004 |
| total records the restart had to process | 115,641 | 199,999 |
| time to finish the topic after the restart | 27.2s | 41.9s |

**At the shipped defaults the advantage is 6.4x, not 15.6x.** PC's 15,693 wasted records are
five seconds of uncommitted completions; llingr's 100,001 are everything it had done since the record
got stuck.

So the honest statement of the differentiator is a **ratio, not a constant**:

```
wasted work on crash  ~  commitInterval x throughput + inFlight        (PC)
                      ~  timeSinceTheStallBegan x throughput           (contiguous-commit design)
```

PC's advantage is the ratio between "how long the record has been stuck" and "how long since the last
commit". **Crash within one commit interval of the stall starting and the two designs lose the same
amount.** That is the caveat that has to travel with the headline number, and it is also a
configuration lever a user can pull: at 500ms the advantage was 15.6x, at 5s with a 20ms handler it was
6.4x, and on a run shorter than two commit cycles it was 1x.

### Against PC: the committed offset can sit FURTHER back than the competitor's

In the retry scenario PC's committed offset averaged **24,827** against llingr's **40,306** on the
same workload. PC always has some record in retry-backoff holding the lowest-incomplete position,
while llingr dead-letters and moves on. Anyone monitoring consumer lag would read PC as further
behind, and be wrong - the metadata says otherwise - but the alarm would still fire. **That is a real
operational cost of this design**, it is not fixed by any of the above, and no throughput benchmark
would ever surface it.

### Retry: 100% completion against 90%, and 5,000 dead letters that a retry would have saved

50,000 records, 10% failing on first delivery and succeeding on retry, PC's retry delay set to 200ms
(its default is 1s, which would have made the arm fifty seconds of waiting). n=3:

| | PC `core` | llingr |
|---|---|---|
| records completed successfully | **50,471 (100%)** | **45,002 (90%)** |
| deliveries | 55,691 | 50,055 |
| retried | 5,220 | 0 |
| **dead-lettered** | **0** | **4,999** |
| wall clock | 4.75s | 4.24s |

Every one of those 4,999 would have succeeded on a second attempt. **State this as a feature
difference, not a defect**: llingr dead-letters on first failure by design and commits the record
anyway - `nexus.WriteDeadLetter`'s own doc comment says so - and a user who wants retries writes them
into their dead-letter handler. What the number shows is where that work lands by default, and that
PC's 10% higher completion rate costs 11% more deliveries and 12% more wall clock.

### What this changes in the positioning

[`market-analysis-llingr.md`](market-analysis-llingr.md) proposed the line *"PC commits past the gaps,
so one slow key never holds up a partition"*. **The first half of that is now measured and the second
half is false as written**: the committed offset is held up, identically, on both engines. The
defensible version is about restart cost, and it has a number:

> One stuck record, one crash: PC reprocessed 6% of the work it had already done; a contiguous-commit
> design reprocessed 100% of it. Nine bytes of commit metadata is the difference.

Two items that note lists are now settled by measurement rather than by reading a marketing page:

- **"The buffer is bounded, so the failure mode is a stall"** - not observed. llingr's
  `CommitPartitionSliceLen` (default 400, min 50, max 2,000) is documented in its own config README as
  *pre-allocating* space for gap tracking, and a 199,998-record gap did not stall it: it completed the
  dataset at full speed and simply committed nothing. **The failure mode is silent redelivery on
  restart, not backpressure.** That is a weaker claim than the note makes, and a more accurate one.
- **"Restart reprocesses everything after its commit point"** - confirmed exactly, at 100.0% across three
  repeats.

### Where it is unfair, and what it does not establish

The full list is in [`bench/README.md`](../../bench/README.md) and should be read before any of these
numbers is quoted, including internally. The short version: the workload is chosen to expose one
design difference; a stuck record that outlives the whole dataset is the worst case for a contiguous
frontier and close to the best case for offset encoding; the two engines use different Kafka clients;
all keys are distinct; there is one partition; and neither engine is tuned.

Not established: anything about **multiple** gaps. Every scenario here has exactly one stuck record,
so the encoding never leaves run-length's best case. PC's metadata is capped at 4KB
(`OffsetMapCodecManager.DefaultMaxMetadataSize`) with backpressure above 75% of it
(`PartitionState.getPressureThresholdValue`), and a scattered incomplete set - many small gaps rather
than one large one - is where that cap would actually be reached. **That is the scenario most likely
to falsify the advantage above, it is the one llingr's own docs describe as their tuning case
("high-jitter workloads where widely varying processing times leave wide gaps"), and it is not
measured here.** It is the obvious next arm.

### Provenance and contention

Same disclosure the concurrency sweep above makes, from the other side: **these runs shared a
12-core laptop and one broker with that sweep**, load average around 8-13 for the window. Two things
bound what that can have done.

**The headline metrics here are counts, not times, and counts do not move with machine load.**
Redelivered records, committed offsets, metadata bytes and dead-letter counts are what the
conclusions rest on. The ones that decide anything are stable: llingr's redelivery rate was
100.0% in every repeat of both configurations, both engines' committed offsets froze at offset 1 in
every repeat, and llingr's dead-letter count was 4,997-5,002 against a predicted 5,000. **PC's own
redelivered count is the loose one** - 5,823-6,978 at a 500ms interval, 15,306-16,411 at 5s, a spread
of about 10% either way - because it depends on where in the commit cycle the crash happened to land.
That spread is inside the mechanism being described, not noise on top of it, and it is an order of
magnitude smaller than the gap it is being compared against.

**The one place contention leans, it leans in PC's favour, so the ratio is an optimistic bound.**
PC's wasted work is roughly `commitInterval x throughput`, while the contiguous-commit design's is
fixed by the scenario at everything completed before the crash. A slower machine therefore *shrinks*
PC's numerator and leaves llingr's alone, inflating the advantage ratio. The model is written out
above precisely so a reader can recompute it for their own throughput rather than take 15.6x or 6.4x as
a constant.

Both harnesses were run against the same long-lived broker (`pc-bench-broker`) and their datasets are
different topics, so neither could read the other's bytes; but they were competing for the same
broker, the same page cache and the same cores. **Re-take the timing columns on a quiet machine before
relying on any of them.** The count columns need no such caveat.

## Related

- [`branch-classic-comparison-demo.md`](branch-classic-comparison-demo.md) - where this surfaced.
- [`next-perf-comparison-matrix.md`](next-perf-comparison-matrix.md) - owns measurement semantics and
  the blessed-numbers pipeline. A version-over-version regression check is a natural member of that
  matrix, and this harness is a candidate input to it.
- [`test-required-perf-lane-scope.md`](test-required-perf-lane-scope.md) - the existing perf lane is
  a required PR gate. It did not catch this, which is worth understanding on its own: whatever it
  measures, it is not this.

---

# The handover for the next performance session - read this section first

**Written 2026-08-21**, at the owner's request, so the next session does not have to reconstruct any
of this from a chat log. Everything below is measured, and every measured claim points at
[`bench/results/high-concurrency-unordered.csv`](../../bench/results/high-concurrency-unordered.csv),
which carries its own conditions in a header comment.

## If you read nothing else

**Parallel Consumer's engine is not the problem. The Kafka client is.**

Four arms, one broker, one dataset, all keys distinct, handler is a sleep:

| Arm | What it is |
|---|---|
| `core` | Parallel Consumer |
| `pool` | plain `KafkaConsumer` + fixed thread pool + semaphore. **No engine.** The Java floor |
| `llingr` | a Go engine, reaching Kafka through franz-go |
| `franz` | plain franz-go + worker pool. **No engine.** The Go floor |

**Three findings, in order of how much they should change what anyone does:**

1. **The two floors are far apart and the two engines are not.** The Java floor reaches **31-67%** of
   the Go floor depending on the operating point, with no engine on either side. Meanwhile the Go
   engine matches its own floor to within a quarter of a percent at 100ms, and PC matches the Java
   floor to within a few percent at most points - **and beats it at two.** Any comparison that reports
   "engine A versus engine B" without both floors is reporting the client libraries.
2. **Nobody's engine matters at a realistic handler delay.** At 100ms per record the engine cost
   disappears into the sleep. Engines only become visible near a zero-cost handler. **Competing on
   engine microseconds optimises the smallest term** - see `market-analysis-llingr.md` section 5a.
3. **The concurrency setting dominates both.** The same build spans a 1.9x throughput range purely by
   changing `maxConcurrency`. That is larger than the entire five-year engine regression this note
   was opened to investigate.

## What is actually PC's to fix, and what is not

**Not ours - a ceiling at ~2,750 records in flight.** Setting `maxConcurrency(5000)` at a 100ms
handler yields ~2,750 in flight. **The bare Java consumer yields 2,848 under identical conditions**,
and both Go arms reach 5,000 exactly. Tracked in
[`bug-in-flight-ceiling-above-2000-concurrency.md`](bug-in-flight-ceiling-above-2000-concurrency.md).
Ruled out by single-variable runs: window length, partition count (1 vs 10), `max.poll.records`
(500 vs 5,000), the loading-factor buffer (dynamic vs a static 25,000), and ordering mode.

**RETRACTED: there is no `UNORDERED` deficit.** An earlier version of this section claimed
`UNORDERED` ran 21% slower than `KEY` at 100ms and 1,000 concurrent, called it *"the single most
promising target in this whole investigation"*, and a fix was written, tested and merged on the
strength of it. **All of that was built on a confound, and the confound was ours.**

The two numbers being compared were taken at different record counts:

```
core,UNORDERED,100000,...,100,1000,1,7318.0    <- 100,000 records
core,KEY,     500000,...,100,1000,1,8875.2     <- 500,000 records
```

**Window length was already known to matter here** - the same note records throughput moving 12,429 ->
19,385 at 5,000 concurrent on record count alone, and states that the short window understates the
rate. It was then used as an uncontrolled variable in the very next comparison.

**Measured properly, at 500,000 records for both**, three runs each:

| 100ms / 1,000 concurrent | Runs | Mean |
|---|---|---:|
| `UNORDERED` | 8,841 / 8,791 / 8,770 | **8,800** |
| `KEY` | 8,875 (single) | **8,875** |

**0.9% apart. The ordering modes perform the same**, and that is the actual finding: the shard *shape*
- ten shards holding a thousand records each versus half a million shards holding one - makes no
measurable difference to throughput at this operating point. It is a more interesting result than the
one it replaces, and it further bounds how much the shard data structures can be costing.

**The fix was reverted.** `perf/resume-shard-scan` made the dispatch scan resume rather than restart at
the head of each shard's skip list. Like-for-like, three runs each at 500,000 records:

| 100ms / 1,000 concurrent | Runs | Mean |
|---|---|---:|
| Before the fix | 8,841 / 8,791 / 8,770 | **8,800** |
| After the fix | 8,807 / 8,784 / 8,868 | **8,819** |

**+0.2%. Nothing.** Unchanged at 100ms/5,000 (19,566 -> 19,372) and at 2ms/1,000; and the apparent 18%
loss at 2ms/5,000 was also a single-sample artefact - that point has a **21% run-to-run spread**, the
noisiest in the grid.

**The rescan is real in the source and is not costing anything measurable.** `getWorkIfAvailable` does
open a fresh iterator at the head of the shard every pass, and records do stay there until they
succeed, so it genuinely is O(in-flight) per pass. The theory was right about the code and wrong about
the consequence. **Do not re-derive it from the source and re-attempt it without a measurement first.**

**The test survives the revert and should stay.** `UnorderedShardScanResumeTest` passes against the
unmodified code and pins a starvation property nothing else covered: a record that becomes selectable
again behind the dispatch scan must still be offered while the shard is continuously fed. It cost two
wrong attempts to write - the first version passed with the fix deleted, because an idle tail hides the
bug.

**Two process lessons, both earned twice today:**

- **A benchmark comparison must fix every axis the harness sweeps, not just the one under test.** The
  harness records records, partitions, ordering, delay, concurrency and client pin as columns precisely
  so this is checkable - and the check was not done.
- **A single run is not a measurement at a noisy operating point.** 2ms/5,000 spreads 21%; three of the
  four conclusions drawn from single runs at that point were wrong.

**Also ours, unattributed: 2ms at 5,000 concurrent**, where both modes sit 22-36% below the Java
floor. No hypothesis yet.

## Three hypotheses that were tested and REFUTED - do not re-run these

Each cost real time. They are recorded so nobody spends it twice.

1. **"Summing work across shards on every poll is O(shards) and expensive."**
   `WorkManager.isSufficientlyLoaded()` calls
   `ShardManager.getNumberOfWorkQueuedInShardsAwaitingSelection()`, which streams every shard and sums.
   It looks like an obvious hot-path cost. **`KEY` mode puts ~500,000 keys through the same code path -
   four to five orders of magnitude more shards than `UNORDERED`'s ten - and is FASTER at 100ms/1,000
   and identical at 100ms/5,000.** If the sum were costly, that could not happen. **Do not patch it for
   performance.**
2. **"It is lock contention."** There is no `synchronized`, `ReentrantLock` or `.lock()` anywhere in
   `WorkManager`, `ShardManager` or `ProcessingShard`. **Nothing locks between "a record is available"
   and "the record is submitted."** The concurrent *collections* remain a separate question - see the
   audit in [`next-performance-regression-testing.md`](next-performance-regression-testing.md) - but
   the refutation above also bounds how much they can be costing.
3. **"The engine is half the speed of the competition, so the engine needs rewriting."** The floors
   say the gap is the client. **A rewrite aimed at the engine targets the smallest available saving.**

## The two experiments worth running next, in order

1. **Virtual threads, as a measurement.** The one thing both Java arms share and neither Go arm has is
   thousands of live platform threads. **[PR #51](https://github.com/astubbs/parallel-consumer/pull/51)
   already implements it** - a `useVirtualThreads` option, `setupWorkerPool` generalised to
   `ExecutorService`, and `synchronized` replaced with `ReentrantLock` to avoid pinning. Crucially it
   reaches the Java 21 APIs **reflectively**, so it compiles under this project's Java 8 target
   (`release.target = 8`) and throws a clear `UnsupportedOperationException` on an older JVM. That
   removes the constraint that looked hardest. What remains is rebasing it across the
   `io.confluent` -> `bz.stub` rename and giving CI a JDK 21 lane, since JDK 17 silently skips its
   tests. Run the same grid with it and see whether the ~2,750 ceiling moves.
2. **A profiler over 2ms/5,000 and 100ms/1,000.** Two well-defined points, both PC's own. An
   async-profiler run separates allocation, control-loop CPU and contention in one pass, and would
   settle in minutes what these controlled runs can only bound.

**What not to do:** rewrite the engine, swap concurrent collections on suspicion, or optimise for a
zero-cost handler. The first targets the smallest term, the second is bounded by refutation 1 above,
and the third is not a workload anyone has.

## Axes the harness now carries, and why each exists

`bench/run-bisect.sh` sweeps **modes, delays, concurrencies, partitions, ordering** and pins
kafka-clients. Every one is a results column, because **a ruled-out suspect still has to be readable
back** - a file swept across an axis whose value is not recorded cannot be interpreted later.

Still missing, and the one that matters most for honesty: **key distribution**. Every number above
uses all-distinct keys, which is a best case for any key-sharded design. The sweep worth adding is
all-unique, uniform over N, Zipf, single hot key, clustered - specified in
[`next-performance-regression-testing.md`](next-performance-regression-testing.md).
