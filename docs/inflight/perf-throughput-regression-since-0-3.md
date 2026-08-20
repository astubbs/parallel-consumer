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

## The first cliff: 0.3.2.0 -> 0.4.0.0, and what it is NOT

The bisect showed a step, not a slope. Throughput halves at exactly one release boundary and stays
down:

| version | run 1 | run 2 |
|---|---|---|
| 0.3.0.2 | 22,079 | 22,655 |
| 0.3.1.0 | 22,338 | 22,611 |
| 0.3.2.0 | 22,451 | 22,247 |
| **0.4.0.0** | **11,334** | **11,551** |
| 0.4.0.1 | 11,321 | 11,388 |
| 0.5.0.0 | 10,457 | 10,260 |
| 0.5.1.0 | 10,309 | 10,907 |
| 0.5.2.0 | 9,858 | 10,721 |
| **0.5.2.8** | **2,583** | **2,467** |
| 0.5.3.2 | 2,536 | - |

(Sweep-harness numbers, internally consistent. Compare within a table, never across tables - the
control run further up used a different produce configuration and different harness instrumentation.)

19 commits sit in the first gap, and one introduces `internal/ExternalEngine.java` - the shared base
for Vert.x and Reactor, which is what this benchmark drives. It changes how much work is requested:

```java
// 0.3.2.0, via the core path
getQueueTargetLoaded() = options.getMaxConcurrency() * DynamicLoadFactor.getCurrentFactor();  // factor: 2 -> 100

// 0.4.0.0, ExternalEngine
protected int calculateQuantityToRequest() {
    return Math.max(0, maxConcurrency - wm.getNumberRecordsOutForProcessing());
}
protected void checkPressure() { }   // no-op: the load factor never steps up
```

### REFUTED: "the old build was over-dispatching"

An earlier revision of this note claimed the old build ignored `maxConcurrency` and ran up to 10,000
requests in flight, making the first cliff the price of a correctness fix. **That was wrong, and it
was wrong because it was inferred from the diff instead of measured.**

The harness now measures peak in-flight **at the stub**, which is what the engine actually had
outstanding rather than what it was configured to allow:

| arm | maxConcurrency | msg/s | peak in flight |
|---|---|---|---|
| 0.3.2.0 | 100 | 21,178 / 22,054 | **100** |
| 0.6.0.0-SNAPSHOT | 100 | 16,563 / 16,532 | **100** |
| 0.6.0.0-SNAPSHOT | 1,000 | 30,435 / 32,270 | 327 / 340 |
| 0.6.0.0-SNAPSHOT | 10,000 | 23,361 / 23,421 | 390 / 381 |

**0.3.2.0 peaked at exactly 100, the same as today.** It was not over-dispatching to the HTTP layer.
The `maxConcurrency x loadFactor` figure governs the **worker pool queue depth** - how many records
may sit queued ahead of the dispatch thread - not the number of concurrent requests. Those two were
conflated. The `ExternalEngine` change is still the boundary the throughput drops at, but "it used to
cheat" is not the explanation.

### What the measurement does establish

1. **At matched concurrency there is a real per-record cost.** Same 100 in, same 100 observed
   in flight, and current is ~24% slower (16,550 vs 21,600). That is not accounting - it is work
   being done per record that was not being done before. **This is where the locking / thread-safe
   collection hypothesis belongs**, and it is now the leading explanation rather than a guess.
2. **The throughput is recoverable by configuration, and then some.** At `maxConcurrency=1000` the
   current build reaches ~31,350 msg/s - faster than 0.3.2.0 ever measured here, and faster than the
   2021 cast.
3. **More concurrency is not monotonically better.** At 10,000 it falls back to ~23,400, and observed
   peak in-flight only reaches ~385 either way, so the engine saturates around 300-400 concurrent and
   further ceiling only adds overhead. A demo or a doc that recommends a number should recommend one
   near that knee, not the largest one.

### The release gate, answered in part

The number **is** recoverable without going back to any old behaviour: raise `maxConcurrency`. What
is not resolved is the ~24% per-record cost at matched concurrency, which is a genuine regression
sitting underneath the recoverable part, and which nothing here has diagnosed yet.

### There is at least one more cliff

`0.5.2.8` measured ~2,500 against `0.5.2.0`'s ~10,300 - a further 4x drop - and `0.5.3.2` stays
there, while the current build recovers to ~16,500-19,300. So the curve is at least three events, and
none of them is diagnosed. Narrowing that one to a single patch release is the next measurement;
every intermediate tag (`0.5.2.1` ... `0.5.2.7`) exists, and the diff across the whole 0.5.2.0-0.5.2.8
range is 152 commits over 69 files, which is why narrowing has to come before reading.

Visible in that range and worth suspicion once narrowed, because they sit on the per-record path:
`ShardKey` (+90), `ShardManager` (+153), `ProcessingShard`, `PartitionState` (+518).

## The owner's original hypothesis, and where it still might apply

Recorded before any code was read, deliberately, so it could be refuted rather than fitted:

> More collections became thread-safe over the range, and there is more locking. The slow path could
> plausibly be reorganised so it does not need the thread-safe collection variants at all - in the
> same spirit as the actor IPC work, which isolated threads and state and pushed work onto message
> passing rather than shared state.

That is a specific, falsifiable claim with a named mechanism, and it predicts particular things: the
regression should track commits that introduced concurrent collection types or lock scopes on the
per-record path, and it should be roughly proportional to records processed rather than to
partitions or rebalances.

**It is now the leading explanation rather than a guess, and the measurement is what promoted it.**
The ~24% gap at *matched* in-flight concurrency is precisely the shape this hypothesis predicts:
identical concurrency, identical work requested, identical records - and more time spent per record.
No dispatch-accounting difference can produce that, because there is no accounting difference left
once both arms are observed holding 100 requests at once.

A competing hypothesis that has NOT been ruled out and must be, before any code is blamed: the two
arms are five years apart in every dependency, not just PC. The vanilla control covers the Kafka
client, but not Vert.x itself, whose own version moved across the range and which is doing the actual
HTTP work in both arms.

**The analysis waits on the bisect**, because the bisect narrows the diff to read from five years to
one release, and reading the wrong five years is the expensive mistake here - as the refuted
over-dispatch reading above demonstrates, at the cost of one wrong committed conclusion. It is then
done by reading the attributed diff directly - in session, not delegated (owner's instruction,
2026-08-20).

## Method note, learned the hard way

**Measure the thing, do not infer it from the diff.** The over-dispatch conclusion was reached by
reading `ExternalEngine` against the code it replaced, it was coherent, it explained the magnitude,
and it was wrong. What killed it was two lines of instrumentation in the stub counting concurrent
requests. Anything this analysis claims about what the engine *does* should be observable from
outside the engine before it is written down.

The actor work is relevant prior art either way - `improvements/lambda-actor-bus`,
`improvements/commit-command-actor`, `improvements/poller-bus-actor` and
`improvements/actor-scheduled` all exist as branches, and per
[`next-fork-branch-archaeology.md`](next-fork-branch-archaeology.md) none of them are referenced by
any document. If the diagnosis is shared-state contention, that is the body of work that was already
aimed at it.

## Related

- [`branch-classic-comparison-demo.md`](branch-classic-comparison-demo.md) - where this surfaced.
- [`next-perf-comparison-matrix.md`](next-perf-comparison-matrix.md) - owns measurement semantics and
  the blessed-numbers pipeline. A version-over-version regression check is a natural member of that
  matrix, and this harness is a candidate input to it.
- [`test-required-perf-lane-scope.md`](test-required-perf-lane-scope.md) - the existing perf lane is
  a required PR gate. It did not catch this, which is worth understanding on its own: whatever it
  measures, it is not this.
