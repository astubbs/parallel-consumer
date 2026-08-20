# Throughput has regressed roughly 30% since 0.3.0.2, measured

<!-- inflight-type: bug -->
<!-- inflight-impact: throughput -->

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

## Next step, and the hypothesis to test it against

**Owner's hypothesis, recorded 2026-08-20, before any code analysis has been done** - stated in
advance deliberately, so it can be refuted rather than fitted afterwards:

> More collections became thread-safe over the range, and there is more locking. The slow path could
> plausibly be reorganised so it does not need the thread-safe collection variants at all - in the
> same spirit as the actor IPC work, which isolated threads and state and pushed work onto message
> passing rather than shared state.

That is a specific, falsifiable claim with a named mechanism, and it predicts particular things: the
regression should track commits that introduced concurrent collection types or lock scopes on the
per-record path, and it should be roughly proportional to records processed rather than to
partitions or rebalances. If the bisect lands the drop on a release with no such change, the
hypothesis is wrong and should be recorded as wrong.

**The analysis waits on the bisect**, because the bisect narrows the diff to read from five years to
one release, and reading the wrong five years is the expensive mistake here. It is then done by
reading the attributed diff directly - in session, not delegated (owner's instruction, 2026-08-20).

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
