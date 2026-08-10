# Kafka Connect on Parallel Consumer

**EXPERIMENTAL — LOCAL ONLY, NOT PUBLISHED. Nothing here delivers records yet.**

The goal: run an **unmodified** Kafka Connect sink connector on Parallel Consumer's engine, so a topology
whose slow stage is blocked behind one record on its partition gets key-level concurrency without its code
changing. Connect gives a task whole partitions and processes their records one at a time, which welds
throughput to partition count; sharding by key instead is what breaks that weld.

This module is the feasibility work for that, and it is early. Read [What does not work](#what-does-not-work-yet)
before forming an impression of what it is.

## What is proven so far

**A generated, patched `WorkerSinkTask` wins class loading over the released jar.** The module unpacks
Kafka Connect 3.9.2's published sources at build time, applies a two-line tracked patch, and compiles the
result into `target/classes` so it precedes the jar on the classpath. No Apache source is committed —
only `src/main/patch/pcconnect.patch`.

The evidence is Kafka's own test suite, not ours: `WorkerSinkTaskTest` runs **twice**, in isolated
surefire forks — a stock arm whose project classes directory is empty, and a patched arm loading the
generated class — and a checked manifest of the exact test identities means a zero-discovery run cannot
pass green. `PatchHarnessTest` additionally pins the patch to exactly two additive hunks and rejects any
addition touching `poll`, `convertMessages`, `deliverMessages`, `commitOffsets`, `preCommit` or
`rebalance`, so "no regression" is bounded to the reviewed delta rather than to a passing suite.

**Several sink tasks can share a partition without ever running concurrently for one key.**
`PcSinkTaskLaneRouter` maps each record to a lane using Parallel Consumer's own `ShardKey`, and
`PcSinkTaskLane` holds a lock across the whole `SinkTask.put()` call. The seriality claim is backed by a
negative control that must fail the interesting way: the same load with the lock bypassed *does* observe
concurrent entry. Without that, a detector that never trips would be indistinguishable from one that
cannot.

## What does not work yet

**There is no delivery path at all.** The patch's bridge is hard-disabled — no property, no setter, no
alternate implementation can enable it. Nothing polls, converts, delivers, rebalances, or commits through
Parallel Consumer. `PcConnectDispatchBridge.enabled()` returns a hard-coded `false`, and it is a *method
call* rather than a constant on purpose: a `static final boolean` constant would be inlined by javac and
the runtime linkage this proves would silently not exist.

Specifically absent, and each is a design step rather than a coding task:

- **Offset commit composition.** `SinkTask.preCommit()` returns a per-task watermark; Parallel Consumer
  tracks sparse completion and commits a frontier. Reconciling those is the hard part and is unsolved
  here. Nothing in this module calls `preCommit`, `flush`, or an offset committer, and **completion means
  the callback returned — which is not a durability claim.**
- **Real connector lifecycle.** No connector is instantiated, started or stopped; the tests drive fake
  `SinkTask`s directly.
- **Real record conversion.** No Connect `Converter` or `HeaderConverter` is used.
- **Rebalance.** `open()` / `close()` are not driven from partition assignment.

Not yet wired, but note the distinction: SMTs, `errors.tolerance` / DLQ, and `ConfigProvider` are
Connect's own features and remain in the runtime being patched. They are untested through this path, not
excluded from it.

## The limit worth knowing before you get excited

Concurrency above the partition count is only available to **key-affine** sinks — ones whose write is
idempotent per key, like a JDBC upsert or an Elasticsearch index by document id. A **partition-affine**
sink, such as S3 or HDFS, derives its output identity from the partition, so splitting a partition across
lanes would collide; those must run partition-affine, where the ceiling is the partition count, which is
what `tasks.max` already gives you.

So the compatibility table is not a footnote. It is the list of connectors this work can help.
See [`connector-compatibility.md`](connector-compatibility.md) — **every row there is currently a
prediction, not a test result.**

## Running it

```bash
./mvnw -pl parallel-consumer-connect -am test
```

The `-am` is required. Without it the reactor's `reactorModuleConvergence` rule fails, the module never
recompiles, and you get a silent false negative.

Note that `-Dtest=` does not work on this module: it applies globally, and the stock regression arm runs
deliberately with an empty classes directory, so a global filter makes it fail to discover the class it
was told to find.

## Why it does not publish

Both this module and `parallel-consumer-streams` produce jars containing compiled, **modified** Apache
Kafka classes. Building them locally distributes nothing; publishing one — including a snapshot on a
routine merge — is redistribution of a modified Apache Kafka, and the trademark, `NOTICE` and Apache 2.0
section 4(b) obligations attach at that moment.

Publication is therefore disabled deliberately, in two halves per module, and must not be re-enabled as a
side effect. See [`docs/inflight/release-experimental-modules-publication-disabled.md`](../docs/inflight/release-experimental-modules-publication-disabled.md)
and [`docs/inflight/next-patched-kafka-packaging.md`](../docs/inflight/next-patched-kafka-packaging.md).

## Where the thinking lives

- [`docs/plans/2026-08-09-001-feat-connect-on-pc-plan.md`](../docs/plans/2026-08-09-001-feat-connect-on-pc-plan.md) — the plan, including the MVP definition and why partition-affine mode delivers none of the strategic value
- [`docs/inflight/pr-connect-on-pc.md`](../docs/inflight/pr-connect-on-pc.md) — current state and the next design step
- [`docs/plans/2026-08-08-001-feat-connect-sink-in-pc-plan.md`](../docs/plans/2026-08-08-001-feat-connect-sink-in-pc-plan.md) — the **superseded** embed direction, kept because its offset analysis transfers
- Issue [astubbs/parallel-consumer#240](https://github.com/astubbs/parallel-consumer/issues/240), mirroring [confluentinc/parallel-consumer#119](https://github.com/confluentinc/parallel-consumer/issues/119)
