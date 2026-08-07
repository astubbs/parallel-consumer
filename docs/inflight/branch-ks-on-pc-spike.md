# branch `feats/ks-on-pc-spike` - Kafka Streams driven by PC's WorkManager (astubbs#255)

A throwaway feasibility experiment, **finished**. No PR: the code is not for merge, and the deliverable
is the write-up.

- **Result:** [`docs/plans/2026-08-08-002-ks-on-pc-spike-result.md`](../plans/2026-08-08-002-ks-on-pc-spike-result.md)
- **Plan:** [`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md`](../plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md)
- **Origin analysis:** [`docs/plans/2026-08-07-002-investigate-kafka-streams-on-pc-report.md`](../plans/2026-08-07-002-investigate-kafka-streams-on-pc-report.md)

**Verdict in one line:** it works - PC's `WorkManager` selects records and a worker pool runs the Kafka
Streams processor chain, output matches stock for both a stateless topology and a non-windowed
aggregation, for 530 lines of patch across 4 `processor.internals` classes and no new PC API.

## Why this note exists rather than being deleted

The branch is done, but three things about it are cross-branch context that no command can answer.

### 1. Do not delete the branch without keeping `pcspike.patch`

`parallel-consumer-streams-spike/src/main/patch/pcspike.patch` is the entire change set and the answer
to "how little had to change". If the branch goes, that file is the only thing worth carrying forward -
everything else is scaffolding. It is pinned to `kafka-streams` **3.9.2** and will need re-deriving
against any other version; on Kafka trunk/4.x `ProcessorContextImpl` is `final` and the record context
is mutated in place, so it will need rethinking, not just rebasing.

### 2. The `junit-platform.properties` leak is a real bug on `master`, found here

`parallel-consumer-core`'s **tests** jar ships `junit-platform.properties` at its root with
`parallel.enabled=true` and a dynamic factor of 20. Every module depending on that jar silently
inherits 20x JUnit parallelism. Being fixed separately; recorded here because this branch is where it
surfaced and because anything else that depends on the core tests jar is affected today.

### 3. `dependency:unpack` restores original file timestamps - it can fabricate a control-arm result

Unpacked sources come back with the archive's timestamps, i.e. *older* than already-compiled classes,
so `maven-compiler-plugin` skips recompiling and keeps the previous build's `.class` files. A control
arm run **without `clean`** therefore tests stale classes and will confidently "confirm" a regression
that is not there. Always `clean`, and verify the compiled result with `javap` before believing any
before/after comparison. This applies to anything in this repo that unpacks sources, not just the spike.

## What is deliberately NOT done, if anyone picks this up

The result document's §8 has the full list. The three that decide whether this is ever more than an
experiment:

- **Offset commit is optimistic** - offsets stay on the stock Streams path and are written by workers in
  completion order, so the spike is not crash-safe. Origin report §4.6 is the fix.
- **There is no distribution shape.** Build-time patching works because `target/classes` precedes the
  `kafka-streams` jar on one module's classpath; nothing like that is available in someone else's
  application. Result document §7.3.
- **Caching must be off**, which changes DSL emission semantics for every topology moved onto the
  parallel path.

## The ready-made worklist

Running Apache Kafka's own `StreamTaskTest` with PC dispatch ON gives 68/101 (it is 101/101 with the
flag off, and 188/188 across all three Kafka test classes). The 33 failures cluster on
offset/commit accounting (11), buffering and pause/resume (5), stream-time punctuation (2), EOS commit
gates (3), close/suspend (5), error wrapping (3) and ordering (1). That is a quantified specification of
the semantic gap, written by Kafka's authors. Run it with:

```
./mvnw test -pl parallel-consumer-streams-spike -Pkafka-upstream-tests
```
