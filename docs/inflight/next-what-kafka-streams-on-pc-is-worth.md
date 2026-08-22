# What Kafka Streams on PC is worth, given the engine numbers of 2026-08-22

<!-- inflight-type: next -->
<!-- inflight-impact: architecture -->

**Antony's question**: given the engine work, how much impact would getting `pc-engine-KafkaStreams`
working actually make? Signpost to the workstream itself is
[`branch-ks-streams-workstream.md`](branch-ks-streams-workstream.md); this is only the value case.

## The arithmetic, and it is not subtle

Kafka Streams' concurrency is **one task per partition, at most one thread per task**. So a topology's
parallelism is its partition count, full stop - regardless of what each record actually does.

For an **I/O-bound** topology - an enrichment calling a database or a REST service, ~100ms per record -
on a 12-partition topic:

| | Concurrent records | Throughput |
|---|---:|---:|
| Stock Kafka Streams, 12 partitions | 12 | **120 rec/s** |
| PC engine at concurrency 5,000 | 5,000 | **18,376 msg/s** (measured today) |
| PC engine at concurrency 20,000 | 20,000 | **24,862 msg/s** (measured today) |

**That is roughly 150-200x**, and the partition count stops being the ceiling.

## Today's work changed the size of the prize, which is the part that is new

Before virtual threads, PC could not hold its own configured concurrency: the shipped engine reaches
**2,950 of a configured 5,000** in flight. So the KS-on-PC win was capped at a few thousand no matter
what was asked for.

`core-dpvt` now holds **40,000 of 40,000**. The applicable range widened by more than an order of
magnitude, and the interesting bound moved off PC entirely - throughput plateaus at ~25,000 msg/s from
concurrency 20,000 upward, which is **something upstream of the engine** and not yet identified.

## Where it is worth nothing, and this has to be said first in any pitch

**Only I/O-bound topologies benefit.** A CPU-bound or purely local-state topology is already at the
machine's limit; PC adds coordination overhead and takes throughput away. The win is entirely "the
thread was waiting on something that was not this JVM".

That is narrow, and it is also **exactly where Streams users hit a wall and start adding partitions** -
a workaround with real costs in rebalance time, broker metadata and per-partition memory. The pitch
that follows is a single sentence: **you do not need 500 partitions to get 500-way concurrency.**

## The four things that decide whether it can be built, none of which are throughput

1. **State stores.** Streams state is per-task and single-threaded. Concurrent processing of one
   partition means concurrent store access. PC's `KEY` ordering serialises per key, which maps onto
   per-key state - but RocksDB access itself still has to be made concurrent or sharded. **This is the
   hard one.**
2. **Stream time and windowing.** Out-of-order completion breaks stream-time advancement, which drives
   windowing and punctuation. `feats/ks-streams-stream-time-lowwater` exists for this.
3. **Exactly-once.** Streams EOS is producer-per-task transactional, and `ExternalEngine` *rejects*
   transactional commit mode outright. Unsolved for the async engines - see
   `next-exactly-once-for-async-engines.md`.
4. **Packaging** - a patched Streams jar and stock Kafka Streams on one classpath. Recorded as a
   coordinates problem rather than a fork problem, and parked.

## Verdict

**The largest single multiplier this project has**, and the only one that changes what a user can build
rather than how fast an existing thing runs - but its blockers are semantic, not performance, and none
of them were touched today. **The engine work raised the ceiling of a prize nobody can yet collect.**

Which is an argument for sequencing, not for enthusiasm: the state-store question decides whether any
of this is buildable, and it is answerable on paper before another engine number is taken.

