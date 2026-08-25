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

## CORRECTION: it is not only I/O-bound topologies. That claim was wrong.

The section above said a CPU-bound or local-state-only topology is "already at the machine's limit"
and gains nothing. **That assumed partition count is at least core count, and stated it as though it
were general.** It is frequently false.

**Streams' parallelism is partition count, and that has nothing to do with what the work is.** A
CPU-bound topology on a 12-partition topic running on a 64-core machine uses **12 cores**. The other
52 idle, and no tuning fixes it, because Streams cannot put two threads on one task. Partition counts
are chosen for throughput planning, ordering granularity or history - routinely *below* the core count
of modern hardware.

So PC helps whenever **partition count is less than available parallelism**. That is a far larger set
than "I/O-bound". Local-state topologies are the same argument: RocksDB access is CPU and disk work,
and twelve partitions give twelve threads doing it.

**The workload only decides the SIZE of the win:**

| Workload | Bounded by | Win |
|---|---|---|
| CPU-bound / local state | cores / partitions | 64 cores over 12 partitions - **about 5x** |
| I/O-bound | latency x target throughput | **150x and up** |

Five times is not "nothing". It is the difference between one machine and five.

## If the state store, stream time and EOS problems are solved

**It stops being a PC feature and becomes a new execution model for Kafka Streams**, and the framing
of this whole project changes with it.

The pitch today is *leave Streams, use PC* - which asks a user to abandon a topology, an operational
model and a body of institutional knowledge. If 1-3 are solved the pitch becomes **keep your topology,
get concurrency decoupled from partitions**. PC stops competing with Streams and becomes the engine
underneath it, which is a far easier sell and a much larger addressable set.

**It also removes the most-cited operational constraint in Kafka Streams.** Over-partitioning to buy
parallelism is the standard workaround, and it costs rebalance time, broker metadata, per-partition
memory and file handles - continuously, and unfixably by tuning. **Removing the reason to
over-partition is worth more than any throughput figure**, because it is a cost people pay forever
rather than a number they compare once.

**What would still be left, and it is engineering rather than conceptual**: standby replicas and
restoration, interactive queries against a concurrently-written store, suppression and caching
semantics, task assignment, and the packaging question already parked.

**And #1 is doing most of the work in "assume I solve it".** `KEY` ordering serialises per key, which
maps cleanly onto per-key state - but a Streams store is per-**task**, and a task spans many keys.
Making that concurrent is either fine-grained locking inside the store access path or sharding the
store by key. Whichever it is, **it decides whether the other three matter**, and it is answerable on
paper before another engine number is taken.

## THE REAL ARGUMENT: head-of-line blocking, which is not about I/O at all

**Antony, again, and this supersedes both framings above.** The sections before this argued about
*throughput* - first that only I/O-bound topologies benefit, then that it is really about partitions
versus cores. Both are true and both are secondary. **The argument that actually matters is latency.**

**One thread per task means records in a partition are processed strictly serially.** So any record
that takes longer than usual adds its full duration to the latency of **every record behind it**,
immediately, and keeps it there until the backlog drains.

**And "takes longer than usual" has nothing to do with I/O.** A non-exhaustive list of things that
stall a Streams thread with no external call in sight:

- a GC pause on that thread
- RocksDB compaction, or a write stall - **a purely local-state topology, from the inside**
- lock contention in a store
- an unusually large message, or a pathological payload to deserialise
- a rare expensive branch - a regex, a big aggregation, a range scan
- a punctuator, which runs on the same thread as the records
- page faults, a cold cache, the OS simply descheduling the thread

**The waste is structural, and this is the part worth saying out loud.** A partition is a *mixture of
keys*. A slow record for key A delays keys B, C and D, which have no semantic relationship to it.
**Streams blocks on PARTITION; the actual ordering requirement is per KEY.** Everything in the gap
between those two is blocking that buys nothing.

**This holds even when partitions exceed cores and the work is uniform and CPU-bound**, which is
exactly the case both earlier sections conceded. There is no configuration of stock Streams in which
it does not hold, because it is a property of the execution model rather than of the workload.

### It also invalidates how this project has been benchmarking

**Every figure in `perf-engine-comparison-2026-08-22.md` uses a UNIFORM per-record delay.** Uniform
work is the best possible case for a serial model: with no variance there is no head of line to block,
so a benchmark built this way measures the one thing that flatters the design PC is arguing against,
and reports nothing about the thing that actually hurts users.

**What to measure instead**: a handler whose duration has a realistic tail - say a p99 at 50-100x the
median - and report **latency percentiles, not just throughput**. Under a serial model p99 input
latency degrades far worse than linearly with the tail, because every slow record's cost is inherited
by its whole queue. Under per-key concurrency it stays close to the handler's own distribution.

That is the measurement that would make the case, and this project does not have it. See
[`next-benchmark-a-model-of-work-not-work.md`](next-benchmark-a-model-of-work-not-work.md), which
already records the ask, and note that **it applies to the PC-versus-Streams case even more sharply
than to the engine comparison it was written for.**

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

