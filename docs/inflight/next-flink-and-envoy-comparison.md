# Flink and Envoy, compared to Parallel Consumer

<!-- inflight-type: register -->
<!-- inflight-impact: coordination -->

Researched 2026-08-21. Two questions that come up repeatedly and deserve a settled answer:
**is Apache Flink a competitor?** and **could Envoy do this?** Short answers: Flink partly, in a way
that is more flattering to PC than expected; Envoy not at all, and not for a fixable reason.

## Flink - and the strongest external validation PC has

**The source is bound by partition count, exactly as a plain consumer is.** A Kafka partition is a
*split*, splits are assigned round-robin to source subtasks, and one partition goes to one reader.
With 10 partitions and source parallelism 20, ten subtasks read nothing. Flink's own docs warn it is
worse than merely wasteful: an idle subtask emits no watermarks and **holds back event time in every
downstream operator** until you add `withIdleness`. Flink's Kubernetes autoscaler caps source
parallelism at the partition count for this reason.

**After `keyBy`, parallelism genuinely is decoupled** - `murmurHash(key) % maxParallelism` gives a key
group, key groups map to subtasks. `maxParallelism` is the key-group count, defaults to roughly
`1.5x` parallelism (floor 128, ceiling 32768). Note the analogous trap: **it is frozen at first run
for a stateful job**, so it is the same class of irreversible up-front sizing decision as partition
count, moved from the broker into the job.

**But "each task is executed by one thread."** Flink decouples parallelism from partitions; it does
not give concurrency *inside* a subtask for synchronous code. So:

| Where the expensive work is | What Flink gives you |
|---|---|
| In or chained to the source | **Full PC-shaped problem, unmitigated** - ceiling is partition count, one thread each |
| After `keyBy`, synchronous | Partition ceiling gone, but **one thread per unit of I/O concurrency** - 500 concurrent HTTP calls costs 500 threads and the slots to hold them |
| After `keyBy`, Async I/O | Concurrency solved cheaply - **but per-key ordering is not available in DataStream** |

**The finding worth carrying into positioning.** Flink's `AsyncDataStream` offers only `orderedWait`
(global order, adds latency, head-of-line blocks the output) and `unorderedWait` (no order) - and
`keyBy` upstream does **not** make `unorderedWait` per-key ordered. Flink's own community wrote
**FLIP-519** to add exactly PC's semantic, describing the gap in PC's own vocabulary:

> *"the Flink system only supports record-level ordered output and unordered output for asynchronous
> lookup join **which cannot ensure the order of process under the same key**"*

It shipped in **Flink 2.1.0 (2025-07-31)** - **for Table/SQL async lookup joins only.** As of
2.4-SNAPSHOT there is still no `keyOrderedWait` in DataStream.

**That is external validation from the most credible possible source: the leading stream processor
identified PC's exact capability as a gap, and has only partly closed it.** It belongs in
`STRATEGY.md` and in the promotional material - stated positively, as what PC does.

**Two more asymmetries.** PC gives **per-key failure isolation** - a wedged key does not stop other
keys. Flink has no equivalent: a failure fails the job or failover region and rewinds to the last
checkpoint, replaying everything for all keys. And **Async I/O requires a genuinely async client** -
its docs explicitly warn against wrapping a blocking client in a thread pool. **PC takes any blocking
code**, which is a real adoption difference.

**Offsets: no equivalent to committing past incomplete offsets, and structurally so.** Flink's
authoritative offset lives in the *checkpoint*; what lands in `__consumer_offsets` is a monitoring
mirror written after a checkpoint completes - the docs say the source "does **NOT** rely on committed
offsets for fault tolerance". A checkpoint is a consistent cut of the whole pipeline, so there is no
per-record completion tracking and no way to express "100 and 102 are done, 101 is in flight". PC's
incomplete-offset encoding has no Flink counterpart.

**The honest framing: they are not competitors on one axis.** Flink is right when the job is stateful
stream processing that also needs I/O concurrency, and the price is a cluster - JobManager,
TaskManagers, a state backend, checkpoint storage, savepoint discipline, and a `maxParallelism`
decision baked in at first run. PC is right when the job is "consume Kafka, do an I/O-bound side
effect per record, keep per-key order" and the alternative is standing up that cluster. PC's costs are
real too: at-least-once only, no event time or windowing, and finite offset-tracking capacity.

## Envoy - no, and not for a fixable reason

**Envoy has two Kafka filters and neither is a consumer.** `kafka_broker` *decodes* the Kafka
protocol for statistics and allow/deny filtering - it never joins a group. `kafka_mesh` is a routing
facade that forwards Produce/Fetch to one of several clusters, **with librdkafka embedded to do the
real work**, and its documented limitations are decisive: Produce v2 only, and *"when requesting
consumer position, the response always contains offset = 0"* - it does not model offsets at all.

**`ext_proc` is the pattern llingr's relay resembles**: Envoy is the gRPC **client**, your processor
is the **server**, over a bidirectional stream, with the filter chain pausing at each step until the
processor replies. Real and widely deployed - which is evidence that "process this in your own
language over gRPC" is mainstream. But its unit of work is **one in-flight HTTP request, bounded by a
client connection**. No key, no ordering guarantee, no backlog, no retry ledger, no completion
tracking.

**Four independent reasons Envoy could not host a consumption engine:**

1. **No durable state** - no state backend, no persistence. Offset progress has nowhere to live.
2. **The threading model forbids the work** - worker threads run non-blocking event loops and must not
   block, so user business logic would have to be an out-of-process `ext_proc` server anyway, which is
   exactly the application PC would live inside.
3. **The unit of work is a client-initiated connection** and dies with it. Broker-driven, self-paced
   consumption of a durable log has nowhere to attach.
4. **No completion tracking** - retries are per-request policy within one request's lifetime.

**The accurate picture: Envoy sits downstream of PC in the request path** - Kafka → your app running
PC → HTTP call → Envoy sidecar → upstream service. Not a competitor, not a host.

*(One conceptual overlap worth naming because someone will raise it: Envoy's ring-hash and Maglev load
balancing give consistent key affinity to upstream hosts. But it routes requests as they arrive - it
owns no queue, persists nothing, and cannot resume.)*

## Related

- [`next-architecture-landscape-comparison.md`](next-architecture-landscape-comparison.md) - the
  standing comparison document; this belongs there when it merges.
- [`next-reclaim-the-category.md`](next-reclaim-the-category.md) - FLIP-519 is external validation of
  the category claim.
- [`branch-language-proxy.md`](branch-language-proxy.md) - `ext_proc` as evidence the sidecar pattern
  is mainstream, and as the counter-example on dial direction.
