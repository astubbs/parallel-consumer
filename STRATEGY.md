---
name: Parallel Consumer
last_updated: 2026-08-07
---

# Parallel Consumer Strategy

## Target problem

Teams processing Kafka records where concurrency is welded to partition count, and one slow
record blocks everything behind it in its partition. Adding partitions is often prohibitive and
still doesn't remove the head-of-line block - and a single-partition topic can't be sped up at
all. Share Groups decouple scaling from partitions but deliver out of order, *and* cannot
acknowledge inside a transaction - so nothing else gives low latency, guaranteed per-key
ordering, and exactly-once at the same time.

## Our approach

We win by doing it in the client. Modifying the broker is extremely difficult, politically and
engineering-wise, and no broker-side answer to high-performance key ordering exists or may ever.
Parallel Consumer works like a client-side sub-broker: a library you add to a pom, invisible to the
cluster, needing no broker version, no feature flag, and nobody's permission to deploy.

## Who it's for

**Primary:** Teams whose downstream - a service, or just a processing step - scales further
horizontally than their broker partitions do, and who need per-key ordering while it happens.
They're hiring Parallel Consumer to decouple how fast they process from how many partitions they
have, without giving up the ordering guarantee Kafka gave them.

## Key metrics

- **Head-of-line blocking avoided** - per partition, highest completed offset minus highest
  sequential succeeded offset: the records processed that vanilla Kafka would still be waiting
  on. Derivable today from two existing gauges; not emitted as its own meter.
- **End-to-end record latency, median and p99** - poll to completion, not just user function
  time. Not measured today - `pc.user.function.processing.time` covers only part of it.
- **Achieved fan-out vs configured max** - whether the concurrency asked for is the concurrency
  delivered. Partly derivable from `pc.shards` and `pc.inflight.records`.
- **Production deployments with a public story** - the lagging signal that the library is trusted
  in anger. Counted by hand.

## Tracks

### Performance

The main track. Minimum per-record latency and maximum concurrency, including the offset encoding
and buffering work that sets the ceiling.

_Why it serves the approach:_ The client-side bet only pays if the client is fast - a sub-broker
that adds latency has no reason to exist.

### Reliability

Bug squashing, with a bias to the correctness bugs: stalls, rebalance handling, offset tracking.

_Why it serves the approach:_ This bet asks users to trust a library with delivery semantics the
broker normally owns, and every lost-record bug is a withdrawal from that one account.

### Observability

Metrics that actually exist end-to-end, plus a web GUI to see inside a running PC.

_Why it serves the approach:_ Moving the queue into the client moves it out of the cluster's view
- PC's state lives in a JVM where standard Kafka tooling cannot reach it, so visibility is the
bill that comes with the choice.

### Flexibility

Let users process records how they want: richer batch modes, and candidates like an HTTP endpoint
server.

_Why it serves the approach:_ A broker has to be generic; a library living inside your
application does not. This is where the backflips are.

## Marketing

**One-liner:** Like a client-side sub-broker that can do backflips.

**Lead with the combination nothing else has: exactly-once, massively parallel, and optionally
key-ordered.**

Each half is unremarkable alone. Kafka has had exactly-once since KIP-98, and KIP-932 Share Groups
now scale consumers past the partition count. Having both at the same time is not available
anywhere else, and that is the line to put in talks, posts and the README's opening rather than
leaving it as a row two screens down a comparison table.

It holds because the broker-native answer to parallelism gives up exactly-once **by protocol, not by
omission**. [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka):

> "Although it is possible to read transactionally written records, the current protocol does not
> include the ability to acknowledge message delivery within an atomic transaction."

> "This means that the delivery behavior is at-least-once."

The mechanism: exactly-once processing needs the consumer's *offset* commit to join the producer's
transaction, and a share group has no offset to contribute - its state is per-record acknowledgement
state held broker-side, which nothing can enlist in a transaction. The KIP lists exactly-once only as
possible future work. Two details worth keeping straight when writing about this: isolation level is
a **group-level** setting (`share.isolation.level`), not per-consumer; and the delivery counts behind
poison-message protection are themselves not exactly-once, so the KIP says they "cannot be relied
upon to be precise".

### Verified - and the verification found two real defects on the way

**Say it exactly as loudly as it is verified.** This is a promise about delivery semantics, and the
README already warns that EoS does not prevent duplicate *replay*. An overstated headline here is the
kind of claim that costs trust rather than winning it.

The validation is `docs/plans/2026-08-07-001-test-transactional-eos-battle-test-plan.md`, which
enumerates every documented transactional guarantee and proves or refutes each one, with a negative
control required before any claim counts as proved. That gate has now fired against us, so this
section is written down as the finding rather than as an aspiration:

**Crash and replay, both batch sizes: the guarantee holds.** An abandoned transaction is invisible,
the replay commits results and their source offset as one set, and the output topic holds each result
exactly once. Proved with observed controls (`TransactionalCrashReplayIT`).

That took a real defect out of the path first, which is the part worth telling honestly. At
`batchSize >= 2` the consumer used to **stall outright** - the produce lock was taken once per poll
context but released per record, the failed release failed the whole batch, and because only a
*success* marks a partition dirty, no commit was ever attempted. The source offset froze at 3 of 201.
That was found by this suite before the fix landed, so astubbs#257 is not a fix we assumed works: the
same test went from RED 5/5 to GREEN 5/5 across it.

A second defect was found the same way and fixed in astubbs#261. When one send in a
`pollAndProduceMany` result set failed terminally, the records already accepted stayed in the
transaction and the next commit published them, so a `read_committed` consumer saw a **partial**
result set for one source offset - 2 of 5. `ProducerManager` installed a producer `Callback` that
throws from `onCompletion`, which pre-empted Kafka's own `maybeTransitionToErrorState` and left the
transaction un-abortable. Both affected claims - C7 `PRODUCE_MANY_ALL_OR_NONE` and C2
`ALL_OR_NONE_PER_SOURCE_OFFSET` - were `REFUTED` and now read `PROVED`.

**So the headline is defensible unqualified: exactly-once, massively parallel, optionally
key-ordered.** Every documented guarantee in the register is proved or attributed, none refuted.

Two things to keep honest when using it. The claim is about Kafka's own topics: the README's existing
warning that EoS does not prevent duplicate *replay* into external systems still stands, and this
work does not touch it. And the register - not this section - is the gate. If a claim is ever refuted
again, this section is the first thing to revisit, exactly as it was the first thing revisited when
one was.
