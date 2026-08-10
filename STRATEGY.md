---
name: Parallel Consumer
last_updated: 2026-08-10
---

# Parallel Consumer Strategy

## Target problem

Teams processing Kafka records where concurrency is welded to partition count, and one slow
record blocks everything behind it in its partition. Adding partitions is often prohibitive and
still doesn't remove the head-of-line block - and a single-partition topic can't be sped up at
all. Share Groups decouple scaling from partitions but deliver out of order, so nothing gives
low latency and guaranteed per-key ordering at the same time.

The same welding happens a level up, and nothing addresses it there. Kafka Streams and Kafka
Connect build their own execution model on top of the consumer, and each processes a task's records
one at a time - so a slow stage blocks its partition no matter what the consumer underneath can do.
Share Groups operate at the consumer layer and leave that untouched.

## Our approach

We win by doing it in the client. Modifying the broker is extremely difficult, politically and
engineering-wise, and no broker-side answer to high-performance key ordering exists or may ever.
Parallel Consumer works like a client-side sub-broker: a library you add to a pom, invisible to the
cluster, needing no broker version, no feature flag, and nobody's permission to deploy.

That bet has a second clause, added when the Kafka Streams work proved it: the same client-side
engine can run *underneath another framework*, not only inside your own application. Patching where
a framework hands records to its processing chain buys key-level concurrency for topologies and
connectors that never called Parallel Consumer at all - measured on Kafka Streams at 57x for the
record that would otherwise have been stuck behind a slow one, and 8x for the typical one.

## Who it's for

**Primary:** Teams whose downstream - a service, or just a processing step - scales further
horizontally than their broker partitions do, and who need per-key ordering while it happens.
They're hiring Parallel Consumer to decouple how fast they process from how many partitions they
have, without giving up the ordering guarantee Kafka gave them.

**Second:** Teams already on Kafka Streams or Kafka Connect with one slow stage - an enrichment
call, a lookup, an external write - who cannot add partitions to make it faster. They do not
describe themselves the way the primary persona does: they arrive with a topology or a sink
connector, not with a consumer loop, and they are not shopping for a consumer library at all. They
hire Parallel Consumer as the engine their existing framework runs on, and the thing they are
buying is that their code does not change.

*A second persona rather than a widened first one, decided here because the Streams work landed
first and the merge-trigger note asked the first arrival to settle it so the second inherits a
decision. Widening the primary sentence would have blurred it: "my downstream outscales my
partitions" and "my topology has one slow stage" are different self-descriptions, reached from
different starting points, and a single sentence covering both describes neither.*

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
