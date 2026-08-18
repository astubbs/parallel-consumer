---
name: Parallel Consumer
last_updated: 2026-08-18
---

# Parallel Consumer Strategy

## Target problem

Teams processing Kafka records where concurrency is welded to partition count, and one slow
record blocks everything behind it in its partition. Adding partitions is often prohibitive and
still doesn't remove the head-of-line block - and a single-partition topic can't be sped up at
all. Share Groups decouple scaling from partitions but deliver out of order, so nothing gives
low latency and guaranteed per-key ordering at the same time.

And even where parallelism is unlocked, teams must fix concurrency and instance counts at
deploy time - quantities only the runtime data can answer. The guess is wrong in both
directions: too low silently leaves throughput on the table, too high floods downstream
systems - and it goes stale as the workload shifts.

## Our approach

We win by doing it in the client. Modifying the broker is extremely difficult, politically and
engineering-wise, and no broker-side answer to high-performance key ordering exists or may ever.
Parallel Consumer works like a client-side sub-broker: a library you add to a pom, invisible to the
cluster, needing no broker version, no feature flag, and nobody's permission to deploy.

The client is also where the ground truth lives: per-record timings, failures, key-level
ordering state. So the second half of the bet: the engine measures and decides at runtime what
configuration used to guess. External autoscalers see a black box and scale on consumption lag;
an engine inside the processing loop can tune itself - and tell infrastructure when more of it
would actually help.

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
- **Discovered concurrency vs sustainable ceiling** - does the engine find and hold its
  plateau? Regresses if the controller hunts, oscillates, or undershoots. Until the
  self-tuning controller ships, read as achieved fan-out vs configured max (partly derivable
  from `pc.shards` and `pc.inflight.records`).
- **Production deployments with a public story** - the lagging signal that the library is trusted
  in anger. Counted by hand.

## Tracks

### Performance

The main track. Minimum per-record latency and maximum concurrency, including the offset encoding
and buffering work that sets the ceiling.

_Why it serves the approach:_ The client-side bet only pays if the client is fast - a sub-broker
that adds latency has no reason to exist.

### Self-tuning

Priority raised 2026-08-18. The engine discovers its own concurrency from runtime measurement
and recommends its own instance count to infrastructure - including the signal integrity work
(accurate timing under every engine) that the controller stands on.

_Why it serves the approach:_ The client-side vantage is the moat here - per-record ground
truth no external controller can see. Paired with key-ordered concurrency, runtime-discovered
scaling is the capability nothing else in the ecosystem offers.

### Reliability

Bug squashing, with a bias to the correctness bugs: stalls, rebalance handling, offset tracking.

_Why it serves the approach:_ This bet asks users to trust a library with delivery semantics the
broker normally owns, and every lost-record bug is a withdrawal from that one account.

**How the bugs stay fixed: prefer an executable invariant to a written one.** The correctness bugs
here recur at the same seams - confluentinc#548 and confluentinc#857 are one defect three years
apart, both a rebalance callback waiting on the control thread, and both "fixed" with the rule
written into a document afterwards. A document fires when someone chooses to read it. So where an
invariant can be stated mechanically it becomes a check that runs: an ArchUnit rule, a gate, a probe
that can tell an internal counter from the truth. Where it cannot, that is worth knowing too, and
saying so beats pretending a paragraph will hold.

_Why it serves the approach:_ a claim about delivery semantics is only as good as what re-tests it,
and this project's own history is the evidence - a fix nobody could prove worked sat for four months
because the test written to prove it could not observe it.

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
