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

### Other runtimes

**An experiment.** One claim below has evidence behind it; the rest are things being tried. There is
no third category, and nothing here predicts that it works. The v1 framing is: Parallel Consumer in
other languages, plus some things we are trying.

Parallel Consumer runs in a sidecar that hands records to an application's worker processes over a
local RPC boundary, so runtimes that are not the JVM get key-ordered concurrency beyond partition
count. **The wrapper is the layer, and Java is the degenerate case of it** - one client model in every
language, with the Java case having one fewer hop underneath because it sits directly on the engine
with no protobuf between.

**The architectural claim, which is the part that is proven: our currency costs a version bump, and
librdkafka's costs a reimplementation.** That is structural and permanent, because the boundary sits
at the process edge rather than the language edge - every language reaches one Java client instead of
each reimplementing a protocol.

**And in the same breath, the qualifier: Parallel Consumer is not current with Kafka today.** The
architecture *can* be current; the product is not yet. Only the first is proven. Catching up is close
to a dependency bump for us, which is precisely the asymmetry the line above claims - but it has not
been done, and writing the claim without this qualifier would be claiming the experiment's outcome as
its premise.

**Who it might fit, as an observation rather than a sizing:** a possibility for users who need to be
more current than librdkafka is - the people who wanted KIP-848 early, or transactions when the C
client was years behind, or who will want Share Groups first. That segment skews sophisticated, and
sophisticated users are the ones most willing to run a sidecar, so the segment that needs the
advantage is also the one that tolerates its cost.

**Against Share Groups, narrowly:** acknowledgement here is local to the sidecar and commits are
batched, where Share Groups acknowledge per message to the broker, so per-record overhead should be
lower - at the cost of a sidecar process where Share Groups need none, and with poison-record
handling staying broker-side there.

**Wrapping the core client APIs is a staged possibility, not a plan.** The sidecar already embeds a
full Java Kafka client, so exposing consume, produce and admin over the same protocol would give every
language the reference client rather than a reimplementation. It is not where to start: for a base
client the per-record hop is proportionally large and C wins for embedded and edge, whereas for
higher-level functionality the hop is noise against processing time. If it is ever picked up, admin
goes first - pure request/response, low frequency, and where librdkafka wrappers are thinnest and
currency bites hardest - then producer, and plain consumer last or never, being the API Parallel
Consumer exists to replace. Start with the simplest subset that works without much thinking, and
extend on evidence.

**Earmarked, not adopted:** whether this is "Parallel Consumer for other languages" or "the Kafka
client for other languages" is a question worth returning to, and the admin wrapper is the cheapest
probe of whether the one-stop-shop framing actually pulls users.

_Why it serves the approach:_ The client-side bet is that the queue belongs in the client. Nothing in
that argument is about the JVM - but every implementation of it has been, which is a limit of the
library rather than of the idea.

## Marketing

**One-liner:** Like a client-side sub-broker that can do backflips.
