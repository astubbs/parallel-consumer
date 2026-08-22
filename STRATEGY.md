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

**And wrapping the client means DELIVERING Share Groups to non-JVM languages, not only competing
with them.** The paragraph above positions Parallel Consumer against Share Groups on per-record
overhead, and that comparison stands. But Share Groups land in the Java client first and librdkafka
follows later - which is the currency argument applied to the newest thing in Kafka. A Go, Python or
Rust team wanting Share Groups early would get them here before their own ecosystem has them, from
the same wrapper that carries everything else. The segment named above as "who will want Share
Groups first" is therefore a segment this fork can serve rather than argue with.

**Wrapping the core client APIs is a staged possibility, and 2026-08-22 removed its main
objection.** The sidecar already embeds a full Java Kafka client, so exposing consume, produce and
admin over the same protocol would give every language the reference client rather than a
reimplementation.

The reason not to start there was that *for a base client the per-record hop is proportionally
large*. **In the embedded configuration there is no hop** - the engine is linked into the calling
process and a frame crosses a function call. Proven in Go, Python, Node and C. So the ordering below
survives, but it now stands on scope and currency rather than on the hop, and the "C wins for
embedded and edge" line is no longer a reason to stay out of C: there is a C client, and it consumes
records.

Admin still goes first - pure request/response, low frequency, thinnest librdkafka wrappers, and
where currency bites hardest - then producer, and plain consumer last, being the API Parallel
Consumer exists to replace. Start with the simplest subset that works without much thinking, and
extend on evidence.

**The earmarked question - "Parallel Consumer for other languages" or "the Kafka client for other
languages" - is answered: the second.** The admin wrapper is still the cheapest first probe, but it
is now the first step of a direction rather than a test of whether to have one.

**Two configurations, one product, and they are not the same offer.**

| | Sidecar | Embedded |
|---|---|---|
| Deployment | a second process | nothing to operate |
| Per-record hop | a local socket | a function call |
| Artifact | one, portable | one per platform |
| Kafka bump | a version bump | a rebuild and re-release, automated |

**Both ship.** The sidecar stays: it is the portable answer, it is what a JVM-free team already
accepts, and it keeps a single artifact. Embedding is for the teams that will not run a second
process and for targets that cannot.

**On the rebuild-per-Kafka-bump cost:** an embedded engine has the Kafka client inside a
platform-specific binary, so currency stops being a dependency change for that artifact. **Owner's
call, and it is a build-pipeline problem rather than a strategic one** - a matrix build is
automated once and then nobody thinks about it again, and the sidecar still offers the pure
version-bump path for anyone who wants it. Recorded here because the objection is the obvious one to
raise and it should be visibly answered rather than absent.

### The whole client, including Kafka Streams

**The direction, stated as a direction and not a plan:** native wrappers for the entire Kafka client
surface - consumer, producer, admin, and **Kafka Streams** - delivered to every language through the
model the language proxy already proves.

**Streams is a better fit for this model than a plain consumer, which is counter-intuitive and is
the whole reason to write it down.** A Streams application is mostly *declarative*: the topology, the
joins, the aggregations, the windowing, the state stores and exactly-once are all engine-side and
never need to cross a boundary. The only thing that must cross is the user's per-record function -
and a per-record function crossing a boundary is precisely what the language proxy already does.

So the hard part of Streams-in-another-language is not the streaming; it is that a topology has no
portable description. That is an IDL to design, in the same class of work as the existing protocol,
rather than an unsolved problem.

**What would need real thought, listed so nobody rediscovers it as a surprise:** state stores the
host wants to read (interactive queries need protocol surface of their own), punctuators (fine - a
scheduled callback is just another kind of work frame in a pull model), and RocksDB under a native
image, which is its own reachability adventure.

**And the honest sequencing:** this is a long way past the current milestone. Admin first, on
evidence, exactly as above. Tracked as a proof-of-concept in
[`docs/inflight/next-kafka-streams-foreign-wrappers.md`](docs/inflight/next-kafka-streams-foreign-wrappers.md).

### Languages with no Kafka client at all

**The reach argument is not mainly about languages that have gRPC and would rather not use it.** It
is about the ones with nothing usable at any level - and that is a different, larger and more
interesting group.

- **R is the standout.** An enormous data-science population, no real Kafka client, and poor gRPC
  support - so both of our existing routes are closed to it and the FFI route is the only one.
- **Zig, Nim and Crystal** are growing and have nothing usable. They also have excellent C interop,
  which makes them the cheapest of this group to reach.
- **Julia** has a thin librdkafka wrapper and weak gRPC, so it is reachable today but badly served.
- **Lua** has nothing, and is the embedded story rather than the application story.
- **The enterprise tail - Delphi, Ada, COBOL** - where "nothing" is literal, and where a C ABI is the
  only integration mechanism anyone will accept.

**For all of them the base client is worth more than Parallel Consumer is**, because they need
`consume` before they need key-ordered concurrency. That is an argument *for* the
admin-then-producer-then-consumer ordering above rather than against it: the wrapper is the reach
mechanism, and Parallel Consumer is what makes the reach interesting once it exists.

C is the proof the group is reachable at all - it consumes records today, and C is what every one of
these runtimes binds through.

_Why it serves the approach:_ The client-side bet is that the queue belongs in the client. Nothing in
that argument is about the JVM - but every implementation of it has been, which is a limit of the
library rather than of the idea.

## Marketing

**One-liner:** Like a client-side sub-broker that can do backflips.

**Claim the feature set. Do not claim general speed.** The one performance claim worth making is
**key-ordered concurrency** - processing a partition's records in parallel while preserving per-key
order, beyond partition count. That capability does not exist in the librdkafka ecosystem in any
language, so there is nothing to be faster *than*; the comparison is against a serial consumer loop
or a hand-rolled worker pool.

**A general speed claim against librdkafka would be a bad trade even if it were true.** It invites a
benchmark on consumption throughput, memory and startup - our weakest axes, and none of them the
reason anyone would adopt this. What we have instead is the reference Java client's feature set and
currency, in a process with no JVM in it.

**The incumbent is not librdkafka.** It is the hand-rolled worker pool with manual offset tracking
that teams write *on top of* librdkafka - which is subtly wrong in most codebases and has no
maintainer. librdkafka sits underneath this picture, not opposite it.
