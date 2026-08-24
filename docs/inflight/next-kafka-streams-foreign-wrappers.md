# Next: Kafka Streams in other languages, through the language-proxy model

<!-- inflight-type: feature -->
<!-- inflight-impact: reach -->
<!-- inflight-labels: needs-design -->

**The proof of concept has now been run.** Adopted as a direction 2026-08-22; the PoC landed
2026-08-23 on `research/kafka-streams-foreign-wrappers` and **it works** - a Python program
describes a five-operator topology, supplies one per-record function, and the sink holds exactly
the right count for all 1000 keys. What follows is the original reasoning, with a
[record of what the run actually found](#what-the-poc-found) at the end. Productionising it is
still sequenced well after the admin wrapper, which goes first.

## The claim worth testing

**Kafka Streams fits the language-proxy model better than a plain consumer does.** That is
counter-intuitive enough to be the whole reason this note exists - the instinct is that Streams is
the *hardest* thing to expose across a boundary, and the structure says otherwise.

A Streams application is mostly **declarative**. The topology, the joins, the aggregations, the
windowing, the state stores, repartitioning and exactly-once are all engine-side, and none of them
needs to cross a boundary. The only thing that must cross is **the user's per-record function** - and
a per-record function crossing a boundary is exactly what
[`parallel-consumer-proxy`](../../parallel-consumer-proxy/) already does, over frames that four
languages have now driven.

So the hard part of Streams-in-another-language is not the streaming.

## The actual hard part: a topology has no portable description

`Topology.describe()` produces a human-readable summary, not a machine-readable definition, and
there is no serialisable builder format. **A topology IDL would have to be designed** - the same
class of work as the existing protocol, rather than an unsolved problem, but it is the bulk of the
work and it is design work rather than plumbing.

The shape to aim for: the host *describes* a topology declaratively and *supplies* the functions.
Stateless operators expressed in the IDL execute engine-side and never cross the boundary at all;
only user code does.

> **The PoC found a cheaper answer than an IDL, and this section's premise is the part it most
> changed.** See [What the PoC found](#what-the-poc-found). An IDL is a *description* of a topology
> that both sides must agree on and keep in step. Replaying the builder *calls* needs no such
> agreement: the host issues one message per builder method, the engine performs it against a real
> `StreamsBuilder` and returns a handle, and the host names that handle in the next call. The
> topology is never described - it is *built*, remotely, one call at a time. The wire carries no
> model of what a topology is, so there is nothing to keep in step.

## What needs thought before anyone starts

> Written before the PoC. **Several of these now have answers rather than guesses** - serdes,
> rebalancing and punctuators in particular - and where the run disagrees with the guess below, the
> run wins. See [What the PoC found](#what-the-poc-found). Kept as written because the guesses that
> were wrong are worth being able to see.

- **State stores the host wants to read.** Interactive queries need protocol surface of their own -
  a get/range/scan over a named store - which is new message types rather than a new mechanism.
- **Punctuators.** Fine, and worth saying so: a scheduled callback is just another kind of work frame
  in a pull model. It does not reintroduce the callback problem the pull model avoids.
- **RocksDB under a native image.** Its own reachability adventure, and the embedded configuration's
  problem rather than the sidecar's. See
  [`perf-native-image-sidecar-works.md`](perf-native-image-sidecar-works.md) for how that goes.
- **Serdes.** Streams is serde-heavy, but the host already owns serialisation in this model and the
  engine already sees bytes. Probably a non-issue; worth confirming rather than assuming.
- **Rebalancing and standby tasks** are considerably heavier than a consumer group's, and the
  liveness and reconnect semantics the proxy already defines were written for the lighter case.

## Why it is worth doing at all

There is no Kafka Streams for Go, Python, Rust or C - and unlike a consumer, nobody hand-rolls a
Streams equivalent, because the state and exactly-once machinery is too much to reimplement. So the
gap is not "a library someone could write badly"; it is a capability those ecosystems simply do not
have.

That makes it the strongest version of the feature-set argument in
[`STRATEGY.md`](../../STRATEGY.md): not a faster client, but a capability that does not otherwise
exist outside the JVM.

## What the PoC found

Run 2026-08-23. A Python program named a source topic, a value transform, a group-by-key, a count
and a sink; the engine assembled a real `StreamsBuilder` from those calls and ran it; every record
that reached the transform was handed back to Python and the stream thread blocked until Python
answered. All 1000 keys came out with exactly the right count. The command is
`demo/run.sh --streams --native`.

### Handles beat an IDL

The single most useful result. **No topology IDL was designed and none was needed.** Each builder
method is one message; the engine performs it against a live `StreamsBuilder` and returns an opaque
handle; the next call names that handle. Five methods covered the whole demo topology.

Why this matters beyond saving the design work: an IDL is a shared model that both sides must
implement and keep in step forever, and every Kafka Streams release that adds an operator becomes an
IDL change plus N client changes. Replaying calls has no shared model to drift. Adding an operator
is adding one message to a `oneof`, which is additive on the wire, and a client that does not know
about it simply never sends it.

**Corrected 2026-08-24: "beat" is too strong, and the comparison says why.** Handles beat an IDL
*for this goal*. Beam chose an IDL because a runner must **reason** about a pipeline - fuse stages,
optimise, and run the same pipeline on Flink or Dataflow. We only need to *build* one, so we can
take the cheaper option; but nothing about our wire is inspectable or portable, because there is no
model in it to inspect. The day cross-engine portability or engine-side optimisation matters, the
IDL comes back and this decision is revisited rather than defended. Note also that Beam does not
pass function references at all - it **pickles the DoFn into the pipeline proto** - so the token
registry here is not the Beam design either. See
[`next-architecture-landscape-comparison.md`](next-architecture-landscape-comparison.md).

**The kill criterion's first condition is met.** A sixth method taking a typed scalar argument
requires no wire redesign - `BuilderCall` is a `oneof` of per-method messages, so a new method is a
new member. The condition that would *not* be met is an argument that is itself behaviour: a serde,
a comparator, another function. Those need the same treatment as the per-record function - register
it, get a token, pass the token - and that pattern is proven, but it is a genuine design step rather
than a free extension.

### The boundary is the cost, and it is not the language

Measured on the demo machine, one stream thread: about **400us per round trip, of which the Python
function itself was 0.2us** - a rounding error, 0.05% of the crossing. A single-thread ceiling of
roughly **2,400 invocations/sec**.

Read it carefully in both directions. It says optimising the foreign function is pointless and only
the crossing is worth attacking, which is the argument for the shared C transport. It also says the
ceiling is per *stream thread*, and in-flight invocations are bounded by thread count, itself bounded
by partitions - so the aggregate scales with threads. JVM Kafka Streams is bounded the same way, so
that part is parity rather than a deficit; the per-invocation hop is what this design costs.

### A slow foreign function does not break the group - the prediction was wrong

Stated prominently because it was a confident prediction that the experiment refuted. The
expectation was that a transform slower than `max.poll.interval.ms` would hold the stream thread
past the interval and get the engine evicted. **It does not.** With a 300ms transform against a 5s
interval, the consumer group stayed `STABLE` with one member for all 78 samples; Kafka Streams
interleaves its polling and keeps its membership. The real symptom is throughput collapsing to about
three records a second and the run never finishing.

The consequence for the design is a mild relief and a new question. The relief: a slow foreign
function is not a liveness hazard the protocol has to defend against. The question: it fails as
*silence* - counts that never arrive - so a host needs some way to tell "still working" from "stuck",
and there is nothing on the wire that carries it.

### The protocol cannot say how the engine is doing

The demo wanted to assert its run was rebalance-free and **could not ask** - the protocol carries no
state, no rebalance signal, no lag, no task assignment. It samples the consumer group through the
Kafka admin API from outside instead, which is a real answer but an odd one: the whole premise is
that the host does no Kafka I/O, and here the host must open an admin connection to learn something
about its own engine.

What it would need: a server-initiated `EngineState` message on the existing stream, carrying the
Streams state enum and assignment changes as they happen. Additive, and it uses the mechanism the
`Invocation` message already established.

There is a trap worth writing down for whoever builds it. A rebalance is a **transient**: a
single-member group that gets evicted rejoins within a second and reads `STABLE` again. The demo's
first version sampled once at the end, pronounced the run rebalance-free, and would have said
precisely the same thing about a run that rebalanced twice in the middle. Any state signal has to be
event-driven or continuously sampled; a poll-at-the-end check is worse than none, because it
produces confident false assurance.

### Serdes are a non-issue except at the sink, where they are not

The note above guessed serdes would be a non-issue because the engine sees bytes. Mostly right: the
source, the transform and the grouping all ran `byte[]` end to end.

**ANSWERED 2026-08-24, and this section is kept because the reasoning still explains why.** Handles
now carry their type: `HandleAssigned` reports a kind plus key and value types, the engine picks the
sink serde from what it recorded, and the `Long` special case below is gone. The demo no longer
hard-codes an eight-byte read - it asks the handle. Plan:
[`../plans/2026-08-24-001-feat-streams-typed-handles-plan.md`](../plans/2026-08-24-001-feat-streams-typed-handles-plan.md).

The exception is where an operator *creates* a value the host did not supply. `count()` produces a
`Long`, so the sink has to write it with `Serdes.Long()` and the host has to know to decode eight
big-endian bytes on the way out. The engine currently special-cases it. That does not generalise:
aggregations, reduces and joins all mint typed values, and either the wire says what type a handle
carries or every one of them becomes another special case. That *was* the next real design
question; it is now closed.

**Answered 2026-08-24: the wire now says what a handle carries.** Every mint records its kind and
key/value types, `HandleAssigned` delivers them, the sink selects its serde from the record (the
special case is gone, and a type with no serde is refused by name rather than written as bytes),
and a Python handle decodes its own sink values - `handle.value_type.decode(raw)`. The design and
its decisions, including why the type is an enum and what a parameterised type would take, are in
`docs/plans/2026-08-24-001-feat-streams-typed-handles-plan.md`.

### Describing the topology turned out to be the cheapest real feature

Added as U9 after the PoC. One request, one answer, and the answer carries the text
`Topology.describe()` produces - which **every Kafka Streams topology visualiser parses**. A host in
a language with no Streams tooling and no way to grow any can now print that string and use all of
it. None of the Python-native stream processors has a `describe()`, so none of them has any of this,
which makes it the clearest single artifact of the wrap-rather-than-reimplement argument.

**Rendering it, and why we cannot simply embed one of those tools, is its own note:**
[`web-topology-rendering.md`](web-topology-rendering.md). Short version -
two of them declare no licence at all and one is GPL-3.0, so pointing users at them is free and
vendoring them is not.

### State durability across a restart - deferred on purpose, 2026-08-25

Restarting the engine and re-querying the table would show state surviving, from the changelog on an
in-memory store and from local disk on RocksDB. **Not being tested yet, and the reason is a
priority rather than a difficulty:** that path is Kafka Streams' own machinery, it is expected to
just work, and testing it proves something about Kafka rather than about this project.

**What is worth proving instead is each dimension of the COUPLING**, because that is the part
nobody has built before. So far: the host defines the topology, the engine calls the host per record
(stateless), the engine calls the host *with state* (a reducer), the host reads engine state
(interactive queries), the host joins two handles into one node (a joiner - the first non-linear
topology), and three foreign functions of three different shapes run in a single topology at once.
The remaining unproven dimension - the host learning the engine's own lifecycle state - is worth
more than durability right now.

Pick durability up when the coupling surface is broad enough that a restart test would exercise
something other than Kafka.

### Deferred capabilities, and what each would actually need

Each of these was deliberately out of scope. This is what the run says they would cost.

| Capability | What it needs |
|---|---|
| **A sixth builder method (scalar args)** | Nothing structural - one more member of the `BuilderCall` `oneof`. |
| **Operators taking behaviour** (serdes, comparators, joiners) | The function-token pattern again, generalised beyond `RecordFunction`. **Joiners done 2026-08-25**, and the design step turned out to be on the wire rather than in the token: with three shapes all arriving as two byte strings, which function to call could no longer be inferred from which fields were present, so `Invocation` now names its `kind` explicitly. Serdes and comparators remain. |
| **Interactive queries** | New message types for get/range/scan over a named store. New surface, not a new mechanism - and unlike the rest it makes the host a *reader* of engine state, which nothing else here does. |
| **Punctuators** | Cheap, as predicted. A scheduled callback is another work frame in the pull model; the invocation correlation already carries everything needed. |
| **More than one foreign operator** | **Done 2026-08-25.** Nothing on the wire was needed - tokens already distinguished functions, as predicted. A map, a join and a reduce now run in one topology, each calling a different Python function. The empirical question the prediction raised is still open: crossings *do* multiply with operator count (each record on the joined path crosses twice), and nothing has measured what that costs. |
| **Engine state and rebalance signals** | An `EngineState` server message. See above, including the transient trap. |
| **Invocation timeout and failure semantics** | Partly built: the registry times out and the mapper throws, and a Python exception reports back as an error rather than a substituted value. Undesigned is what Streams should *do* with it - fail the thread, skip the record, or route to a dead letter. That is a product decision, not a protocol one. |
| **Exactly-once** | Untouched. The invocation is a blocking call out of a transactional stream thread, and what a foreign function's side effects mean inside a transaction is a genuine open question rather than a plumbing task. |

### What this does not show

Worth restating so a green result is not over-read. One foreign operator, five builder methods, one
partition-bounded stream thread, at-least-once, no joins, no windows, no interactive queries, no
punctuators, in-memory state only. **Parity is the goal, not the result.** The PoC shows the model
works; it does not show the surface is covered.

## Sequencing, stated plainly

**This is a long way past the current milestone.** The admin wrapper goes first, then producer, and
the base consumer last. Streams is downstream of all of it, and downstream of the open items in
[`../plans/2026-08-22-001-feat-shared-c-transport-plan.md`](../plans/2026-08-22-001-feat-shared-c-transport-plan.md) -
the AOT-versus-AOT measurement, GC coexistence, and reachability beyond the happy path.

## Prior art

- [`../plans/2026-08-22-002-feat-kafka-streams-foreign-wrappers-plan.md`](../plans/2026-08-22-002-feat-kafka-streams-foreign-wrappers-plan.md) -
  the plan the PoC was built from, including the scope boundaries the findings above are measured
  against. The engine is [`parallel-consumer-proxy-streams`](../../parallel-consumer-proxy-streams/)
  and the client is the `streams` package of the Python client.
- [`perf-embedding-the-engine-over-ffi.md`](perf-embedding-the-engine-over-ffi.md) - the four
  bindings this would build on, and the hazards each surfaced.
- [`../plans/2026-08-22-001-feat-shared-c-transport-plan.md`](../plans/2026-08-22-001-feat-shared-c-transport-plan.md) -
  the shared C transport, its kill criterion, and CTD8 on why Node stays on the sidecar.
- [`STRATEGY.md`](../../STRATEGY.md) - *Other runtimes*, where the wrap-the-whole-client fork is
  taken and the ordering is set.
