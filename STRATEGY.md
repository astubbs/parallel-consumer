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

## How it is maintained

A claim about the fork, not the library - and one that decides whether anyone should adopt it.
Upstream is abandoned. A revived fork is only worth depending on if the revival is more durable than
the thing it replaced, and "an AI wrote a lot of code quickly" is a reason to trust it *less*, not
more.

**The bet: every failure is converted into a mechanism, so the fork's reliability compounds instead
of depending on whoever is paying attention.** A fixed bug that leaves no gate behind is a bug the
next person re-introduces. So the work is not finished when the tests pass; it is finished when
someone can name how it would have gone red had it been wrong.
[`docs/compound-engineering.md`](docs/compound-engineering.md) owns the loop and its worked chain;
[`docs/agent-harness.md`](docs/agent-harness.md) owns the layer that gives a rule teeth.

**The second half of the bet is about working memory.** Every codebase carries knowledge that is not
in the code - what was tried and abandoned, which test lies, which branch must not merge before
something else happens. A team absorbs that by osmosis; an agent arrives with the code and nothing
else, every session. So the fork treats that knowledge as an artefact rather than a culture:
`docs/inflight/` is a structured wiki of what is true about the code right now, delivered into an
agent's context at session start rather than waiting to be searched for. Whether that generalises
beyond this repo is untested - but a revived fork with no team is the case that most needs it.

What that looks like in practice, and how it would be falsified:

- **Defects arrive with a reproduction that is proven to fail without the fix**, not merely a test
  that passes with it. Falsified by a regression test that stays green when its fix is reverted -
  which has happened here and was caught by control arm.
- **A green check that asserts nothing is treated as an outage.** A mutation lane scoring zero
  mutants and a self-test suite printing `FAIL` while exiting `0` were both found and fixed;
  `misdirection` is the highest-ranked class of open work, above data loss, because everything else
  is measured through the instruments.
- **What was learned is written where the next agent will meet it**, not where someone would have to
  know to look. Falsified by a rediscovery - a problem solved twice because its write-up existed and
  was never opened.

The risk this carries, stated plainly: **volume is not evidence.** The same mechanisation that
produces a fix, its reproduction, its guard and its write-up in one sitting can produce four
plausible artefacts built on one wrong premise. The mitigations are the control arm, the negative
control, and a human who refuses the first confident answer - all three earned their place by
catching real errors, repeatedly.

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
portable description. ~~That is an IDL to design~~ - **and the proof of concept showed it is not.**
See below.

**And the honest sequencing:** this is a long way past the current milestone. Admin first, on
evidence, exactly as above. Tracked in
[`docs/inflight/next-kafka-streams-foreign-wrappers.md`](docs/inflight/next-kafka-streams-foreign-wrappers.md).

### One dependency, five capabilities, ten languages

**The packaging is the product, and it is a stronger claim than any single capability in it.**

A user installs one thing and imports one name:

```python
import parallel_consumer   # admin, consumer, producer, parallel consumer, streams
```

Each of those competes with something established. `confluent-kafka` already does admin, consumer
and producer well, and does not need replacing. **The bundle competes with nothing**, because the
last two entries do not exist for that user at any price. A Python team that wants topic
administration, key-ordered concurrency beyond partition count, and a stream topology cannot
assemble that today from anything - not because the pieces are expensive, but because two of them
are absent from the ecosystem.

**And the same surface, in every language.** The value compounds across the fan-out rather than
per-language: one mental model for a polyglot organisation, one set of documentation, one set of
examples that translate. That is an argument no single-language library can make, and it is
available to us only because every binding is a transport swap over identical frames
([`docs/language-bindings.md`](docs/language-bindings.md)).

**What this changes about how the work is described.** The transport is an implementation detail the
user should never have to know about - whether a call goes over a socket or a C ABI is our decision
to revisit, not their dependency. It follows that the unified surface should be designed before the
individual pieces are published, because a facade retrofitted over five separately-shipped libraries
is a different and worse thing than one designed as a whole. This is a reason to sequence the
packaging decision early even though the capabilities land over time.

**The honest counterweight, and it is real.** A single dependency that carries a JVM sidecar is a
heavier install than a library that does one thing, and a user who wants only a producer is being
handed an engine they will not run. The answer is probably that the pieces stay separately usable
underneath the unified surface rather than that the surface is abandoned - but that is a design
question, and "one import" must not become "one enormous artifact you cannot opt out of".

**For Java, this argument does not apply** and should not be made - a Java team already has all five
natively. See the version-decoupling case below for the one that does.

### What the Kafka Streams proof of concept settled

Run 2026-08-23. A Python program described a five-operator topology, supplied the per-record
function, and the counts came out exactly right. Four things it changed, three of which are
positioning rather than engineering.

**No IDL was needed, so the bulk of the cost above does not exist.** Replaying builder calls against
a live builder and handing back opaque handles gets the topology built with no shared model of what
a topology *is* - so nothing can drift as Kafka Streams adds operators, and a new operator is one
more member of a protobuf `oneof`. The caveat is real and belongs next to the claim: this buys
cheapness by giving up inspectability. Beam chose an IDL because a *runner* must reason about a
pipeline to fuse and optimise it and to run it on more than one engine. We only build, so we can
take the cheaper road; if cross-engine portability ever matters, the IDL returns.

**Nothing of the user's code crosses, and that beats the obvious comparison.** Beam serializes the
user function into the pipeline - a pickled DoFn travelling in the proto - because its harness may
run on another machine. That is the origin of a well-known Python tax: closures and lambdas that
will not pickle, and functions that may not hold a live resource. Our harness is the host process
itself, so we send a token and call the function where it already lives. **A user can capture
anything - a database handle, a loaded model, an open socket - because nothing is ever serialized.**
For the machine-learning audience in the section below, that is not a detail; a loaded model is
precisely the thing that cannot be pickled into a pipeline.

**The nearest shipped competitor is PyFlink, and the differentiator is that we need no cluster.**
PyFlink and PySpark are the same shape as this - a Python API over a JVM engine, Py4J for the
control plane - and both are attached to a cluster a platform team operates. **Kafka Streams is a
library: it runs inside the user's own process.** So "Kafka Streams from Python" means embedded,
stateful, exactly-once stream processing with no infrastructure to stand up, which PyFlink
structurally cannot offer. On the other side, the Python-native reimplementations - Quix Streams,
Faust, Bytewax - are embedded but have to rebuild state, windowing and exactly-once themselves.
Nothing occupies the intersection.

**The durability argument, which is the one to lead with.** The Python-native field has a record of
abandonment: Robinhood's Faust was abandoned and survives as a community fork, and Bytewax became
community-maintained when its backing company wound down. Reimplementing state, windowing and
exactly-once is more than a small team can sustain. **Wrapping the real engine means inheriting
Apache Kafka's maintenance instead of competing with it** - and it means inheriting its tooling too.
The proof of concept can print `Topology.describe()`, which every Kafka Streams visualiser parses,
so a Python user gets diagramming that no Python-native framework has or can easily grow.

**What still needs real thought, updated by the run:** typed handles, because an operator that mints
a value the host never supplied (a `count` produces a `Long`) currently has to be special-cased, and
every aggregation and join has that shape - this is the next real design question. Then engine state,
because the protocol cannot report a rebalance and the demo had to ask Kafka's admin API from
outside. Interactive queries need protocol surface of their own. Punctuators are confirmed cheap.
Exactly-once is genuinely open: what a foreign function's side effects mean inside a transaction is
not a plumbing question. RocksDB under a native image remains its own reachability adventure.

**One prediction was refuted and is worth remembering, because the instinct is strong and wrong:** a
foreign function slower than the poll interval was expected to get the engine evicted from its
group. It does not - Kafka Streams interleaves its polling and keeps its membership. Throughput
collapses instead. Slowness is a performance problem here, not a liveness hazard.

### The gap runs both ways, and the return direction may be the larger market

Everything above points one way: those ecosystems have no Kafka Streams, so give them Kafka Streams.
**The same mechanism points the other way, at a bigger and better-funded audience.**

Kafka Streams users are on the JVM. The JVM has no PyTorch, no scikit-learn, no transformers, no
spaCy, no pandas. Today a team that wants one of those inside a topology has two options: export the
model into something the JVM can run, which constrains what they can build, or stand up a service
and call it over the network from inside the topology - a second deployment, a second scaling story,
serialization twice, and hand-rolled retry semantics.

**Our constraint is already their status quo.** A Kafka Streams operator is synchronous, so an HTTP
call from inside one blocks the stream thread exactly as a call to a foreign function would, with
the same rebalance exposure when it runs long. Against what they do now, this is not a compromise
they must accept - it is the same blocking, minus the network hop and minus the second service.

**And it needs a strict subset of the work.** A JVM team writes its topology in Java, so there is no
topology description to carry and no handle protocol to design. All that crosses is the per-record
function - the piece the language proxy already carries in four languages. The direction that serves
non-JVM users needs everything; this one needs only the shared core.

**The honest counterweight:** model serving over the network buys things a co-located process gives
up - independent scaling, GPU pooling, model versioning and canarying. Teams already invested in a
serving platform are not the ones to sell this to. The teams to sell it to are the ones who stood up
a Flask service because they had no other way to get a Python function into a stream.

### The one Java case, and it is not the obvious one

Worth settling because it keeps being asked. A Java team already has all of this natively,
in-process, with no boundary - so a wrapper is *strictly worse* for them on every axis that first
comes to mind. Virtual threads are an argument for using Parallel Consumer directly, not through a
proxy; a boundary crossing is pure loss when you are already on the JVM.

**The case that does hold is version decoupling, and it is stronger than it first sounds.** The
sidecar runs its own JVM and its own Kafka client, so the client version stops being tied to the
application's runtime. An application pinned to an old JDK, or to an old client jar it cannot move
off without a dependency fight, reaches modern broker features by talking to a sidecar instead of by
upgrading itself.

Share groups (KIP-932) are the worked example, and the reasoning is easy to get backwards. It is
true that share groups need broker support - but they need a **new client** as well, and that is the
half an old application cannot supply. A sidecar supplies it. The same argument applies to every
future Kafka feature that lands client-side, which is what makes this a standing case rather than
one feature's workaround.

**Scope it honestly:** this is a migration and legacy-estate argument, not a reason for a greenfield
Java service to adopt a proxy. Do not sell it as one.

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
