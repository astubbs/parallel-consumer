# Dimensions of the Kafka Streams foreign coupling, ranked by what would break it

<!-- inflight-type: register -->
<!-- inflight-impact: blind-spot -->

A **register**, not a task list: it is read to decide what to prove next, and it has no done state -
each dimension either gets proved and moves to the top section, or gets falsified and reshapes the
design. astubbs#242's PoC is trying to break its own premise, so the ordering here is **by how
likely a dimension is to falsify the approach**, not by how much work it is.

The premise under test: a language with no Kafka Streams implementation can describe and run a real
one by naming builder calls over a wire, with its own functions called back per record. The engine
is shared, so the per-language surface is thin - which is the whole bet. A dimension that forces
per-language engine work is a dimension that breaks the bet.

[`next-kafka-streams-foreign-wrappers.md`](next-kafka-streams-foreign-wrappers.md) owns the PoC's
findings and its deferred-capability table; what is here is only the ranking of what to attack next.
EOS is deliberately absent: it is optional in Kafka Streams, and a release that never supports it is
still a release.

How each proved dimension gets tested in every binding rather than only in Python is a separate
question with its own note -
[`test-cross-binding-streams-conformance.md`](test-cross-binding-streams-conformance.md) **owns
it**.

## Proved

The host defines the topology; the engine calls the host per record (stateless); the engine calls
the host *with state* (a reducer); the host reads engine state (interactive queries); the host joins
two handles into one node, making the topology a graph rather than a chain; and three foreign
functions of three different shapes run in one topology at once.

## Ranked, most likely to falsify first

Dimension 1 has now run and did **not** falsify; it is kept in place, with its result, rather than
moved, because the register's value is partly the record of what was predicted and how it came
out.

### 1. Re-entrancy - RUN 2026-08-25: the hang is real, and it does NOT falsify the approach

**The prediction was confirmed exactly, and the conclusion drawn from it was wrong.** Recorded here
in full because the register said the opposite, and a reader who found only the corrected text would
have no way to tell that the reasoning had been overturned rather than never written.

**What the prediction said:** a host function calling the engine mid-invocation deadlocks, because
the Python reader thread runs the user's function inline and then waits on an event only that same
thread sets.

**What ran:** `get(timeout=1.5)` from inside a mapper blocked 1.505s and raised; the invocation was
answered at 1.507s. The query *was sent* and the engine answered it instantly - the answer was
structurally undeliverable. A control arm changing exactly one term (dispatch the user function to
a worker thread, source untouched) flips the outcome to a correct answer in 0.0s, so inline
execution on the reader thread is the cause and not a correlate. It is the whole class, not a `get`
quirk: `describe` and builder calls hang identically. Only the non-waiting crossings - `register`,
`start`, `close` - are safe from inside a function.

**Why the falsification inference was wrong.** The register argued that if host-to-engine calls
cannot happen during an invocation, the wrapper is confined permanently to pure value-transforming
functions. The premise turns out to be false: **the wire is already multiplexed and needs no
change.** On the Java side `transmitLock` guards only outbound sends and is released before
blocking, and `onGet` runs on the gRPC transport thread - so the engine genuinely serves a query
while every stream thread is blocked, which is why the answer existed to be abandoned. The
deadlock is purely client-local, in one design decision in one Python file.

**So this is a defect, not a limit of the approach**, and the Processor API is not ruled out. The
fix has an order that matters: **correlate `Get`/`Describe` first** - see
[`bug-streams-queries-share-one-answer-slot.md`](bug-streams-queries-share-one-answer-slot.md),
which is needed regardless of re-entrancy - and only then move user functions off the reader
thread. Doing them the other way round makes things worse, because more concurrent queries hitting
one answer slot is exactly the failure the correlation fixes.

Characterisation tests are in the tree pinning current behaviour
(`parallel-consumer-proxy-clients/parallel-consumer-proxy-client-python/tests/test_streams_reentrancy.py`);
they are to be **inverted, not deleted**, when the fix lands.

**Two JVM-side hazards the run turned up, neither a deadlock.** gRPC serialises a single stream's
inbound callbacks, so `onGet` performing a live store read delays `InvocationResult` delivery for
every in-flight invocation - a head-of-line coupling that will present as random latency. And there
is no flow control: no `isReady` or `setOnReadyHandler` anywhere in the module, so a host that
stops reading lets the engine's outbound buffer grow while stream threads keep emitting. One thing
the run could not settle by reading, and which needs a JVM experiment: whether a Kafka Streams
store lock can be held across a blocked stream thread.

### 2. One record in, many out - and the missing answer states

`InvocationResult` carries a single `value`. `flatMapValues`, `flatMap` and `branch` are core DSL,
and `filter` needs *no output* as an answer distinct from both a value and an error. A tombstone -
a legitimate null value - is a fourth state, and null, empty and absent are currently conflated.

Cheap on the wire (a repeated field, an explicit answer kind), but the correlation model's unstated
"exactly one answer per invocation" assumption has never been tested.

### 3. Record metadata - headers, timestamp, topic/partition/offset

`Invocation` carries key, value, aggregate and right, and nothing else. A topology that branches on
a header or on event time cannot be described at all.

Additive, so no structural risk - which makes it a **release blocker rather than a research
question**. [`next-serialization-and-record-metadata.md`](next-serialization-and-record-metadata.md)
already argues exactly this for the frozen proxy wire; the streams wire has the identical hole and
is still free to change.

### 4. Windowing

The one most likely to force a wire *redesign* rather than an addition. A windowed key is composite
- key plus window bounds - and `DataType` is a flat enum of bytes and long, so the type model has no
way to say what a windowed handle carries. Suppression adds buffering the host cannot see.

Unlike EOS, windowed aggregation is not optional in practice.

### 5. Rebalance with an invocation in flight

A stream thread blocked inside the host when its task is revoked. The demo's slow-function arm
covers `max.poll.interval.ms` expiry; revocation *during* an open invocation does not, and the
aggregate a host is mid-computation on may belong to a task it no longer owns.

## Prior art worth reading before starting any of these

The `feats/ks-*` branch family is astubbs#255 - Kafka Streams running *on* Parallel Consumer, a
different project from this wrapper. One of its artifacts is directly useful here anyway:
`feats/ks-streams-refuse-unsupported-surface` marks 59 Kafka Streams DSL overloads as unsupported,
which is a ready-made map of the surface this wrapper has not covered and a measure of how wide
"just implement the DSL" actually is.
