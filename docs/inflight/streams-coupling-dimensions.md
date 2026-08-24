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

## Proved

The host defines the topology; the engine calls the host per record (stateless); the engine calls
the host *with state* (a reducer); the host reads engine state (interactive queries); the host joins
two handles into one node, making the topology a graph rather than a chain; and three foreign
functions of three different shapes run in one topology at once.

## Ranked, most likely to falsify first

### 1. Re-entrancy - the host calling the engine while an invocation is open

**Predicted to fail, from reading the code, and not yet run.** The Python reader thread executes the
user's function inline in `_on_invocation`; `StreamsSession.get` then blocks on an event only that
same thread sets. So a function that queries a store from inside a mapper should deadlock until its
timeout, with the engine's stream thread blocked behind it the whole time.

It is not really about queries. The entire Processor API is re-entrant - `context.forward()`,
`context.commit()`, `context.schedule()`, any store read inside a processor - so if host-to-engine
calls cannot happen during an invocation, the wrapper is confined permanently to pure
value-transforming functions. The answer decides whether the wire stays request/response or has to
become properly multiplexed, and that is much cheaper to learn before a second binding exists than
after.

Costs nothing to build: everything needed already ships. **State the prediction before running it,
and report it whichever way it lands** - `docs/investigating.md` owns why.

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
