---
title: Kafka Streams for Non-JVM Languages - Plan
type: feat
date: 2026-08-22
topic: kafka-streams-foreign-wrappers
artifact_contract: ce-unified-plan/v1
artifact_readiness: requirements-only
product_contract_source: ce-brainstorm
execution: code
---

# Kafka Streams for Non-JVM Languages - Plan

Tracking issue: astubbs#242 (the language-proxy fan-out). Branch `research/kafka-streams-foreign-wrappers`.

## Goal Capsule

**Objective.** Prove that a non-JVM program can define and run a stateful Kafka Streams topology through the existing language-proxy model, and surface what the protocol must gain to support it.

**Product authority.** This is a feasibility proof, not a shippable capability. Shipping order is settled elsewhere and unchanged: the admin wrapper first, then producer, then plain consumer, with Streams downstream of all of it. Nothing here moves that queue.

**Open blockers.** None. The question that blocked planning - how general the handle protocol must be - is settled in Key Decisions.

---

## Product Contract

### Summary

A Python program describes a stateful Kafka Streams topology by replaying builder calls over the proxy protocol, registers one per-record function, and gets correct per-key counts out of the sink topic. The engine runs real Kafka Streams in the sidecar and owns all state.

### Problem Frame

Kafka Streams does not exist outside the JVM, and unlike a consumer nobody hand-rolls a substitute - the state, changelog and exactly-once machinery is too much to reimplement. So the gap in Go, Python, Rust and C is not a library someone could write badly; it is a capability those ecosystems do not have at all.

The demand is first-hand rather than inferred. While consulting for Confluent, the owner was asked repeatedly whether Kafka Streams would be available in other languages, and had to answer no every time. That is a recurring question from paying customers, from the people best placed to want it.

What changed is the boundary. The language proxy already carries a per-record function across a process edge over protobuf frames, proven in four languages. A Streams application is mostly declarative - topology, joins, aggregations, windowing, state stores, repartitioning and exactly-once all execute engine-side and never cross anything. The only thing that must cross is the user's function, which is the problem already solved.

### Key Decisions

- **Replay the builder calls; do not insulate the Kafka Streams API.** (session-settled: user-directed - chosen over a semantic topology description: exposing the API as simply as possible is the goal, and coupling the wire to that API is accepted rather than mitigated.) Governs R1.
- **A reference token crosses the boundary, never a callable.** The engine names which function it wants; the host calls its own function. Nothing re-enters the foreign runtime on a foreign thread. Governs R2.
- **The foreign function sits beside engine-side state, not inside the aggregator.** (session-settled: user-directed - chosen over a foreign aggregator: keeps the boundary out of a state store's read-modify-write.) Governs R3, R4.
- **Sidecar, not embedded.** The claim under test is that Streams fits the language-proxy model, and the sidecar is that model; going embedded would drag in RocksDB under GraalVM for no gain on the question asked.
- **Feature parity is the goal, not speed.** (session-settled: user-directed - it does not need to beat JVM Kafka Streams; it needs to exist.) Governs R8.
- **Real handles, five methods.** (session-settled: user-directed - chosen over both a hardcoded chain and a general remote-builder protocol: handles prove the mechanism generalises, while a fixed method set keeps the argument type system out of the PoC. A sixth method becomes a known increment rather than an unknown.) Governs R1.
- **A new message pair, not another arm on `Report`.** A returned value is not an outcome, and invocations need request/response correlation where offsets need identity. Governs R6.

### Actors

- A1. The Python application - describes the topology, registers and runs its own functions, owns serialization.
- A2. The sidecar engine - builds and runs the Kafka Streams application, owns the state store, its changelog and its commits.
- A3. Kafka - the source topic, the changelog topic and the sink topic.

### Requirements

**Describing the topology**

- R1. Python describes the topology by issuing builder calls over the protocol, receiving an opaque handle for each call and naming prior handles as arguments to later ones. The PoC's method set is exactly five: source, `mapValues`, `groupByKey`, `count`, sink.
- R2. Python registers each per-record function under a reference token; no callable, address or source text crosses the boundary.

**Executing it**

- R3. The engine builds and runs a real Kafka Streams application from the described calls, owning the state store, its changelog and its commits.
- R4. When an operator reaches a Python-supplied function, the engine emits an invocation naming that token and blocks the operator until the matching result returns.
- R5. Record keys and values cross as bytes; the engine never deserializes them.

**Extending the protocol**

- R6. A correlated request/response message pair carries invocations and their results, distinct from `Report`.
- R7. The addition only adds to the frozen v1 wire and ships a capability token, per the protocol's own amendment rule.

**Proving it ran**

- R8. The oracle is the per-key counts in the sink topic matching a known seeding exactly; no throughput figure from the run is quotable.

### Key Flows

- F1. Describing the topology
  - **Trigger:** The Python application starts and opens a session.
  - **Actors:** A1, A2
  - **Steps:** Python issues builder calls in order, holding each returned handle; it registers its function against a token; it signals the description complete; the engine builds the topology and starts Kafka Streams.
  - **Outcome:** A running Streams application whose one transform operator is bound to a token.
  - **Covered by:** R1, R2, R3

- F2. Processing one record
  - **Trigger:** A record arrives on the source topic.
  - **Actors:** A1, A2, A3
  - **Steps:** A Streams thread walks the record into the transform operator; the engine emits an invocation carrying the token, the value bytes and a correlation id, then blocks that thread; Python pulls the invocation, looks the token up in its own table, calls its own function, and pushes back a result carrying the same correlation id; the engine unblocks the thread and returns the value into the topology; the aggregation and its state store proceed entirely engine-side.
  - **Outcome:** The per-key count advances in the state store and is emitted to the sink.
  - **Covered by:** R2, R4, R5

```mermaid
sequenceDiagram
    participant K as Kafka
    participant S as Streams thread (engine)
    participant P as Python
    K->>S: record
    S->>S: walk topology to transform operator
    S->>P: invoke(correlation, token, value bytes)
    Note over S: thread is BLOCKED here
    P->>P: look up token in its own table, call its own function
    P->>S: result(correlation, value bytes)
    S->>S: aggregate, state store, changelog
    S->>K: per-key count to sink
```

### Acceptance Examples

- AE1. Counts match the seeding
  - **Covers R3, R8.**
  - **Given** a source topic seeded with a known number of records over a known number of distinct keys,
  - **When** the run completes,
  - **Then** the sink topic holds exactly the expected count for every key, and no key is missing or extra.

- AE2. No callable crosses
  - **Covers R2.**
  - **Given** a topology naming a registered function,
  - **When** the engine invokes it,
  - **Then** the invocation carries an integer token and the value bytes, and carries no address, no code and no serialized closure.

- AE3. A slow function stalls its thread
  - **Covers R4.**
  - **Given** a Python function slower than the consumer's poll interval,
  - **When** it is invoked,
  - **Then** the Streams thread stays blocked and the group rebalances. The PoC records this as the expected consequence of a synchronous operator rather than mitigating it.

### Scope Boundaries

**Deferred for later**

- Joins, windows, suppression, interactive queries, punctuators and exactly-once - the same surface astubbs#255 already refuses on the JVM side.
- Embedded execution as a native image, and with it RocksDB under GraalVM.
- More than one foreign operator in a topology, and the question it would answer about boundary crossings multiplying with operator count.
- Conformance scenarios for the new capability. Adding one to the harness without a runner driving it fails `ScenarioCoverageTest`, so this is a real cost to schedule rather than an oversight.

**Outside this work**

- Parallel Consumer beneath Kafka Streams (astubbs#255). It is the source of key-level parallelism and it is assumed not to land; nothing here may depend on it.

### Dependencies and Assumptions

- **Assumed: astubbs#255 never lands.** In-flight invocations are therefore bounded by the number of Streams threads, itself bounded by partitions - not by key count. This PoC demonstrates the capability, not key-level parallelism, and must say so wherever its result is reported.
- **The sidecar gains a `kafka-streams` dependency it does not have today.** No proxy, protocol or client module references Kafka Streams; the only existing usage is an examples module where Streams feeds Parallel Consumer.
- **Assumed: capability negotiation still exists when this lands.** There is a standing owner's call to retire it in favour of client/sidecar version lockstep, which would change how R7's addition is gated.

### Success Criteria

- Someone who does not know the internals can run one command and see per-key counts that match the seeding.
- The reported result states the concurrency ceiling plainly, so no reader infers key-level parallelism from it.

### Outstanding Questions

All remaining questions are answered during planning or codebase exploration; none block it.

- How the correlated pair is framed on the wire, and how correlation ids are allocated.
- Whether the Streams application runs inside a sidecar that can also hold a Parallel Consumer session, or its own.
- Timeout and failure semantics when an invocation never returns, and what the blocked operator does then.

### Sources and Research

- [`docs/inflight/next-kafka-streams-foreign-wrappers.md`](../inflight/next-kafka-streams-foreign-wrappers.md) - the direction, adopted 2026-08-22.
- [`docs/plans/2026-08-22-001-feat-shared-c-transport-plan.md`](2026-08-22-001-feat-shared-c-transport-plan.md) - the shared C transport, its kill criterion, and the four bindings.
- [`docs/inflight/perf-embedding-the-engine-over-ffi.md`](../inflight/perf-embedding-the-engine-over-ffi.md) - Go, Python, Node and C, and the hazards each surfaced.
- [`STRATEGY.md`](../../STRATEGY.md), *Other runtimes* - the wrap-the-whole-client fork and the shipping order.
- [`parallel-consumer-proxy-protocol/src/main/proto/parallelconsumer/proxy/v1/proxy.proto`](../../parallel-consumer-proxy-protocol/src/main/proto/parallelconsumer/proxy/v1/proxy.proto) - the frozen-schema header, and `Report` whose only success payload is records to produce.
- [`parallel-consumer-proxy/docs/protocol-specification.md`](../../parallel-consumer-proxy/docs/protocol-specification.md) - capabilities as the sole versioning mechanism.
- [`parallel-consumer-examples/parallel-consumer-example-streams`](../../parallel-consumer-examples/parallel-consumer-example-streams) - the only existing Streams usage, where Streams feeds Parallel Consumer.
- `docs/inflight/branch-ks-streams-handover.md`, on branch `feats/ks-on-pc-spike` - astubbs#255's nine branches, open defects and traps. Not present on this branch.
