# HTTP strategy: dialects, compatibility surfaces, and adapters

Captured 2026-08-15 while the thinking was fresh. **Ideas, not decisions** — the point is to get the
option space down before it evaporates, and to name what would settle each choice. A proper ideation
pass is owed; this is the seed for it.

The frame that makes the rest coherent: **one set of semantics, many encodings.** The engine and its
meaning stay single-sourced; what varies is how a client speaks to it. Everything below is an
encoding or an audience, never a second engine.

## 1. The native HTTP dialect — server-sent events plus POST

Dispatch over **SSE** (server-sent events: server-to-client push over ordinary HTTP, supported
everywhere), reports over **POST** carrying the echoed token.

- **Preserves the push model.** The engine still decides what to hand out and when, which is the
  design the plan chose over the rejected credit/pull ledger.
- **Built as a gateway inside the sidecar**, translating to the same engine seam the gRPC transport
  uses — two encodings of one implementation.
- **Why it earns its place**: a gRPC client is *unusable* where the client can only reach the sidecar
  over HTTP — corporate proxies mangling HTTP/2, restricted networks, some hosted platforms. That is
  a deployment constraint, not a preference. Secondarily, the long tail of runtimes with poor gRPC
  support, and anyone who wants to try it with `curl` first.

## 2. Compatibility surfaces — speak their protocol, not ours

**The cheapest adoption path is speaking someone else's protocol rather than asking them to adopt
yours.** Two candidates, both cheap *if* the HTTP gateway exists anyway:

- **kafka-pixy's API** — a pixy user changes a hostname and gains key-ordered concurrency beyond
  partitions, with no code change.
- **Confluent REST Proxy's API** — the same trick, much larger installed base.

**The concession to be deliberate about**: both are **pull**-shaped, because they predate ubiquitous
SSE. A compatibility shim may reintroduce pull *for compatibility clients only*; it must never become
the native dialect. Two different things sharing a transport.

**The unknown that decides feasibility**: whether their consume semantics have room for ours. If
their endpoint hands out one message per partition assignment, a compatible implementation may be
unable to hand out several from one partition without violating what its clients expect — which would
mean compatibility without the concurrency benefit, i.e. pointless. **Read their API before believing
either way.**

## 3. Adapters — Dapr, and why it is the highest-leverage of all of these

A **Dapr** pub/sub component backed by this engine (Dapr is a Cloud Native Computing Foundation
sidecar runtime; see [`next-study-dapr-and-kafka-proxies.md`](next-study-dapr-and-kafka-proxies.md)).
Dapr supports *pluggable components* implemented as gRPC services, so this looks feasible.

- **It multiplies the two axes rather than adding them.** Dapr solves coupling; this solves
  concurrency. A Dapr user today accepts partition-bounded concurrency as the price of portability;
  this removes the trade with no change to their programming model.
- **It reaches an audience without writing a client** — one component, not eleven libraries.
- **It is complementary, so it is adoptable**: nothing is cannibalised, and their pluggable-component
  model exists precisely to let outsiders do this.

**Two unknowns**: whether Dapr's deliberately broker-agnostic contract can express per-key ordering,
an in-flight ceiling and per-record acknowledgement without leaking Kafka concepts — if it is
message-shaped rather than work-shaped, the adapter cannot deliver the benefit. And the deployment
shape: their sidecar talking to ours is two sidecars, which is a smell; whether a component can host
the engine directly is the question.

## 4. Clients for the HTTP dialect — generated surface, hand-written controller

Yes, the HTTP dialect still deserves client libraries, and the reason is **not** the HTTP mechanics.

- **The user-facing case comes first**: nobody wants to hand-wire a session POST, an SSE reconnect
  loop, JSON shapes, base64-decoding of keys and values, and token bookkeeping. `client.poll { ... }`
  is the product; the alternative is a protocol tutorial. **The dialect should be invisible at the API
  surface** — a Rust developer forced onto HTTP by a network constraint should still write the same
  code they would over gRPC.
- **The correctness case second**: the dispatch queue, the ceiling counting *unresolved* records, the
  transport never blocking, session death observable with its cause. Every client that went wrong in
  this project went wrong exactly there, and no HTTP call gives you any of it.
- **Generation covers the boring middle.** The gRPC clients are generated from the `.proto`; the HTTP
  ones can be generated from an **OpenAPI** specification (the standard for describing HTTP APIs),
  whose generators cover far more languages than this project targets. **Caveat that decides how much
  is really generated**: OpenAPI describes request/response well and streaming poorly, so the SSE
  consumption and the controller stay hand-written per language.

## 5. What this does to the conformance suite

Nothing structural, which is the encouraging part. Its binding key is already *(language, dialect)* —
`core` is no wire at all, `java-direct` an in-process call, `java-grpc` a real stream. HTTP bindings
are more rows; the scenarios and assertions do not change, because they assert semantics rather than
encoding.

**The risk to manage is combinatorial**: eleven languages times two dialects is twenty-two clients.
Most of those cells should not exist. gRPC where the ecosystem supports it well; HTTP where it does
not, or where the network forbids it. **Per-language judgement, not a universal rule** — and demand
decides, per the invitation in the clients' README.

## What would settle these, in order

1. **Read the contracts** — Dapr's component interface, kafka-pixy's and REST Proxy's consume/ack
   semantics. Every judgement above rests on assumptions about them.
2. **Decide whether the HTTP gateway is one listener or two** — the sidecar is already parked to
   embed the web dashboard post-v6, so an HTTP listener is likely arriving regardless. One listener
   with two purposes beats two listeners.
3. **Settle the security posture for an HTTP surface.** Loopback-only and its opt-in apply unchanged,
   but a browser-reachable origin makes cross-origin questions real in a way gRPC never did.
4. **Then** design the wire shape, and only then decide which languages get generated clients.

Related: [`parked-http-dialect-and-generated-clients.md`](parked-http-dialect-and-generated-clients.md)
holds the original framing and the link to the 2021 issue that named REST first;
[`parked-a-c-client-and-the-ffi-question.md`](parked-a-c-client-and-the-ffi-question.md) is partly
superseded by all of this.
