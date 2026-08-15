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

## 4b. The architectures an HTTP surface makes possible — each a different product

An HTTP dialect is not one shape. Each of these presents a *different architecture* to the user, and
they are not interchangeable:

- **SSE push** — the native dialect. Engine decides, client obeys. Closest to what exists today.
- **`GET` a unit of work, `PUT` its result** — a plain REST resource model. Pull, dumb clients,
  survives any network, trivially cacheable and load-balanced. This is the shape kafka-pixy and the
  REST Proxy already have, and the one a compatibility surface would speak. Its cost is latency and
  the client owning flow control.
- **WebSocket** — bidirectional over one connection, browser-native, and the closest thing to gRPC's
  streaming that a browser can use directly.
- **A browser client.** JavaScript in a tab as a worker, taking key-ordered units of work from the
  sidecar. Genuinely new territory rather than a variation, and the one that most changes who the
  product is for.

**The browser case needs its own security answer before anyone builds it.** Every posture this
project has rests on loopback-only with an opt-in: a browser-reachable surface breaks that
completely, and brings cross-origin rules, credentials that cannot live in a page, and the
DNS-rebinding class the original plan's security notes already flagged. Treat "the browser can be a
worker" as an interesting demo until that is answered, and never as a default.

**Generated clients apply to all of these**, and the shapes differ in how much can be generated: a
`GET`/`PUT` resource model is almost entirely generatable from an OpenAPI specification, while the
SSE and WebSocket shapes are mostly not. That is an argument for the resource model being the
*compatibility* surface and the streaming one being native — the generator gets the boring half,
which is exactly the half compatibility needs.

## 4e. Why choose one entry point at all?

The question that dissolves most of the choices above: **one engine can host several entry points
simultaneously.** gRPC, the native HTTP dialect, a compatibility surface, and the dashboard are all
just adapters onto the same engine seam — there is no reason a sidecar serves only one.

That changes the framing from "which dialect do we pick" to "which adapters ship enabled", and each
becomes an independent, individually deferrable decision rather than a fork in the road. It also
means a single deployment can serve a Rust worker over gRPC, a legacy client over a REST-Proxy-shaped
surface, and an operator's browser, at once.

**What it costs, and must be settled deliberately**: several listeners (or one multiplexed port), a
security posture per surface rather than one for the process, and an admission model that currently
assumes **a single connection owns the session**. Multiple entry points make "which client owns this
session?" a real question rather than a tautology — that is the design work, and it is more
interesting than any individual dialect.

## 4f. The dashboard is another entry point, and it knows things the others do not

The embedded web GUI ([`parked-sidecar-embeds-web-gui.md`](parked-sidecar-embeds-web-gui.md)) has
always been justified by the sidecar being otherwise a black box to its operator. Seen as an *entry
point* rather than a feature, more follows:

- **It shares the HTTP listener** the native dialect needs, so building one serves both. One listener,
  two purposes — decide that before either is built, not after.
- **It knows which client is connected**, because the handshake tells it: the language, the negotiated
  capabilities, the executor count, the effective options. So it can show a *language-aware* view —
  the same engine state, described in the terms of whoever is attached, and diagnostics that name the
  right client's behaviour rather than generic engine internals.
- **It is the only surface that can explain the thing this product is hardest to believe about**:
  concurrency beyond partition count with ordering intact. Seeing in-flight work per key, per shard,
  live, is worth more than any amount of prose — for an operator debugging, and for a sceptic
  evaluating.

## 4g. Patch their project rather than reimplement it — the Kafka Streams precedent

There is already a precedent in this repository for **adapting someone else's runtime to fit
Parallel Consumer rather than cloning it**: the Kafka Streams work. The same move applies to
kafka-pixy.

Instead of implementing a pixy-compatible surface here, **patch pixy so it can use this engine**, and
**post the change upstream**. If it lands, their users get concurrency beyond partitions without
adopting anything of ours, and the maintenance sits with the project whose API it is — which directly
answers the surface-area risk below.

**Why it might not fit**: pixy is Go, so "use this engine" means calling the sidecar, and its consume
semantics are pull-shaped and partition-assigned. If its API cannot express handing out several
records from one partition concurrently, the patch delivers nothing. **Read their consume path before
proposing anything** — and prefer an upstream conversation to a fork, since a fork is the expensive
outcome dressed as the easy one.

## 4h. The larger vision, and the fork inside it: sidecar or server?

Raised 2026-08-15: serve pixy, Dapr and a work-queue protocol at once, with durable persistence in
Kafka — "a higher-performance, more flexible Kafka, extended by Streams and Connect". Coherent, and
worth writing down. But it contains a fork that must not blur, because the two destinations have
almost nothing in common operationally.

**Session ownership is settled either way**: PC owns the connection. Whatever front-ends exist, the
engine decides what is dispatched and to whom.

**The sidecar's constraints are load-bearing, not incidental.** Everything built so far depends on
them:

- loopback-only, therefore **no authentication needed**;
- one application per sidecar, therefore **no multi-tenancy**;
- the connection *is* the session, therefore admission is trivial;
- credentials may travel the wire because **only the spawning process can reach it**.

**A shared work-queue server discards all four simultaneously** and inherits what any multi-tenant
service must have: authentication and authorisation, tenant isolation, quotas and noisy-neighbour
control, high availability, upgrade and compatibility paths, and an operational surface someone must
run. That is not a larger sidecar. It is a different product built on the same engine, and it should
be decided as one rather than arrived at by accretion.

**One concrete dependency for the "transactions" half**: exactly-once is *unreachable through the
proxy today* — `ExternalEngine` throws on transactional commit mode, which is why the interaction
model kept produce on the engine side. It is recorded as post-v6 with core changes sanctioned. Any
durable-work-queue story rests on that landing first.

**What is genuinely attractive here**, and survives either choice: Kafka as the durable substrate
(replay, retention, ordering, one source of truth) with work-queue semantics layered on top, reachable
by anything. That is the product story either a sidecar or a server would tell. The difference is who
operates it — and the sidecar answer is "the application team, invisibly", which is a large part of
why it is adoptable at all.

## 4c. Is this worth it, or is it reinventing the wheel?

The honest question, asked 2026-08-15, and it deserves recording rather than enthusiasm.

**The case that it is reinvention**: Confluent's REST Proxy and Dapr exist. If all someone wants is
Kafka from Python, that is solved, and has been for years.

**The case that it is not**: none of them lifts the partition ceiling while keeping per-key ordering.
That combination is the product, and the bridge-into-a-queue-broker pattern it removes is common and
expensive. The ingredients are all old; the combination appears to be new.

**But the real risk is neither** — it is **surface area against a single maintainer.** Eleven client
libraries, two dialects, several package registries, per-ecosystem vulnerability exposure, issue
queues and user expectations in languages nobody here writes daily. That is what kills projects of
this shape, far more often than lack of value.

What is already mitigating it, and should stay non-negotiable: the clients are **facades** with
almost no logic; the shared conformance suite means one definition of correct rather than eleven; and
the generated-where-possible rule keeps hand-written surface small. **Every new dialect, adapter and
compatibility surface must be judged against that risk, not against how interesting it is** — which
is the discipline this document exists to impose on its own contents.

The practical version: **demand decides.** The clients' README already invites requests for missing
languages. The same test should govern every idea here — build the one somebody asked for, not the
one that completes the matrix.

## 4d. The marketing problem this creates

Breadth is itself a risk: a landing page trying to say all of this says nothing. Too many stories
means no story, and the natural instinct — a comprehensive page covering every architecture — is the
worst option available.

**Lead with one**: *if you bridged Kafka into a queue broker to get concurrency, ordering and
retries, you do not need to.* That names the reader's existing architecture back to them. Everything
else — languages, dialects, adapters, the browser — is discovered *after* someone has a reason to
care, and belongs in documentation rather than on the front page.

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
