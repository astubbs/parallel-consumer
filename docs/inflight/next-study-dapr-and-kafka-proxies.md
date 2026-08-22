# Study Dapr and the Kafka proxies — then decide whether to integrate with them

Raised 2026-08-15 while working out what the proxy actually is. Nothing here is decided; the point is
that **this project now has close architectural neighbours and has never studied any of them**, which
is a gap while writing an announcement that will invite the comparison.

## Who they are

**Dapr** (Distributed Application Runtime) — a **CNCF** project. CNCF is the Cloud Native Computing
Foundation, the Linux Foundation body that hosts Kubernetes, Prometheus and Envoy. Dapr is a sidecar
runtime: an application talks to a local sidecar over HTTP or gRPC, and the sidecar provides
"building blocks" — publish/subscribe, state, bindings, service invocation. Its Kafka pub/sub
component consumes from a topic and delivers each message to the application's endpoint.

**kafka-pixy** — a Kafka proxy exposing gRPC and REST, with consumer groups and explicit
acknowledgement, so languages with poor Kafka clients can still use Kafka.

**It is abandoned** (owner, 2026-08-15): no commits in about four years, with open requests to support
Kafka versions from around 2019. That fact changes what it is for here, in two directions:

- **Dropped as a requirement.** Owner's call: a compatibility surface speaking its protocol buys
  access to no users, and §4g of [`next-http-strategy-ideas.md`](next-http-strategy-ideas.md) —
  *patch their project rather than reimplement it* — has no upstream left to patch. Neither should be
  carried as live options.
- **Kept as a comparison point**, which is the more valuable half. It solved a closely adjacent
  problem with the same materials, and abandonment does not invalidate its architecture or its build.

**It is also the strongest evidence that this sidecar is not that sidecar.** The proxies existed for
*reachability*, native clients solved reachability underneath them, and a proxy with no remaining
reason to exist stops getting commits. Concurrency was never solved and cannot be solved by better
native clients, because it is a consumer-model problem rather than a client-quality one. Read their
adoption as a warning about *purpose*, not as a forecast for anything sharing their shape.

### Questions to answer about it before this work merges

Raised 2026-08-15, deferred deliberately, and scoped to the client fan-out rather than to the
protocol:

- **Did they ship a C++ client wrapping their gRPC protocol?** If so it is the only other worked
  example of the problem this fork solved from scratch, and the one where the answer was least
  obvious.
- **How does their C++ build compare with ours** — toolchain, dependency acquisition, how the
  generated stubs are produced and vendored, and what it costs a user to build?
- **How does their client style compare** — the shape of the surface, the threading model, and where
  they drew the line between generated code and hand-written controller?

## How they relate to this project

**Architecturally, Dapr is the closest thing that exists**: sidecar, any language, local protocol,
the application never speaks Kafka. That similarity is the reason to study it and the reason the
announcement must address it directly rather than hope nobody asks.

**The purposes are different, and that is the whole answer.** Dapr exists for **portability** - swap
Kafka for Redis or NATS without touching application code. Its Kafka component is an ordinary
consumer group, so concurrency is bounded by partition count and ordering is per partition. This
project exists for **concurrency**: key-ordered work distribution that is *not* bounded by partitions.

That difference is not a transport problem, which is why no sidecar has solved it: it needs the
offset-map encoding that lets out-of-order completion be committed safely without losing at-least-once.
Years of work in core, and nothing about it is visible at the wire.

**So the honest positioning**: Dapr and kafka-pixy answer *"how do I reach Kafka from Python?"* This
answers *"how do I get more concurrency than partitions without losing key ordering?"* — and, via the
proxy, in Python. Someone with no ordering requirement and no concurrency ceiling has little reason
to prefer this; someone who has hit the partition ceiling has no alternative in that space today.

## The clarifying axis: they solve COUPLING, this solves CONCURRENCY

The sharpest framing to come out of the conversation, and it should shape both the study and the
announcement: **none of these neighbours is about performance.**

- **Dapr** is about **coupling** — the application should not know or care which broker is underneath,
  so it can be swapped without touching code. Portability is the product.
- **kafka-pixy and Confluent REST Proxy** are about **reachability** — a language with a poor Kafka
  client should still be able to use Kafka. Access is the product.
- **This project** is about **concurrency** — work distributed with per-key ordering, beyond the
  partition ceiling, without leaving Kafka. Throughput-under-ordering-constraints is the product.

They are **orthogonal axes, not competitors**, and that has three consequences worth acting on:

1. **The announcement should not frame it as a rivalry.** "We are better than Dapr" is both wrong and
   easy to refute. "Dapr solves a different problem, and here is which" is true, more useful to a
   reader, and cannot be argued with.
2. **It strengthens the adapter idea considerably.** A Dapr component backed by this engine is
   *complementary*: a Dapr user keeps their portability and gains concurrency they could not otherwise
   have. Nothing is cannibalised, because nothing overlaps.
3. **It explains an asymmetry in this project's own history.** Language reach was the *side effect*
   here — a consequence of putting the engine behind a protocol — whereas for the neighbours it is the
   entire purpose. That is why their sidecars stop at partition-bounded delivery: they never needed
   anything more, because reaching Kafka was the goal rather than getting more out of it.

## Two integrations worth evaluating rather than assuming

**A Dapr component backed by this engine.** Dapr supports *pluggable components* implemented as gRPC
services, so a pub/sub component backed by Parallel Consumer looks feasible: Dapr users would keep
their existing programming model and get concurrency beyond partitions underneath it. That is a large
audience reached without writing another client, and it is the one integration that could matter more
than any additional language.

**kafka-pixy over this proxy.** Less obvious. Its value is protocol translation, and replacing its
consumer core would make it a *client* of this sidecar rather than an adapter of it — which is what
the HTTP dialect already does more directly. Evaluate, but expect the answer to be no.

Both are speculation until someone reads their actual contracts. **That reading is the task**, not the
integration.

## What to look for while studying them

Steal freely; these are solved problems elsewhere:

- **Dapr's component contract** — how it models subscription, delivery, acknowledgement and retry over
  a local protocol. It is a mature version of the surface this project froze, and disagreements are
  worth understanding before v1 hardens further.
- **How they handle a dying application** — the problem the lease and reconnect units solved here.
- **How they configure credentials** to a sidecar, which is this project's least comfortable area.
- **How they document and version a local protocol** across many languages, and how their clients are
  generated or hand-written — directly relevant to
  [`parked-http-dialect-and-generated-clients.md`](parked-http-dialect-and-generated-clients.md).
- **kafka-pixy's HTTP shape**, alongside Confluent's REST Proxy, before designing this project's own
  HTTP dialect. Note that both pull, because they predate ubiquitous server-sent events, and that
  pull is the model this project's plan deliberately rejected.

## The framing that came out of this conversation, worth keeping

**Parallel Consumer gives you queue semantics directly from Kafka, so the hop into a queue broker
stops being necessary.** The common architecture is Kafka as the backbone, bridged into RabbitMQ, SQS
or Celery whenever work needs per-key ordering, retries or high concurrency — which costs a second
system, two sets of delivery semantics, and data leaving Kafka (forfeiting replay, retention and
ordering), plus a bridge that can lose or duplicate.

The narrow, defensible version — the wide one is wrong, since this offers no routing or exchanges, no
priority queues, no arbitrary delayed delivery and no per-message time-to-live:

> If you bridged Kafka into a queue broker to get concurrency, ordering and retries — you do not need
> to.

**Is this a broker?** Behaviourally it is broker-shaped: ordered dispatch, acknowledgements,
redelivery with attempt counts, leases, worker liveness, reclaiming a dead worker's records. What
keeps it from being one is **durability** — it owns only the in-flight view, Kafka remains the sole
source of truth, and nothing is lost when the sidecar dies because nothing was acknowledged. The
original issue said as much in 2021: *"a server side queue implementation"*.

These claims belong in `STRATEGY.md` if they survive scrutiny, since that is the document that holds
claims the work must keep true — and in the announcement
([`release-v6-phoenix-theme-and-announcement.md`](release-v6-phoenix-theme-and-announcement.md)).
