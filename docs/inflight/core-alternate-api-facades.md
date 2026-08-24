# Alternate API facades: meet developers at the Kafka API they already use

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - needs a product decision -->

From the Codex strategy review of 2026-08-22/23 (breakdown in
[`core-engine-thesis.md`](core-engine-thesis.md)). One item: **thin API-compatible facades over the
PC engine as an adoption ladder**, so migration starts at an API a team already runs rather than at
a rewrite.

```
KafkaConsumer  ->  PC KafkaConsumer facade  ->  PC ShareConsumer facade  ->  PC native API  ->  Streams
```

Each step exposes more PC capability without forcing a day-one rewrite; the engine underneath never
changes. The strategic consequence, stated in the review's words: *the public API is becoming
interchangeable - the execution engine is the product.*

## The KafkaConsumer-shaped facade: mostly already tracked, reframed

The safe-exposure problem, the 1.0 gate (astubbs#158, astubbs#139) and the existing
`FullConsumerFacade` implementation on `origin/features/consumer-interface` are owned by
[`next-expose-consumer-and-admin-apis.md`](next-expose-consumer-and-admin-apis.md) - this note adds
only the framing that was missing there: the facade is not just "expose consumer ops to PC users",
it is a **drop-in migration path** - keep the familiar low-level API, replace the execution model
underneath. That is the mirror image of Streams-on-PC (keep the familiar high-level API, replace
the execution model), and the symmetry is the product philosophy: preserve the application model,
improve the engine.

## The KafkaShareConsumer-shaped facade: the new idea

A `KafkaShareConsumer`-shaped API backed by PC on a **classic consumer group** - no Share Groups on
the broker at all. The acknowledgement vocabulary maps directly onto PC's per-record completion:

| Share API | PC semantics |
|---|---|
| `ACCEPT` | success / advance the frontier |
| `RELEASE` | retry |
| `REJECT` | terminal failure / skip (future DLQ signal) |

What it offers over the real thing: per-key ordering, records from many polls in flight (the real
API is batch-synchronous - see
[`next-what-survives-share-groups.md`](next-what-survives-share-groups.md)), client-side retry
policy, and **no 4.2+ broker requirement** - the Share Groups programming model on the clusters
that cannot run Share Groups.

**Bounds, so nobody over-claims:** this is API-shape compatibility, not semantic equivalence. No
broker leases, no delivery-count archival, no cross-instance redelivery of released records - the
broker-side machinery is exactly what is not there. Any pitch must say so in the first breath, per
the Share Groups honesty rule already in STRATEGY.md.

**Do not conflate with the inverse idea:**
[`next-parallel-consumer-on-share-groups.md`](next-parallel-consumer-on-share-groups.md) is PC
running *on* a real share consumer (broker keeps the state). This note is the Share *API shape*
over a classic group. Same vocabulary, opposite direction, different constraints.

**Cost:** the review called it "almost embarrassingly cheap" given the existing batch API - mostly
API vocabulary and lifecycle translation. Treat that as a prediction to be tested, not an estimate.
