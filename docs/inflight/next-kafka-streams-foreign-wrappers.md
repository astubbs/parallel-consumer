# Next: Kafka Streams in other languages, through the language-proxy model

<!-- inflight-type: feature -->
<!-- inflight-impact: reach -->
<!-- inflight-labels: needs-design -->

**A proof of concept to run, not a plan to execute.** Adopted as a direction 2026-08-22; sequenced
well after the admin wrapper, which goes first.

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

## What needs thought before anyone starts

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

## Sequencing, stated plainly

**This is a long way past the current milestone.** The admin wrapper goes first, then producer, and
the base consumer last. Streams is downstream of all of it, and downstream of the open items in
[`../plans/2026-08-22-001-feat-shared-c-transport-plan.md`](../plans/2026-08-22-001-feat-shared-c-transport-plan.md) -
the AOT-versus-AOT measurement, GC coexistence, and reachability beyond the happy path.

## Prior art

- [`perf-embedding-the-engine-over-ffi.md`](perf-embedding-the-engine-over-ffi.md) - the four
  bindings this would build on, and the hazards each surfaced.
- [`../plans/2026-08-22-001-feat-shared-c-transport-plan.md`](../plans/2026-08-22-001-feat-shared-c-transport-plan.md) -
  the shared C transport, its kill criterion, and CTD8 on why Node stays on the sidecar.
- [`STRATEGY.md`](../../STRATEGY.md) - *Other runtimes*, where the wrap-the-whole-client fork is
  taken and the ordering is set.
