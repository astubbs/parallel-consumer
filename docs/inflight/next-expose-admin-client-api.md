# Next: should PC expose an AdminClient surface?

<!-- inflight-type: feature -->
<!-- inflight-impact: coordination -->

**Genuinely unasked. Searched exhaustively 2026-08-21 and there is no prior art of any kind** - no
issue in either repository, no inflight note, no plan, no feature record, no roadmap entry proposing
an AdminClient surface for users. Every `AdminClient` hit in the repo is **test tooling**: the chaos
suite using `describeConsumerGroups` as a zombie probe, and the conformance work using the Admin API
as an external oracle.

Split from [`next-expose-consumer-and-admin-apis.md`](next-expose-consumer-and-admin-apis.md) so the
decision is not buried inside the much larger Consumer-API work, which has its own issues, its own
1.0 gate and an unmerged implementation. **These are different questions and only one of them is
already scoped.**

## The question

Kafka's `AdminClient` covers topic creation and description, consumer-group description, offset
listing and deletion, partition and broker metadata, ACLs and configs. A PC user commonly needs some
of that *around* their consumer - most often lag, group state, and topic existence at startup.

Today they build their own `AdminClient` beside PC's consumer, restating bootstrap servers,
credentials, TLS and any SASL configuration that PC already holds.

## The case for

- **PC already has the configuration.** Duplicating credentials and TLS setup is the actual friction,
  not the API surface.
- **Lag is a recurring ask.** `astubbs#157` (`confluentinc#484`, *"Does Parallel-consumer have state
  that we can read from?"*) and upstream Discussion 815 (*"Getting/Setting consumer state"*, **zero
  replies**) are both users reaching for information PC could supply.
- **The web GUI and metrics work want the same data.** `astubbs#215` (running-instance visibility)
  and the observability ideas both need group and offset information; an internal admin surface would
  serve them, and exposing it publicly afterwards is a smaller step.

## The case against

- **It is not part of consuming.** PC's value is the processing model; re-exporting an unrelated Kafka
  client is scope creep with a maintenance cost and no differentiation.
- **A user constructing their own is a few lines**, and it is the shape every Kafka tutorial teaches.
- **It widens the public API right when we are trying to settle it.** `roadmap.yaml`'s
  `api-settlement` blocks 1.0 and `astubbs#139` (thread-safe public API) is its gate. Adding surface
  before that settles works against both.
- **AdminClient calls are blocking and remote.** Exposed naively on PC's threads, a slow
  `describeConsumerGroups` against an unhealthy broker becomes a stall inside the control loop.

## A middle option, probably the right one

**Expose the resolved configuration, not the client.** A method returning the effective consumer
configuration - or a pre-built `Properties`/`Map` suitable for constructing an `AdminClient` - gives
the user the thing they actually lack (credentials, TLS, bootstrap) without PC owning an admin
lifecycle, a threading contract, or an API surface it must then support for ever.

The user writes `AdminClient.create(pc.getAdminConfig())` and everything else is Kafka's, documented
by Kafka, and versioned by Kafka.

## The cautionary version, from the competitor

llingr defines a `BrokerQuery` hook on its broker port with exactly one query type
(`CommittedOffsets`) - and **both shipped adapters implement it as a documented no-op, and the engine
never calls it.** It is threaded through the builder, stored as a field, and has no call site.

**A placeholder for this surface that does nothing is worse than not having one**: it appears in the
interface, invites use, and silently returns empty. If PC adds an admin surface it should be
functional on day one or absent.

## What would decide it

1. **Does anyone actually ask?** No issue exists, which is itself evidence. Worth watching rather than
   building.
2. **Does the web GUI need it internally?** If `astubbs#215` builds admin-backed lag or group views,
   the internal client exists anyway and the question becomes whether to expose what is already there.
3. **Settle `astubbs#139` first.** Any new public surface inherits the thread-safety contract that is
   currently unwritten.

**Current recommendation: do nothing yet.** Record the middle option, wait for a user to ask, and
revisit when the GUI work forces the internal decision.

## Related

- [`next-expose-consumer-and-admin-apis.md`](next-expose-consumer-and-admin-apis.md) - the Consumer
  API half, which is scoped, tracked (astubbs#158), gated (astubbs#139) and already implemented once
  on an unmerged branch.
- [`web-gui-observability-ideas.md`](web-gui-observability-ideas.md) - the most likely forcing
  function.
