# Internal machinery as customer features - the AWS move, with an 11-language multiplier

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - a standing principle plus an inventory, not scheduled work -->

From the owner's side of the follow-up Codex conversation, 2026-08-29/30 (breakdown root:
[`core-engine-thesis.md`](core-engine-thesis.md)). The principle, in the owner's words: *"use our
internal machinery as customer features whenever exposing it is cheaper than building a separate
subsystem"* - the move that turned Amazon's internal infrastructure into AWS.

## The inventory the conversation named

Machinery the fleet-coordination direction needs *anyway*, each a candidate exposure:

- **Global rate limiting** ([`core-distributed-throttling.md`](core-distributed-throttling.md)) -
  the contract/envelope system as a product surface of its own.
- **The actor bus** ([`core-actor-revival.md`](core-actor-revival.md)) - if the internal actor
  framework is revived and a Kafka-Streams-backed mailbox built, that is a durable actor system.
- **Feature flags** - fleet-wide toggles the control plane needs for itself
  ([`web-control-plane.md`](web-control-plane.md) expiring overrides are half of one already).
- **Distributed tracing** ([`core-record-semantic-tracing.md`](core-record-semantic-tracing.md)) -
  transparent context propagation for users who never configured a tracer.
- **Fleet observation / subsystem logging** - the global view the coordination layer maintains,
  offered as "set this option to true".
- **Per-record selection and command** - the control plane's ability to act on *anything that is
  waiting*, at record granularity ([`web-control-plane.md`](web-control-plane.md)).

## The multiplier that makes this different from the usual platform trap

Everything above sits under the shared engine, so building it once for internal need means having
built it in **every bound language simultaneously** - expose the rate limiter and there exists, as
a side effect, a Python, Go, Rust, C# and Swift global rate limiter with Kafka carrying the
agreement. No other route to "a polyglot actor bus" costs this little, because the polyglot
boundary (astubbs#242, astubbs#293) is already paid for.

## The filter, so this stays a principle and not a sprawl

Each exposure competes with a dedicated market (flags vs LaunchDarkly, tracing vs the OTel vendor
ecosystem) and buys a public-API stability duty. Two tests before any exposure, both already on
record: the engine-thesis integration filter (*what unnecessary coupling does this remove?*), and
the AWS test itself - the machinery must be needed internally regardless, so exposure is marginal
cost, never a build-to-sell. Expose behind a flag, mark experimental, and let demand argue for
promotion.
