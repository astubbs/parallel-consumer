# Fleet capacity coordination: the story's claims, extracted and bounded

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - vision horizon; nothing here blocks or schedules current work, and the beyond-Kafka step is a product decision not yet taken -->

The follow-up Codex conversation (2026-08-29/30) closed with a piece of vision fiction,
preserved verbatim at
[`docs/ideation/2026-08-29-the-story-of-hasten.md`](../ideation/2026-08-29-the-story-of-hasten.md).
This note extracts the architectural claims the story smuggles in, routes each to its owner, and
marks the boundaries - because fiction is where scope creep hides best.

## Genuinely new claims, each routed

1. **Global resource envelopes, coordinated probing.** Sixty-three applications share one learned
   database envelope across five Kafka clusters; when it must be re-probed after recovery, *one*
   experiment runs globally instead of sixty-three controllers probing independently. This is the
   post-v1 endpoint of [`core-distributed-throttling.md`](core-distributed-throttling.md) - and it
   does NOT reopen that track's v1 decision (no coordination substrate, self-assessment only). The
   story is what the substrate is *for* if it is ever built.
2. **Participants beyond Kafka.** "One wasn't using Kafka at all... but they all used the
   company's standard HTTP and database libraries. So they were there." The largest category
   change in either weekend: the runtime as a general capacity-governance participant, not a Kafka
   consumer. The story's own demo scene dramatises the risk ("so it's a service mesh? an APM?
   FinOps?") - crossing this line puts the project in four adjacent markets at once. **That product
   decision has since been TAKEN** (owner, session of 2026-09-01): the line is crossed deliberately,
   the service-mesh framing is not treated as a risk, and the reasoning is that coverage of the
   non-Kafka half is a precondition for the global-optimisation claim rather than an expansion of it
   - an envelope learned from a partial view is simply wrong. The mechanisms this claim never named
   - credit vending to external callers, ingesting existing monitoring, transparent interception,
   protocol compatibility - are owned by
   [`core-non-kafka-participants.md`](core-non-kafka-participants.md).
3. **QoS policy classes.** `emergency/911` as *a promise, not a cluster*: guaranteed capacity
   through a dependency graph when the class has work, borrowable by everyone when it does not.
   [`core-slo-objective-api.md`](core-slo-objective-api.md)'s importance-aware allocation promoted
   to company-wide policy.
4. **Forecast provisioning.** "It had seen 46 Tuesdays" -
   [`core-capacity-fingerprinting.md`](core-capacity-fingerprinting.md) gains a temporal
   dimension: fingerprints have seasonality, and provisioning can precede load.
5. **Deploy-time amplification regression.** A healthy-looking deploy goes yellow because calls
   per record to a sibling service moved 1.1 -> 2.4, projected to exhaust that service by 14:20.
   [`core-retry-economics.md`](core-retry-economics.md)'s amplification metric composed with
   fingerprinting's regression detection, cross-service - the sharpest near-term idea in the
   story.
6. **The FinOps projection.** Recommendations priced at the bottleneck ("$3,180/month buys
   $11,400/month of removable compute, because 22 applications over-provision to absorb the shared
   database's latency") - and the economics behind it, the best articulation of the cost story so
   far: **every team pays for the same uncertainty separately; a global view reserves it once and
   lends it out**. The 1:20pm exchange completed the frame: a dollar of infrastructure has no
   intrinsic value - its value is what it unlocks elsewhere in the graph (spare CPU in a DB-bound
   application is worth ~nothing; $500 of database releases executable work in three
   applications) - so the system's cleanest description is *continuously moving infrastructure
   spend toward the current global constraint, and moving it again when the constraint moves*.
   And the problem is capacity over TIME, not just now: shock reserve, forecast demand,
   provisioning lead time, failure reserve and cost are dimensions of one optimisation. Feeds
   [`perf-benchmark-cost-to-slo.md`](perf-benchmark-cost-to-slo.md).
7. **The coordination architecture.** A small control plane *over Kafka* - application records
   never transit it, every runtime decides locally, collectively one scheduler, no cluster to
   operate. Embedded-not-cluster extended to the fleet layer, and the only form of claim 1 that
   survives the positioning. The owner added the dogfooding requirement: the coordination topic's
   pulse frequency is itself dynamic, governed by the same contract system, **because the broker
   is a shared resource too** - a scheduler that exempts its own traffic from its scheduling has
   not understood its thesis. The handoff extends this: Kafka itself becomes a resource-graph
   *node* - broker CPU, disk, network, leadership and replication as observable, optimizable
   capacity - kept replaceable, with no invasive broker modification required initially. And the positioning line worth keeping: the fleet layer is *Kafka's
   child at the edge* - completing client-side what the broker's partition-level orchestration
   starts, never replacing it.
8. **The definition and the adoption mechanics.** One sentence - *"coordinates how your company
   spends execution capacity"* - and a rollout that happens by dependency bump and import change
   ([`core-ecosystem-adapters.md`](core-ecosystem-adapters.md) dramatised). Plus one genuinely new
   insight about contracts: **service owners publish them because it gives them control over how
   the rest of the company consumes their service** - the contract system sells itself to the
   provider side, not only the consumer side.

## The boundary

This is a three-to-five-year horizon compressed into one Tuesday. None of it is roadmap: v6 and
the open PRs are unaffected, the arbitration and attribution notes stand on their own merits, and
the beyond-Kafka step (claim 2) must be argued for separately if it is ever argued for at all. The
story's value is that the ladder from "a record was waiting" to fleet coordination never changes
principle - which is the thesis note's argument told as narrative.
