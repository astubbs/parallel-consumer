# Distributed throttling - ideation done, direction not chosen

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - after v6, direction not yet chosen -->


The ask is astubbs#228 (mirror of confluentinc#24), with confluentinc#766 as the production
demand evidence - and it converges with astubbs#227 (mirror of confluentinc#21, dynamic
concurrency control), the fork's oldest open self-tuning ambition. Ideation ran 2026-08-17,
extended 2026-08-18; eight ranked, code-verified directions:
[`docs/ideation/2026-08-17-distributed-throttling-ideation.html`](../ideation/2026-08-17-distributed-throttling-ideation.html)
Read that before restarting - it
holds the bases, the rejection table, and the prior-art autopsies. Related abandoned branches
(`features/rate-limiting`, `features/dynamic-concurrency-control`, `feature/auto-tuning-pressure`,
plus upstream draft PR confluentinc#22) are now catalogued in `docs/refactoring.md`'s idea bank.

Auto-scaling (astubbs#227) now has its own note - [`core-auto-scaling.md`](core-auto-scaling.md) -
split 2026-08-18 because the two efforts will reach mergeable PRs at different times and carry
distinct prototype trails. Idea 8 in the ideation doc is the shared convergence record.

## The end MVP goal is the rate-limiting feature itself (owner, 2026-09-01)

The Hasten navigator track and astubbs#228's feature ask are one deliverable - the owner's framing:
the navigator micro-MVP (astubbs/parallel-consumer#392, the in-process rung) is, finished and kept
on focus, **PC's global rate limiting feature**. So the track's end MVP goal is to ship that
feature - not the PR-level rung goals along the way - two birds, one stone: the Hasten scheduler
research produces the headline user-facing capability astubbs#228 has asked for since upstream.
What distinguishes it from a bolt-on limiter stays the product edge and must survive the rungs:
engine-integrated (declare a tag, no limiter code in the user function, no blocked workers),
soft/cooperative credits (degrades honestly, never deadlocks), and first-class attribution ("you
waited because api-a, next credit at T"). The remaining rung between here and that MVP is the
Kafka-coordinated allocator behind the same `ResourceAllocator` seam
([`core-shared-execution-resources.md`](core-shared-execution-resources.md) owns the design).

Decisions that gate any build:

- **Standalone throttle vs self-scaling controller** (idea 8): does rate limiting ship as its
  own feature, or as one signal into the auto-scaling controller (see `core-auto-scaling.md`)?
  Capacity limits are discoverable; contractual quotas are not - so explicit ceilings and
  adaptive discovery are complements composed by `min()`, which argues for the strategy-menu
  shape (idea 5) either way.
- **Enforcement fork**: per-shard gate + `availableAt` deferral (idea 2) vs Little's Law
  in-flight controller (idea 3). Internal plumbing, not user policy - pick one, don't offer both.
- "Who owns the number" is dissolved by the strategy menu: partition-share, downstream-signal
  (structured rate-limit exception from the user function), and adaptive all ship as
  implementations of one SPI; users pick per deployment.

**The same follow-up then designed the candidate mechanism in full:**
[`core-shared-execution-resources.md`](core-shared-execution-resources.md) - named resources,
Kafka-delegated renewable capacity leases, consumable per-quantum credits with a
failure-wastes-never-violates bias, hard-vs-adaptive resource semantics, and an equal-share v1
that needs no new distributed algorithm. It answers this note's gating decisions without closing
them: it takes the strategy-menu shape (idea 5), puts the adaptive envelope on the *resource*
(resolving idea 8's standalone-vs-controller fork as "both, hierarchically"), and picks the
admission-constraint enforcement seam (ideas 2/3). The decisions stay open until the owner adopts
them; the design is now written down rather than re-derivable.

**Addition from the follow-up Codex conversation, 2026-08-29/30: contracts are per-SERVICE, and
functions share them.** The declared ceiling ("Stripe: 100 concurrent, 1,000 req/s") is scoped to
the downstream service, not to a function - so `payments.capture`, `payments.refund` and
`subscriptions.renew` wanting 170 between them compete for a known 100-call budget, allocated by
marginal benefit ([`core-per-function-capacity-arbitration.md`](core-per-function-capacity-arbitration.md)).
The controller treats the contract as a hard ceiling and learns *below* it, so a known limit is
never rediscovered experimentally per deployment - which is the min-composition decision above,
restated with the service as the scope. The governance view ("requested 170, allocated 100, per
function") belongs to [`web-control-plane.md`](web-control-plane.md).

Idea 7 (decorrelated retry jitter) is independently shippable and needs none of these.
Next step when picked up: ce-brainstorm idea 8's scope boundary (what ships in the controller
vs the standalone strategies) into requirements.
