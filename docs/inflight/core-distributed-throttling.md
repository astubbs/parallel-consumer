# Distributed throttling - ideation done, direction not chosen

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - after v6, direction not yet chosen -->


The ask is astubbs#228 (mirror of confluentinc#24), with confluentinc#766 as the production
demand evidence - and it converges with astubbs#227 (mirror of confluentinc#21, dynamic
concurrency control), the fork's oldest open self-tuning ambition. Ideation ran 2026-08-17,
extended 2026-08-18; eight ranked, code-verified directions:
[`docs/ideation/2026-08-17-distributed-throttling-ideation.html`](../ideation/2026-08-17-distributed-throttling-ideation.html)
(branch `feats/ideate-distributed-throttling` until merged). Read that before restarting - it
holds the bases, the rejection table, and the prior-art autopsies. Related abandoned branches
(`features/rate-limiting`, `features/dynamic-concurrency-control`, `feature/auto-tuning-pressure`,
plus upstream draft PR confluentinc#22) are now catalogued in `docs/refactoring.md`'s idea bank.

Auto-scaling (astubbs#227) now has its own note - [`core-auto-scaling.md`](core-auto-scaling.md) -
split 2026-08-18 because the two efforts will reach mergeable PRs at different times and carry
distinct prototype trails. Idea 8 in the ideation doc is the shared convergence record.

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

**Addition from the owner, 2026-09-03: staggered division - a grant gains a phase.** Four
tokens a second, spread as one every 250ms instead of four at the top of the second; each shard
assigned its own slot; random jitter *within* the slot so shards sharing one do not align.

**ANSWERED 2026-09-05: Google Doorman has shipped this mechanism for years, and its design document
is the most useful thing this note could read.** Renewable time-bounded capacity leases, vended to an
*embedded client library* that then decides in-process with no per-call permit server, with a
configured or server-computed safe capacity to fall back on when the vendor is unreachable - verified
in Doorman's source, not merely its prose. It is archived read-only since 2024-11-29 and was
self-described alpha, so it is an answered question rather than a live competitor; prior art does not
expire. **Treat it as a free design review**: it already settled lease expiry semantics, refresh
intervals, and what a client does when the vendor is gone, and it states plainly that a cooperative
system offers no protection against misbehaving clients.

The conservation-law safety bias sits inside that same explored space rather than outside it -
Doorman makes the equivalent choice a per-client configuration option and ships an explicitly
contract-violating *optimistic* mode beside it, while DRL's shipped designs bias toward
over-admission under partition. The strict corner is unoccupied, and both papers explicitly defer the
adversarial analysis where it would matter.

**Envoy RLQS is the citation this design should carry**, per the owner, 2026-09-05. Envoy's rate
limit quota service is independent evidence that a **delegated-credit resource plane is a sensible
architecture for high-performance distributed quotas** rather than an odd invention - which is a
different and more useful kind of prior-art finding than "somebody got here first". It says the shape
is validated. Unverified in this repository; recorded as `claimed` in
[`core-hasten-adjacent-systems-register.md`](core-hasten-adjacent-systems-register.md) until someone
reads the primary source.

**What is unanswered is the substrate** - carrying divisible leases on a durable log, which Doorman
(server tree plus etcd), SIGCOMM 2007 Distributed Rate Limiting (UDP gossip), Kueue (API server) and
DBOS (Postgres) each miss on a different axis. And what none of them offers is any of it to a team
that already runs Kafka and did not set out to adopt a rate limiter.
[`core-hasten-adjacent-systems-register.md`](core-hasten-adjacent-systems-register.md) owns the
per-question state; the dated sweep owns the evidence.

Prior art, stated so no novelty claim escapes: **spreading one process's permits evenly is the
leaky bucket as a meter**, and Guava's `RateLimiter`, GCRA (redis-cell in the ideation's table) and
nginx's `limit_req` without `nodelay` all do it - it is thirty years old from ATM traffic shaping.
**What none of them can do is coordinate the *phase* across shards.** Four shards each smoothly
emitting one a second are collectively bursty - all four fire at :000 - and assigning shard *k* to
slot *k* is time-division multiplexing, which no limiter product offers because none of them owns
the shards. The enforcement fork above already names the mechanism: idea 2's per-shard gate with
`availableAt` deferral *is* pacing once the `availableAt` values are spread rather than clustered
and the shard owner hands each shard its offset. Round-robin allocation (KTD9 in the language-proxy
plan) already staggers in *sequence*; giving each position a time is the whole addition. Jitter's
job falls out of the same picture: with more participants than slots, the ones sharing a slot need
spreading, and *bounded* jitter keeps them inside it - the grant-path sibling of idea 7.

The interval rule, settled with the owner: **never pace faster than you can serve** - the interval is
the larger of `1/limit` and the observed service time. That subsumes "pace only when the limit
binds": when our own capacity is the constraint the natural rate is already under the limit and the
rule is inert, which is the min-composition of ceilings restated with time. Two riders: a resource
that declares burst tolerance may be granted up to it within a slot, so pacing is the default only
for resources whose window shape is unknown; and jitter is uniform within the slot band, never
across it.

**Where the value actually lands, and what it costs.** A downstream token bucket *tolerates* bursts
up to its bucket size - Stripe at 100/s with burst 100 is content with 100 at :000 - so smoothing
gains nothing there. It wins against sliding-window downstreams, where an edge burst (four at :900
and four at 1:000) reads as eight-in-a-second and is refused; against concurrency-limited or
latency-sensitive downstreams, where arrival smoothness *is* capacity; and for our own aggregate
honesty. It *costs* the burst exploitation the adaptive controller was designed to find (idea 8's
rationale). So it is a per-resource option, not a default, and it is the first grant property that
belongs to the resource contract rather than to the caller.

**The generalisation is the part that reaches everything.** Pacing is not a rate-limiter feature
here; it is an eligibility predicate with a time -
[`core-scheduled-intent.md`](core-scheduled-intent.md) already says retry backoff is "an admission
predicate, nothing more", and a paced grant is the same predicate on the grant. **A credit becomes a
time-slot allocation, and rate limiting becomes scheduling** - the third axis on a grant after
hard-ceiling-versus-overshoot and batch-of-one-versus-N
([`core-non-kafka-participants.md`](core-non-kafka-participants.md)). What it costs each client
shape: inside PC, nothing new, the scheduler already holds work for eligibility; for the paced
persona, nothing at all - the trigger stream *is* the pacer and this is what its schedule looks
like; for a delegated-credit client, enforcement moves client-side in the Guava shape, with only the
phase offset coming from the shard owner.

Idea 7 (decorrelated retry jitter) is independently shippable and needs none of these.
Next step when picked up: ce-brainstorm idea 8's scope boundary (what ships in the controller
vs the standalone strategies) into requirements.
