# Auto-scaling - self-discovered concurrency, and instance-count recommendation

<!-- inflight-type: feature -->


The ask is astubbs#227 (mirror of confluentinc#21): stop making users pick `maxConcurrency` -
it is wrong in both directions (too low silently wastes headroom, too high floods downstreams,
confluentinc#766) and the right value depends on the runtime data in the assigned partitions,
which is unknowable at compile time. Priority raised 2026-08-18: with key-ordered concurrency,
this is a candidate killer feature - no known competitor does runtime-discovered, per-instance
adaptive concurrency. Split from [`next-distributed-throttling.md`](next-distributed-throttling.md)
because the two will likely reach mergeable PRs at different times and carry distinct prototype
trails; idea 8 in the shared ideation doc
([`docs/ideation/2026-08-17-distributed-throttling-ideation.html`](../ideation/2026-08-17-distributed-throttling-ideation.html))
is the convergence record.

Two dimensions, deliberately staged:

- **Per-instance concurrency (build first, easier).** Adaptive controller steps concurrency up
  until measured performance degrades or failures rise, then contracts - TCP-congestion shape.
  **The cause of the plateau never needs diagnosing**: host too slow, Kafka fetch bandwidth,
  downstream saturation - the controller reacts identically, which is exactly why runtime
  discovery beats compile-time configuration (the operator cannot enumerate the causes either).
  Each instance converges independently, so instances legitimately run *different* concurrency
  depending on the data in their assigned partitions - the right value is a function of the
  runtime data, unknowable ahead of time, and it changes as the data changes. No coordination
  substrate (no Redis/quota tokens) for v1 - dynamic self-assessment only. Design to the
  optimal downstream (services that return rate-limit-exceeded, surfaced via a structured
  engine-recognised exception - which also frees users from computing their own retry
  intervals); degrade to inference from performance/failures when downstreams communicate
  nothing.
  **Relationship to rate limiting, resolved**: dimension 1 ships with no rate-limiting
  infrastructure and no rate config - the controller consumes *ceilings as inputs*
  (min-composition, ideation idea 5/8), not a distributed substrate. The genuinely shared
  pieces are the enforcement seam (ideation ideas 2/3) and the structured exception; design
  the SPI together, ship the features independently.
- **Instance count (later, harder).** PC never spawns instances itself (rejected: overlaps
  provisioning/infra, messy). Instead PC *recommends*: expose a suggested-total-instance-count
  metric that infrastructure (HPA/KEDA external metrics is the natural consumer) acts on. PC
  owns the hysteresis: raise the recommendation only after a sustained cool-down (fixed ~5min,
  or dynamic from observed variance) of "at my sustainable max concurrency, more instance-local
  concurrency does not help but the workload is still behind". Same in reverse to shrink.
  Rebalance is the natural acknowledgement - PC observes whether the recommendation was acted
  on via group membership change. Cap the recommendation at partition count (instances beyond
  it are idle by construction - the confluentinc#766 topology). This beats lag-based
  autoscaling, which flaps and scales uselessly when the bottleneck is downstream, because the
  metric encodes "another instance would actually help", not "we are behind".

**Dimension-2 signal design (settled 2026-08-18): local delta vote, no global number.** An
instance genuinely cannot know the right global count - it only knows its own state - so no
instance ever emits a "suggested total". Each exposes a delta vote, **deliberately clamped to
+1 / 0 / -1** even when an instance believes more would help - bounded small steps are how the
fleet converges without oscillation, AIMD-style (+1 "plateaued at my sustainable local
concurrency and still behind" / -1 "underutilized" / 0 otherwise). Infrastructure sums the
votes (or HPA averages an equivalent headroom gauge - `desired = ceil(current x metric/target)`
- convergence for free, no leader, no new channel).
Lifecycle rules:

- **Post-rebalance cooldown, per instance.** Any membership or assignment change invalidates
  every instance's assessment - new partition set, new data mix. After a rebalance each
  instance votes 0 while it re-converges: run for a while, re-assess its own performance and
  the jitter/variance in its records, only then resume voting. This also makes the
  acknowledgement loop implicit: infrastructure acts -> rebalance -> everyone cools down ->
  fresh votes reflect the new topology. No explicit ack protocol needed.
- **All cooldown windows are dynamic by design**, derived from the fluctuation observed in the
  instance's own recorded metrics - high jitter/variance means a longer window before trusting
  an assessment, steady metrics mean a shorter one. A fixed value (~5min) is only the fallback
  floor, not the mechanism. Applies to both the post-rebalance window and the pre-vote window
  (raising a vote away from 0), so a transient burst does not summon an instance.
- Later phase, only if ever needed: the partition-assignor `userData` leader channel
  (catalogued in the ideation doc's rejection table as the deferred lease-allocator) could
  compute a coordinated decision - but the delta-vote design likely makes it unnecessary.

**Earmarks.** The partition-count cap is per-subscription today; it must become the sum across
subscribed topics once per-topic processing functions land (astubbs#254 / confluentinc#372;
related: astubbs#245 runtime subscription change, astubbs#236 topic priorities). Separately,
Kafka share groups (KIP-932) relieve the partitions-cap constraint at the protocol level - a
share-group-aware PC could recommend instance counts beyond partition count; note for the
positioning story, not v1. And the pairing spark from 2026-08-18: auto-scaling inside a Kafka
Streams topology - astubbs#255 already tracks giving a Streams topology PC's per-key
parallelism, and a self-tuning PC under a Streams operator would bring runtime-discovered
concurrency to the ecosystem's own processing engine; far future, but the two features
multiply. This whole feature is candidate STRATEGY.md material ("the engine
every language re-implements badly" positioning) - fold in via ce-strategy when direction is
confirmed.

Prior art (design references, bitrotted - catalogued in `docs/refactoring.md` idea bank):
`features/dynamic-concurrency-control` @6f85eac41 (Netflix concurrency-limits Gradient2Limit as
the worker pool, auto-scale module extraction started, README section written) and
`feature/auto-tuning-pressure` @f4aa09788 (hand-rolled self-tuning of backpressure; the
`DynamicLoadFactor` no-step-down gap is its fossil). Upstream draft PR confluentinc#22
@ba6b71f10 has DIVERGED from the fork branch - see astubbs#305's map note. Prerequisite:
async-engine timing metrics are inaccurate under Vert.x (confluentinc#766) - the controller's
signal integrity depends on fixing that first.

**Positioning (safekeeping until the ce-strategy run; keep depersonalised - no vendor
commentary).** The consumer-side programming model has barely evolved in a decade: every team,
in every language, gets a poll of records and must then answer "how do I process these quickly,
in order, with retries?" - and re-implements that engine, usually badly, usually per project.
Recent protocol work (share groups, KIP-932) changes *delivery* semantics but leaves the
processing engine unaddressed; the evolution the consumer model has been waiting for is at the
engine layer, and PC is the working demonstration. The same universality applies to scaling:
even without PC, every operator must guess an instance count, the guess is workload-dependent,
and the workload changes at runtime. External autoscalers treat the consumer as a black box
(consumption lag is the state of the art - it cannot distinguish "more instances would help"
from "the downstream is the bottleneck"); an engine that lives inside the processing loop can
scale from per-record ground truth. Key-ordered concurrency + runtime-discovered scaling, with
bindings for every language, is the "client as engine" story - candidate for the strategy doc's
core positioning.

Next step when picked up: ce-brainstorm the per-instance controller (dimension 1 only) into
requirements; instance-count recommendation is a follow-on with its own note when dimension 1
lands. Branch plan: this docs branch merges as one unit; implementation work starts in its own
worktree from master afterwards (do not stack implementation on a docs branch).
