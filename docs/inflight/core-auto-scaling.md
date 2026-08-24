# Auto-scaling - self-discovered concurrency, and instance-count recommendation

<!-- inflight-type: feature -->
<!-- inflight-impact: throughput -->

> **Status, 2026-08-24: dimension one is no longer deferred and its direction is chosen.** The
> per-instance controller is built and landed opt-in and off by default on astubbs#333 - admission
> is the control variable rather than the pool, the control-law math is a native port of Gradient2
> with attribution, and it runs against a real broker across a rebalance. The state tag that used to
> sit here said *deferred - after v6, direction not yet chosen*, and both halves of that had become
> false.
>
> **Dimension two remains deferred and genuinely direction-unchosen** - see the staging below, which
> still stands as written for the instance-count half.
>
> What dimension one still owes is tracked, item by item, in
> [`pr-333-adaptive-concurrency-outstanding.md`](pr-333-adaptive-concurrency-outstanding.md); the
> capabilities it should grow next are in
> [`core-adaptive-concurrency-future-modes.md`](core-adaptive-concurrency-future-modes.md). The
> headline debt is that the control law has **no fixed point below the ceiling**: its reference is
> relative with no anchor, so on any workload that degrades gracefully the target walks upward
> indefinitely. That is the difficulty the owner reports having hit repeatedly when first attempting
> this, years before this attempt - it is the hard part of the problem domain, not a defect
> introduced here.
>
> **Two plans now carry dimension one's remaining work, deliberately split** (2026-08-24). They were
> drafted as one change and the shape did not survive review, because stopping the climb and knowing
> where to stop are separable problems:
>
> - [`docs/plans/2026-08-24-001-feat-admission-ratchet-plan.md`](../plans/2026-08-24-001-feat-admission-ratchet-plan.md)
>   - **implementation-ready.** Kills the ratchet by excluding samples taken while the engine was
>     starved rather than saturated, and by making the latency baseline falsifiable instead of
>     self-referential. Reports ordering starvation on the way. Adds no operator-facing parameter.
> - [`docs/plans/2026-08-24-002-feat-admission-optimisation-objective-plan.md`](../plans/2026-08-24-002-feat-admission-optimisation-objective-plan.md)
>   - **requirements-only on purpose.** Answers *what is the controller optimising* - elasticity
>     against a threshold, with a latency number as a ceiling and never a target - but carries
>     seventeen unresolved questions and is gated on the measurement nobody has taken yet, whether
>     the controller helps at all.
>
> The order matters and is the finding worth carrying: **an objective is what makes the controller
> useful; it is not what stops it climbing.**


The ask is astubbs#227 (mirror of confluentinc#21): stop making users pick `maxConcurrency` -
it is wrong in both directions (too low silently wastes headroom, too high floods downstreams,
confluentinc#766) and the right value depends on the runtime data in the assigned partitions,
which is unknowable at compile time. Priority raised 2026-08-18: with key-ordered concurrency,
this is a candidate killer feature - no known competitor does runtime-discovered, per-instance
adaptive concurrency. Split from [`core-distributed-throttling.md`](core-distributed-throttling.md)
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

## Measured evidence for the premise (2026-08-20)

[`perf-throughput-regression-since-0-3.md`](perf-throughput-regression-since-0-3.md) produced numbers
that bear directly on this design, from a repeatable harness (`bench/run-bisect.sh`):

- **The knee is real and neither obvious bound finds it.** On one workload, `maxConcurrency` 100 gave
  ~16,300 msg/s, 1,000 gave ~28,300, and 10,000 gave ~23,800 - non-monotonic, with observed peak
  in-flight saturating around 340-420 regardless of the ceiling. Too high is not merely wasteful, it
  is *slower*, which is the silent failure mode this feature exists to remove.
- **The existing adaptive mechanism is not doing the job.** `DynamicLoadFactor`'s stepping was
  measured to contribute nothing beyond its initial constant of 2 on that workload, and
  `ExternalEngine` disables it outright - so Vert.x, Reactor, Mutiny and the proxy have no adaptive
  element at all. astubbs#155 (`confluentinc#402`) separately reports it pegging at 100/100. A
  controller that is either inert or saturated is not regulating.
- **`messageBufferSize`, the documented manual escape hatch, silently does nothing on the external
  engines**, because it configures the load factor that `ExternalEngine` never reads.

Read as: the manual knob is hard to set, the adaptive mechanism that exists does not work, and the
documented workaround does not apply to four of the five engines.

## Inline execution: the bottom of the adaptive-concurrency range

**Antony, 2026-08-22, on why PC is ~17% behind a bare Go consumer at 0ms delay: "would this be
something we'd slide into the internal auto scaling work? it could be a reasonable workload when pc is
running in ks and doing in memory stuff."** Yes to both, and the second point is the one that makes it
worth building.

**What it is.** When the user function is consistently fast and nothing is backpressured, run it
**inline on the polling thread** instead of dispatching it. PC's per-record cost with an empty user
function is roughly a dozen data-structure operations and **two thread handoffs** - register, shard
insert, occupancy add, population admit, select, claim CAS, submit, run, mailbox return, control-loop
drain, state transitions, shard remove, offset accounting. A bare consumer pays none of it. That cost
only earns its place when the user function is slow enough to amortise it, and at 0ms there is nothing
to amortise.

**Why it belongs in the auto-scaling work (astubbs#227) rather than beside it.** Adaptive concurrency
already has to measure user-function duration and react to it. "The function is fast enough that
concurrency is costing more than it returns" is simply **the bottom of that control range** - the same
input, the same loop, one more decision. Built separately it would be a second thing observing the
same signal and deciding on its own, which is the state-duplication shape this codebase keeps paying
for.

**And the workload is real, which is the part I initially got wrong.** I dismissed this as optimising
a case that does not exist in production. It does: **a Kafka Streams operator doing in-memory work** -
a filter, a map, a projection, a local-store lookup - is genuinely sub-microsecond, and that is
precisely the topology PC would be running under
[`next-what-kafka-streams-on-pc-is-worth.md`](next-what-kafka-streams-on-pc-is-worth.md). A Streams
topology is a *chain* of such operators, most of which do nothing expensive; paying a dispatch per
record per operator would be absurd.

**The catch, and it is what makes this a control problem rather than a flag.** Inline execution
happens on the polling thread, so a record that turns out to be slow blocks polling - and therefore
every other partition - for its duration. That is head-of-line blocking, reintroduced by the
optimisation, which is the exact thing PC exists to remove. **So it must be reversible the instant a
function stops being fast**, which needs the same hysteresis, the same measurement window and the same
safety margin the load-factor work needs. It is not a switch.

**Measure the prize before building it**: the gap it would close is ~17% at 0ms delay, narrowing to
9% by 50ms and gone by 100ms. That is PC's worst case by construction - the one operating point where
its entire reason for existing is switched off. Under a Streams topology, though, that operating point
is the common case rather than the pathological one, which is what changes the answer.

