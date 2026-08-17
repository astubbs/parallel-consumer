# Auto-scaling - self-discovered concurrency, and instance-count recommendation

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
  Each instance converges independently, so instances legitimately run *different* concurrency
  depending on the data in their assigned partitions. No coordination substrate (no Redis/quota
  tokens) for v1 - dynamic self-assessment only. Design to the optimal downstream (services
  that return rate-limit-exceeded, surfaced via a structured engine-recognised exception - which
  also frees users from computing their own retry intervals); degrade to inference from
  performance/failures when downstreams communicate nothing.
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

**The aggregation problem (dimension 2, open).** Each instance sees only its own performance,
so per-instance "suggested total" numbers will conflict. Candidate resolutions, simplest first:

- **Don't ask instances for a total at all** - an instance genuinely cannot know the right
  global count; it only knows its own state. Have each expose a local signal instead: a delta
  vote (+1 "plateaued locally and still behind" / 0 / -1 "underutilized") that infrastructure
  sums, or a saturation/headroom gauge that HPA's own algorithm already aggregates by
  averaging across pods (`desired = ceil(current x metric/target)`) - convergence for free, no
  leader, no new channel.
- If a single agreed number is ever needed: the group already has a leader and a data channel -
  the partition-assignor `userData` protocol (catalogued in the ideation doc's rejection table
  as the deferred lease-allocator) lets members ship demand up and the assignment leader ship
  one decision down, fenced by generation. Kafka-Streams-style control topics are a heavier
  alternative; later phase either way.
- If operators aggregate raw totals anyway: median over mean (outlier-resistant), never max.

**Earmarks.** The partition-count cap is per-subscription today; it must become the sum across
subscribed topics once per-topic processing functions land (astubbs#254 / confluentinc#372;
related: astubbs#245 runtime subscription change, astubbs#236 topic priorities). Separately,
Kafka share groups (KIP-932) relieve the partitions-cap constraint at the protocol level - a
share-group-aware PC could recommend instance counts beyond partition count; note for the
positioning story, not v1. This whole feature is candidate STRATEGY.md material ("the engine
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

Next step when picked up: ce-brainstorm the per-instance controller (dimension 1 only) into
requirements; instance-count recommendation is a follow-on with its own note when dimension 1
lands.
