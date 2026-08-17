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
