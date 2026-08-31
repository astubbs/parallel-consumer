# Scale-in proof: test the removal before Kubernetes removes anything

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - recombines the dimension-1 controller's existing measurement and actuation; no new machinery -->

From the follow-up Codex conversation, 2026-08-30 ~5:31pm (breakdown root:
[`core-engine-thesis.md`](core-engine-thesis.md)).

Everyone builds autoscaling around scale-out; the controller has already measured the thing
scale-*in* needs: how much useful capacity the workload actually requires. So run the
counterfactual before acting: tell the twelve instances' controllers to collectively behave as
though they had 11/12ths of their capacity, and watch throughput, residence and **backlog
trajectory** (the guard against "we only look fine because demand is low"). Healthy -> eleven
suffice; iterate downward. A deliberate experiment where `CPU < 30% for 10 minutes -> remove pod
-> hope` guesses. The machinery is entirely existing: admission constraint (astubbs#333),
residence (astubbs#359), backlog from Prescience-adjacent state, and the controller's
experiment-and-recover discipline.

**The deliverable is a metric, and it may sell better than throughput:**

```
CURRENT INSTANCES  12   ESTIMATED REQUIRED  7   PROVEN SAFE  8   OVERPROVISIONING ~33%
```

- "Proven safe" is load-bearing vocabulary: the number was *tested*, not modelled - the same
  experimental-evidence differentiator as
  [`core-bottleneck-attribution.md`](core-bottleneck-attribution.md).
- Per-function **instance-equivalents** (orders 3.2, payments 1.1, email 0.4) fall out of the
  per-function layer ([`core-per-function-capacity-arbitration.md`](core-per-function-capacity-arbitration.md)),
  giving **cost attribution by Kafka function** - "fraudCheck accounts for 41% of this
  application's execution capacity" - which for large Kafka estates is plausibly more
  commercially valuable than another 2x benchmark, and feeds the economics track
  ([`perf-benchmark-cost-to-slo.md`](perf-benchmark-cost-to-slo.md)).

Caveat to keep: a scale-in proof is valid for the traffic it ran under. It must state its window
and confidence ("proven safe at Tuesday-morning load"), and the seasonality dimension of
[`core-capacity-fingerprinting.md`](core-capacity-fingerprinting.md) is what stops an overnight
proof licensing a Black-Friday removal.
