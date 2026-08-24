# The adaptive-concurrency trust pack: documentation and demonstrations good enough to be believed

<!-- inflight-type: feature -->
<!-- inflight-impact: process -->

Owner's directive, 2026-08-25: **the architecture and user-feature documentation will need to be
genuinely good to get people to trust this** - along with test demonstrations, and a demo tuned for
humans to run themselves. Adaptive concurrency asks users to hand a production knob to a controller;
nobody does that on a claim, and a trust-rebuilding fork gets one chance at the first impression.

Three deliverables, distinct jobs:

1. **Architecture and user-feature documentation.** What the controller does, what it optimises,
   what brakes it, what the gauge's constraint values mean, and what the operator gives up
   (the default configuration's brake is the throughput plateau band, not any latency bound - the
   plan's KTD7 consequence). The feature-docs half is the law plan's U11 obligation; this note holds
   the bar it has to clear: an evaluating operator should be able to predict what the controller
   will do to their workload before enabling it.
2. **Test demonstrations.** The falsifier suite is the proof layer, but proofs convince reviewers,
   not evaluators. The demonstrations worth surfacing in docs: the ablation pair (the old law
   walking 20 to 60 on a flat plateau; the new law stepping once and retracting to the knee), the
   descent walk (50 to 20 in accelerator steps, each kept because throughput held), and the
   comparison IT's phase table. Real numbers from named tests a reader can re-run, cited by test
   name - never prose claims.
3. **A human-runnable demo, tuned to show it** - the comparison IT's shape (three arms, load that
   moves mid-run, the adaptive arm visibly backing off and growing back) packaged so a human can
   run it themselves and watch, not parse a surefire report. One command, a live view of target
   versus throughput per arm. The [`web-three-reveal-demo.md`](web-three-reveal-demo.md)
   centrepiece (1 partition, 10k keys - the architecture demonstrating itself) is the bigger
   sibling and stays its own item; this one is narrower: the adaptive claim specifically, runnable
   the day the feature ships rather than gated on the full demo programme. The existing
   `AdaptiveConcurrencyClosedLoopIT` and `AdaptiveConcurrencyComparisonIT` are the raw material -
   what is missing is the packaging and the tuning-for-watchability.

Related, not duplicated here: [`docs-content-series.md`](docs-content-series.md) (the investigations
series carries these numbers outward once they exist), [`core-auto-scaling.md`](core-auto-scaling.md)
(the feature's umbrella), and the graduation ruling - on-by-default when proven - which is what this
pack exists to earn.
