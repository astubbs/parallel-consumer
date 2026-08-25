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

## The provenance story - owner-approved copy for the feature docs (2026-08-25)

Antony's ruling: the lineage below is itself a selling point ("great in many ways") and must appear
in the user-facing feature documentation in substance - wording may be adapted to the docs' voice:

> The first version was a port of Netflix's open-source concurrency-limits library (the Gradient2
> algorithm, which watches latency ratios). Our falsifier harness convicted it - on perfectly flat
> load it ratcheted the target from 20 up to 60 for no benefit - so it was replaced with a
> clean-sheet design. The replacement is our own code, but the ideas are borrowed with attribution:
> the "does growth still pay" statistic is standard economics/engineering (elasticity, a log-log
> regression slope), and the probing discipline - you cannot see capacity you are not using, so
> periodically spend a little to look - is how Google's BBR congestion control and IETF RFC 7661
> solved the identical problem for network bandwidth. Envoy's adaptive concurrency and Uber's
> Cinnamon informed what to measure.

Two supporting facts worth stating alongside it, both verifiable from the code rather than claimed:

- The convicting run is a named, re-runnable test (the old-law arm of the falsifier harness in
  `AdmissionLawFalsifierTest`) - deliverable 2's real-numbers-not-prose-claims bar applies.
- The controller's memory is deliberately small and simple: a rolling list of one-line window
  summaries bounded by a wall-clock horizon (`AdmissionElasticityEstimator`), plus one standing
  verdict - kilobytes, no persistence, relearns from scratch on restart by design.

Research record with provenance caveats:
[`../plans/2026-08-24-006-research-controller-prior-art.md`](../plans/2026-08-24-006-research-controller-prior-art.md).

The promotional material must also carry the **Share Groups composability argument** - adaptive
concurrency is a killer addition *on top of* Share Groups, not a rival to them, because delivery is
not processing and the how-many-at-once question survives any delivery protocol.
[`next-what-survives-share-groups.md`](next-what-survives-share-groups.md) owns that argument
("Share Groups still hand you the parallelism problem").

Related, not duplicated here: [`docs-content-series.md`](docs-content-series.md) (the investigations
series carries these numbers outward once they exist), [`core-auto-scaling.md`](core-auto-scaling.md)
(the feature's umbrella), and the graduation ruling - on-by-default when proven - which is what this
pack exists to earn.
