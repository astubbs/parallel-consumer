# The bench harness and the language-proxy demos are measuring the same thing twice

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

Owner's call, 2026-08-25. **Two workstreams built workload generators, engine arms and result
rendering independently, and they are tightly related** - they must be reconciled rather than left
to drift into two vocabularies for one idea.

- **The benchmarking work** (`perf/bench-harness-and-results`, astubbs#362): eleven arms, controlled
  arrival, a work model (`BENCH_DELAY_P99`, `BENCH_FAILURE_RATE`), key distributions, three latency
  families, and committed CSV results.
- **The demonstration work** (the language-proxy descendants - the per-language demos astubbs#331,
  the uber demo astubbs#332, and the comparison demo astubbs#328): the same shape of workload, run
  to be *watched* rather than recorded.

## Why this matters rather than being tidiness

- **A demo that produces a number nobody can reproduce is the failure mode this project just spent a
  week undoing.** The demos should generate their load through the harness's work model, so what an
  audience sees on screen and what a results file says are the same experiment.
- **The bench already has the arms the demos need** - the Kafka Streams threading model, the plain
  consumer floor, the proxy path every language client runs on - and the demos already have the
  thing the bench lacks: a *view*. The three-reveal demo idea
  ([`web-three-reveal-demo.md`](web-three-reveal-demo.md)) is exactly a bench workload with a face.
- **Two work models will disagree.** The bench's took four defect-fix rounds to become honest (call
  sites missed, per-thread RNG, percentiles rendered after teardown). A second implementation
  written for the demos will rediscover those, silently, in front of an audience.

## What reconciliation would mean, concretely

Not yet decided - the open question is which direction the dependency runs. Candidates: the demos
consume `bench/` as a load-generation library; or the shared parts (work model, key distribution,
arrival schedule, result schema) are extracted so both consume them; or the demos keep their own
thin driver and only the *definitions* are shared. Whoever picks this up should read
[`docs/solutions/best-practices/benchmark-a-rival-on-the-semantics-it-actually-offers.md`](../solutions/best-practices/benchmark-a-rival-on-the-semantics-it-actually-offers.md)
first - the method is the part worth preserving in either direction.

**Sequencing:** both sides are in flight as PRs, so this is a post-merge reconciliation, not a
blocker on either. Do it before the demos are used for anything public, because that is the point
at which a number from one and a claim from the other start being read as the same evidence.
