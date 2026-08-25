# Adaptive concurrency: what astubbs#333 leaves open

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

The controller landed opt-in and off by default on astubbs#333; the control-law rewrite on this
branch then closed items -1, 0, 1, 2, 3 and 5 of what this note used to track - the open actuator
loop, the missing objective, the probe cycling, the self-contaminated baseline, the missing
`starved` vocabulary, and `maxConcurrency` under virtual threads.
[`docs/plans/2026-08-24-003-feat-admission-control-law-design.md`](../plans/2026-08-24-003-feat-admission-control-law-design.md)
is the authority on how (each item is named where its answer is designed); the law and its tests
carry the why in javadoc; `admission-gradient2-port` tags the point to return to if steering on
throughput proves worse. This note keeps only what is still open.

## Merge prep: this PR gets a Codex review (owner directive, 2026-08-25)

Before this PR merges, run a **Codex review** in addition to the usual `@claude review this` gate -
a cross-model review catches what a same-model review is structurally blind to, and this work (a
control law with falsifier-backed behaviour claims) is exactly the complexity tier it is reserved
for. The Codex plan is small, so it is spent strategically, not routinely; this PR qualifies.

## Nothing has measured whether it helps on real hardware

The value claim - lower end-to-end latency at a given arrival rate, or a higher sustainable arrival
rate, against a static guess - is only measurable below saturation, on the arrival-controlled
harness, on real hardware. The adaptive arm lands on its own branch cut from that harness rather
than here, so the two can merge independently. Until it produces a result, the roadmap entry stays
short of `implemented`: that ladder reserves it for work proven in use.

**This does not gate the law work, and the change of gate was deliberate** (2026-08-24): the
ratchet was a correctness defect, and a correctness fix is not held hostage to a value measurement.
What the measurement still gates is any **published claim** - the design's sequencing section owns
that, including why the weak form (*beats a badly chosen static number*) is not worth publishing.

Two things shape what that measurement should expect. Every calibration constant was chosen against
simulation plus the CI broker, not real hardware - re-fit against measured curves before trusting a
number. And the in-container three-arm comparison (`AdaptiveConcurrencyComparisonIT`) already shows
the adaptive arm beating the hand-tuned static on every strict phase; the bench arm's job is the
same comparison where the numbers are publishable.

### The comparison that makes the point (owner, 2026-08-23)

Run a workload that can clearly go faster than it is being allowed to, with the core engine pinned
at a static concurrency of twenty. Then run it again with adaptive turned on, and report the
difference. The result is not in doubt; the point is having the number, and it transfers to every
alternative product, none of which can do this at all because none of them own the dispatch
decision.

Worth strengthening before it is published: a deliberately-low static arm makes the claim true but
easy, and the obvious rebuttal is *so tune your config*. Run a THIRD arm - static, hand-tuned to the
best value a careful operator would find - so the claim becomes the one that actually survives
scrutiny: adaptive matches a hand-tuned configuration without the hand-tuning, and beats it the
moment conditions move away from whatever that tuning assumed. The second half is the real product
argument, and it needs the workload to change partway through the run to show it.
