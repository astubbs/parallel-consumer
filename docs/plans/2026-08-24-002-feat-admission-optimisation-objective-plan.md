---
title: An Optimisation Objective for Admission - Plan
type: feat
date: 2026-08-24
topic: admission-optimisation-objective
artifact_contract: ce-unified-plan/v1
artifact_readiness: requirements-only
product_contract_source: ce-plan-bootstrap
execution: code
---

# An Optimisation Objective for Admission - Plan

## Goal Capsule

- **Objective:** Give the admission controller something to optimise *for*, so it settles at a defensible operating point rather than merely stopping somewhere. Today its objective is relative - keep latency near its own average - which means it can never say *that latency increase was not worth it*, because it never measures throughput at all.
- **Readiness: requirements-only, deliberately.** The design below is settled. It is **not** implementable yet: a five-reviewer pass found roughly a dozen unresolved defects, listed in full under Resolve Before Planning. Enriching this to implementation-ready means answering those, not writing units around them.
- **Depends on:** `docs/plans/2026-08-24-001-feat-admission-ratchet-plan.md`, which supplies the per-window throughput measure and the starved/clean classification this objective needs, and which fixes the ratchet independently. **That plan is not blocked by this one** - the split exists precisely so the ratchet fix ships without waiting.
- **Gated on:** a measured result from the benchmark arm (`docs/inflight/pr-333-adaptive-concurrency-outstanding.md` item 4). Nothing here should be built while it is unknown whether the controller helps at all.

---

## Product Contract

### Summary

Replace the relative objective with an absolute one: grow while additional concurrency still buys proportionally more throughput, stop when it does not. The operating point becomes a stated choice rather than an emergent accident.

### Problem Frame

Fixing the ratchet stops the controller climbing forever. It does not tell it where to stop. Without an objective, where it settles is a by-product of the brake's tolerance - defensible only by accident.

The owner's question, and the reason this exists: *if service time rises from 20ms to 30ms, is that bad, or good because throughput went up?* The current law cannot answer, because it has no notion of worth, only of change. Three answers are defensible - optimise throughput, optimise latency, or find the point that balances them - and choosing one is a product decision, not an implementation detail.

### Key Decisions

- KD1. **The objective is throughput elasticity against a threshold, not a peak search.** Grow while `d ln(throughput) / d ln(concurrency)` exceeds `1/(r+1)`; contract below it. This is Kleinrock's power generalised - his tangent condition in log form - and it subsumes the family: `r=1` is textbook power, and the idealised utilisation it targets is `r/(r+1)`. (session-settled: user-approved - chosen over peak-seeking on power directly: at a peak the derivative is zero, so peak detection is second-order on a curve that is flat by construction, while the elasticity residual crosses zero with non-zero slope. Same measurements, better conditioning.)
- KD2. **`r = 3`: elasticity threshold 0.25, idealised utilisation ~75%.** (session-settled: user-directed - chosen over textbook power's `r=1`, which targets 50% and would cap throughput near half of what a downstream can deliver; and over the ~85% implied by the .NET thread pool's shipped bias. 75% has independent precedent in Flink's and Dataflow's default utilisation targets, though see the caveat in Resolve Before Planning about how far that precedent transfers.)
- KD3. **Power is computed from throughput and in-flight, never from latency.** Little's Law makes power proportional to throughput squared over mean in-flight identically, so a measured latency term carries no information the other two lack and only adds a noisy denominator.
- KD4. **A stated latency number is a ceiling, never a target.** A target presumes achievability: chase 20ms against a 50ms floor and the controller contracts to a single slot, destroys throughput, never arrives, and looks correct from the inside because it is obeying its objective. It also presumes a cliff where operators hold a tolerance region. An unreachable ceiling is *reported as a binding constraint*, not pursued. The same argument applies to a throughput floor.
- KD5. **Estimation is by dither and demodulation, not consecutive-window differencing.** A small perturbation correlated against the throughput response, with the noise floor from adjacent bins. Consecutive-window comparison attributes every drift to the last action. (session-settled: user-approved - the .NET thread pool ships exactly this design for exactly this problem.) **Carries the largest share of the unresolved defects below.**

### Requirements

- R1. The controller estimates how throughput responds to concurrency, and grows or contracts on that estimate against a configured threshold.
- R2. The operating point is a single parameter with a default, documented by the symptom that selects each position rather than as a bare number.
- R3. The objective path consumes throughput and in-flight only.
- R4. An optional latency ceiling brakes the objective; when it is below anything achievable, that is reported rather than pursued.
- R5. The perturbation's cost to a healthy workload is bounded and visible - an operator must be able to tell a by-design sawtooth from an unstable controller.

### Success Criteria

- The settled operating point sits at a stated position relative to the measured knee, and its throughput and latency are both recorded against the pre-change law's settled point. **The objective must be shown to be worth having, not merely harmless** - a criterion the prior draft lacked entirely.
- The controller remains live: growth and contraction both observed, with the estimate actionable in a stated minimum fraction of windows.
- Two instances against one shared downstream both settle, and neither suppresses the other's estimate below its actionable threshold.

### Scope Boundaries

Not here: the ratchet fix (its own plan, and it ships first); catch-up mode; rate-limit feedback; `maxConcurrency` under virtual threads - though note that a controller with a real objective needs a ceiling far less than one without, so that question should be revisited when this lands.

---

## Resolve Before Planning

Every item below is a defect a reviewer found in the prior draft, verified against the code. **This document stays requirements-only until they are answered** - they are design questions, not implementation detail, and writing units around them would produce exactly the plan that was just rejected.

### The estimator

1. **Where does the objective arm even sit?** The law evaluates six arms before the gradient. One holds whenever in-flight is below half the limit - precisely the under-filled windows the estimate must score. The ratchet plan deletes that arm and the starvation probe-up, which changes the landscape; this plan must be re-grounded against the law *as it is after* that change, not as it was.
2. **The elasticity denominator is undefined.** Commanded target, or achieved in-flight? They diverge exactly when it matters: if in-flight does not follow a raised target because the shards cannot fill it, a target denominator lies, and an in-flight denominator can approach zero and make elasticity explode or flip sign.
3. **The target is an integer slot count.** A "small" perturbation is at least one slot - 25% at a target of four, 3% at thirty-two. Sub-slot perturbations truncate to nothing and the demodulator correlates against a perturbation never applied.
4. **Clamping turns the square wave into a half wave.** At the ceiling the up-phase cannot be applied, so the correlator sees a systematic term that reads as real elasticity - a contract signal produced entirely by the clamp. Same at the floor.
5. **The controller's own action re-enters the dither band.** Growth steps are *caused by* the demodulator, so they are correlated with the dither by construction. If they land preferentially on up-phase windows, the controller injects energy at its own measurement frequency and biases the estimate upward - a ratchet returning through a new door. Randomised window length defends against poll periodicity, not against self-correlation.
6. **The window length is itself target-derived**, so perturbing the target modulates the measurement interval at the dither frequency - the one frequency the demodulator cannot separate from signal.
7. **The window series has deliberate gaps.** Under-sampled windows hold without touching state, cooldown windows are discarded, pause discards a window outright. Whether the dither advances across them, and whether they enter the correlator as gaps, zeros or a reset, are three choices with three different answers.
8. **A rebalance reseeds the target from whatever value is published at that instant** - including a dither down-phase, which would permanently lose those slots. Where the estimator's state lives decides whether a rebalance clears it or leaves it correlating across a discontinuity.
9. **The perturbation is unbounded.** Scaling amplitude with measured noise is the wrong sign: a larger swing raises throughput variance, which reads as more noise. And the down-phase is real lost admission on every cycle, forever, on a healthy workload.
10. **It is inert under observe-only mode**, where the published target steers nothing - so the estimate would be demodulated from a response that cannot exist, and the gauges would publish a fabricated objective on the mode whose entire purpose is measurement without acting.

### The proof

11. **A drift statistic passes maximally when the controller never acts.** Any guard - an unmet signal-to-noise minimum, a brake, a starvation hold - leaves the arm inert, drift reads zero, and the result is indistinguishable from success. Any falsifier here needs a liveness assertion that can fail independently.
12. **The simulation cannot confirm this objective.** It derives completions from modelled latency, so power there reduces to a function of the model's own constants and a power controller finds the knee regardless of its law. Any simulation result is a wiring check only.
13. **The contaminated-baseline gate cannot host the ablation** as-is: it drives a constant completion count at every limit, so throughput is constant by construction and elasticity is identically zero.
14. **Fleet behaviour is unexamined.** N instances each perturbing independently against one shared downstream present as noise to each other, and can either suppress every instance's signal below its acting threshold or correlate into an aggregate oscillation no single instance can see.

### The product surface

15. **The utilisation claim rests on assumptions this workload violates.** The `r/(r+1)` mapping is a queueing result whose conditions - general arrivals, finite buffers, burstiness, heterogeneous service cost - are a complete description of a Kafka consumer. The parameter can be documented as an aggressiveness setting defined by its elasticity threshold and observed behaviour; it cannot honestly be documented as a promised utilisation fraction.
16. **The default is defended against the wrong comparison.** `r=3` is argued against `r=1`, never against the operator's actual reference point: a hand-tuned static config that extracts near-full capacity. On a steady workload that config will out-throughput this and the operator will turn the feature off. The mitigating argument - adaptive wins when conditions move - is exactly the benchmark arm this is gated on.
17. **A new tunable with no symptom-keyed decision rule** is either inert or becomes folklore copied from a blog post. The operator it gets hardest for is the one the feature was meant to help.

---

## Sources / Research

- Kleinrock's power metric: the normalised definition, the tangent characterisation, the throughput-squared-over-in-flight identity, the generalised `r` family, and the conditions under which the clean results do not survive.
- BBR's app-limited handling and its separate windowed estimators; Envoy's minimum-latency recalibration; CoDel's minimum-over-interval.
- .NET thread pool hill climbing: dither with demodulation against an adjacent-bin noise floor, randomised sample interval, and a shipped operating point well above textbook power.
- Uber Cinnamon: the same baseline drift in production, fixed by a veto and bounds rather than an objective - the evidence that the ratchet fix and this objective are separable.
- Flink's true processing rate and backlog-suspends-utilisation; Dataflow's backlog-time signal and key-count ceiling; and the limits of transferring scale-out time constants to an in-process limit.
- In-repo: `docs/inflight/pr-333-adaptive-concurrency-outstanding.md` item 0 (the objective question and the ceiling-not-target ruling), `docs/inflight/core-adaptive-concurrency-future-modes.md` (the knee-versus-plateau distinction catch-up depends on).
