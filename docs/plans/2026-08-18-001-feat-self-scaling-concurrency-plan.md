---
title: Self-Scaling Concurrency - Plan
type: feat
date: 2026-08-18
topic: self-scaling-concurrency
artifact_contract: ce-unified-plan/v1
artifact_readiness: requirements-only
product_contract_source: ce-brainstorm
execution: code
---

# Self-Scaling Concurrency - Plan

## Goal Capsule

- **Objective:** An opt-in adaptive controller that discovers each Parallel Consumer instance's concurrency at runtime, replacing the user's `maxConcurrency` guess with measured adaptation. This plan owns the per-instance controller only (dimension 1 of astubbs#227); instance-count recommendation, predictive scaling, and the distributed-throttling strategy work are not active scope.
- **Product authority:** `STRATEGY.md` Self-tuning track (priority raised 2026-08-18); astubbs#227 (mirror of confluentinc#21); ideation record `docs/ideation/2026-08-17-distributed-throttling-ideation.html` idea 8.
- **Open blockers:** none. Two early-investigation dependencies are named in How This Work Fits Together; neither blocks planning of this scope.

---

## Product Contract

### Summary

Add an adaptive concurrency mode to the core engine: when enabled, a controller governs the admission target (records in flight) from measured service time and failure signals, stepping up until performance degrades and contracting when it does, always beneath an effective maximum. The thread pool stays fixed at the cap; admission is the control variable.

### Problem Frame

`maxConcurrency` is a compile-time guess about a runtime quantity. Too low silently strands throughput nobody files an issue about; too high floods downstream services - the confluentinc#766 production shape. The right value depends on the data currently in the instance's assigned partitions, differs between instances in the same group, and shifts with time of day and workload - so no static number stays correct, and the operator cannot know the plateau's cause (host, Kafka fetch bandwidth, downstream capacity) even in principle. PC's own `maxConcurrency` javadoc has pointed at this gap since 2020, and two abandoned prototype branches show the intent without a landing.

### Key Decisions

- KD1. **Control admission, not the pool.** The controller adjusts the in-flight admission target; the worker pool sits fixed at the effective maximum. One control variable that already feeds both dispatch quantity and poller backpressure, and works identically on every engine. (session-settled: user-approved - chosen over runtime pool resizing: admission is the universal knob; async engines have no pool to resize.) Governs R2, R7.
- KD2. **Native implementation; port the control-law math with attribution.** Algorithms adapted from Netflix concurrency-limits (Gradient2/Vegas family) as ported, attributed code - not a dependency. (session-settled: user-approved - chosen over depending on the library: keeps the zero-dependency core; their interfaces do not fit min-composed ceilings or rate-limit signals.) Governs R3, R5.
- KD3. **Core engine only in v1.** The service-time signal is trustworthy on the core engine today; async engines (Vert.x/Reactor) time future creation, not completion, and follow after the async timing fix. (session-settled: user-directed - chosen over all-engines-at-once or a degraded failures-only mode: ship where signals are honest; investigate the async fix early so other engines follow quickly.) Governs R1.
- KD4. **Opt-in flag plus optional seed.** Adaptive mode is off by default (experimental). An optional initial-scale parameter seeds the starting point; when unseeded, the default errs aggressive rather than crawling up from nothing. (session-settled: user-directed - chosen over starting from minimum: a cold-start crawl wastes the deploy window.) Governs R1, R4.
- KD5. **Rate-limit signals are deferral instructions, never failures.** When the structured rate-limit exception (separate deliverable) lands, the controller consumes it without touching failure history or failure metrics. (session-settled: user-approved - chosen over reusing the failure path: throttling that reads as failure poisons error dashboards and burns retry attempts.) Governs R5.
- KD6. **Ceilings compose by minimum.** Effective limit = min(hard cap if configured, discovered per-service ceilings when available, adaptive capacity estimate) - and an effective maximum always exists (user cap, else a generous documented default). (session-settled: user-approved - chosen over a single authoritative limit source: complements compose safely; an unbounded controller violates the rip-out criteria.) Governs R2, R8.
- KD7. **The pressure-system issue stays with its own issue.** astubbs#155 (load-factor mis-firing warning; the original stall it reported is already fixed via the confluentinc#547/confluentinc#606 lineage and the fork's confluentinc#857-family work) is not scope here - but address it before or alongside this work, and adaptive mode must make its failure class impossible when enabled. (session-settled: user-directed - chosen over folding the fix into this plan: separate investigation, prominent call-out here.) Governs R6.

### Requirements

**Controller behavior**

- R1. Adaptive concurrency is an opt-in mode on the existing options surface, available on the core engine; when disabled (the default), behavior is byte-for-byte today's static `maxConcurrency` semantics.
- R2. The controller governs the admission target (records in flight), never exceeding the effective maximum; an effective maximum always exists - the configured cap, else a documented default ceiling.
- R3. Adaptation is bidirectional: step up while measured performance holds, contract on degradation or failure signals; the step-down path is a first-class requirement, not an afterthought.
- R4. The starting point is the user's optional initial-scale seed; unseeded, a documented aggressive default applies.
- R5. v1 signals are per-record service time (execution time, not queue-inclusive sojourn time) and failure rate; the signal intake is designed so the structured rate-limit exception (separate deliverable) can plug in as a third signal without controller rework.

**Safety and coexistence**

- R6. Enabling adaptive mode settles the relationship with `DynamicLoadFactor`: the controller and the load-factor heuristic must not adjust overlapping arithmetic concurrently - freeze, bound, or subsume it such that astubbs#155's failure class cannot occur while adaptive mode is on.
- R7. The admission target propagates through the existing dispatch-quantity and poller-pause arithmetic; a shrinking target must throttle intake rather than stall in-flight work.
- R8. Stability guardrails mirror the rip-out criteria: bounded ramp rate, no unbounded growth, and demonstrable ramp-up on an underutilized workload - each verifiable in tests.

**Observability**

- R9. The current discovered concurrency and the reason for its last movement are visible as metrics, following the existing metrics conventions; naming avoids the taken `RateLimiter` name and the overloaded term "throttle".
- R10. An observe-only mode computes and reports what the controller would do without acting - usable as the validation path before trusting enforcement.

### Success Criteria

- On the same workload, adaptive mode achieves lower average per-record latency and higher throughput than the operator's static guess - via reclaimed headroom (guess too low) or an unflooded downstream (guess too high).
- Instances in one group legitimately converge to different concurrency values reflecting their assigned partitions' data.
- Steady state is not expected; tracking changing conditions is the point. Failure looks like: runaway growth, too-fast ramp, or no ramp at all (the rip-out triad, covered by R8).

### Key Flows

- F1. Adaptation cycle
  - **Trigger:** controller evaluation tick while adaptive mode is enabled.
  - **Steps:** sample service-time and failure signals since the last tick; compute the new capacity estimate (ported control law); clamp to min(effective maximum, discovered ceilings); publish the admission target; dispatch and poller arithmetic consume it on their next pass; movement and reason recorded (R9).
  - **Outcome:** admission target tracks the workload; no worker thread blocks; disable restores static behavior (R1).
  - **Covers:** R2, R3, R5, R7, R9.

### Acceptance Examples

- AE1. **Covers R1, R4.** Given adaptive mode enabled with initial scale 10, when the instance starts, then admission begins at 10 and moves from there; with adaptive mode absent, behavior is identical to today.
- AE2. **Covers R3, R8.** Given a downstream that degrades above 30 concurrent calls, when the controller steps past it, then measured service time rises and the target contracts below 30 - and oscillation stays within the bounded ramp rate.
- AE3. **Covers R2, KD6.** Given a configured cap of 24, when the capacity estimate exceeds it, then admission holds at 24 and the estimate's excess is never dispatched.
- AE4. **Covers R10.** Given observe-only mode, when signals would move the target, then the movement is reported in metrics and actual admission stays at the static configuration.
- AE5. **Covers R6.** Given adaptive mode enabled, when the load-factor heuristic would previously have stepped (or mis-warned per astubbs#155), then only the settled owner of that arithmetic acts, and the astubbs#155 warning cannot fire spuriously.

### Scope Boundaries

Deferred for later (recorded future directions, not v1):

- Async engines (Vert.x/Reactor/Mutiny) - follow the async timing fix (early investigation, separate work).
- Instance-count recommendation (dimension 2) - staged design lives in `docs/inflight/core-auto-scaling.md`.
- Predictive scaling, both flavours: learned history (in-memory, best-effort first; external state optional later) and schedule-configured known events.
- Any distributed coordination substrate (no Redis/quota tokens in v1).
- The structured rate-limit exception API itself - separate deliverable; R5 only reserves its socket.
- The astubbs#155 fix itself - its own issue; R6 constrains this feature's interaction with that subsystem.

### How This Work Fits Together

<!-- ce-section: work-relationships -->

This plan owns the per-instance adaptive controller. The surrounding breakdown is the current understanding, not a committed roadmap:

- Async-engine timing fix (accurate completion-time under Vert.x/Reactor)
  - **Enables** extending this controller beyond the core engine. **Can proceed independently of** this plan; investigate early.
- Structured rate-limit exception API (serviceKey, retryAfter, deferral-not-failure semantics)
  - **Enables** per-service discovered ceilings (R5's third signal). **Can proceed independently** - it improves retry UX standalone.
- astubbs#155 load-factor hardening
  - **Shares** the buffer/pressure arithmetic this controller must coexist with (R6). Recommended to address before or alongside this work.
- Instance-count recommendation (dimension 2, `docs/inflight/core-auto-scaling.md`)
  - **Depends on** this plan: an instance can only vote +1/0/-1 once it knows its own plateau.
- Distributed throttling strategy menu (`docs/inflight/core-distributed-throttling.md`)
  - **Shares** the min-composition ceiling model (KD6) and the future SPI shape. **Still to decide:** whether the throttle ships standalone or as a signal into this controller.

### Dependencies / Assumptions

- Netflix concurrency-limits is assumed Apache-2.0 (verify at planning before porting any code; carry attribution per the fork's existing header discipline).
- Core-engine service-time accuracy confirmed in-repo: the user-function timer wraps the synchronous call directly.
- JDK `ThreadPoolExecutor` supports runtime resizing if a later need arises; v1 does not require it (pool fixed at cap).
- `DynamicLoadFactor` today is monotonic step-up with a parameterized range (default initial 2); its residual astubbs#155 defect is a mis-firing warning, not an active stall.

### Outstanding Questions

Deferred to planning:

- The unseeded aggressive default value, ramp constants, and evaluation tick cadence.
- Option and metric naming (avoiding `RateLimiter` and "throttle" collisions).
- Whether idle pool threads use `allowCoreThreadTimeOut` or simply park.
- The exact freeze/bound/subsume mechanism for R6.

### Sources / Research

- `docs/ideation/2026-08-17-distributed-throttling-ideation.html` - idea 8 and the verified code map (dispatch seam, deferral mechanics, metrics conventions).
- `docs/inflight/core-auto-scaling.md` - staged two-dimension design, positioning, earmarks.
- astubbs#227 (confluentinc#21), astubbs#155 (confluentinc#402), confluentinc#766.
- Prior prototypes (design references, bitrotted): `features/dynamic-concurrency-control` @6f85eac41 (Gradient2Limit/SimpleLimiter/BlockingAdaptiveExecutor wired as the pool; `parallel-consumer-core-auto-scale` module extraction begun) and `feature/auto-tuning-pressure` @f4aa09788; upstream draft PR confluentinc#22.
- Netflix concurrency-limits (Gradient2/Vegas control laws) - the math to port, not the dependency.
