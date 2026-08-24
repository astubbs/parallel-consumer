---
title: What the Admission Controller Should Actually Do - Clean-Sheet Design
type: feat
date: 2026-08-24
topic: admission-control-law
artifact_contract: ce-unified-plan/v1
artifact_readiness: requirements-only
product_contract_source: ce-plan-bootstrap
execution: code
---

# What the Admission Controller Should Actually Do - Clean-Sheet Design

## Goal Capsule

- **Objective:** State what the controller must do - what makes the target rise, what makes it fall,
  what makes it hold, what forces it to re-measure from the floor - and how each of those decisions
  can be proven wrong - and derive the mechanism from that, rather than patching the arm list
  inherited from the Gradient2 port.
- **Why this exists:** Three planning attempts failed review, and all of their fatal findings share a
  root. Each proposed removing a mechanism whose reason for existing was a property of *this engine*,
  and each was designing around code that was already committed. Nothing here is shipped; the
  committed law is a first draft and is treated as one.
- **Supersedes:** `2026-08-24-001-feat-admission-ratchet-plan.md` (its premise is contradicted -
  see Finding 1) and absorbs `2026-08-24-002-feat-admission-optimisation-objective-plan.md` (its
  cost estimate was wrong - see Finding 2).

---

## Two findings that invalidate the prior split

### Finding 1: the ratchet is not separable from the objective. The split was wrong.

The two plans were split on the argument that *an objective is what makes the controller useful; it
is not what stops it climbing*. Prior art contradicts this directly.

- **Uber Cinnamon hit this exact bug in production** - a periodically-reset latency target that
  drifted ever upward under sustained load - and fixed it by computing the **covariance between
  in-flight and throughput** and vetoing on negative covariance. They did not patch the baseline;
  they added an absolute objective. ([Cinnamon Auto-Tuner: Adaptive Concurrency in the
  Wild](https://www.uber.com/en-IN/blog/cinnamon-auto-tuner-adaptive-concurrency-in-the-wild/))
- **Netflix/concurrency-limits#137**: when every request returns at the same client timeout, RTT
  volatility is zero, so `Gradient2Limit` grows to `maxLimit`. Open.
- **envoyproxy/envoy#38338**: under sustained degradation both `sampleRTT` and `minRTT` are elevated, so the
  headroom term drives concurrency *up* while the system is struggling. Closed as not-planned.

The common mechanism: **a ratio cannot detect a steadily-bad absolute level.** Every relative
objective has this hole, and neither open issue has a fix inside that frame. The ratchet is not a
defect in the baseline's maintenance. It is the defining behaviour of a purely relative objective,
and it is not patchable without an absolute one.

That is why Plan A collected four P0s: it kept the relative objective and tried to fix the ratchet by
deleting arms, which removed the accelerator and the anti-strand probe without replacing what they
did.

### Finding 2: throughput is already measured. The objective's cost estimate was wrong.

`ClosedAdmissionWindow` already counts outcomes per window, fed per record from `handleFutureResult`
on the control thread - and the objective's throughput is **useful completions per actual elapsed
time: `successCount`, never `totalOutcomeCount()`**. The distinction is load-bearing for phase 4 of
the comparison test: rate-limit rejections *are* completions (they land in the ignore or
overload-drop counters), so a total-outcome rate stays high exactly when useful throughput collapses
- the one thing a throughput objective exists to see. `IGNORE` and `OVERLOAD_DROP` outcomes are
excluded from the numerator precisely so fast rejections read as a collapse rather than as sustained
completion rate. The objective plan asserted the controller "never measures throughput at all" and
priced it as a new signal. It is one division away, and it needs the window's *actual* elapsed time
rather than its nominal 1s, because windows drift (`windowOpenedAt = now` at tick time, so an idle
consumer produces one 4-second window and restarts).

The objective was deferred largely on cost. That reason is gone.

---

## The plant: what the controller is actually steering

Before any control law, what does moving the target *do*? The design surface answers this, and the
answer is not what the committed law assumes.

**The worker pool is fixed at construction and never resized.** `AbstractParallelEoSStreamProcessor`
builds it from `newOptions.getMaxConcurrency()`, `core == max`, with an **unbounded** queue, and
nothing calls `setCorePoolSize`/`setMaximumPoolSize` anywhere. So the admission target does not
control concurrency. It controls *feeding*. Above the pool size, extra admitted records land in the
executor queue and wait.

**This produces a live open-loop defect in the committed code.** `AdmissionController` resolves
`enforceCeiling = leftAtLibraryDefault ? ADAPTIVE_DEFAULT_CEILING : staticTarget` - so a user who
leaves `maxConcurrency` at its default of 16 and turns on `ENFORCE` gets a ceiling of **64 against a
pool of 16**. Above 16:

1. raising the target adds queue depth, not concurrency;
2. the service-time tap brackets only `usersFunction.apply(context)`, so queue wait is **excluded**
   from the measurement;
3. measured latency therefore does not degrade as the target climbs;
4. the gradient never sees degradation and the additive headroom wins another slot every window.

That is a ratchet generator with no feedback at all, sitting in the committed ceiling default. It is
also the defect the very first review round flagged as *"the effective maximum collides with pool
sizing"*; the plan carried a fix and the implementation took the ceiling substitution without the
pool resize.

**The closed-loop IT did not catch it** because it set `maxConcurrency` to 32, keeping the target
under the pool size, where the loop genuinely is closed. The ratchet it *did* observe (17/18 -> 20,
knee at 12) is the real baseline-contamination one, on a closed loop. Both exist; they are different
bugs with the same symptom.

**Under virtual threads none of this applies.** The pool is unbounded, every accepted task gets a
thread, and the admission target is the *only* bound that exists. The loop is closed by construction.

### The feedback map, which is what previous designs kept tripping over

Lowering the target does **not** drain the buffer. Records are still admitted from the broker; they
are just not dispatched. So:

- in-flight falls, lagged by one user-function duration plus a mailbox hop;
- buffered work in the shards goes **up** relative to in-flight;
- `isSufficientlyLoaded()` therefore becomes *more* likely true, so the **poller** pauses sooner -
  intake falls second-order, through a different actuator, on a different thread;
- the in-flight distribution narrows, and the law tests median and spread **as fractions of the
  limit** - so a contraction manufactures its own starvation evidence. This is the documented reason
  the probe-up arm exists, and it is why deleting that arm stranded the controller low in Plan A.

Any design that changes the target must state what it expects each of these to do.

---

## What the controller optimises

**Throughput, subject to absolute brakes. The latency baseline is deleted.**

The argument is not that latency is uninformative. It is that a *learned latency baseline* is the
one signal the controller's own actions contaminate, and every known failure of this controller class
runs through that contamination. Throughput has no baseline to poison: it is an absolute rate,
measured per window, and the controller cannot flatter itself by redefining what normal means.

Three further reasons specific to this library:

1. **A user function is an arbitrary black box.** Netflix/concurrency-limits#171 states the problem precisely: when a
   downstream dependency slows, threads block, latency rises - and the *correct* response is **more**
   concurrency, not less. When the local pool saturates, the correct response is less. Latency alone
   is the same observation in both cases. Throughput distinguishes them: if throughput still rises as
   concurrency rises, add concurrency; if it has stopped rising, stop. For a library whose whole
   proposition is "your function can be anything", this is a requirement, not a refinement.
2. **It answers the owner's question arithmetically.** *Service time rose from 20ms to 30ms - bad, or
   good because throughput went up?* A throughput objective answers without a configured number.
3. **It removes the second-order conditioning problem.** Kleinrock's power is maximised where its
   derivative is zero, which is a flat region by construction. Throughput elasticity crosses its
   threshold with non-zero slope. Same measurements, better conditioning. (This was already settled
   as KD1 in the objective plan and survives unchanged.)

**Latency remains, as a ceiling and never a target** - the ruling from the inflight note stands
whole, including that an unreachable ceiling is *reported as a binding constraint* rather than
pursued.

### The dither is not needed, and that deletes ten of the seventeen blockers

The objective plan specified estimation by injected perturbation and demodulation, copying the .NET
CLR ThreadPool. Ten of its seventeen blockers were properties of that estimator: sub-slot
perturbations truncating to nothing, clamping turning the square wave into a half wave, the
controller's own steps re-entering the dither band, the window length being target-derived and so
modulating the measurement interval at the dither frequency.

**Uber's covariance test needs no dither.** It correlates in-flight against throughput over the
controller's *natural* movement history. A controller that already rises and falls under an
accelerator and a brake is generating its own excitation; the estimator reads the resulting
trajectory rather than injecting one. Blockers 3, 4, 5, 6, 9 and 10 evaporate, and R5 - bounding the
perturbation's cost to a healthy workload - stops being a requirement because there is no
perturbation.

What survives from that list and must still be answered: the elasticity denominator (blocker 2 -
commanded target or achieved in-flight, and they diverge exactly when it matters), the window-series
gaps (blocker 7), rebalance reseeding (blocker 8), fleet behaviour (blocker 14), and the whole
product surface (15-17).

---

## Rise, fall, hold, escape

### RISE

**Two terms, and the additive one is load-bearing.**

With the gradient clamped to `[0.5, 1.0]`, `limit * gradient` can never exceed `limit`, so the
additive term is not a tuning refinement - it is the only thing standing between this law and a
provable one-way ratchet *downward*. With `q = 0` and `g <= 1` the limit is monotone non-increasing
for **all** inputs: every transient spike is permanent and no input sequence recovers it. Plan A
proposed a brake-only design and this is why it was fatal.

The accelerator must be **named and derived, not felt**. Solving `L = L*g + q` gives the fixed point

```
g* = 1 - q(L)/L
```

so `q` does not merely set the growth *rate* - it sets the **steady-state cost you are choosing to
buy**. Pick the tolerated cost; that fixes `q`. `q(L) = sqrt(L)` self-scales (Envoy's choice, and
`GradientLimit`'s rationale verbatim: a fixed queue "becomes too small for large limits but still
prevents the limit from growing too much"); a constant does not.

**Guard the arithmetic.** Netflix/concurrency-limits#35 is the cautionary case: with `smoothing < 1` and +/-1
increments, `trunc(0.8n + 0.2(n+1)) = n` - the controller *never moved at all*, and it shipped. Our
`smoothing = 0.2` already turns a nominal `+4` into an effective `+0.8` per window.

**Growth is gated on the limit being binding, not on the buffer being full.** RFC 7661 states the
rule to copy: a sender that *is* limit-bound may grow even while app-limited; only a sender that is
**not** limit-bound must not. This is the distinction Plan A collapsed.

And here PC is better placed than any of the prior art. Netflix, Envoy and TCP are all forced to use
`inflight >= limit/2` as a proxy for "the limit is binding". **We can measure the real thing**, and
three signals already exist and are unused by admission:

| Signal | Accessor | What it settles |
|---|---|---|
| Dispatch under-served | `lastWorkRequestWasFulfilled()` | we asked for more than the shards gave |
| Selectable work, ordering-aware | `WorkManager#getUpperBoundOnSelectableWork()` | whether the shards *could* have yielded more |
| Poller throttled | `BrokerPollSystem#isPausedForThrottling()` | whether **we** caused the emptiness |

Together these separate the three cases the current window aggregates cannot: the topic is empty; the
shards are ordering-blocked; we throttled our own intake. The third is self-inflicted and must never
be read as evidence of anything.

**And the separation is reported, not only consumed.** Each of the three cases reaches the operator
through the constraint gauge - including an ordering-starved value, which is the requirement
`pr-333-adaptive-concurrency-outstanding.md` item 3 opened (*the single most valuable diagnosis
available*) and which a law that only uses these signals internally would leave exactly as unmet as
it is today.

### FALL

**A relative brake and an absolute one. Both, because a ratio cannot see a steadily-bad level.**

- **Relative:** negative covariance between in-flight and throughput over a bounded history - more
  concurrency bought less work. This is the term Uber added to fix precisely our bug, and it cannot
  be fooled by a drifting baseline because it has no baseline.
- **Absolute:** the failure fraction (exists); the optional latency ceiling (KD4); and
  **offset-encoding back-pressure**, `PartitionState#isBlocked()` / `isAllowedMoreRecords()`, which
  is a real admission constraint the controller currently cannot see at all - a partition refusing
  more records because the commit metadata would not fit.

Note that the AIMD `BACKOFF` arm is currently **unreachable in production**:
`AdmissionOutcomeClassifier.classifyFailure` returns `IGNORE` for every cause, so `OVERLOAD_DROP` is
never produced. One of the two brakes in the committed law is dead code.

### HOLD

**When the limit is not binding: preserve, never decay.**

RFC 7661 replaced RFC 2861 for exactly this reason. RFC 2861 decayed `cwnd` when application-limited;
it was found *"too conservative for many common rate-limited applications"* and applications
responded by **padding their streams with junk data** to keep the window inflated. That is the
definitive symptom of a controller whose own gating manufactured its evidence, and it is the failure
mode Plan A would have reintroduced.

If a decay is wanted at all it must be bounded (RFC 7661 caps the non-validated period at five
minutes) and must floor at the initial value, never at 1.

**Also hold when there is no evidence.** Envoy's `resetSampleWindow` returns without deciding when
the histogram is empty; Knative returns an *invalid* scale result on `ErrNoData`. Absence of data
yields no decision - not a conservative one.

### ESCAPE

**N consecutive windows pinned at the floor forces a re-measurement, on a path no gated signal can
suppress.**

This is the anti-strand hatch, and it is the one mechanism that must not depend on any signal the
controller's own gating can suppress. Envoy uses N=5 and fires the minRTT timer immediately;
Uber arrived at the same design independently (repeated lower-bound hits reset `targetLatency`).

Envoy's four safeguards for the probe window all transfer and all matter:

1. **remember and restore** the pre-probe limit (`deferred_limit_value_`) - do not re-derive it;
2. **clear the sample history on entry** so pre-probe samples cannot contaminate the measurement;
3. **suspend normal limit updates** for the duration;
4. **jitter** the start (15%) so a fleet does not probe in lockstep.

And the floor itself must never sit below one accelerator step, or the accelerator cannot act at the
floor and the floor becomes absorbing. `GradientLimit` clamps to `[queueSize, maxLimit]`; CoDel
refuses to drop when it would leave the queue below one MTU. `LIMIT_FLOOR_SLOTS = 1` against
`q = sqrt(L)` satisfies this, but it must be stated as an invariant rather than left to coincidence.

---

## The actuator must be fixed before any law is worth tuning

**`maxConcurrency` is a maximum concurrency, not a thread count** (owner's ruling, 2026-08-24). If the
engine is auto-scaling, how a concurrency ceiling gets honoured - platform pool, virtual threads, no
pool at all - is the engine's business, not a number the user should be nominating.

That reframe dissolves the open-loop defect instead of patching it. **The controller sizes the pool to
its own target, and the mechanism's full contract is:** construct the pool with `maximumPoolSize` at
the resolved ceiling, and have the controller track its target with `corePoolSize` as the single live
knob. Both halves are load-bearing. `setCorePoolSize` above `maximumPoolSize` throws
`IllegalArgumentException` (JDK 9+), and today the pool is built `core == max == maxConcurrency` - so
steering `corePoolSize` alone against the current construction throws at the first step above 16 in
exactly the default configuration this section flags. And under the unbounded work queue,
`maximumPoolSize` is inert for thread creation (workers beyond core are only added when the queue
rejects an offer, which an unbounded queue never does), so setting it to the ceiling at construction
reserves nothing - it does not reintroduce the rejected size-once-from-ceiling cost. The shrink half
holds as stated: lowering `corePoolSize` interrupts idle workers, and with a zero keep-alive the
surplus threads exit as the queue empties - exactly the drain-down behaviour a contraction wants.
Then:

- the target **is** concurrency rather than a feeding rate, so the loop is closed at every value
  instead of only below the pool size;
- no queue depth accumulates, so the excluded-queue-wait measurement problem disappears with it
  rather than needing to be corrected for;
- platform threads and virtual threads come to mean the same thing, where today the target is the
  only bound under one and a feeding rate under the other;
- `maxConcurrency` becomes a pure safety ceiling, which is the honest reading of its name and
  resolves the "asks for a number that may not exist" problem under virtual threads (inflight item 5).

Note the units: the target is in **slots** - one slot is one concurrent user-function invocation, i.e.
one in-flight batch - so the pool size is the slot count, not the record count the seam derives from
it.

An alternative considered and rejected: size the pool once from the resolved ceiling. It closes the
loop but reserves the ceiling's worth of threads permanently, which is tolerable at 64 and absurd at
the ceilings a virtual-thread deployment would want.

Independently: the service-time tap excludes queue wait, so it does not measure what a caller
experiences. `WorkContainer#getResidenceTime()` and the existing `RECORD_RESIDENCE_TIME` timer
already capture end-to-end time including queueing and retries. If a latency signal is kept as a
ceiling, that is the one to use.

## The overload signal: owned elsewhere

How a user reports downstream pressure is a **public API** decision, and it now has its own artifact:
[`2026-08-24-004-feat-downstream-pressure-signal-plan.md`](2026-08-24-004-feat-downstream-pressure-signal-plan.md)
**owns that topic** - the context-method decision (an owner's ruling: *exceptions are for aborting,
not for messaging*, overriding the ideation's exception design), the constraints inherited from the
throttling ideation, the never-store-a-discovered-limit rule and its falsifiability argument, and the
per-service scope boundary. What binds *this* design from that one:

- the signal is what makes the dead `OVERLOAD_DROP` socket real, and the comparison test's rate-limit
  phase cannot be written until it exists;
- a pressure signal is retried like a failure but **never counted as one**;
- hard signals feed the fixed layer, soft signals feed the adaptive layer, layers compose by `min()`;
- the adaptive estimate is necessarily **aggregate** - the engine cannot see inside the user function,
  so per-service ceilings only ever come from declared or reported limits, never from discovery.

---

## How each decision can be proven wrong

The falsifier problem has a name and it is not a design problem. Every assertion written so far was a
**safety** property - "the limit stays in [a,b]" - and safety properties are satisfied vacuously by
inaction (Alpern & Schneider, *Defining Liveness*, IPL 21:181-185, 1985; every property is the
intersection of a safety and a liveness property, so a suite of only safety assertions is provably
incomplete). Three fixes, all mechanical:

**1. The unfalsifiable test was a defect in the workload, not the assertion.** With one constant
arrival rate and one constant service time, an entire interval of limits produces identical
observable output - the correct limit is **not identifiable**, so no assertion over that experiment
can be falsifiable. This is closed-loop identification bias, and the standard remedy is a persistently
exciting input. **Rule: every scenario must move the true optimum at least twice, in dimensions the
controller estimates independently.** Then a frozen trace is provably wrong at >= 1 point.

**2. An absolute oracle, computed from the scenario's own parameters.** Little's Law gives it
directly: with a simulator where the downstream service rate `mu_max` and the uncongested per-record
service time `W0` are *set*, the correct limit is `L* = mu_max * W0`. It is known before the test
runs and it moves automatically when the scenario changes - which is what makes assertion (1)
possible. Two one-sided absolute assertions then bracket it without naming a hand-fitted band:
throughput >= 0.9 * mu_max (a too-low limit fails) and p95 residence <= 1.25 * W0 (a too-high limit
fails, because queueing shows up as W > W0).

**3. A negative control, asserted in code.** Implement `FrozenLimit`, `AlwaysMaxLimit`,
`AlwaysMinLimit`; parameterise every scenario over the controller; assert each mutant **fails**.

This is not a hypothetical gap. Envoy's `gradient_controller_test.cc` - the strongest existing test
suite for this problem - asserts its brake strictly (`EXPECT_LT` every window) but two of its three
accelerator assertions are non-strict `EXPECT_GE`/`EXPECT_LE`, **which a frozen controller passes**.
And Netflix ships **no test at all** for `Gradient2Limit`, the algorithm we ported; its only
end-to-end harness is annotated `@Ignore("These are simulations and not tests")` and contains zero
assertions.

### The highest-value single test needs no plant change at all

**The initial-condition sweep.** Fix the plant so `L*` is constant for the whole run. Parameterise
over `initialTarget` in {1, 2, 5, 20, 50, ceiling} and additionally seed internal accumulators to
pathological values. Assert convergence from **every** starting point within a deadline.

It works because converging *from elsewhere* is liveness, while sitting on the answer is not. A
frozen controller passes exactly one arm and fails the rest. This is the sampled form of a
region-of-attraction test, and it is also the shape RFC 5166 has specified since 2008: measure
convergence by **starting from a deliberately wrong state and bounding the time to reach the right
region** (its delta-fair convergence time starts two flows at 100/101 and 1/101 of capacity).

### And the comparison that cannot be gamed

Run the adaptive controller against `FixedLimit(c)` for a spread of `c`, across several scenarios,
interleaved, same seeds. Require the adaptive arm to beat **every** fixed baseline in aggregate.
`FrozenLimit(c)` *is* `FixedLimit(c)` - it is definitionally in the baseline set and **cannot beat
itself**. The suite fails it by construction.

This also subsumes the owner's benchmark framing, and strengthens it: the third arm - static,
hand-tuned to the best value a careful operator would find - is what makes the claim survive the
obvious rebuttal (*so tune your config*). The workload must change partway through the run, or only
the weak half of the claim is tested.

---

## What this deletes

- The long-run latency EWMA (`ServiceTimeExpAvg` as the gradient's reference) and with it the
  anti-drift decay, the probe-down arm, `baselineRecoveryMode`, and `preProbeShortTimeNanos`. All of
  them exist to manage a contaminated baseline that no longer exists.
- The dither estimator and its ten blockers, never built.
- `Outcome.OVERLOAD_DROP`'s dead AIMD arm, unless the classifier is given real causes to return.

## What must be preserved, and why

Stated as obligations rather than as code, because this is where the previous plans failed:

- **Something must accelerate.** A brake-only controller is provably monotone non-increasing.
- **Something must distinguish self-inflicted emptiness from real capacity.** A contraction narrows
  the in-flight distribution and manufactures its own starvation evidence; that is a property of this
  engine, not of the committed code.
- **Something must escape the floor on an ungated path.**
- **The floor must never sit below one accelerator step.**

---

## Sequencing, and what the benchmark actually gates

**Order: fix the actuator, rewrite the law, measure, then claim.**

1. **The actuator fix comes first** - `maxConcurrency` must bound the pool and the ceiling together.
   Tuning a control law whose actuator is disconnected above its own default measures nothing. This
   is an edit to the branch the admission package lives on; the package is not on master or on the
   PR base, so nothing needs unwinding and nothing sequences around a merge.
2. **The law rewrite lands in one move**, not as a migration. A half-migration leaves a throughput
   term and a latency ratio in the same law disagreeing, with arm order deciding which wins - which
   is how the present six-arm pile-up happened. `admission-gradient2-port` tags the Gradient2 port
   complete and green, as the point to return to if steering on throughput proves worse.
3. **Then the benchmark.**

**The benchmark no longer gates the objective, and the earlier gate was wrong once the objective
became the ratchet fix.** The prior plan held that nothing should be built while it is unknown
whether the controller helps at all. That reasoning was sound when the objective was an enhancement,
and it fails now: the ratchet is a *correctness* defect - the target climbs without bound on any
workload that degrades gracefully - and a correctness fix is not held hostage to a value
measurement. If the benchmark returned "adaptive does not help", the response would be to not ship
the feature, never to ship it with the climb still in it.

**What the benchmark does still gate is any published claim**, and the claim must be stated
precisely because its weak form is not worth publishing. At a *given arrival rate*, adaptive delivers
lower end-to-end latency than a static `maxConcurrency`, or sustains a higher arrival rate before
falling behind - measurable only below saturation, which is why arrival control is required rather
than running both arms flat-out. Beating a badly chosen static number is trivial and invites *so tune
your config*. The claim worth having is that adaptive **matches a carefully hand-tuned static config
without the hand-tuning, then beats it once conditions move away from what that tuning assumed** -
which needs the third hand-tuned arm and a workload that changes partway through the run.

**The arrival-controlled harness this needs is already on this PR's base**, `perf/engine-concurrency`,
pushed - the commit that added controlled arrival and skewed keys is `perf(bench): controlled arrival,
skewed keys, and the two work-model no-ops that would have voided the run`. What is missing is that
this branch has not merged that base since it arrived. So the harness is a merge away, not a piece of
work waiting to be done.

Recorded because the first reading of this was wrong in a way worth not repeating: the local branch
`perf/bench-arrival-and-key-skew` has no remote of its own, which was read as the work being unpushed.
A branch name without a remote says nothing about whether its **commits** are on one - here every one
of them is already contained in the pushed base. Ask `git merge-base --is-ancestor <branch>
<pushed-ref>`, never `git branch -a`.

## The comparison integration test

**The claim under test: a compiled setting cannot change and the load can.** So a test that holds
conditions still cannot express the claim at all - it can only ask whether adaptive beats a static
number on the workload that number was chosen for, which is the weak form nobody should publish.

### Three arms, and the middle one is the real opponent

1. **Static, deliberately low** - makes the claim true and easy. Kept only as a floor.
2. **Static, hand-tuned** - swept over a range against **phase 1 only**, best value chosen. This is
   what a careful operator does: they tune against the load they can see at deploy time. **This arm
   is the actual opponent**, and it is also the negative control - a controller that never moves
   degenerates into exactly this arm and therefore cannot beat it.
3. **Adaptive.**

### The workload has to move under all three

Four phases, one run, arrival rate held by the controlled-arrival harness so the comparison is made
below saturation. The downstream is simulated inside the user function - which is the honest place
for it, since the user function *is* the abstraction over whatever the real downstream is.

| Phase | What changes | What each arm should do |
|---|---|---|
| 1. Steady | Downstream healthy, fixed capacity and service time | Hand-tuned arm is optimal **by construction**. Adaptive must **match within tolerance**, not beat it. |
| 2. Downstream degrades | Concurrency capacity falls; service time rises under load | Static over-drives it and queues. Adaptive backs off. |
| 3. Downstream recovers, then exceeds phase 1 | Capacity rises above where it started | Static leaves headroom unused. Adaptive grows into it. |
| 4. Rate limiting | Downstream stops slowing and starts **rejecting** above a token-bucket rate | Static keeps hammering the limiter. Adaptive settles under the rate. |

**Phase 1 is the phase that protects honesty.** The assertion there is *match within tolerance*,
never *beat* - a controller that pays a small exploration cost on a steady workload is behaving
correctly, and asserting that it wins would force a design that games steady state. Phases 2 to 4
carry the strict inequalities.

### Phase 4 is not a harder version of phase 2, and it is the interesting one

A degrading downstream slows down; a rate-limited one **rejects while staying fast**. Successful
calls keep returning at their normal latency while a growing fraction fail immediately - so latency
looks *fine* or even improves, while useful throughput collapses. **A latency-steered controller
cannot see this at all, and may even grow into it.** A throughput-steered one sees it directly. That
is the single sharpest argument in this design for the objective change, and phase 4 is where it
becomes an assertion rather than a claim.

### It has a prerequisite: the overload socket is currently dead code

`AdmissionOutcomeClassifier.classifyFailure` returns `Outcome.IGNORE` for **every** cause, so
`OVERLOAD_DROP` is unreachable and the AIMD backoff arm never fires in production. A rate-limit
rejection is precisely the failure shape that socket was reserved for - one that *structurally names*
overload rather than being a business failure. **Phase 4 cannot be written until the classifier
actually classifies**, and a test asserting behaviour on rate limiting while every rejection is
scored `IGNORE` would pass for the wrong reason.

This is the second prerequisite the design carries, alongside the pool/ceiling fix.

### What is measured, and the oracle

Measure **end-to-end residence time at the given arrival rate**, plus useful completion rate. Never
the target value itself - `perf/split-shard-inflight` is the standing warning here: it moved dispatch
10x and end-to-end 0%. A controller that moves impressively is not a controller that helps.

The oracle is absolute and per phase, not a fitted band: the simulated downstream's capacity and
uncongested service time are *set* by the test, so Little's Law gives the correct admission target
for each phase directly, and it moves when the phase moves. That is what makes the assertions
falsifiable rather than merely descriptive - a frozen controller is provably wrong in at least three
of the four phases.

## Resolve Before Planning

1. **The elasticity denominator** - commanded target or achieved in-flight. They diverge exactly when
   it matters, and an in-flight denominator can approach zero and make elasticity explode or flip
   sign. (Carried from the objective plan, blocker 2, unresolved.)
2. **Covariance history length and its interaction with window drift.** Uber uses 50 intervals at
   2-30s. Our windows drift with the control-loop cadence, so a fixed count of windows is a variable
   span of wall-clock time.
3. **Where the estimator's state lives across a rebalance** (blocker 8), and how the deliberate gaps
   in the window series enter it (blocker 7).
4. **Whether `maxConcurrency` sizes the pool, caps the ceiling, or both** - and what it means under
   virtual threads. This is prerequisite and may deserve its own change.
5. **Whether the latency ceiling reads service time or residence time.** Residence time is what a
   caller experiences and is already a percentile-capable `Timer`; service time is what the current
   tap measures and excludes queue wait.
6. **Fleet behaviour** (blocker 14) - N instances against one shared downstream, now without a dither
   to correlate, but the covariance test still reads a shared plant.
7. **The product surface** (blockers 15-17) - the parameter cannot honestly be documented as a
   promised utilisation fraction, and a new tunable with no symptom-keyed decision rule becomes
   folklore.
8. **The tolerated steady-state cost that fixes `q` - a product decision, not a tuning constant.**
   The RISE section derives that `q` sets the throughput a healthy workload permanently gives up for
   adaptivity, and the phase-1 *match within tolerance* band is the same number seen from the test
   side - the figure any published claim will be attacked on. Deriving them from one stated choice
   keeps the law's constant and the claim's overhead figure from diverging; leaving it unowned means
   whoever implements first defaults it silently.

## Sources

Primary sources read directly: `Gradient2Limit`, `GradientLimit`, `VegasLimit`, `AIMDLimit`,
`WindowedLimit` (Netflix concurrency-limits); Envoy `gradient_controller.cc`, its test suite and
config proto; RFC 5681, RFC 7661 (obsoleting RFC 2861), RFC 8289, RFC 5166; the BBR drafts;
Netflix/concurrency-limits issues 34, 35, 37, 137, 147, 152, 171, 173, 189 and PR 125;
envoyproxy/envoy#38338; the Uber Cinnamon posts; the CLR hill-climbing algorithm; Alpern & Schneider
(1985); draft-irtf-iccrg-tcpeval-01.

Not independently verified, and deliberately not relied on above: Pantheon's internal methodology
(binning, per-run ellipses, replication error), the Chen & Kuo metamorphic-PID relations, MSER-5, and
the deterministic-simulation-testing references. They are leads for the test plan, not citations.

For the overload signal specifically: RFC 6585 (429 Too Many Requests) and RFC 9110 section 10.2.3
(`Retry-After`, previously RFC 7231 section 7.1.3) - the source of the deferral-not-a-rate reading
above.

In-repo prior art, which predates this design and should be read before re-opening any of it:

- [`docs/ideation/2026-08-17-distributed-throttling-ideation.html`](../ideation/2026-08-17-distributed-throttling-ideation.html) -
  the eight ranked, code-verified directions, the rejection table and the prior-art autopsies. **The
  strategy menu (idea 5) and the convergence record (idea 8) are where the downstream-signal option
  originates**; this design's signal section is that idea reached from the controller's side.
- [`docs/inflight/core-distributed-throttling.md`](../inflight/core-distributed-throttling.md) -
  **owns** the strategy-menu shape, the standalone-versus-controller decision, and the `min()`
  composition. astubbs#228 (mirror of confluentinc#24), with confluentinc#766 as demand evidence.
- [`docs/inflight/core-auto-scaling.md`](../inflight/core-auto-scaling.md) - the umbrella note for
  astubbs#227, and the decisions of 2026-08-24 recorded there.
- [`docs/inflight/pr-333-adaptive-concurrency-outstanding.md`](../inflight/pr-333-adaptive-concurrency-outstanding.md)
  (items -1 to 5) and
  [`docs/inflight/core-adaptive-concurrency-future-modes.md`](../inflight/core-adaptive-concurrency-future-modes.md).
- [`docs/inflight/branch-engine-concurrency-family.md`](../inflight/branch-engine-concurrency-family.md) -
  what each measurement and optimisation branch was cut to answer, including the refutations.
- The abandoned prototypes catalogued in `docs/refactoring.md`'s idea bank - `features/rate-limiting`
  (bucket4j POC), `features/dynamic-concurrency-control` (Netflix concurrency-limits), and
  `feature/auto-tuning-pressure` - plus upstream draft PR confluentinc#22.
- The design-surface inventory of the engine's observables and actuators that this document's plant
  section rests on.
