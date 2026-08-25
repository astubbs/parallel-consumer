# Adaptive concurrency: what astubbs#333 leaves open

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

The controller landed opt-in and off by default on astubbs#333. What is outstanding is below, in the
order it is worth doing. Item 4 is the only one that can answer *does it help*.

The widest gap - that **every** adaptive test was a unit test, so the controller and a genuinely
running engine had never been observed composing - is closed on this branch by
`AdaptiveConcurrencyEnforceIT`: two instances in `ENFORCE` against a real broker, ramping, losing
nothing and surviving a rebalance. It also found what only a real engine could - a lazy-init race in
`PCModule` that built two controllers and left one of them reporting its seed forever, since fixed
and written up in
[`docs/solutions/logic-errors/a-racing-lazy-singleton-announces-itself-as-a-duplicate-meter-2026-08-24.md`](../solutions/logic-errors/a-racing-lazy-singleton-announces-itself-as-a-duplicate-meter-2026-08-24.md).
Its sibling `AdaptiveConcurrencyClosedLoopIT` closes the next gap - a handler whose latency is a
function of the concurrency the controller chose, so the loop is actually closed - and that is where
item 2 below came from.

## Merge prep: this PR gets a Codex review (owner directive, 2026-08-25)

Before this PR merges, run a **Codex review** in addition to the usual `@claude review this` gate -
a cross-model review catches what a same-model review is structurally blind to, and this work (a
control law with falsifier-backed behaviour claims) is exactly the complexity tier it is reserved
for. The Codex plan is small, so it is spent strategically, not routinely; this PR qualifies.

## Status, 2026-08-24: items 0 to 3 are answered by one design, and the split that preceded it was wrong

[`docs/plans/2026-08-24-003-feat-admission-control-law-design.md`](../plans/2026-08-24-003-feat-admission-control-law-design.md)
is the authority on how. What is kept here is why each item exists and what it costs, which that
design assumes rather than restates. The two plans it replaces are left in place as the record of
how the argument got here:
[`...-001-feat-admission-ratchet-plan.md`](../plans/2026-08-24-001-feat-admission-ratchet-plan.md)
(superseded - its premise is contradicted) and
[`...-002-feat-admission-optimisation-objective-plan.md`](../plans/2026-08-24-002-feat-admission-optimisation-objective-plan.md)
(absorbed - its settled design survives, its cost estimate did not).

**The split was the finding, and then it was the mistake.** The ratchet fix and the objective were
planned as one change; a five-reviewer pass returned forty-seven findings and four reviewers
independently reached the same restructuring, so they were split on the argument that *an objective
is what makes the controller useful; it is not what stops it climbing*. External prior art
contradicts that directly. **Uber Cinnamon hit this exact bug in production and fixed it by adding a
throughput-covariance veto - an absolute objective - not by patching the baseline.**
Netflix/concurrency-limits#137 and envoyproxy/envoy#38338 are the same failure, both still open,
because a ratio cannot detect a steadily-bad absolute level. The ratchet is not a defect in how the
baseline is maintained; it is the defining behaviour of a purely relative objective.

The ratchet-only plan then collected four P0s of its own, all of the same shape: it kept the relative
objective and tried to fix the ratchet by deleting arms, which removed the accelerator and the
anti-strand probe without naming what would do their jobs.

**One correction to item 0 below, found while re-grounding the design:** throughput is *already*
measured. `ClosedAdmissionWindow#totalOutcomeCount()` counts completions per window, so the
objective's headline cost is one division, not the new signal item 0 prices it as. It needs the
window's *actual* elapsed time rather than its nominal one second, because windows drift.

## -1. The default ceiling exceeds the worker pool, so the loop is open above sixteen slots

Found while mapping the plant, and it is the most concrete defect here. The worker pool is built once
at construction from `maxConcurrency` - `core == max`, unbounded queue - and nothing ever resizes it.
So the admission target does not control concurrency; it controls *feeding*. Above the pool size,
extra admitted records sit in the executor queue.

`AdmissionController` then resolves `enforceCeiling` to `ADAPTIVE_DEFAULT_CEILING` (64) when
`maxConcurrency` is left at its library default (16) under `ENFORCE`. **The ceiling is four times the
pool.** Above sixteen slots: raising the target adds queue depth, not concurrency; the service-time
tap brackets only the user function so queue wait is excluded; measured latency therefore does not
degrade; and the additive headroom wins another slot every window, forever. An open loop and a second
ratchet generator, independent of the baseline contamination in item 2.

`AdaptiveConcurrencyClosedLoopIT` cannot see it - it sets `maxConcurrency` to 32, keeping the target
under the pool size, where the loop genuinely is closed.

The first review round flagged this as *the effective maximum collides with pool sizing*; the plan
carried a fix and the implementation took the ceiling substitution without the pool resize. It is
prerequisite to any law work - tuning a controller whose actuator is disconnected above its own
default measures nothing. Options are in the design's plant section; it interacts with item 5, since
under virtual threads the pool is unbounded and the target is already the only bound.

## 0. What is the controller actually optimising? (unanswered, and it is upstream of the ratchet)

Raised by the owner, 2026-08-23, as the thing that was hardest to model when first thinking about
this: **how do you define the target performance at all?** If service time rises from 20ms to 30ms,
is that bad - or good, because throughput went up? Three defensible answers: optimise throughput,
optimise latency, or find some worthwhile midpoint where latency may keep rising as long as
throughput rises with it.

**The plan never answered this, and the ratchet below is the consequence.** The ported law's
objective is implicit and purely *relative*: keep short-term latency near its own long-run average.
That target has no anchor, which is exactly why the baseline drifts upward and the target ratchets -
the law cannot say *this latency increase was not worth it*, because **it never measures throughput
at all**. It has no notion of worth, only of change.

The three answers are real modes, not a false trichotomy, and each has an established shape:

- **Throughput** - hill-climb until more concurrency stops producing more completions. Needs no
  user-supplied number, but keeps pushing until something saturates, which is how you flood a
  downstream that degrades gracefully.
- **Latency** - hold a stated ceiling (p99 under N ms). The only mode with an absolute anchor, and
  the only one that requires the operator to know a number they often do not have.
- **The midpoint** - the classical answer is Kleinrock's *power*, throughput divided by response
  time, which is maximised exactly at the knee where queueing begins. It needs no configured number,
  it answers the 20ms-to-30ms question arithmetically (worth it only if throughput rose by more than
  1.5x), and it is precisely the elbow the closed-loop test builds. It is also the mode that cannot
  ratchet the way the current law does, because a rising latency unmatched by rising throughput
  lowers power and is therefore *visible as loss* rather than absorbed as the new normal.

Choosing one is a product decision, not an implementation detail. Note what it costs: power and
throughput modes both need a completions-per-window measure the controller does not currently take -
it samples service time, outcomes and in-flight, but never rate. That is a small addition to the
window and a large addition to what the controller can reason about.

### A number cannot be a target, because you cannot know it is reachable

The owner's follow-up, and it rules out the obvious design. Suppose the operator says *20ms*. Two
things go wrong with treating that as a goal to aim at.

**It presumes achievability.** If runtime conditions put the real floor at 50ms, a controller
chasing 20ms contracts forever - down to a single slot, destroying throughput, still missing the
target, and looking from the inside like it is working perfectly. It is obeying its objective. The
objective was impossible.

**It presumes a cliff where people have a region.** 5ms and 10ms are both *instant* to most use
cases, so doubling there is a fine trade for throughput; 20ms might not be. What an operator
actually holds is a tolerance region with a soft floor and a hard edge, not a point.

The design conclusion: **a latency number is a CEILING, never a target.** Seek the knee (power,
above), and let an operator optionally clamp the result - and when the clamp turns out to be below
anything the system can reach, say so as a binding constraint rather than strangling throughput in
pursuit of it. That is a new reported state, and it is the honest failure mode: *you asked for 20ms,
nothing here goes below 50ms, so I am optimising throughput and telling you rather than pretending.*
The same argument applies to a throughput floor.

## 1. The probe-down cycles forever when it has already learned the answer

At the cap with flat latency the probe-down fires on its cadence, steps the target down by its
ratio, finds no improvement, and lets the target regrow - then fires again five windows later,
forever. Observed in the triad's own trace: `28 <-> 32` indefinitely.

**Averaging the two is the wrong fix** (it surrenders admission the probe just proved was safe); the
right one is to stop probing. Back the probe *cadence* off exponentially once a probe finds no
improvement, cap the backoff, and reset it when something changes - a latency shift, a rebalance, or
the target moving for another reason. Envoy's jittered minRTT recalibration and TCP's persistent
timer are the precedents.

Worth doing rather than tolerating: at the current cadence the engine spends roughly half its life
below a cap it has already proven is fine, which is a permanent throughput loss on exactly the
healthy workloads the feature is supposed to leave alone.

## 2. The long baseline absorbs the degradation it is supposed to be the reference for

There is no fixed point below the ceiling. Against a synthetic downstream with a genuine elbow
(`AdaptiveConcurrencyClosedLoopIT` - latency `base * max(1, inflight/knee)^2`, knee 12, base 80ms,
cap 32), the target ramps cleanly to the elbow, the first contraction lands as predicted, and the
controller then hunts between two adjacent slots. But the pair **walks upward for the whole run** -
17/18, then 18/19, then 20 - reproduced to within a second across three runs:

```
Hunting band by 20s slice (knee is 12):
[  0- 20s]=2..16   [ 20- 40s]=17..18   [ 40- 60s]=17..18   [ 60- 80s]=18..19   [ 80-100s]=18..19
```

The mechanism is `ServiceTimeExpAvg` as used for the long baseline: a 600-window EWMA that keeps
folding in the degraded latency the controller itself is causing. As the baseline creeps toward the
operating latency, the short/long ratio falls, the gradient relaxes toward 1.0, the additive queue
headroom wins another slot, and latency rises again. The PR-88 anti-drift decay only pulls a
**stale-high** baseline down; nothing pulls a slowly-inflating one back, and the probe-down arm never
fires because it is gated on being at the cap. Simulating the same curve for 400 windows walks the
target from 17 to 27, still climbing.

So the reference the law contracts against is contaminated by the law's own actions - the same
contamination the probe-down arm exists to fix at the cap, arriving by a slower route below it. The
probe-down cadence not being gated on the cap would address it; so would a baseline that only accepts
downward samples freely and upward ones grudgingly. Both need measuring, not choosing. The IT
deliberately does **not** assert a band, because a band fitted to a 100-second run would go red the
day someone lengthened it.

The ratchet is now **visible in the log** rather than only in a simulation: every movement line
carries the long-run baseline, the short-term figure's ratio to it, and the tolerance that ratio is
tested against, so a baseline creeping upward while the ratio sits harmlessly below 1.50 can be read
straight off a run.

## 3. The controller cannot report the most useful thing it knows

The requirements say a starved workload is reported as `starved`. The decision-reason enum has no
such value: the starvation signature produces the bounded probe (reported as `PROBING`) and
otherwise falls through to `APP_LIMITED`. So the constraint gauge can never tell an operator *your
workload is ordering-starved and admission cannot help you* - which, on the skewed-key measurements,
is the single most valuable diagnosis available.

Either the enum gains the value or the requirement's vocabulary changes. The documentation currently
describes what the code emits, not what the plan promised.

## 4. Nothing has measured whether it helps

The value claim - lower end-to-end latency at a given arrival rate, or a higher sustainable arrival
rate, against a static guess - is only measurable below saturation, on the arrival-controlled
harness. The adaptive arm lands on its own branch cut from that harness rather than here, so the two
can merge independently. Until it produces a result, the roadmap entry stays at `in-progress`: that
ladder reserves `implemented` for work proven in use.

**This no longer gates the law work, and the change of gate is deliberate** (2026-08-24). The
objective plan held that nothing should be built until it was known whether the controller helps at
all. That was right while the objective was an enhancement and is wrong now the objective *is* the
ratchet fix: the ratchet is a correctness defect, and a correctness fix is not held hostage to a
value measurement. If this measurement came back negative the answer would be to not ship the
feature, never to ship it with the climb still in it. What it still gates is any **published
claim** - the design's sequencing section owns that, including why the weak form of the claim (*beats
a badly chosen static number*) is not worth publishing.

Two things shape what that measurement should expect. Every calibration constant is a placeholder -
the default ceiling, the window length, the failure threshold, the probe steps - and the simulation
they were chosen against uses an invented latency curve that needs re-fitting to measured data.
And the ramp is **linear**: growth is a constant additive step per window regardless of how much
headroom exists, while contraction is proportional to the degradation. Reaching a wide ceiling from
a low start therefore takes time proportional to the distance, which is what the seed option exists
to skip.

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

## 5. `maxConcurrency` under virtual threads asks for a number that may not exist

**`maxConcurrency` under virtual threads asks for a number that may not exist.** The README calls it
the upper limit, and under a platform pool it genuinely is one. Under virtual threads there may be
no meaningful technical maximum at all - nobody is going to nominate 10,000 as their concurrency
even though the runtime would carry it, and far more. So the option quietly reverts to the thing
this whole feature exists to remove: a guess at a runtime quantity. Options include treating it as
purely a safety ceiling that defaults far higher under virtual threads, deriving it from a
downstream signal rather than a thread count, or accepting *unset* as a first-class value meaning
*discover it*. Unresolved, and it interacts with item 0 - a controller with a real objective needs a
cap far less than one without.

## One correction worth carrying

The in-flight ceiling the controller will discover is **not** unexplained. It is platform threads,
settled with a control containing no Kafka and no Parallel Consumer; virtual threads remove it
entirely. The performance hypothesis register still lists it under "Still open" as the strongest
surviving candidate, which its own settled note contradicts - worth reconciling there, and worth
knowing here, because on platform threads the controller converging near that figure is the
controller being right, not the controller failing to climb.
