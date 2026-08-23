# Adaptive concurrency: what astubbs#333 leaves open

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

The controller landed opt-in and off by default on astubbs#333. Four things are outstanding, in the
order they are worth doing. The last is the only one that can answer *does it help*.

The widest gap - that **every** adaptive test was a unit test, so the controller and a genuinely
running engine had never been observed composing - is closed on this branch by
`AdaptiveConcurrencyEnforceIT`: two instances in `ENFORCE` against a real broker, ramping, losing
nothing and surviving a rebalance. It also found what only a real engine could
([`bug-pcmodule-admission-controller-lazy-init-race.md`](bug-pcmodule-admission-controller-lazy-init-race.md)).
Its sibling `AdaptiveConcurrencyClosedLoopIT` closes the next gap - a handler whose latency is a
function of the concurrency the controller chose, so the loop is actually closed - and that is where
item 2 below came from.

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

Two things shape what that measurement should expect. Every calibration constant is a placeholder -
the default ceiling, the window length, the failure threshold, the probe steps - and the simulation
they were chosen against uses an invented latency curve that needs re-fitting to measured data.
And the ramp is **linear**: growth is a constant additive step per window regardless of how much
headroom exists, while contraction is proportional to the degradation. Reaching a wide ceiling from
a low start therefore takes time proportional to the distance, which is what the seed option exists
to skip.

## One correction worth carrying

The in-flight ceiling the controller will discover is **not** unexplained. It is platform threads,
settled with a control containing no Kafka and no Parallel Consumer; virtual threads remove it
entirely. The performance hypothesis register still lists it under "Still open" as the strongest
surviving candidate, which its own settled note contradicts - worth reconciling there, and worth
knowing here, because on platform threads the controller converging near that figure is the
controller being right, not the controller failing to climb.
