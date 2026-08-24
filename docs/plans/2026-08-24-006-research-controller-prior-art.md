---
title: How Real Limiters Grow, Escape, and Get Proven - the Prior-Art Survey
type: research
date: 2026-08-24
topic: admission-controller-prior-art
execution: knowledge-work
---

# How Real Limiters Grow, Escape, and Get Proven

The external survey behind `2026-08-24-003-feat-admission-control-law-design.md`, recorded so the
citations and negative results are not lost with the session that produced them.

**Provenance, stated up front.** Targets 1 and 2 came from primary sources opened directly on
2026-08-24: the Netflix concurrency-limits source (`Gradient2Limit`, `GradientLimit`, `VegasLimit`,
`AIMDLimit`, `WindowedLimit`), Envoy's `gradient_controller.cc`, its tests and config proto,
RFC 5681 / 7661 / 8289 / 5166, the BBR drafts, Netflix issues 34, 35, 37, 137, 147, 152, 171, 173,
189 and PR 125, envoyproxy/envoy#38338, the Uber Cinnamon posts, and the CLR hill-climbing
description. Target 3's four load-bearing claims were spot-verified verbatim (RFC 5166's delta-fair
convergence, the tcpeval topologies, Alpern & Schneider 1985, Netflix's assertion-free `@Ignore`d
simulation). **Not independently verified** and deliberately not relied on: Pantheon's internal
methodology, the Chen & Kuo metamorphic-PID relations, MSER-5, and the
deterministic-simulation-testing references - leads, not citations.

## Target 1 - what makes a limit rise

- **Gradient2's gradient is clamped to [0.5, 1.0], so the additive `queueSize` term is the ONLY
  accelerator.** With `q = 0` and `g <= 1` the limit is provably monotone non-increasing for all
  inputs - no input sequence recovers it. Fixed point: solving `L = L*g + q` gives
  `g* = 1 - q(L)/L`, so **`q` sets the steady-state cost you are buying**, not just the growth
  rate. (That derivation presumes the multiplicative gradient - it does not transfer unchanged to
  an event-brake law; 003 carries the re-derivation as a Resolve item.)
- Sizing precedent: Gradient2 `q = 4` (constant, smoothed to +0.8/window effective);
  `GradientLimit` `4*sqrt(L)`; Envoy `sqrt(L)` with a gradient clamped [0.5, **2.0**] - two
  accelerators where Netflix has one. `GradientLimit`'s javadoc: a fixed queue "becomes too small
  for large limits but still prevents the limit from growing too much".
- **Arithmetic deletes accelerators silently.** Netflix issue 35: with smoothing < 1 and +/-1
  steps, `trunc(0.8n + 0.2(n+1)) = n` - VegasLimit never moved at all, and it shipped.
- **A purely relative accelerator has no ceiling.** Netflix 137 (stable timeout: zero RTT
  volatility, grows to max, open) and envoyproxy/envoy#38338 (sustained degradation elevates both
  sampleRTT and minRTT, headroom drives concurrency up, closed not-planned). A ratio cannot see a
  steadily-bad absolute level - the design's Finding 1.
- Growth-evidence taxonomy: TCP slow start and BBR ProbeBW are **fully speculative**
  (cost-bounded); Netflix gradients are speculative gated on `inflight >= limit/2`; Vegas is
  evidence-based (measured queue estimate); `AIMDLimit` +1/window means 20 -> 200 takes **180
  seconds** - the practical meaning of evidence-only growth.
- Chiu & Jain's AIMD-uniqueness result binds only when **multiple controllers share one
  bottleneck** - the fairness axis. A single instance is not forbidden multiplicative increase.
- **The CLR ThreadPool hill-climber is the production precedent for throughput-only steering of a
  work server**: injects a small wave into thread count, extracts the throughput response at the
  wave frequency (Goertzel, adjacent bins as the noise floor), moves by gradient x confidence,
  randomises its sample interval, backs off probing at the floor but never stops. No latency model
  at all. This - not the request servers - is the precedent class 003's delete-the-baseline ruling
  rests on.

## Target 2 - distinguishing "no work" from "at capacity"

- **BBR app-limited, the full mechanism**: the marker is set precisely when the window is NOT the
  binding constraint; every rate sample carries the taint; a tainted sample may **raise** the
  bandwidth estimate but never lower it; state transitions treat tainted rounds as *no decision*.
  Reducing the sending rate is thereby physically incapable of reducing the capacity estimate.
- **RFC 7661 (obsoleting RFC 2861) is the transferable rule**: when not limit-bound, **preserve,
  never decay** - RFC 2861's decay-on-idle was "too conservative" and applications **padded their
  streams with junk data** to keep cwnd inflated, the definitive symptom of gating that
  manufactures its own evidence. A sender that IS limit-bound may grow even while app-limited. Any
  decay must be bounded (NVP <= 5 min) and floor at the initial value.
- **The Netflix library has the gate but not the escape, and its maintainers know**: issues 147 and
  152 (a pessimised limit cannot regrow under low load), 34 (the author: min-since-startup "can
  prevent the limit from growing", and the naive periodic-raise fix causes upward drift under
  prolonged load), 37 (one lucky fast sample permanently depresses the baseline).
- **Envoy's escape hatch**: five consecutive windows at the floor fire the minRTT recalibration
  immediately, on a path no gated signal can suppress - with four safeguards: remember-and-restore
  the pre-probe limit, clear sample history on entry, suspend normal updates, jitter the start.
  **Uber independently arrived at the same design** (repeated lower-bound hits reset
  targetLatency).
- **Uber Cinnamon**: naive periodic target resets produced "ever-increasing targetLatency" in
  production; fixed with the **covariance of in-flight against Little's-Law throughput over a
  50-interval history**, forcing the target down on negative covariance - an experiment on the
  actuator's own history that a drifting baseline cannot fool. NOTE for 003: Cinnamon **kept** its
  latency law and added the veto - the augmentation-versus-deletion question this raised is ruled
  on in 003 (work server, CLR precedent), not ignored.
- **CoDel**: never drop below one MTU's worth - the floor must never sit below one accelerator
  step, or the floor becomes absorbing. `GradientLimit` clamps to `[queueSize, maxLimit]` for the
  same reason.
- **The trap's formal name is closed-loop identification bias**; the standard remedy is persistent
  excitation. Kafka consumers are better placed than TCP: offered load is directly observable
  upstream of the gate (buffered-but-unadmitted work), where TCP/Netflix/Envoy are forced onto the
  `inflight >= limit/2` proxy.
- **Latency cannot say whose fault it is** (Netflix 171): a slower dependency and a saturated local
  pool read identically in latency, and the correct responses are opposite. For a library whose
  user function is a black box, a second orthogonal signal is a requirement.

## Target 3 - proving a controller works

- **Safety properties are satisfied vacuously by inaction** (Alpern & Schneider 1985: every
  property is an intersection of safety and liveness; a suite of only safety assertions is provably
  incomplete). Every band assertion written before this survey was safety-shaped.
- **An unfalsifiable test is a workload defect, not an assertion defect**: one constant arrival
  rate and one constant service time leave the correct limit unidentifiable - no assertion over
  that experiment can be falsifiable. Every scenario must move the true optimum at least twice.
- **The absolute oracle is Little's Law**: set the plant's `mu_max` and `W0`, and `L* = mu_max *
  W0` is known before the run and moves with the scenario. Two one-sided brackets (throughput >=
  0.9 mu_max; p95 residence <= 1.25 W0) bound it without a fitted band. (In slots: divide by
  batchSize - 003 owns the units note.)
- **The highest-value single test is the initial-condition sweep** - converging *from elsewhere* is
  liveness; sitting on the answer is not. RFC 5166 has specified this shape since 2008 (delta-fair
  convergence from a maximally-unfair start).
- **The negative control must be asserted in code**: FrozenLimit / AlwaysMax / AlwaysMin
  parameterised over every scenario, each asserted to FAIL. In the beat-every-fixed-baseline
  comparison, FrozenLimit *is* a baseline and cannot beat itself - failure by construction.
- **The field's own suites have the exact hole**: Envoy's brake assertions are strictly
  `EXPECT_LT` while two of its three accelerator assertions are non-strict (`EXPECT_GE`/`LE`) - a
  frozen controller passes them. Netflix ships **no test at all** for `Gradient2Limit`; its only
  end-to-end harness is `@Ignore("These are simulations and not tests")` with zero assertions.
- Recovery-testing vocabulary worth keeping: initial-condition/region-of-attraction sweep;
  disturbance rejection (plant changes, setpoint doesn't); anti-windup round-trip (pin at max for
  H, release, assert recovery time does not grow with H); metamorphic relations with **strict**
  inequalities (doubled load => strictly higher settled limit), which frozen controllers fail by
  producing identical outputs.

## Design implications carried into 003

Accelerator named and derived, not felt; if the gradient is capped at 1 the additive term is
mandatory; an absolute brake beside any relative one; freeze-never-decay when not limit-bound, on
the arrival-side signal rather than the inflight proxy; an unconditional floor escape with Envoy's
four safeguards; the floor never below one accelerator step; baselines must expire and be
re-probed; a second orthogonal signal because latency cannot assign blame; guard the arithmetic;
every accelerator assertion strict and liveness-shaped with the frozen mutant asserted to fail.
