# Adaptive concurrency: what astubbs#333 leaves open

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

The controller landed opt-in and off by default on astubbs#333. Four things are outstanding, in the
order they are worth doing. The first is the widest gap between *the math is right* and *the feature
works*; the last is the only one that can answer *does it help*.

## 1. It has never run against a real broker

**Every adaptive test is a unit test.** Eleven test classes, all under `src/test`, none under
`src/test-integration` - so mock consumers and injected clocks, never a broker. The engine-level
rip-out triad is driven through the tick path with real engine state rather than a live control
loop, deliberately (a running loop races the test thread advancing the injected clock, and the
alternative is real one-second windows, which buys wall time and flakiness). The consequence is that
the controller and a genuinely running engine have never been observed composing: real poll cadence,
real commit interaction, real rebalance, real thread scheduling.

What is needed is small and unglamorous - stand an instance up in `ENFORCE` against a real broker
and assert it runs, does not stall, loses no records, and survives a rebalance. Not a performance
claim; a liveness and correctness claim.

## 2. The probe-down cycles forever when it has already learned the answer

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
