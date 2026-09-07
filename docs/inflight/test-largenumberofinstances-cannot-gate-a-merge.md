# `largeNumberOfInstances` measures the protocol - so it cannot gate a merge

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

## The decision the 2026-09-01 handoff left open, now answerable

`MultiInstanceRebalanceTest.largeNumberOfInstances` sits in the `Performance Tests` lane, which is a
**required** status check on master. Its own javadoc says a single run's outcome is not a verdict on
PC. Those two facts were already in tension on 2026-09-01; what was missing was knowing whether the
residual failures were PC's or the protocol's, because the answer decides the remedy.

**They are the protocol's, measured** - see the `EXPLAINED` section of
[`test-largenumberofinstances-residual-failures-measured-not-explained.md`](test-largenumberofinstances-residual-failures-measured-not-explained.md).
In every failing run no coordinator request was slow; what varies is whether the monkey's churn opens
a join phase that outlasts the 12s detector, and that is a property of hardware timing: 4 in 60 on
the Linux runner, 0 in 22 on an M2 desktop. A required check that fails one run in fifteen for a
reason no change to PC can move is a merge blocker that carries no information about the change
being merged.

## What the profile is worth, and to whom

It is a **capacity measurement of the stack under a membership storm**: how much churn the
consumer-group protocol plus PC survives, on given hardware. That number is worth having - a drop in
it after a PC change would be signal. It is not a correctness gate, and `scriptedChurnRoundsCompleteWithoutStall`
already gates correctness for the same code paths, 17/17 green.

## Options, and a recommendation

The operator ruled out one option on 2026-09-01: **do not make the lane non-gating**, because GitHub
runners perform reliably enough that a baseline shift is real signal. That constraint stands. Within it:

1. **Move the three capacity profiles to a scheduled, non-required lane that records the rate.** The
   `experiments.yml` workflow already runs this exact test on a weekly schedule and uploads a tally;
   the throughput report already compares a per-run share against a rolling master median. The rate
   belongs beside that: a number with a trend, not a tick. Cost: a workflow edit and an `@Tag` split.
   This is the recommendation.
2. **Assert a rate rather than an outcome inside the gating lane** - run the profile N times per CI
   run and fail below a floor. Honest, but N large enough to distinguish 7% from 20% is a runner-hour
   per PR, on every PR, for a number that changes only when the protocol or the hardware does.
3. **Leave it quarantined indefinitely.** The quarantine lane keeps running it on every push and
   reports without gating, which is option 1 without the trend. Acceptable as the interim; not a
   destination, because "quarantined" reads as "broken" and this test is not.

## What would reopen this

A PC change that moves the rate. The mechanism is the protocol's, but the *exposure* is PC's:
how long an instance is a member before it is stopped, and how many are stopped at once, are the
harness's choices, and a future PC close path that (for instance) delays LeaveGroup would lengthen
every open phase. The rate is the instrument that would show it, which is the argument for option 1
over option 3.

## Related

- [`test-largenumberofinstances-residual-failures-measured-not-explained.md`](test-largenumberofinstances-residual-failures-measured-not-explained.md) - the measurement this rests on
- [`perf-throughput-regression-gate.md`](perf-throughput-regression-gate.md) - the rolling-median comparison the rate could reuse
