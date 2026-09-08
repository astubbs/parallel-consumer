# `largeNumberOfInstances` measures the protocol - so it cannot gate a merge

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->
<!-- inflight-vetted: 2026-09-08 - the decision is implemented: the three profiles carry @Tag("capacity"), bin/performance-test.sh excludes it, and both scheduled runners collect their tallies; what stays open is the trend line, listed under Still open -->

## The decision the 2026-09-01 handoff left open, now answerable

`MultiInstanceRebalanceTest.largeNumberOfInstances` sits in the `Performance Tests` lane, which is a
**required** status check on master. Its own javadoc says a single run's outcome is not a verdict on
PC. Those two facts were already in tension on 2026-09-01; what was missing was knowing whether the
residual failures were PC's or the protocol's, because the answer decides the remedy.

**They are the protocol's, measured** - see
[`docs/solutions/test-flakiness/large-instances-residual-is-a-join-phase-held-open-by-churn-2026-09-05.md`](../solutions/test-flakiness/large-instances-residual-is-a-join-phase-held-open-by-churn-2026-09-05.md).
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

## DECIDED 2026-09-07: option 1. IMPLEMENTED 2026-09-07, except the trend line

The operator took option 1 - move the capacity profiles to the scheduled lane and record the rate.
Recorded here because this note is where the question was posed, and a note still reading "recommends"
after the call has been made is the kind of stale record that gets re-litigated.

### What landed

`@Tag("capacity")` beside the existing `@Tag("performance")` on all three capacity profiles;
`bin/performance-test.sh` - the required `Performance Tests` check - now passes
`-Dexcluded.groups=quarantined,capacity`; the tag is in the pom's default `excluded.groups` and in
the three CI wrappers' hardcoded copies of it, so a future capacity profile that forgets the
`performance` tag is still out of the gating lanes. **All three now run in the scheduled
`experiments` workflow, not just `largeNumberOfInstances`** - `pc_run_performance` passes
`-Dincluded.groups=performance` with an EMPTY exclusion, so any test it is pointed at is selected,
but no runner had ever pointed it at `cooperativeStickyRebalanceShouldNotStall` or
`gentleChaosRebalance`. Before `bin/exp-measure-capacity-profiles-failure-rate.sh` (added 2026-09-07
to close that gap), those two ran nowhere at all once this change excluded them from the gate - the
"a test that never runs is not a passing test" trap AGENTS.md names. `largeNumberOfInstances`'s
`@Quarantined` annotation and its
`docs/quarantined-tests.md` entry went in the same change, per rule 3 - a profile that no longer
gates does not need quarantining, and leaving both would say two different things about one test.
`scriptedChurnRoundsCompleteWithoutStall` was not touched.

`MultiInstanceRebalanceTest#capacityProfilesAreOutOfTheGatingLane` pins which profiles carry which
tags, and is untagged so it runs in the gating integration lane. It reads annotations, so it cannot
prove the lane's FLAGS match - that stays a human check, for the reason the last bullet below gives.

**The scale guard needed a fix the tag split would otherwise have broken silently**, and it is the
kind worth naming: `capacityProfileVerdict` read `m.getAnnotation(Tag.class)`, and `@Tag` is
`@Repeatable` - `getAnnotation` returns **null** once a second one is present. Adding `@Tag("capacity")`
would therefore have reclassified every capacity profile as a correctness gate, forbidding
`-Dperf.scale` on exactly the tests it exists for, with nothing red to say so until somebody ran a
scaled measurement and read the AssertionError as a product problem. It now reads
`getAnnotationsByType`.

### Still open, and deliberately not done here

**The rate is not yet recorded as a trend.** The scheduled `experiments` run tallies each batch, but
nothing compares a batch against a rolling median the way `bin/check-throughput-regression.mjs` does
for throughput, so a drift in the rate would still have to be noticed by a human reading two
artifacts. That is option 1's second half and it is a separate change - see the third bullet below
for why a fixed bound must not be used for it.

**What implementing it involved**, kept so the reasoning is not re-derived if any of it is revisited:

- Split the three capacity profiles off `@Tag("performance")` onto a tag the gating lane excludes and
  `experiments.yml` includes. `largeNumberOfInstances`, `cooperativeStickyRebalanceShouldNotStall` and
  `gentleChaosRebalance` move together: they share the profile shape, and leaving two behind would put
  the same measurement on both sides of a gate.
- `scriptedChurnRoundsCompleteWithoutStall` **stays where it is.** It is the deterministic correctness
  twin, 17/17, and it is what keeps these code paths gated once the capacity profiles leave.
- The rate wants recording the way the throughput report already records its share - against a rolling
  median rather than a fixed threshold, because the number moves with hardware and a fixed bound would
  become a second gate by accident.
- **An override REPLACES the pom's default `excluded.groups` rather than adding to it - that is how a
  quarantined test kept gating merges once already.** `bin/performance-test.sh` now passes
  `-Dexcluded.groups=quarantined,capacity` for exactly this reason (it used to pass an empty override,
  which excluded nothing). Whatever tag a future split uses, check what the lane actually selected
  rather than that the flags look right - the failure mode is a lane that runs nothing and passes.
- The `@Quarantined` annotation comes off in the same change or not at all: a profile that no longer
  gates does not need quarantining, and leaving both would say two different things about one test.
- **All three capacity profiles need a runner that actually selects them, not just the exclusion
  flags that let them through.** `bin/performance-test.sh`'s tag exclusion and `pc_run_performance`'s
  empty `-Dexcluded.groups=` only matter to a test some script actually points at; check
  `.github/workflows/experiments.yml` and every `pc_run_performance` caller name all three methods,
  not only `largeNumberOfInstances`. Missed the first time - fixed 2026-09-07 by
  `bin/exp-measure-capacity-profiles-failure-rate.sh` and a second weekly schedule slot.

The change that recorded this decision deliberately did not implement it: that one carried the
measurement which justifies it, not the lane move, and the two wanted separate review. The lane move
landed on 2026-09-07 with the quarantine registry clear-out.

## Options, and the reasoning behind the choice

The operator ruled out one option on 2026-09-01: **do not make the lane non-gating**, because GitHub
runners perform reliably enough that a baseline shift is real signal. That constraint stands. Within it:

1. **Move the three capacity profiles to a scheduled, non-required lane that records the rate.** ← **CHOSEN** The
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

- [`docs/solutions/test-flakiness/large-instances-residual-is-a-join-phase-held-open-by-churn-2026-09-05.md`](../solutions/test-flakiness/large-instances-residual-is-a-join-phase-held-open-by-churn-2026-09-05.md) - the measurement this rests on
- [`perf-throughput-regression-gate.md`](perf-throughput-regression-gate.md) - the rolling-median comparison the rate could reuse
