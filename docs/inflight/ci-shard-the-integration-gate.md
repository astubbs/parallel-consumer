# Shard the Integration Tests gate across runner jobs

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->
<!-- post-merge: checked - the state below names the PR in the past tense; see its own wording. -->
<!-- inflight-state: deferred - the two-shard split LANDED in astubbs/parallel-consumer#442; what
     remains open here is only the four-shard follow-up, which was built, measured and deliberately
     not taken. Without this the note reads as wholly open in the session index, because it leads
     with IMPLEMENTED and absent means open. -->

The `Integration Tests` lane is the PR build's critical path - 620s against ~500s for the next
slowest. Sharding it across runner jobs is the lever that actually moves it.

<!-- post-merge: checked-begin - names astubbs/parallel-consumer#442 explicitly and in the past
     tense, so it stays true once that PR has merged and its branch is gone. Nothing here says
     "this branch" or "this PR", and the arrangement it describes is the one that landed: a single
     PR, not a stack. -->
**IMPLEMENTED in astubbs/parallel-consumer#442**, which carries the whole split: two shards, the
857 probe split four ways, and the heavy set re-derived from the measurements that followed. ONE
named heavy set plus a catch-all defined by subtraction - not the Chaos Pain Suite's four balanced
bins, and the difference is the point (below).

**FOUR SHARDS WERE BUILT AND MEASURED, AND DELIBERATELY NOT TAKEN.** Green at 355s against the two
shards' 416s, but 1318s of runner time against 792s, and ~500s of MANUFACTURED extra test work -
per-shard fixed costs are paid per shard, the same way per-fork costs were when forkCount went 4->6.
61s of critical path did not justify a 66% machine-time increase and four lists to maintain instead
of one. Preserved on `ci/shard-integration-four` if that trade ever looks different.
<!-- post-merge: checked-end -->

## Why four shards needed the 857 probe split first, and two did not

Modelled from measured per-class times, and the model reproduced the two-shard measurement to
within 3s (predicted 516s, measured 519s):

| config | critical path | runner-minutes |
|---|---:|---:|
| 2 shards, probe intact | 516s modelled, **519s measured** | 870s |
| **4 shards, probe intact** | **516s modelled** | 1417s |
| 2 shards, probe split | 440s modelled, **450s measured** | 780s |
| 2 shards + split + rebalance, cumulative (**shipped**) | **416s measured** | 792s |
| 4 shards, probe split | 337s modelled, **355s measured** | 1318s |

**The pre-split model was wrong about the split itself**, and it matters for anyone re-deriving
this: it assumed splitting a ~356s `@RepeatedTest(20)` class four ways gives four 89s classes. The
measured classes are **138-166s** - the repetitions carry per-class fixed cost that used to be paid
once. So the split buys less than arithmetic suggests, and splitting FURTHER would pay less again.

`Rebalance857CommitSyncDeadlockProbeIT` was `@RepeatedTest(20)` in one ~356s class, and forks
cannot split a class - so whichever shard held it WAS the critical path, at 516s, for any shard
count. **Four shards bought nothing while it was intact** and cost 547 extra runner-seconds.
Order matters more than count, and the MEASURED attribution says so more clearly than the model did:
splitting the probe bought 69s (519 -> 450), rebalancing the heavy set another 34s (450 -> 416) - and
those two are CUMULATIVE, not alternatives, nor even independent. The rebalance moves
`Rebalance857CommitSyncDeadlockProbeIT` into the heavy shard, which is a 134s class only BECAUSE the
split happened; at its pre-split 356s it would have blown the heavy shard past the catch-all
immediately. The split created the granularity the rebalance needed.

Going from two shards to four would buy 61s more (416 -> 355) for 526 extra runner-seconds. An
earlier version of this paragraph said the split was worth 161s and the shard count 18s; both came
from the pre-split model, which assumed the four probe classes would be 89s each when they measure
138-166s. Same lesson as the serial-build work above, arrived at with better numbers.

**The second required check.** `Integration Tests (heavy)` produces its own status context, and adding
a job does not add a requirement, so the heavy shard's classes would have been non-gating. It was added to
the `master` ruleset's required checks alongside `Integration Tests` while astubbs/parallel-consumer#442 was
open; both shards gate. <!-- post-merge: checked -->

## Why this is the remaining lever, and why it was not taken first

Measured 2026-09-03 - full write-up in
[`docs/plans/2026-09-03-001-investigate-integration-gate-wall-time.md`](../plans/2026-09-03-001-investigate-integration-gate-wall-time.md).

**Within-job overlap is exhausted.** 1528s of test time runs in 420s of wall on four forks - about
91% parallel efficiency, which is near-linear and only happens because these tests spend most of
their time waiting on the broker rather than burning CPU. Six forks dropped to 75% efficiency AND
inflated total work 11%, so `forkCount=4` is the ceiling, not a starting point. Near-linear to 4 and
degrading at 6 is also the signature of a 4-core box.

So more overlap has to mean **more jobs**, which is this note. What stops it being the obvious first
move is the shape of the remaining cost:

**Each shard re-pays the serial build.** Of the 604s Maven step, ~136s is not tests at all -
`testCompile` 60s, `compile` 42s, javadoc 14s, delombok 8s, Truth codegen 7s. That part is paid once
per JOB, so two shards pay it twice and four shards four times. At today's numbers a 4-way split
costs roughly 400 extra runner-seconds before it saves anything.

**And test work only converts at 1:3.6.** Because four forks already overlap the waiting, removing
3.6s of test time buys 1s of wall - while removing 1s of *serial build* time buys a full second. The
build reduction is therefore worth ~3.6x per second AND it is the exact cost sharding multiplies.
Doing it first is not a detour from sharding; it is the thing that makes sharding pay.

## Keeping it from decaying - the part that is not the split itself

A partition sized from measurements is only right on the day it is measured. The failure is silent:
nothing goes red as the lists go stale, the lane just gets slower than it needs to be. Three
properties are what stop that here, and only the third is unusual:

- **The catch-all is defined by SUBTRACTION.** A new test runs there by default and can never
  belong to no shard. This is the inversion of N explicit bins, whose failure mode is a new class
  running nowhere with nothing going red.
- **A rename fails its named shard loudly while the catch-all keeps running the test.** Each named
  shard asserts a failsafe report for every class it was assigned, so the suite stays complete and
  the LIST is what gets reported as wrong. Failing in the safe direction is deliberate.
- **`bin/check-integration-shard-balance.mjs` recomputes the optimal partition from RECORDED
  per-class times and reports the drift.** So the same runs that make the lists stale also measure
  how stale they are: drift becomes a visible number instead of quiet decay. It is advisory by
  default - a shared runner's wall-clock is not stable enough to block a merge on, with 119s of
  measured noise on this lane - and takes `--fail-over <seconds>` for a caller that wants it
  blocking. It also names classes that are in a list but have no recorded history at all, which is
  what a rename or a deletion looks like before the build catches it.

Two guards sit underneath all of that and are about correctness rather than balance: no class may
appear in two lists (checked on every invocation - it would run and be paid for twice, and both
shards would pass), and every failsafe report in every shard must come from an `integrationTest`
package. That last one is what caught `-Dit.test=!Class` silently running the entire unit suite
under failsafe - a failure of 126 EXTRA tests, which no "ran at least N" gate can see.

## DEFERRED: the four-shard arrangement, built and measured, not taken

Kept as future work rather than a rejected idea - it works, it is faster, and the reason it is not
in use is a cost trade that could reasonably be revisited. The branch is
`ci/shard-integration-four`; everything below was measured, not modelled.

| arrangement | critical path | runner-minutes | per-shard walls |
|---|---:|---:|---|
| single job | **620s** | 620s | - |
| 2 shards, probe intact | **519s** | 870s | 476 / 519 |
| 2 shards + probe split | **450s** | 780s | 330 / 450 |
| 2 shards + split + rebalanced (**in use**) | **416s** | 792s | 416 / 376 |
| **4 shards + probe split** | **355s** | 1318s | 355 / 311 / 332 / 320 |

**Four shards buys 61s over the arrangement in use, for +526 runner-seconds.** That is a 15%
critical-path gain for a 66% machine-time increase, plus four lists to maintain instead of one. Not
worth it today. What would change the answer: runner-minutes becoming free or irrelevant, the suite
growing enough that the catch-all dominates again, or someone needing the gate under ~6 minutes for
a specific reason.

**Two things that run measured and are easy to get wrong from arithmetic alone:**

- **Shards MANUFACTURE work.** Total test time was 1545s at two shards and 2074s at four - the same
  tests. Per-shard fixed costs (JVM start, broker start, fixture setup) are paid per shard, exactly
  as per-fork costs were when `forkCount` went 4->6 and cost 11% more CPU for the same suite. Any
  model that treats total work as constant across shard counts will over-promise, and the one used
  here did.
- **Four shards is NOT worth having without the probe split.** Modelled at 516s either way with the
  probe intact, because a single unsplittable 356s class sets the floor for whichever shard holds
  it. Order matters more than count.

**If it is picked up, re-derive rather than restore.** The four-way lists on that branch were sized
from `Rebalance857CommitSyncDeadlockProbeIT` as one class and are stale by construction.

**And it needs a four-way calculator, which does not exist yet.**
`bin/check-integration-shard-balance.mjs` deliberately models only the shipped shape - one named
heavy set plus a catch-all - and searches only two-way "largest N classes" splits, because that is
the choice the guide in `bin/ci-integration-test.sh` actually offers a maintainer. Its LPT packer
already takes a bin count (`lpt(classes, n)`), so extending it means parameterising the shard count
and the reporting, not writing a new packer. Do that first; reading its current two-way number as a
four-way optimum would give the wrong partition shape.

## What this will need when it is picked up

- **Size the split from measured per-class durations, longest-first over N bins** - the chaos suite's
  design note is the precedent and `.github/workflows/maven.yml`'s chaos matrix comment records how
  it was done. The integration suite does not divide evenly: `PartitionStateCommittedOffsetIT` at
  160s puts a floor under any arrangement, and `Rebalance857CommitSyncDeadlockProbeIT` at ~340s puts
  a higher one until it is split (branch `optimize/ig-exp002-probesplit`, green and unmerged).
- **A single entry point that selects the shard's classes**, so this matrix and any on-demand
  dispatch cannot select tests differently - `bin/chaos-test.sh` and its `CHAOS_SCENARIOS` are the
  model, including its refusal to pass a shard whose requested tests produced no failsafe report.
  A shard that goes quietly idle is the failure mode to design against.
- **Do not raise `forkCount` inside a shard to compensate.** Measured harmful; see the plan document.

## Cost, stated honestly

Sharding buys critical path and spends aggregate runner-minutes. That was an explicitly acceptable
trade when this work was scoped (public repo, minutes are free), but it is a real trade and the
build-overhead multiplication above is the part that makes it worse than it first looks.
