# `largeNumberOfInstances`: the residual failures are measured now, and still not explained

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->

**Renamed 2026-09-01.** This note was `...-residual-failures-unmeasured.md`, and the "unmeasured"
was true for as long as every run happened on a machine that never failed. It stopped being true when
the test failed on Linux, so the filename was making a claim the contents contradicted.

## The claim

`MultiInstanceRebalanceTest.largeNumberOfInstances` documented its acceptance as *80%+ pass rate
(currently ~90%)*, and its javadoc attributes the residual failures to the broker rather than to PC:

*(Update 2026-08-18: the test has since been split - the rate is now explicitly the profile's
measured OUTPUT rather than an acceptance gate, and a deterministic correctness twin,
`scriptedChurnRoundsCompleteWithoutStall`, gates in the integration lane. The aggressive profile's
parameters and churn are unchanged, deliberately, so the residual rate this note is about remains
the baseline to measure against.)*

> the remaining ~10% failure is the Kafka consumer group protocol under extreme membership churn
> (`assignedPartitions=0`), not a PC bug

## Why that needs settling before the rate is tuned

**It is asserted, never measured.** No experiment separates "the group coordinator cannot converge at
this churn rate" from "PC has a defect that only appears at this churn rate", and the two produce the
same visible outcome: instances alive, assignment empty, no progress.

That matters more than it looks, because the obvious response to a flaky stress test is to back the
parameters off until it passes - and if any part of the residual is PC's, backing off **hides a real
defect** rather than removing a confound. That is precisely the shape that let the confluentinc#857
deadlock survive four months: astubbs/parallel-consumer#68 gave every test an uncontended broker, the
suite went green, and the defect was untouched.

## What would settle it

A control arm. Same churn against a plain `KafkaConsumer` group with no PC in the path, or PC
instrumented to distinguish "coordinator never assigned us partitions" from "we were assigned and
made no progress". If the bare consumer group fails at the same rate, the claim holds and the
parameters are simply past what Kafka converges at. If it does not, the residual is ours.

Until then the javadoc should say the claim is unverified rather than state it as fact - and it now
does.

**Update 2026-08-18: the capacity profiles now take a scale factor**, `-Dperf.scale=<n>`, which makes
a cheaper version of that control arm available before anyone builds a bare-consumer harness. If the
residual is the coordinator failing to converge at this churn rate, the failure rate should move with
scale; if it is a PC defect, a defect does not care how many partitions there are. That is weaker
than the bare-consumer arm and does not replace it - a rate that moves with scale is consistent with
both a coordinator limit and a load-sensitive PC bug - but a rate that does NOT move with scale is
hard to explain as "Kafka cannot converge at this size", and it costs one flag rather than a new
harness. Correctness profiles deliberately cannot read the factor, and a guard enforces it
(`onlyCapacityProfilesMayScale`).

## Related

- `docs/solutions/workflow-issues/prove-the-problem-exists-before-writing-the-fix.md`
- `docs/solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md` -
  the astubbs/parallel-consumer#68 precedent, where an infrastructure change made a suite green
  without fixing anything

## Measured 2026-08-28 - and the measurement does not answer the question

Ten consecutive runs of `largeNumberOfInstances` on this tree, on an M2 Mac Pro: **ten passes, no
failures, not one "No progress beyond N records" line.** amrynsky's January report of "every other
run of this test is failing" does not reproduce.

**Correction, same day: those ten runs WERE at the historical configuration.** An earlier version of
this entry said the profile had been dialled away and the measurement therefore answered nothing.
That was wrong. `-Dperf.scale` multiplies the baseline and, absent, is 1.0 - which its own commit
states "reproduces every historical number exactly". So the runs were at 500k records, 80 partitions
and 12 instances: amrynsky's configuration. **Ten green at the historical size is real evidence that
the January rate does not reproduce here**, on this hardware.

What it still cannot separate is hardware from code. A fast desktop is not a CI runner, and the
residual this note is about was reported on one.

**What would actually answer it**, in order of cost:

- Pin the capacity profile explicitly rather than letting it dial, and re-run. Ten passes at a fixed,
  stated load is a result; ten passes at a load the machine chose is not.
- Run it on the constrained hardware the soak harness note argues for - one CPU, 4GB - which is far
  closer to what a CI runner or a pod gets than this desktop is.
- Only then compare against January's claim, and say which profile each side was running.

**The general lesson, which is the same one three times this week.** A green run is only evidence
about the thing that actually ran. The reproducer was inverted; the deadlock probe's window never
opened; and here the test itself was reshaped between the claim and the measurement. Checking what
ran costs one `git log` and it has changed the answer every single time.

## The scale sweep, 2026-08-28 - CONFOUNDED, and the confound is instructive

Ran scales 1, 2 and 4 on a desktop, three runs each. Scale 1 passed three times; scales 2 and 4
failed every run. A rate rising that sharply with size would, on its face, be the coordinator
struggling to converge.

**It is not evidence of that, because the sweep was run past the machine's documented capacity.**
The test's own javadoc gives the intended scales: `0.25` for a laptop, `4` for the 32-core highcpu
runner. So the laptop's baseline is a QUARTER, and running a desktop at 4 is roughly sixteen times
its intended load. The failures are overload.

The failure mode confirms it: the higher-scale runs did not trip the `No progress beyond N records`
assertion at all. They timed out waiting for the workload to finish - expecting 1,000,000 records at
scale 2 and 2,000,000 at scale 4 - with `TimeoutException` counts climbing steeply between them.
That is a machine not finishing in time, not a fleet failing to converge.

**Which puts a question against an assumption in the test itself.** Its javadoc states that a timeout
here "is a NO_PROGRESS verdict (a genuine stall), never 'the machine was slow'". This run is a
counter-example: the machine WAS slow, because it was deliberately overloaded, and the result was
timeouts. The claim may hold at the intended scale and clearly does not hold generally, so it is
worth narrowing before a future run reads an overload timeout as a stall.

**What a valid version of this experiment needs.** Hold the machine's relative load constant and vary
only what is under test - which means running the sweep on hardware sized for each scale, or scaling
down to `0.25` and comparing `0.25` against `0.5` on this desktop rather than `1` against `4`. The
question "does the rate move with scale" is answerable; this sweep did not answer it.

**Unchanged by all of this:** ten runs plus three at scale 1, the historical configuration, all
passed. That result stands and is the one worth carrying forward.

## The valid scale sweep, 2026-08-28: no failures at any sane scale

Re-ran at 0.25 and 0.5 - the documented laptop baseline and double it - three runs each. **All six
passed.** With the thirteen earlier runs at scale 1, that is nineteen green across three scales on
this hardware, and no failure to compute a rate from.

**"Does the rate move with scale" therefore has no answer here, because there is no rate.** That is a
different and weaker result than "the rate is flat", and it is stated that way on purpose: flat would
have pointed at PC, and this points nowhere. What it does establish is that amrynsky's January
"every other run" does not reproduce on this machine at any scale it is documented to run at.

**What still separates the hypotheses** is the bare-consumer control arm this note has always asked
for, or the same sweep on hardware that actually fails. A desktop that never fails cannot tell a
coordinator problem from a PC problem.

Runtime, for whoever plans the next one: roughly 70 seconds a run at 0.25 and under two minutes at
0.5. Cheap enough to run in bulk, which is the argument for hunting on the constrained containers the
soak-harness note proposes rather than on a fast desktop that passes everything.

## 2026-09-01, on Linux: it fails here, and the failure is a stall rather than an overload

The desktop that never failed was one machine. On a Linux box - 32 physical cores, but
`/etc/environment` pins `JAVA_TOOL_OPTIONS=-XX:MaxRAM=48g -XX:MaxRAMPercentage=20
-XX:ActiveProcessorCount=8`, so every JVM sees eight - `largeNumberOfInstances` at the default scale
failed **once in ten consecutive runs**, plus a preceding pilot run that passed. The box was
otherwise idle: no other build, no other container.

**This is the first failure of this test reproduced anywhere but CI**, which is what the previous
entries were missing: nineteen green on an M2 Mac Pro could not separate the hypotheses because
nothing ever went wrong on it.

**It is a stall, not the overload the August sweep measured.** The detector's own verdict:

> `No progress beyond 432799 records after 11 rounds. [FLAT for 12s - it stopped rather than ran out
> of time | elapsed=62s | ...]`

`FLAT` is the discriminator the tracker was built to report - the count stopped moving rather than
moving too slowly. The confounded 2026-08-28 sweep failed the other way, with `TimeoutException`
at scales the machine could not finish, and that difference is why this run is evidence and that one
was not.

### What the ambient probe caught, and why it moves the claim

The autopsy block fired on the failing run and named a cause:

> `ZOMBIE_MEMBER/REBALANCE_BLOCKED: group ... dwelling in PreparingRebalance for 15s (bound 15s) - a
> member is not answering the rebalance (protocol-unresponsive)`

with `rebalanceDwell=15441ms`, `lagStagnation=33032ms`, and a long list of frozen partitions each
carrying comparable lag. The group is stuck in `PreparingRebalance` **waiting on a member**, and the
commit path is full of `RebalanceInProgressException` for as long as that lasts.

**That cuts against the claim this note is named for.** "The remaining ~10% is the Kafka consumer
group protocol under extreme membership churn" describes a coordinator that cannot converge. What
the probe describes is a coordinator doing its job and blocking on a member that went silent - which
is a member-side story, and the members are PC instances.

**It does not settle it, and the reason is specific.** The capacity profile's churn kills instances
continuously, and an instance killed mid-`JoinGroup` is by construction a member that stops
answering. So the observation is consistent with both a benign race in the harness's stop path and a
PC defect that wedges a live member. The question is now much sharper than it was - **is the
unresponsive member one the harness stopped, or one still running?** - and that is a question about
identity, not about rates.

### Three instrumentation gaps stand between that question and its answer

Found while trying to answer it from the artefacts this run already produced:

- **`ProgressTracker.withDiagnostic(...)` has never been called.** The hook exists, its javadoc names
  `pc::describeProgress` as the intended argument, and `grep -rn withDiagnostic --include=*.java` finds
  only the declaration and its own documentation. So the verdict line ends with *"no consumer
  diagnostic supplied"* on every stall this test has ever produced - including this one. Wiring it is
  the cheapest thing on this list and it is what separates "not trying" from "trying and not
  finishing".
- **`dumpInstanceState` deliberately cannot report the assignment.** Its own comment says
  `assignedPartitions` was dropped when astubbs/parallel-consumer#393 deleted the mirrored accessor,
  and that reading the live assignment would need a consumer handle the dump does not have. That
  accessor is exactly what would name the silent member.
- **Two of the per-run log silos are empty on every passing run** - `probes.log` and
  `pc-poll-rebalance-lifecycle.log` - because the probe reports peaks at DEBUG on a clean pass and
  only writes the autopsy on failure. The other silos do carry content on passes. The consequence is
  that a failing run has **no passing run to be read against**: whether a 15s dwell is anomalous or
  routine under this churn is not answerable from what the runs record. Inverted, and unexplained:
  `pc-shard-work-state.log` is populated on passes and **empty on the failing run**.

### Reproducing

`bin/exp-measure-large-instances-failure-rate.sh` with `JAVA_HOME` set to a real JDK 17 - its
`pc_experiment_java_home` looks for an SDKMAN candidate that does not exist outside the machine it
was written on, and says so rather than failing, so an unset `JAVA_HOME` runs under whatever the
wrapper finds. Roughly ninety seconds a run at the default scale.

## 2026-09-01: enabled in the gating lane, deliberately, and what that is now testing

All three capacity profiles in `MultiInstanceRebalanceTest` are enabled and `Performance Tests` is a
required check, so this test now blocks merges. That reverses a three-day-old decision to hold them
out, and the reasoning is worth keeping because it is not the obvious one.

**The ten-run measurement predates the control-loop fix.** One failure in ten was measured on a tree
that still evaluated a shard-wide sum as an unguarded `log.trace` argument on every control-loop pass
(`docs/solutions/performance-issues/slf4j-defers-formatting-not-argument-evaluation-2026-09-01.md`). So that rate describes the
unfixed tree, and quoting it as this test's failure rate going forward would be quoting a number for
code that no longer exists.

**The failure mode makes the connection plausible rather than idle.** The failure was a stall with
the group dwelling in `PreparingRebalance` waiting on a member that stopped answering - and a control
thread doing an O(shards) scan every pass, fastest under saturation, is a candidate reason a live
member answers late. That is a hypothesis, not a finding: the profile also kills instances
continuously, so a silent member may simply be one the harness stopped. **The sharp question is
unchanged - is the unresponsive member one the harness stopped, or one still running?** - and the
three instrumentation gaps above still stand between it and an answer.

**What the enablement buys, and what it costs.** It buys the rate on the fixed tree, which nothing
else was going to produce: this test has never run on master, so there is no history to mine and no
baseline series to consult. It costs merge-blocking on an unlucky run. A red run here is a prompt to
read the `AMBIENT PROBE AUTOPSY` block, not a verdict on PC - and if the rate does not improve, the
honest reading is that the log-argument defect was not this test's problem, which is itself a result
worth having.

### First result on the fixed tree, 2026-09-01: it passed

All three capacity profiles ran in the gating lane at `92c5d5b70` and passed - `MultiInstanceRebalanceTest`
3 tests, 170.9 s, no failures, no `AMBIENT PROBE AUTOPSY` block and no `No progress beyond` line.

**One pass is not a rate**, and this note exists because a rate is what the question needs. The Linux
measurement that produced one failure in ten was on the unfixed tree, so the honest position is that
this test's failure rate on the current tree is **unknown, with one green observation**. The cheapest
way to get a real number is `bin/exp-measure-large-instances-failure-rate.sh` on the fixed tree - the
same harness, so the two rates would be comparable, which is what makes it worth doing rather than
waiting for CI to accumulate runs.

## 2026-09-01, on CI, on the FIXED tree: the stall reproduces, and that answers the open question the wrong way

Second run of the enabled capacity profiles on CI. `largeNumberOfInstances` **ERRORED at 154.6 s**,
run `33506680281`, head `55edffaf4` - and the signature is the Linux failure's, line for line:

```
No progress ... [FLAT for 13s - it stopped rather than ran out of time | elapsed=152s
                 | no consumer diagnostic supplied]
ZOMBIE_MEMBER/REBALANCE_BLOCKED: group dwelling in PreparingRebalance for 15s (bound 15s)
                 - a member is not answering the rebalance (protocol-unresponsive)
peaks: rebalanceDwell=15584ms lagStagnation=27500ms
```

**THE CONTROL-LOOP FIX DOES NOT ADDRESS THIS FAILURE MODE.** That tree carries the supplier form, and
the stall arrived anyway with the same detector verdict, the same probe violation and a comparable
dwell. The reasoning that enabling these tests would put the hypothesis under test was right; the
hypothesis lost.

**A speculation recorded here on the same day is hereby weakened.** It was suggested that a control
thread doing an O(shards) scan every pass was a candidate reason a live member answers a rebalance
late - which is why the measured one-in-ten rate was said to describe the unfixed tree and not to
transfer. The rate may still not transfer, but the *mechanism* does not survive: removing the scan did
not remove the stall. Treat the throughput defect and this stall as **two separate problems** that
happened to be found in the same week.

**What the rate is now: unknown, with one pass and one failure on the fixed tree.** Two runs is not a
rate and this note's whole subject is that a rate is what the question needs. It is recorded because
CI logs expire and this is the first CI reproduction on the fixed tree.

### The instrumentation gap bit again, exactly as predicted

The verdict still ends `no consumer diagnostic supplied` - `ProgressTracker.withDiagnostic(...)` is
still never called, so the run cannot say what PC believed it was doing. That is now the difference
between a sighting and a diagnosis, and it is the cheapest item on this note's list.

One thing this run adds that earlier ones did not: the frozen-partition dump shows **the whole
assignment stagnant with comparable lag** (roughly 520-570 across the partitions listed, all stagnant
27 s), rather than one partition or shard lagging. That is consistent with a group-wide block rather
than a per-shard wedge, which narrows where to look next.

## 2026-09-02, on CI, on the FIXED tree: the third sighting, and the dwell keeps landing in the same place

Third run of the enabled capacity profiles on CI, and the second failure.
`largeNumberOfInstances` **ERRORED at 157.1 s** (suite 238.3 s), run `33579370081`, job
`100090259660`, head `650a8236b` - the merge of master that brought the branch level. Signature
matches the 2026-09-01 CI failure and the Linux one before it:

```
No progress beyond 489208 records after 11 rounds. [FLAT for 13s - it stopped rather than ran out
                 of time | elapsed=155s | no consumer diagnostic supplied]
ZOMBIE_MEMBER/REBALANCE_BLOCKED: group 'group-1-152534349' dwelling in PreparingRebalance for 15s
                 (bound 15s) - a member is not answering the rebalance (protocol-unresponsive)  x7
peaks: rebalanceDwell=15531ms lagStagnation=33120ms
```

Scenario: `expected: 500000 commit: PERIODIC_CONSUMER_ASYNCHRONOUS order: UNORDERED max poll: 500`.
Frozen partitions: the whole assignment stagnant at 24 s with lag clustered 410-463.

**There is no seed to record, and that is a property of this test rather than an omission.**
`largeNumberOfInstances` is not a chaos scenario and never constructs a `ChaosSeed`, so nothing
pins its interleaving and a replay is not available the way it is for the `bug-857-family.md`
captures. What identifies this run instead, once the CI logs expire, is what is written above: the
run and job ids, the head, and the group id `group-1-152534349` with topic
`MultiInstanceRebalanceTest-input-163585785-1253452590`.

**The rate on the fixed tree is now one pass and two failures in three runs.** Three runs is still
not a rate, and this note's whole subject is that a rate is what the question needs. It is recorded
because CI logs expire.

### One new observation: the dwell peak is suspiciously repeatable

Across the three failures the rebalance dwell peaks are **15441 ms, 15584 ms and 15531 ms** - a
spread of under 150 ms on a measurement that is otherwise free to be anything. The stagnation peak
is not tight in the same way (33032, 27500, 33120 ms), so this is not simply the whole run being
deterministic.

A dwell that always stops within a fifth of a second of the same value looks like a **timeout being
hit rather than a wait being observed** - some bound at roughly 15.5 s ending the dwell every time.
Worth identifying which one: if the coordinator is evicting the silent member on a fixed timer, the
member's silence is the thing to measure and the dwell is just that timer read back. That would
make `rebalanceDwell` far less informative than it looks, since it would report the bound rather
than the member's actual response delay.

Stated as an observation, not a finding - three points is enough to notice a pattern and not enough
to attribute it, and the probe's own violation bound is also 15 s, which could be doing some of the
work. Checking it costs reading the group's `rebalance.timeout.ms` against the probe's bound.

**Unchanged:** the sharp question is still whether the unresponsive member is one the harness
stopped or one still running, and the verdict still ends `no consumer diagnostic supplied` because
`ProgressTracker.withDiagnostic(...)` is still never called. Two CI failures have now each been one
wiring change away from saying what PC believed it was doing.
