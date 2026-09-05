# `largeNumberOfInstances`: the residual failure is measured, reproduced, and now EXPLAINED

**Filename deliberately unchanged, 2026-09-04.** It says "not explained" and that is no longer true -
but it is cited from the `@Quarantined` annotation's `tracking` field, `docs/quarantined-tests.md`,
`docs/refactoring.md`, two sibling notes and a solutions write-up. Renaming it now would break eight
citations for a title that has to change again the moment the fix lands, so the claim is corrected
here instead. This is the same trade `docs/inflight/test-retry-queue-behaviour-untested.md` took.

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

## A candidate mechanism, and the two-arm experiment that will refute or support it - 2026-09-03

**Written before the result, deliberately.** The prediction is stated here first so that a run which
refutes it cannot be re-read afterwards as having supported something narrower.

### The candidate

The three sightings agree that a member stops answering the rebalance, and this note's open question
has been *which* member. A live one now has a mechanism, and it survives every exclusion already
recorded above:

- `RetryQueue`'s lock is **fair** - `new ReentrantReadWriteLock(true)` (grep
  `ReentrantReadWriteLock` in `RetryQueue.java`).
- `RetryQueue.remove()` takes `writeLock().lock()` **unbounded** - no `tryLock`, no timeout.
- It is reachable from the rebalance callbacks **on the broker-poll thread, inside
  `consumer.poll()`** - which is where the whole group waits.
- The controller thread holds the **read** lock for a whole scan (`ShardManager.getLowestRetryTime`,
  grep `retryQueueIterator`) and takes the **write** lock on *every successful record*
  (`ShardManager.onSuccess` -> `this.retryQueue.remove(wc)`).

So the lock is on the hot path at this profile's record rate, it is fair, and a rebalance callback
queues behind whatever holds it. A member that cannot get through its callback cannot send JoinGroup,
and the coordinator dwells in `PreparingRebalance` - the recorded signature, with the whole
assignment frozen rather than one shard wedged.

**Crucially it is mode-independent**, which is what the two prior exclusions were not: the
confluentinc#857 revoke deadlock closes only in `PERIODIC_CONSUMER_SYNC` and the transactional revoke
wait is gated on transactional mode, whereas this profile is `PERIODIC_CONSUMER_ASYNCHRONOUS`.

### This is not a new diagnosis - the defect is already fixed in an open PR nobody connected to it

astubbs/parallel-consumer#431 fixes exactly this lock, on exactly these callback paths
(`RetryQueue.tryRemove`, declining rather than waiting). It was found by the confluentinc#857
**defect-class sweep**, not by this stall, and its own body describes the second edge as "one scan
away from closing into a stall" - a near-miss found by pattern. Its `RetryQueueRebalancePathTest` is
3/3 red against master, **two of the three timing out on the write lock**, and green after.

**What is missing is the join between the two**: no run of `largeNumberOfInstances` has ever happened
on a tree carrying that fix. The link is shape, not observation, and this note does not claim
otherwise.

### The prediction, stated now

Two arms differing in **exactly** the three main-source files of astubbs/parallel-consumer#431
(`RetryQueue`, `ShardManager`, `ProcessingShard`), both carrying the same new fleet diagnostic, both
run by `bin/exp-measure-large-instances-failure-rate.sh 30` on the idle self-hosted `highcpu` box,
**sequentially - never concurrently**, because two 12-instance fleets on one machine reproduce the
overload confound that invalidated the 2026-08-28 sweep:

<!-- post-merge: checked -->
- **control** (the tree without astubbs/parallel-consumer#431) - *predicted to fail around 1 run in
  10*, reproducing the recorded rate and signature. Actions run 33752432746.
- **treatment** (the same tree plus that fix, and nothing else) - *predicted not to fail*, or to fail
  materially less often. Actions run 33802869646.

The two arms are named by run id rather than by branch, because the branches carrying them are
temporary and the runs are the durable record.

**What each outcome licenses, agreed before the data:**

- Control fails at roughly the recorded rate and treatment does not: the retry-queue lock is
  supported as the mechanism, and the flapper's fix is astubbs/parallel-consumer#431 rather than
  anything new. Support, not proof - 30 runs cannot separate a large effect from a total one.
- **Both fail**: the candidate is refuted and must be recorded as such here. The lock is then not
  the mechanism, whatever its other merits, and the fleet diagnostic below is what the next sighting
  has to work from.
- **Neither fails**: nothing is learned about the mechanism, and the honest reading is that the box
  was not the one that fails - the same weaker-than-it-looks result the nineteen desktop runs
  produced. It is not evidence for the fix.

### The instrumentation that lands with it

`ProgressTracker.withDiagnostic` had **zero call sites in the whole tree**, which is why all three
sightings end "no consumer diagnostic supplied". It is now wired to a per-instance fleet description
carrying `started` and `closePending` - the pair that answers this note's actual open question, since
a harness-stopped instance mid-close reads `started=false, closePending=true` and a live one does
not. It goes in the thrown message rather than only the log, because a CI log is truncated and a
failsafe XML is not.


## RESULT, 2026-09-04: the candidate is REFUTED and the mechanism is identified

Both arms ran on the idle self-hosted `highcpu` box, sequentially, 30 iterations each.

| arm | tree | failures |
|---|---|---|
| control (run 33752432746) | without astubbs/parallel-consumer#431 | **2 / 30** |
| treatment (run 33802869646) | + that fix, nothing else | **1 / 30** |

**The retry-queue lock is not the mechanism.** 2/30 against 1/30 is no difference worth a claim, and
the treatment arm's failure carries the control's signature line for line. The prediction written
above said both arms failing refutes the candidate; both arms failed, so it is refuted, and
astubbs/parallel-consumer#431 should not be described anywhere as fixing this test. The control arm
DID reproduce at 6.7%, consistent with the recorded one-in-ten, so the experiment had the power to
show an effect and there was none.

**A measurement trap this nearly walked into, recorded because it would have inverted the result.**
The runner's `/tmp` persists, so the treatment artifact's tally contained **all 60 rows** - both
arms - not its own 30. Read naively that is "3 failures in 60" and a halved rate. The `ref=` column
in `bin/exp-measure-large-instances-failure-rate.sh` is the only reason the arms could be separated
at all; `experiments.yml`'s header had predicted exactly this and called it a known limitation.

### What the fleet diagnostic showed, which is the actual answer

This note's standing question was whether the silent member is one the harness stopped or one still
running. **It is one the harness stopped - and it is silent because of PC's close path.**

In the worse control failure, **10 of 12 instances sat in `state=CLOSING`** with
`closedOrFailed=false`, their last completed poll pass **23-25 seconds earlier**; the two survivors
were `RUNNING` and polling 146ms earlier. The treatment failure is identical: ten
`closePending=true`, the same two alive.

The chain, every link observed:

1. A chaos stop calls `pc.close()` -> `closeDontDrainFirst()` -> `transitionToClosing()`.
2. `BrokerPollSystem.handlePoll()` is guarded on `runState == RUNNING || DRAINING`, so the instance
   **stops calling `consumer.poll()` the moment it enters `CLOSING`** - while its `KafkaConsumer` is
   still an open member of the group. The consumer is closed later, in `doClose()`.
3. A member that does not poll does not send JoinGroup. The coordinator dwells in
   `PreparingRebalance`: the recorded `ZOMBIE_MEMBER/REBALANCE_BLOCKED`, "a member is not answering".
4. The close cannot finish either. All ten logged *"Execution or timeout exception while waiting for
   the control thread to close cleanly (state was CLOSING)"*, while `ConsumerOffsetCommitter` filled
   with `RebalanceInProgressException` - logged on `pc-broker-poll-PC-0` and `PC-11`, the only two
   instances still running.
5. So the rebalance waits on the closing members and the close waits on the rebalance. **After
   `waitForClose` times out the PC stays in `CLOSING` for good** - consumer never closed, member
   never leaves. That is why the detector reports `FLAT` rather than slow: it does not recover.

**`DRAINING` does not have this problem, and that is the tell.** `drain()`'s own comment says the
poller "must keep calling `consumer.poll()`" while draining, and `isCloseInProgress()` is documented
as deliberately NOT true for `DRAINING` for that reason. `CLOSING` took the opposite choice, and the
`closeDontDrainFirst()` path - which is what `close()` calls - goes straight there.

### This is not a test-only defect

Any application closing a PC instance while its group is rebalancing does this to its peers; a
rolling restart of a PC fleet is the ordinary case. The test is aggressive enough to hit it reliably
at ~7%, not doing something an application would not.

### What is NOT yet established

Which of the two calls in the closing instance actually holds it - `maybeDoCommit()` retrying a
commit that cannot succeed mid-rebalance, or `consumerManager.close()` inside `doClose()`. Both are
on the path and the logs show the commit failing; neither has been isolated. **The fix should not be
chosen before that is pinned**, because "keep polling until the consumer is closed" and "do not
block the close on a commit that cannot land" are different repairs.


## The chaos monkey's victim selection was excluding one instance - fixed elsewhere, 2026-09-04

Noticed while reading the monkey during this investigation: `submitChaosMonkey` drew
`(int) ((size - 1) * Math.random())`, which is `0 .. size - 2`, so the highest-indexed secondary was
**never** toggled - excluded outright rather than merely unlikely. One instance in the fleet never
churned.

**It is not the cause of the stall**, and nothing here depends on it: the stall reproduces with ten
instances closing at once, and the excluded instance is one the monkey simply never touched. It is
recorded here because it **changes what this profile measures**, and this note owns the profile's
rate. Fixed in astubbs/parallel-consumer#441, which deliberately does not re-measure - so **the
2/30 baseline above was taken with the old selection**, and a rate compared across that merge is not
comparing like with like.


## THE BLOCKING CALL IS NAMED, 2026-09-04 - and it is the astubbs/parallel-consumer#80 defect one state along

Run 33824474506, 3 failures in 60 (5%). Every stuck instance's `pc-broker-poll` thread:

    RUNNABLE parked-at=sun.nio.ch.EPoll.wait
      via NetworkClient.poll <- ConsumerNetworkClient.poll
          <- ConsumerNetworkClient.awaitPendingRequests(ConsumerNetworkClient.java:355)
          <- AbstractCoordinator.close(AbstractCoordinator.java:1140)
          <- ConsumerCoordinator.close(ConsumerCoordinator.java:987)
          <- ClassicKafkaConsumer.close(ClassicKafkaConsumer.java:1140)

**The poll thread is inside `consumer.close()`, waiting on the group coordinator.** Not a PC lock,
not a sleep, not a commit retry - all three of those were candidates and all three are refuted by
this frame.

### The complete chain

1. A stop calls `pc.close()` -> `closeDontDrainFirst()` -> `transitionToClosing()`.
2. `BrokerPollSystem.handlePoll()` is guarded on `runState == RUNNING || DRAINING`, so the instance
   **stops calling `consumer.poll()`** while its consumer is still an open group member.
3. `doClose()` -> `consumerManager.close(DEFAULT_TIMEOUT)` -> `consumer.close(30s)` ->
   `AbstractCoordinator.close()` -> `awaitPendingRequests()`, which waits on the coordinator.
4. The coordinator cannot answer: the group is in `PreparingRebalance` waiting for JoinGroup from
   its members - **including the ones now sitting in this wait**. Each closing member is waiting for
   a coordinator that is waiting for it.
5. So the close burns its full 30s budget. Throughout it the consumer is a group member that does
   not poll and does not answer: `ZOMBIE_MEMBER/REBALANCE_BLOCKED`. The observed 23-25s poll-pass
   age is that budget, and `waitForClose` (a shorter budget) gives up first, which is why ten
   instances logged a `TimeoutException` while still `CLOSING`.
6. The aggressive profile has up to 6 of 11 instances closing at once, so the group freezes
   wholesale and the fleet stops: detector `FLAT`, whole assignment stagnant at comparable lag.

### This is a known defect class, fixed once already, and CLOSING did not get the fix

astubbs/parallel-consumer#80 fixed exactly this for `DRAINING` - "draining PC stops polling - 10kHz
busy-spin + zombie partition hold". Its guard,
`BrokerPollSystemDrainTest.drainKeepsPollingConsumer_staysRebalanceResponsiveWithoutSpinning`, states
the reason in its own javadoc: *"rebalance participation (rejoin / revoke-ack) happens inside
`consumer.poll()`; a draining consumer that never polls cannot respond to rebalances while its
background heartbeat keeps it a live member"*. Owning write-up:
`docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md`.

`CLOSING` never received that fix, and **`close()` reaches it without passing through `DRAINING` at
all** - `closeDontDrainFirst()` transitions straight to `CLOSING`, so the most common close bypasses
the protected state entirely.

### The invariant to restore

> **A PC instance that has not yet left the group must keep polling it.**

`DRAINING` honours it since astubbs/parallel-consumer#80. `CLOSING` does not, and hands Kafka's
`consumer.close()` a 30-second budget to discover that.

### Not a test-only defect

Any application closing a PC instance while its group is rebalancing holds up every peer for up to
the close timeout. A rolling restart of a PC fleet is the ordinary case; this profile just does it
often enough to catch.


## A verification that measured the wrong tree, 2026-09-05 - recorded so its number is never quoted

Run 33836808270 was dispatched to verify the fix and returned **4 failures in 60**. That number says
nothing about the fix, and it is written down here so nobody later finds it and reads it as
"the fix did not work".

It was dispatched at head `576b4f04` - the **first** version of the fix, which polled from
`BrokerPollSystem.handlePoll()`'s `CLOSING` branch. That version is now known not to fire for the
interleaving that matters: `transitionToClosing()` runs on the caller's thread and can land mid-poll,
after which the woken poll completes the `RUNNING` iteration and `doClose()` runs immediately, so the
branch is skipped. CI caught it as a unit failure the same hour. The experiment was already queued
behind other batches by then, so it measured a tree whose fix largely does not engage - and duly
reported the unfixed rate.

Its 4/60 sits with every other pre-fix measurement rather than against them: 2/30 control, 1/30 with
astubbs/parallel-consumer#431, 3/60 and 3/60 on the two diagnostics-only trees. Six trees, one rate,
about 5%.

**The lesson is the one this corpus already carries three times, and it caught a fourth here: a run is
evidence only about the thing that actually ran.** The instrument that saved it was the `ref=` column
in the tally, added to `bin/exp-measure-large-instances-failure-rate.sh` for a different reason
entirely - separating two concurrent arms on a shared `/tmp`. Without it the artifact would have been
read as "the verification run", because that is what it was dispatched as.

Re-dispatched at `eace35700`, the first tree carrying the discharge poll in `doClose()` plus the
review fix that keeps it paused. That result is the one this note is waiting on.


## THE FIX DOES NOT FIX IT - measured 2026-09-05, and the rate did not move

Run 33913401545, head `eace3570`: the first tree carrying the discharge poll in `doClose()` plus the
review fix that keeps the assignment paused. **2 failures in 60.**

That is not a result. Against 3/60 and 3/60 on the two diagnostics-only trees and 2/30 on the
control, 2/60 is the same rate. Six trees before it, one rate, about 5%; this is the seventh.

**And the signature is unchanged**, which is the part that matters more than the number. The failing
run's fleet dump still reads `Instance 1..6: closePending=true state=CLOSING`, with the ambient probe
still reporting `ZOMBIE_MEMBER`/`REBALANCE_BLOCKED`. Whatever the discharge poll achieved, instances
still sit in `CLOSING` as silent group members and the fleet still freezes behind them.

### What this does and does not overturn

**Still established**, and not weakened by this: the mechanism. Ten of twelve instances parked in
`ClassicKafkaConsumer.close -> ConsumerCoordinator.close -> AbstractCoordinator.close ->
awaitPendingRequests` is an observation, not an inference, and a closing member that has not left the
group cannot answer a rebalance. The diagnosis stands.

**Overturned**: that ONE poll before `consumer.close()` is enough to discharge what the coordinator is
waiting for. It is not. A rebalance is not one round trip - JoinGroup and SyncGroup are separate
exchanges, and a single 1ms poll can complete neither reliably, let alone both, while eleven other
members are churning.

**The two review-found corrections keep their value regardless** - the `CLOSING` pause arm stops the
discharge poll being a live fetch, and the `ConsumerManager` one-attempt allowance is what lets any
poll happen during close at all. They are prerequisites for any version of this fix, not consolation.

### What the next attempt has to do differently

Poll *until the member has actually left or the rebalance has settled*, bounded - not once. The
candidates, in the order they should be tried:

- **Keep polling in `CLOSING` until the assignment is empty or a short deadline passes**, then close.
  That is the DRAINING shape - a loop, not a single call - and it is what astubbs/parallel-consumer#80
  actually did for its state.
- **Leave the group explicitly before closing** (`unsubscribe()`), so the member stops being a member
  at the same moment it stops answering, instead of afterwards. Cheaper to reason about, but it runs
  the revoke callback on this thread and that path has its own history.
- **Bound the close budget** so a member that cannot leave holds the group for 2s rather than 30s.
  Mitigation, not a cure, and it should not be reached for first.

**Do not read the third as the easy one.** Backing the timeout off is what makes the rate look better
without the mechanism changing, and this note exists because that move has already cost this project
four months once.


## A deterministic reproducer was built, and it does NOT reproduce - 2026-09-05

`ClosingMemberRebalanceIT`, on `ManagedPCInstance` (the capacity profile's own harness), forces the
window the profile only draws by chance: the admin client is polled until the coordinator reports
`PREPARING_REBALANCE`/`COMPLETING_REBALANCE`, and only then are members closed. A matrix over the two
variables the profile has and the green twin lacks, moved one at a time:

| assignor | simultaneous closers | victims' close | survivors |
|---|---|---|---|
| eager | 1 | 0.0s | kept consuming |
| cooperative | 1 | 0.0s | kept consuming |
| eager | 3 | 0.0s, 0.0s, 0.0s | kept consuming |
| cooperative | 3 | 0.1s, 0.0s, 0.0s | kept consuming |

Commit mode `PERIODIC_CONSUMER_ASYNCHRONOUS` throughout, as in the profile. **Every close was
instant and no survivor stalled.** (The full detached run reported all four cases as errors - each
one in the at-least-once ledger await, 150s draining 150k records at measured throughput, after every
liveness and close-duration assertion had already passed. That is a harness sizing miss, now widened,
not a property failing.) Run against BOTH master's product code and the tree carrying the `doClose()`
discharge poll: identical.

### Three things this settles, one it does not

- **A member closing mid-rebalance is handled cleanly by Kafka and by PC.** With the coordinator
  loggers raised: `onLeavePrepare` with a valid generation, LeaveGroup sent within 2ms, LeaveGroup
  answered within ~10ms, `Control loop ending clean` within ~0.3s, survivors re-synced ~2.6s later.
  Three at once changes nothing. This is why the discharge-poll fix measured 2/60: it repairs a case
  that was not broken.
- **The first cut of this reproducer reported a 15-second freeze that was an exhausted topic** - the
  last committed offsets summed to exactly the 4,000 produced. It now sizes and GUARDS its backlog
  (`REMAINING_FLOOR`, checked before the close and after the liveness window) so a run that cannot
  discriminate fails saying so, rather than as a fake stall. Recorded because "the survivors consumed
  nothing" is exactly what a real freeze looks like, and only the offsets told them apart.
- **"Close took 15.2s" was the instrument, not the close.** The duration was read from the main thread
  after a 15s await expired. It is now recorded on the closer thread the instant `close()` returns,
  and reads NaN rather than a number if it never did.

**What it does not settle:** the profile's stall. Ten of twelve instances parked in
`AbstractCoordinator.close -> awaitPendingRequests` for ~25s remains an observation nothing here
reproduces. What the profile has that this matrix does not is the storm itself - restarts joining while
other members are mid-close, toggles every 0-500ms, 12 members on 80 partitions, a background producer.
The next reproducer has to carry one of those, and the cheapest guess is **a member closed while its
own first JoinGroup is still unanswered** - a just-restarted instance - because that is the one state
in which `maybeLeaveGroup` sends nothing (`generation.hasMemberId()` is false) and the coordinator is
left waiting on a member that has already gone. Untested; stated as the next arm, not a finding.

### On the `doClose()` discharge poll and the `ConsumerManager` one-attempt allowance

The `doClose()` discharge poll and the `ConsumerManager` one-attempt allowance are measured not to
move the profile's rate, and now measured not to be needed for the single or three-way close. They
are harmless and guarded, but a change that fixes nothing observable should not ship under a
commit message that says it fixes the stall. Whether to keep them as hygiene or drop them is a
reviewer's call; this note recommends **dropping them**, so that what lands is only what can be shown.


## The close-path wait is MEASURED now, 2026-09-05: LeaveGroup is not answered until the join phase completes

`ClosingMemberRebalanceIT.closingAMemberWhoseOwnJoinIsUnansweredMustNotHoldTheGroup` closes the
JOINING member the instant the coordinator reports `PREPARING_REBALANCE` - while that member's own
JoinGroup (with member id, after `MEMBER_ID_REQUIRED`) is in flight. Eager and cooperative, one joiner
and three. With the coordinator loggers raised, every joiner shows the same sequence:

```
32:43.758  JoinGroup sent (member id assigned)
32:43.890  onLeavePrepare gen=-1 memberId=member#5   <- closed 130ms later, JoinGroup still pending
32:43.890  LeaveGroup sent
32:46.517  settled members: "group is already rebalancing"   <- their next heartbeat, 3s later
32:46.525  settled members: Successfully joined gen=2        <- join phase completes
32:46.529  LeaveGroup response arrives                       <- 4ms after that, 2.64s after it was sent
32:46.535  Control loop ending clean (CLOSED)
```

A LeaveGroup from a stable member is answered in ~10ms (the settled-member cases, and A in the
single-close run). A LeaveGroup from a member whose JoinGroup is pending is answered **only when the
join phase completes** - the coordinator has already removed the member, but the response is not
delivered until every other member has (re)joined. Close therefore costs one full join phase, which
is one heartbeat interval (~3s) in a healthy group and unbounded in one whose join phase keeps
failing to complete. Passed at 2.7s against a 10s bound, so it is a guard on the bound, and a
record of the cost.

### What this does and does not close

It closes the question this note has carried since the stack was captured: **what are the stuck
instances waiting for in `awaitPendingRequests`.** Their own LeaveGroup response, which waits on the
join phase. That is no longer an inference.

It does NOT reproduce the profile's stall, because here the join phase completes in 3s and there it
evidently does not for ~25s. What keeps a 12-member join phase from completing for 25s under
continuous churn is the remaining unknown, and it is a different experiment: the storm itself, at
reduced scale, with these loggers raised - not another single-event arm. Two candidates, both
untested: a member that is neither polling nor left (the coordinator waits on it for the rebalance
timeout), or a join phase that is repeatedly restarted by the next toggle before it can complete.

### What this rules out for the fix

Polling more before close - the discharge poll - could never have helped: the wait is for a
response the coordinator will not send until other members act, and no amount of polling by the
closer changes that. Any repair is one of: not waiting for that response (a bounded consumer close
once LeaveGroup is sent - the coordinator already has it), or keeping the join phase completing under
churn, which is a group-level property this note cannot yet name the lever for.


## The storm itself, on the machine that never fails: a hard 3-second ceiling - 2026-09-05

`largeNumberOfInstances` at `perf.scale=0.5`, three iterations, the two coordinator loggers raised.
All three passed (this machine has never failed it), and that is the point of running them here:
they are the healthy baseline the Linux failures now have to be read against.

| iteration | leaves | joins | worst LeaveGroup | worst JoinGroup |
|---|---|---|---|---|
| 1 | 89 | 172 | 2.83s | 3.00s |
| 2 | ~100 | ~180 | 2.87s | 3.00s |
| 3 | see log | see log | ~2.9s | 3.00s |

Under the profile's own churn - a toggle every 0-500ms across eleven secondaries - **no LeaveGroup and
no JoinGroup ever waited longer than one heartbeat interval.** The join phase always completed on the
next heartbeat. Nothing here waits 25 seconds, so the Linux stall needs something churn alone does not
produce.

**What the Linux batch dispatched at `3fcecf6e` (run 33939841725) therefore decides**, with the same
loggers on and the same analyser: if a failing iteration shows LeaveGroups or JoinGroups unanswered
for ~25s, the members and the instant are named and the fleet dump says what each was doing; if every
latency there is also ~3s and the fleet still froze, the wait is not in the coordinator protocol and
the search has been one layer too low.
