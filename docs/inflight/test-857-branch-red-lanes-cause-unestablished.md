# The confluentinc#857 branch is several times slower in transactional mode - the red lane is a symptom

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-labels: concurrency -->

**Written before the experiment that tests it, so the prediction cannot be fitted to the result.**
Dated 2026-08-31.

## What is red, and how it was found

`Integration Tests` and `Performance Tests` have been red for weeks with no diagnosis, because
`gh run view --log-failed` returns a truncated log for these jobs - it ends mid-run with no error
line, no `BUILD FAILURE` and no failsafe summary, which reads like a job that failed for no reason.
Fetched instead through the run-logs archive
(`gh api repos/astubbs/parallel-consumer/actions/runs/<id>/logs`), the route
[`../solutions/workflow-issues/gh-run-view-log-truncation.md`](../solutions/workflow-issues/gh-run-view-log-truncation.md)
owns:

- **Integration Tests** - `TransactionAndCommitModeTest.testTransactionalDefaultMaxPoll`, several
  parameterised repetitions, all failing *"All keys sent to input-topic should be processed and
  produced, within time"* with `PERIODIC_TRANSACTIONAL_PRODUCER` and KEY ordering. One repetition
  errors rather than fails, on `PCInternalRuntimeException: No progress beyond 13000 records after
  8 rounds`.
- **Performance Tests** - `MultiInstanceHighVolumeTest.multiInstance`, the same assertion shape,
  with `PERIODIC_CONSUMER_SYNC` and KEY ordering.

## What is already ruled out, and by what

- **Ambient CI load is not the explanation.** Both lanes are green on
  astubbs/parallel-consumer#381 and astubbs/parallel-consumer#352, on the same infrastructure. That
  was the first hypothesis and this is the control that kills it.
- **The `tryLock` deadlock fix cannot be the cause of the integration failure.**
  `testTransactionalDefaultMaxPoll` runs `PERIODIC_TRANSACTIONAL_PRODUCER`, and the AB-BA cycle that
  fix replaces cannot close in that mode - the cycle's second edge lives in `ConsumerOffsetCommitter`,
  constructed only for the consumer-commit modes. Reasoned in full in
  [`bug-857-transactional-revoke-wait.md`](bug-857-transactional-revoke-wait.md).

## What points the other way, and must not be waved through

- The ambient probe autopsy on the failing repetition says **"probe clean - no rebalance dwell, no
  lag stagnation, no frozen partitions observed: the fault is likely in the test itself, not
  consumer-group progress"**.
- `TransactionAndCommitModeTest` is a **named member of a documented flake family** whose signature
  is exactly this - awaitility timeouts and "No progress beyond N records" under parallelism, driven
  by CPU starvation. See
  [`../solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md`](../solutions/test-flakiness/parallel-integration-tests-flaky-under-concurrency-2026-07-28.md).
  The run had `forkCount=4`.

**The repo's own rule is that a clean probe is not proof - check its thresholds before believing
one** - and the green lanes on two other PRs are hard to reconcile with a purely ambient cause. So
the two bodies of evidence disagree, which is why this is an experiment and not a conclusion.

## The hypothesis under test

**Cluster 2 - `ThreadConfinedConsumer` and `ConsumerOwnership` - is the candidate.** Unlike the
deadlock fix, it wraps the consumer for **every** commit mode including transactional, so it is the
only part of the confluentinc#857 branch that could plausibly reach a transactional test. It is also
the largest part of that branch by main-code volume and the part with no dedicated evidence: the four-cell
control that verified the deadlock fix measured the lock change, not this.

**This hypothesis is not recorded anywhere prior.** The PR note was searched for an existing
prediction about cluster 2 causing red lanes and contains none. It is a fresh guess from the shape of
the failure, and is written here so the result can refute it.

## Prediction, before running

Both failing tests, several repetitions each, on the branch and on master, same machine, arms
alternated:

- **If cluster 2 is the cause** - the branch fails and master does not, at a clearly different rate.
- **If it is the known flake family** - both arms fail at comparable rates, and the branch is not
  special.
- **If neither reproduces locally** - the experiment is VOID and says nothing in either direction.
  This is the likely outcome and must be reported as such rather than as "the branch is fine": CI
  runs `forkCount=4` under contention this machine will not reproduce, and a green pair here is the
  same non-evidence that voided the 2026-08-31 seed replay. A local run that never fails has not
  tested the hypothesis.


## RESULT, 2026-08-31 - the prediction held, and the finding is bigger than the lane

**The branch is roughly four to ten times slower than master on this test.** Measured, not inferred.

Three repetitions per arm, alternating, on a machine whose load was sampled per run. Both arms
carried an IDENTICAL instrument - the same two test files and the same inert `describeProgress()` -
so the only difference left was the branch's main-code work.

| arm | outcomes | records/second observed |
|---|---|---|
| master | green throughout | ~5,000 - 7,100 |
| the branch | red, red, green | ~650 - 1,800 |

**The load-bearing comparison is the pair where BOTH arms passed.** A failing run's rate is capped by
the deadline it hit, so red-versus-green would be an artefact. In the repetition where the branch
went green it still ran at ~1,300-1,800 against master's ~6,500-6,900 at comparable load. Slower
when it passes, not merely slower when it fails.

**This is why the rate instrument had to exist.** The pass/fail deadline had been reporting "flaky"
for weeks - the same tree passing or failing on the machine's mood - while the rate says the same
thing on every run in both directions. The earlier pass/fail arm of this experiment came within one
green run of being read as "the branch is fine".

### What this does and does not establish

- **Establishes:** the branch causes it. Master does not reproduce, including under heavier load than
  the branch failed at, and the gap persists on green runs.
- **Does NOT establish which part of the branch.** The `tryLock` fix cannot be responsible - wrong
  commit mode - which leaves cluster 2 as the pre-registered candidate, but no arm has isolated it.
  The next experiment is a third tree: master plus cluster 2 alone, or the branch with cluster 2
  reverted. Until that runs, "cluster 2" remains the hypothesis it was written down as.
- **Scope:** one test, transactional mode, KEY ordering, one machine. The `Performance Tests` lane
  failure (`MultiInstanceHighVolumeTest`, consumer-sync mode) has not been measured this way and may
  or may not share a cause.

### The consequence for the branch

A four-fold throughput regression is not a red lane to be explained away; it is a defect. It also
sharpens the decomposition question already recorded below: the branch's measured claim is about
twenty lines of lock change, verified on both assignors, and this regression is in the part that
verification never touched.

### The mechanism, 2026-09-01 - and it refutes the hypothesis above

**The branch is not slow. It is IDLE.** The diagnostic added for this run reports, at the moment the
deadline expires with thousands of records still unconsumed:

```
pc: workRemaining=0 recordsOutForProcessing=0 state=RUNNING closedOrFailed=false
```

Both ends are zero. PC has nothing outstanding and nothing in flight, is RUNNING and has not failed -
so it is not processing slowly and it is not blocked. It believes there is no work, while the records
are sitting on the topic. **This is an ingestion failure wearing a throughput failure's clothes.**

Three independent readings agree and none of them would have said this alone:

- **JFR, differentially against master.** Normalised per second - which the raw counts do not do,
  since master finishes in a few seconds and the branch runs the full deadline - monitor waits are
  the SAME on both arms, the branch PARKS LESS than master, and it samples less CPU. Not contended,
  not computing.
- **The both-ends diagnostic**, which is the reading that names it. A completion counter alone would
  have shown a flat line and left "not finishing" and "not trying" indistinguishable; zero-and-zero
  is a third state that says neither.
- **The rate**, which is what made the failure legible enough to investigate at all.

**This refutes the cluster 2 hypothesis as stated.** That guess was about per-call overhead in the
consumer wrapper, and overhead cannot produce an idle instance with an empty pipeline. Cluster 2 is
not exonerated - `ThreadConfinedConsumer` wraps `pause`/`resume`/`paused` among everything else - but
the mechanism to look for is now a pause that is never lifted, or work that is never fetched, and no
longer "it got slower".

**The shape is the confluentinc#857 symptom itself**: paused consumption, here in
`PERIODIC_TRANSACTIONAL_PRODUCER`. A branch whose subject is that defect exhibiting that defect in a
mode its fix does not cover is the thing to look at next.

**What has NOT been established:** whether partitions are actually paused. `describeProgress()` does
not report pause state, and it cannot simply call `consumer.paused()` - that would trip
`ThreadConfinedConsumer`'s ownership guard from the test thread. Establishing it needs pause state
tracked somewhere reachable, which is the next step and a small one.

### Master does not reproduce it - six reps, 2026-09-01

Thirty test executions on master: no failures, no idle reading, 6,061-8,125 records/second
throughout. The branch idles and fails repeatedly in the same conditions at 648-1,777.

**So this is a regression the branch introduces, not a latent master defect it exposes.** That was
worth settling explicitly, because "a fix branch reproduces the symptom its family is about" invites
the reading that it has surfaced one of the reported bugs. On this evidence it has not.

**The limit, stated rather than glossed:** these were the SAME conditions repeated, not harsher ones.
Master is clean under load it has not been pushed past - more records, a tighter
`max.poll.interval.ms`, or a second instance were not tried. A latent master defect needing sharper
conditions is not excluded by this.

### Not paused - so not "paused consumption" at all, 2026-09-01

With pause state added to the diagnostic, the failing run now says:

```
pc: workRemaining=0 recordsOutForProcessing=0 state=RUNNING closedOrFailed=false
    pausedPartitions=0 (observed 1358ms ago)
```

**Nothing is paused, and the poll thread is alive** - the observation is barely a second old, so the
loop is still completing its pause passes. The instance is polling, unpaused, with an empty pipeline,
while records sit unconsumed on the topic.

**That eliminates the reading this note was built around.** The zero-and-zero state looked like the
confluentinc#857 symptom - paused consumption - and it is not. PC is not being stopped from fetching;
it is fetching and ending up with no work. The candidate area moves to INGESTION: records polled and
then discarded or never registered, which epoch or staleness filtering would do silently and which
would produce exactly this signature. `EpochAndRecordsMap` and `PartitionStateManager` are both
touched by the branch and both sit on that path.

**Two hypotheses have now died to this instrument** - cluster 2 overhead, then paused consumption -
which is the argument for the instrument rather than for either guess. Each was plausible, each was
what an experienced reader would have assumed, and each took one line of output to refute.

### CORRECTION, 2026-09-01: it is NOT idle. Two of PC's own counters disagree.

**The "idle" reading above was over-read, and this section corrects it.** With debug logging on -
verified as reaching the run, 720k DEBUG lines including PC's own classes - the load gate and the
diagnostic print three consecutive lines at the same instant:

```
isPoolQueueLow? workAmountBelowTarget false 281 vs 64
calculateQuantityToRequest target: 320, current queue size: 319, requesting: 1, loading factor: 5
pc: workRemaining=0 recordsOutForProcessing=0 state=RUNNING pausedPartitions=0
```

**PC's executor queue holds 319 records against a target of 320, and it is stepping the loading
factor UP - while `workRemaining()` and `recordsOutForProcessing` both read zero.** It is saturated
and slow, not starved. `consumed` and `producedAck` track each other exactly at ~600/s.

So the sequence of hypotheses this file records now reads: cluster 2 overhead (refuted), paused
consumption (refuted), idle/not-fetching (**refuted here, by me, having asserted it**). The lesson is
not about any of the three - it is that a single counter read at one moment supported a confident
wrong reading three times, and each time an adjacent instrument disagreed.

### The finding that replaces it, stated narrowly

**Two of PC's own accounting views disagree at the same instant**, reproducibly. The load gate
counts 281-319 records queued; `getNumberOfIncompleteOffsets()` and `numberRecordsOutForProcessing`
both report zero.

**What is NOT established, and must not be assumed:**

- **Whether master shows the same disagreement.** It has not been compared. Master passes this test,
  so its failure path never prints the diagnostic, and no mid-run sampling has been done on either
  arm. Until that control exists, this may be a long-standing property of these metrics rather than
  anything the branch caused - and the throughput regression is measured separately and stands on its
  own evidence regardless.
- **Whether it has any consequence beyond diagnostics.** The obvious worry is that an instance
  believing no offsets are incomplete may commit offsets for records it has not processed, which in
  this mode would skip them on restart. That is a hypothesis about a mechanism, not an observation:
  no such commit has been demonstrated.

**The epoch hypothesis is refuted.** Zero epoch-stale skips and zero no-epoch-yet skips across the
whole run, with debug confirmed live. The branch's only changes to that path are a comment and a
trace line.

**The transactional revoke wait is not involved either.** This test creates ONE consumer and the run
contains ZERO revocations, so `onPartitionsRevoked` never executes. That also makes this failure
unlike the rest of the confluentinc#857 family, which is rebalance-centred throughout: whatever this
is, it happens in the steady state.

### The coherence check, and why the direct-pull stack changes what it should watch

A coherence line was added to `describeProgress()`: it flags **work queued for execution while PC
believes no offsets are incomplete and nothing is out for processing**. That is a different KIND of
check from every probe this repo has - those watch LIVENESS, sampling one number over time, and a
system can be perfectly live while lying about what it holds. This watches whether PC's separate
views of its own state can all be true at once.

**It has not fired yet, and is therefore unarmed.** The run that would exercise it passed - the
branch's failure is load-dependent - so nothing has demonstrated the check firing on a tree that
should fail. Until that happens its silence means nothing, which is this repo's standing rule for
detectors and applies to this one as much as to the ambient probe it was written in response to.

**astubbs/parallel-consumer#361 removes the signal it depends on.** Direct pull hands work SELECTION
to the workers - they block on the shards and take their own records rather than the control loop
pushing into an executor queue - so `executorQueueDepth()` would read zero permanently and the
condition would never be reachable again. It would not fail; it would go quiet, which is worse. Any
version of this check that is meant to survive that stack has to source "work PC is holding" from
something direct pull still has, not from the executor's queue.

**And the finding itself may not be novel.** astubbs/parallel-consumer#336 derives the poller load
gate by conservation rather than from maintained totals, and the note on
`numberRecordsOutForProcessing` describes it as the last counter of its family after the others were
dissolved into derived views. astubbs/parallel-consumer#335 adds `ExecutionState` for concurrent
claims. The disagreement recorded above sits inside the area that stack is rebuilding, so it must be
checked against those branches before being treated as a new defect - and the throughput regression
is measured separately and does not depend on it either way.

## Why it matters beyond the branch that found it

If cluster 2 is implicated, the question is not only "fix it" but whether that work belongs on a PR
whose measured claim is about twenty lines of lock change. The decomposition that produced
astubbs#375, astubbs#376 and astubbs#381 left cluster 2 in place as the largest unextracted
piece, on the grounds that its fix is a design decision about consumer ownership rather than a patch.
A red lane traceable to it would be an argument for separating it.
