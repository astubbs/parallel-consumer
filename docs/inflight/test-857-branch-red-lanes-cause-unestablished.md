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

## Why it matters beyond the branch that found it

If cluster 2 is implicated, the question is not only "fix it" but whether that work belongs on a PR
whose measured claim is about twenty lines of lock change. The decomposition that produced
astubbs#375, astubbs#376 and astubbs#381 left cluster 2 in place as the largest unextracted
piece, on the grounds that its fix is a design decision about consumer ownership rather than a patch.
A red lane traceable to it would be an argument for separating it.
