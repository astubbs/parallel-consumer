# Two CI lanes are red on the confluentinc#857 branch, and the cause is not established

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

## Why it matters beyond the branch that found it

If cluster 2 is implicated, the question is not only "fix it" but whether that work belongs on a PR
whose measured claim is about twenty lines of lock change. The decomposition that produced
astubbs/parallel-consumer#375, #376 and #381 left cluster 2 in place as the largest unextracted
piece, on the grounds that its fix is a design decision about consumer ownership rather than a patch.
A red lane traceable to it would be an argument for separating it.
