# Draft response to astubbs#411 (`confluentinc#830`) - posted by the pre-release sweep, not by this PR

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- inflight-state: deferred - until the pre-release sweep, or an explicit instruction to post -->
<!-- post-merge: exempt-file - a drafted issue reply, held until the sweep posts it. It deliberately
     outlives the PR that wrote it, so it cannot be written in post-merge terms. -->

Not posted. Post only on explicit instruction; delete this file when it is posted, not when its PR
merges.

---

Reading this again while scoping astubbs#225, we found something that changes what your report means
for the fork: **the fix that closed it upstream cannot fire for the failure you described.**

`confluentinc#839` added a catch for `InvalidPidMappingException` around the produce-and-ack block,
so PC would shut down rather than retry forever. But the ack wait goes through a `Future`, and
`FutureRecordMetadata.valueOrError` raises `new ExecutionException(exception)` — so the exception that
reaches that catch is an `ExecutionException`, not an `InvalidPidMappingException`. It falls past the
typed catch into the generic handler, which wraps it as
`PCInternalRuntimeException("Error while waiting for produce results", ...)`.

That is exactly the stack trace in the original report — from a build that already had the fix. The
record is then marked failed and re-dispatched onto the same, still-invalid producer, which is the
loop you saw.

The regression test passes because it mocks a *synchronous* throw from `produceMessages`, which is a
path the failure never takes.

**We have not reproduced this on a current build.** The reasoning is from reading the current tree
against the kafka-clients sources, not from a run. A reproduction needs a producer whose send future
completes exceptionally — a `MockProducer` with `errorNext`, or a real broker-side producer-id expiry
like the two days of inactivity that triggered yours.

## What we are doing about it

Your own suggestion — *"close the producer that is causing this error and create a new producer
instance"* — is the direction the fork has taken. The requirements for it are in
`docs/plans/2026-09-02-001-feat-recoverable-producer-fencing-plan.md`, tracked as astubbs#225.

Two honest caveats:

- **It will not reach your configuration unchanged.** Recovery requires Parallel Consumer to build the
  producer itself, from configuration, so it can build a replacement. Supplying a `Producer` instance —
  the only option today, and what you were doing — stays supported and deprecated, but cannot recover,
  because a `KafkaProducer`'s configuration cannot be read back out of it.
- **The loop itself is a separate defect**, tracked in
  `docs/inflight/bug-411-wrapped-send-failure-spins-forever.md`. Whether to close it on the deprecated
  path as well — so the condition at least terminates rather than spinning — is an open decision.

Thanks for the original report and for the fix attempt. The diagnosis in it was right; it was the
interception point that was off by one layer.
