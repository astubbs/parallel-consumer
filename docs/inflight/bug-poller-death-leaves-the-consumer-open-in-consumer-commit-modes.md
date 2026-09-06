# A dead broker-poll thread leaves the Kafka consumer open in the consumer-commit modes

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->

Impact is `stall` rather than `reliability` because the consequence is one named thing that stops and
stays stopped: the dead member's partitions are consumed by nobody until a Kafka timeout finally
sends the LeaveGroup PC never sent. `reliability` is for a shortfall with no single defect behind it;
this one has a single defect and a single missing call.

## The defect

When the broker-poll thread dies - the concrete trigger that surfaced this is a rebalance callback
throwing out of `consumer.poll()`, but any exception escaping `BrokerPollSystem.controlLoop` does it -
PC closes itself, and in `PERIODIC_CONSUMER_SYNC` and `PERIODIC_CONSUMER_ASYNCHRONOUS` (the default)
that close never closes the Kafka consumer. No `LeaveGroup` is sent. The member stays in the group,
its partitions stay assigned to it, and nothing consumes them.

**Which timeout ends it is not `session.timeout.ms`, and the distinction matters for how long the
stall lasts.** The consumer object is still open, so its heartbeat thread is still running and keeps
the session alive. What eventually fires is the heartbeat thread's own poll-interval check -
`AbstractCoordinator.handlePollTimeoutExpiry`, reached from `heartbeat.pollTimeoutExpired` - which
sends the LeaveGroup after **`max.poll.interval.ms`** (five minutes by default) with the log line
"consumer poll timeout has expired". `session.timeout.ms` only governs the case where the JVM itself
is gone. So a PC instance that died at 10:00 in the default mode holds its partitions until about
10:05, heartbeating the whole time, and the coordinator has no way to tell it is dead.

## The mechanism, traced in September 2026

Every anchor below is a greppable identifier, per the root `AGENTS.md`; the trace is of the code as it
stood when the sweep fix below merged.

1. **The poll thread dies and does not close the consumer on the way out.**
   `BrokerPollSystem.controlLoop`'s `catch (Exception e)` logs, calls `notifyPollerDied`, and rethrows;
   there is no `finally`. The consumer close the poller owns, `maybeCloseConsumerManager`, is reached
   only from its `doClose()` on the `CLOSING` arm of the loop's `switch (runState)` - a loop that has
   just exited.
2. **The control thread notices and closes PC.** `AbstractParallelEoSStreamProcessor.controlLoop` ends
   each pass with `brokerPollSubsystem.supervise()`, which throws `PCInternalRuntimeException` when
   the poller's future is done. `supervisorLoop`'s `catch (Exception e)` sets `failureReason` and runs
   `doClose(shutdownTimeout)` in a `finally`.
3. **`doClose` reaches the consumer only in transactional mode.** `innerDoClose` calls
   `brokerPollSubsystem.closeAndWait()`, whose `pollControlResult.get` rethrows the poller's death as
   an `ExecutionException`; the call site catches it and *already warns about exactly this outcome* -
   "the consumer may not be closed, in which case this member will not leave its consumer group
   promptly". Then `maybeCloseConsumer()` runs, gated on `isResponsibleForCommits()`, which is
   `committer instanceof ProducerManager`. In the consumer-commit modes that is false, and the method
   returns without touching the consumer. Nothing after it does either.
4. **Ownership would not block the fix.** `ConsumerManager.close` takes the consumer with
   `tryClaimOwnership`, which succeeds when the poll loop has released or never claimed;
   `ConsumerManager`'s own javadoc records that, at the time of the trace, nothing calls
   `claimConsumerOwnership()` at all, so the guard cannot refuse the control thread today, and the lifecycle
   `ThreadConfinedConsumer` describes (release as the poll task's last act) would let it through
   once wired.

The two `isResponsibleForCommits()` methods are an XOR over commit mode by design - "exactly one
thread closes the consumer" - and `docs/refactoring.md` (the interface-not-a-rename entry) owns
making that polymorphism official. The design assumed the thread that is responsible is alive at
close. After a poller death, in the consumer-commit modes, it is not, and the XOR leaves nobody.

## How it surfaced, and what is and is not fixed

<!-- post-merge: checked -->
astubbs/parallel-consumer#451 fixed `PartitionStateManager.resetOffsetMapAndRemoveWork`, the revoke
sweep, to survive a partition whose assignment callback failed (epoch recorded, no state installed).
Tracing how that sweep is reached after the failure produced this note: the exception kills the poll
thread, so the sweep's only remaining route is the close sequence, and in the consumer-commit modes
the close sequence never reaches the consumer. The sweep fix is therefore a live path only in
`PERIODIC_TRANSACTIONAL_PRODUCER` mode, and insurance in the default modes -
`PartitionStateManagerRevokeAfterFailedAssignmentTest`'s class javadoc carries that trace, and this
note is the defect it points at. Nothing in that PR changes the close sequence.

## What would settle it

- **Reproduce it as a unit test first.** A PC in `PERIODIC_CONSUMER_SYNC` over a `MockConsumer` whose
  `committed(Set)` throws (the fixture `PartitionStateManagerRevokeAfterFailedAssignmentTest` already
  has), driven through `subscribe` and a `MockConsumer.rebalance`, then wait for
  `isClosedOrFailed()` and assert `MockConsumer.closed()` - expected RED today. The same test in
  `PERIODIC_TRANSACTIONAL_PRODUCER` mode is the control arm and should be green.
- **The fix shape, for the decision.** `doClose` should close the consumer whenever the poll thread
  has already exited, whatever the commit mode: the reason the consumer-commit modes leave it to the
  poller is thread confinement, and a poller whose future is done holds nothing. The cheapest
  expression is for `maybeCloseConsumer` to close when `isResponsibleForCommits()` OR the poll
  future is done; the principled one is the shared interface `docs/refactoring.md` asks for, with
  "the poller has exited" as a state both implementors can read. Whoever picks this up should decide
  which, and should keep the `closeAndWait` warning honest either way - today it says "may not be
  closed" about a case where the answer is known.
- **Say which timeout in the user-facing warnings.** Both the `closeAndWait` warning and the
  `maybeCloseConsumer` failure warning name `session.timeout.ms`; while the JVM is up the operative
  one is `max.poll.interval.ms`, per the trace above.

## Related, and how each differs

- `docs/inflight/core-163-poll-path-has-no-error-seam.md` - the policy question upstream of this: PC
  has no handler for an exception thrown out of `consumer.poll()`, so the poll thread dies. That note
  says "PC then closes the whole instance", which is true of PC's own state and not of the consumer;
  it links here for the rest. A seam that let the poll loop *continue* would remove this defect as a
  side effect; one that terminates the instance would still need this fix.
- `docs/inflight/bug-shutdown-teardown-race.md` - the opposite arm of the same close: teardown that
  runs while the poll thread is still *alive*. This note is the poll thread being *dead* and the
  close assuming it is not.
- `docs/solutions/architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md`
  - the transactional-mode version of "consumer never closed, no LeaveGroup, group waits out the
  timeout", caused by the confinement guard refusing the control thread, and fixed. That fix is why
  the transactional arm here works; the consumer-commit arm was never in its scope.
- astubbs#166 (confluentinc#597, closed, fixed in 0.5.3.1) - "PC does not close the Kafka consumer
  if the commit fails during close" - the nearest issue on the tracker, and a different trigger on
  the same symptom. No open issue matches this defect; `gh issue list --state all` searched for the
  poll thread dying, LeaveGroup, the consumer not being closed and the session timeout found nothing
  else.
- Prior art: `node bin/inflight.mjs prior-art LeaveGroup poller-death maybeCloseConsumer
  session.timeout` returned the confluentinc#857 family (the LeaveGroup-under-churn measurements,
  the revoke-path deadlock write-ups, the control arm), the commit-failure seam plan
  (astubbs#317), the crash-fidelity note (a crashed member that stops heartbeating - which this one
  does *not*), and the `maybeCloseConsumer` producer-close fix (astubbs#423). None records a poller
  death leaving the consumer open in the consumer-commit modes.
