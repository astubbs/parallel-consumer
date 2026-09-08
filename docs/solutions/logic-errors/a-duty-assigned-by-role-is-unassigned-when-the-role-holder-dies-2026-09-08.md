---
title: "A duty assigned by role is unassigned when the role-holder dies"
date: 2026-09-08
category: logic-errors
module: parallel-consumer-core/internal
problem_type: logic_error
component: service_object
severity: high
symptoms:
  - "The broker-poll thread dies and the Kafka consumer is never closed, in PERIODIC_CONSUMER_SYNC and PERIODIC_CONSUMER_ASYNCHRONOUS (the shipped default)"
  - "No LeaveGroup is sent, so a dead member keeps its partitions and nothing consumes them"
  - "The stall lasts about max.poll.interval.ms, not session.timeout.ms, because the open consumer keeps heartbeating"
  - "PC reports itself closed and failed while the consumer it owned is still in the group"
root_cause: logic_error
resolution_type: code_fix
related_components:
  - BrokerPollSystem
  - AbstractParallelEoSStreamProcessor
  - ConsumerManager
  - PartitionStateManager
tags:
  - shutdown
  - leavegroup
  - commit-mode
  - poll-thread-death
  - defect-class
  - issue-857
---

# A duty assigned by role is unassigned when the role-holder dies

## Problem

Closing the Kafka consumer was assigned **by role**: whichever component commits also closes the
consumer. `BrokerPollSystem.maybeCloseConsumerManager` does it in the consumer-commit modes,
`AbstractParallelEoSStreamProcessor.maybeCloseConsumer` does it in
`PERIODIC_TRANSACTIONAL_PRODUCER`, and the two `isResponsibleForCommits()` predicates behind them
are an XOR over commit mode. The design intent is sound and is written down - exactly one thread
closes the consumer, so that a revoke fired by that close can commit inline.

**The assignment has an unstated premise: the role-holder is alive at close time.** When the
broker-poll thread dies - anything escaping `BrokerPollSystem.controlLoop`, which logs, calls
`notifyPollerDied` and rethrows with no `finally` - it never reaches its own `doClose`, so it never
runs the close it was assigned. The control thread notices (`supervise()` surfaces the dead
poller's future), closes the instance, and reaches `maybeCloseConsumer` - which asks "am I the
committer?", gets `false` in the consumer-commit modes, and returns.

Nobody closes the consumer. No `LeaveGroup`. The XOR that guarantees *at most one* closer had
silently become a guarantee of *at most one* and no guarantee of *at least one*.

## Symptoms

- The dead member's partitions stay assigned to it and nothing consumes them.
- **The timeout that ends it is `max.poll.interval.ms`, not `session.timeout.ms`** - and that
  distinction is worth keeping, because every warning on this path used to name the wrong one. The
  consumer object is still open, so its heartbeat thread is still running and keeps the session
  alive; what finally evicts the member is that thread's own poll-interval check,
  `AbstractCoordinator.handlePollTimeoutExpiry`, reached from `heartbeat.pollTimeoutExpired`, which
  logs "consumer poll timeout has expired". `session.timeout.ms` governs the case where the JVM
  itself has gone away.
- `isClosedOrFailed()` reports true while the consumer is still in the group, so a supervisor that
  restarts on that signal brings up a replacement that cannot get the partitions.
- Nothing goes red. The instance closes, the error is reported, and the group quietly stalls.

## What Didn't Work

**Mirroring "the consumer is closed" into a flag.** The first shape of the fix was a
`volatile boolean consumerClosedByPoller` on `BrokerPollSystem`, set after its close returned. It
works, and it is the defect class
[`a-mirror-of-state-another-component-owns-is-a-contract-nobody-wrote.md`](../architecture-patterns/a-mirror-of-state-another-component-owns-is-a-contract-nobody-wrote.md)
names: a second source of truth for a fact, carrying a synchronisation contract nobody writes down.
It was replaced before it was committed, once the fact turned out to be **derivable**: `runState`
reaches `CLOSED` at exactly one place in the class - the statement immediately after
`maybeCloseConsumerManager()` in `doClose()` - so "this system closed the consumer" is
`runState == CLOSED && isResponsibleForCommits()`, read from the one lifecycle that already exists.
The `Consumer` interface has no `isClosed()`, which is what makes the mirror tempting; the answer
was to ask a different question rather than to keep a copy.

**Dropping the commit-mode arm entirely.** `if (!pollerClosedIt)` is shorter and passes the same
tests, but it changes the `closeAndWait`-timed-out case: with the poll thread *still running*, it
tells the control thread to close a consumer another thread is actively polling. The guard that
should refuse that (`ThreadConfinedConsumer`) is installed but not yet wired - nothing calls
`claimConsumerOwnership()`, so ownership never leaves `UNCLAIMED`, whose documented meaning is that
every thread is allowed. A shorter condition would have converted a documented, refusable race into
a silent one.

## Solution

Add the case neither predicate can express - **the designated closer is dead** - and prove it
rather than assume it:

```java
public boolean pollThreadEndedWithoutClosingTheConsumer() {
    boolean pollThreadFinished = pollControlThreadFuture.map(Future::isDone).orElse(true);
    boolean thisSystemClosedIt = runState == CLOSED && isResponsibleForCommits();
    return pollThreadFinished && !thisSystemClosedIt;
}
```

and at the call site, `AbstractParallelEoSStreamProcessor.maybeCloseConsumer`:

```java
if (isResponsibleForCommits() || brokerPollSubsystem.pollThreadEndedWithoutClosingTheConsumer()) {
```

Both facts are read from state `BrokerPollSystem` already owns. `runState` is `volatile`, and the
poll thread's future completing is itself the happens-before edge for everything that thread did.

**It is deliberately false while the poll thread is alive.** That keeps the `closeAndWait`-timed-out
case exactly as it was - a decision the ownership guard owns, not this predicate - so the fix adds
no new path on which a consumer is closed under a live poller.

**This is not a reconciliation of the two `isResponsibleForCommits()` methods**, which
[`../architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md`](../architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md)
warns against by name. They have never disagreed and neither is changed.

The pin is
`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/internal/PollerDeathClosesTheConsumerTest.java`.

## Why This Works

- **The control thread is a legitimate closer once the poller has finished.** A finished poll loop
  holds nothing; the only reason the consumer-commit modes deferred to it was thread confinement,
  and confinement has no claimant after the thread ends.
- **The condition is provable, not assumed.** "The future is done" is a fact about a thread that has
  ended, so acting on it cannot race the thread it describes.
- **Derived state cannot drift.** There is nothing to keep in step, no reset hook, and no window in
  which the answer is stale - the failure mode the mirror would have carried does not exist.

## The controlled experiment

Stated before it was run: the transactional arm is GREEN today because there the control thread was
already the designated closer; both consumer-commit arms are RED. One term differs between arms -
the commit mode - and everything else in the fixture is identical.

Observed on the unmodified tree: `PERIODIC_TRANSACTIONAL_PRODUCER` passed;
`PERIODIC_CONSUMER_SYNC` and `PERIODIC_CONSUMER_ASYNCHRONOUS` both failed on
`value of: closed()  expected to be true`, each after waiting out the full Awaitility timeout. With
the two-line change and nothing else, all three arms pass in about two seconds. The green control
arm is what makes the two reds attributable to the defect rather than to the fixture: it proves the
same fixture does kill the poll thread, does drive PC to a terminal state, and does observe a real
`consumer.close()`.

## Prevention

- **When a duty is assigned by role, ask what happens if the role-holder dies.** An XOR guarantees
  *at most one* actor. Getting *at least one* out of it needs a separate argument, and here that
  argument was "the thread is alive", which is precisely what fails. Any `if (I am the X) do the
  cleanup` is the shape; the question is whether some other participant can observe that X did not.
- **Prefer deriving over mirroring, and look for the derivation before accepting the copy.** The
  mirror is the obvious fix and is usually available; the derivation usually exists too, in a
  lifecycle the component already maintains. Check whether the state you would copy is already
  implied by a state machine that has exactly one transition into the relevant state.
- **A backstop must report only what it can prove.** "Nobody has closed this yet" invites acting
  under a live owner. "The owner has finished and did not close it" cannot.
- **Name the right timeout in an operator-facing warning.** `session.timeout.ms` and
  `max.poll.interval.ms` describe different failures, and an operator who acts on the wrong one
  waits for a rebalance that is not coming for another four minutes. An open consumer heartbeats;
  only a gone JVM does not.
- **Test a mode-conditional path in every mode.** This defect lived in the shipped default while the
  mode that was covered incidentally - transactional - was the one that worked. A parameterised
  `@EnumSource(CommitMode.class)` costs nothing and is what turned "a gap somebody traced" into "two
  red arms and a green control".

## Related

- [`../architecture-patterns/a-mirror-of-state-another-component-owns-is-a-contract-nobody-wrote.md`](../architecture-patterns/a-mirror-of-state-another-component-owns-is-a-contract-nobody-wrote.md) -
  the class the rejected first shape belonged to, and the reason the derivation was looked for.
- [`../architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md`](../architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md) -
  why the poll/control split exists and why the two `isResponsibleForCommits()` methods must not be
  "reconciled". This fix adds a third case beside them rather than merging them.
- [`../runtime-errors/a-throwing-meter-registry-kills-the-poll-thread-and-strands-close.md`](../runtime-errors/a-throwing-meter-registry-kills-the-poll-thread-and-strands-close.md) -
  one concrete way the poll thread dies, and the same downstream close. That write-up guards the
  *cause*; this one repairs what the close does once the thread is gone, so the two are
  complementary and neither subsumes the other.
- [`../runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md`](../runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md) -
  the other way the poll thread stops producing: it parks rather than dying, so the future never
  completes and this predicate correctly stays false.
- `docs/inflight/bug-shutdown-teardown-race.md` - the opposite arm of the same close, teardown while
  the poll thread is still *alive*, which this deliberately does not touch.
