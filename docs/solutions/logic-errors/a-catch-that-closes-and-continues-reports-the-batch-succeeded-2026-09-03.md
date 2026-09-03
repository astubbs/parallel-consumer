---
title: "A catch arm that reports and carries on, on a path whose continuation MEANS success"
date: 2026-09-03
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: transactional-produce
applies_when:
  - "Adding a catch arm to a method whose normal return value is a success verdict"
  - "Adding a catch arm to a close or teardown sequence with steps still to run after it"
  - "An offset is committed for records a read_committed consumer never sees, with no error logged"
  - "A KafkaProducer's IO thread survives pc.close() after a fencing or a poisoned transaction"
  - "Deciding whether a swallowed exception is safe because 'nothing acts on it today'"
related_components:
  - ParallelEoSStreamProcessor
  - ProducerManager
  - AbstractParallelEoSStreamProcessor
tags:
  - exactly-once
  - transactions
  - close
  - producer-leak
---

# A catch that closes and continues reports the batch succeeded

## The class

**A catch arm that logs, does something about the failure, and then falls out of the block - on a
path where falling out of the block is itself the success signal.** The arm reads as handled: it
names the exception, it takes an action, and a reviewer's eye stops at the action. What it does not
do is stop the method from going on to say "fine".

Two instances shipped in this repository, in the same subsystem, written years apart by different
people. Both were found by reading, not by a failing test, and both are fixed by
astubbs/parallel-consumer#423.

The two shapes the continuation takes:

- **The return value is the verdict.** `ParallelEoSStreamProcessor#processAndProduceResults` returns
  a list of produce results; returning it at all is what tells the caller the batch worked.
- **The remaining statements are the teardown.** `ProducerManager#close(Duration)` had
  `closeProducer(timeout)` after the transaction-cleanup block, so anything escaping that block
  skipped the one step that must not be skipped.

**The tell is the same in both: the arm changes what happens to the FAILURE, and nothing about what
happens to the WORK.** Ask of every such arm - what does the code do next if I do nothing more here,
and is that the right answer for a caller who now knows nothing went wrong?

## Instance 1 - the swallowed `InvalidPidMappingException` marked the whole batch succeeded

`processAndProduceResults` caught `InvalidPidMappingException`, called `closeOnException`, and fell
through to `return results` with a partial - usually empty - list. `runUserFunctionInternal` then
called `onUserFunctionSuccess` for **every** `WorkContainer` in the batch. Output never produced,
records recorded as done: the same exactly-once violation astubbs#261 fixed in a different shape.

**Why it survived review for years: an accident of the close path masked it.**
`closeOnException` sets `failureReason` and calls `closeDontDrainFirst`, which transitions to
CLOSING and then blocks in `waitForClose` on the control future. Called from a worker thread, that
worker is exactly what `innerDoClose`'s `awaitTermination` is waiting on - so the entire shutdown
runs (mailbox drain, `commitOffsetsThatAreReady`, the consumer and producer closes, and `doClose`'s
finally setting CLOSED) **before** `closeOnException` returns to the catch arm. The succeeded
containers are then posted to a mailbox nobody will drain again.

So on master nothing committed those offsets - not because the verdict was right, but because the
close happened to be synchronous and to finish first. That is the trap worth carrying forward: **a
wrong verdict protected by an unrelated accident is a live defect, not a latent one**, because
whatever refactor moves the accident ships the data loss. astubbs/parallel-consumer#410 is
redesigning that exact close path.

The fix is a rethrow after the close, wrapped as `PCInternalRuntimeException` the way the generic
arm beside it wraps, so the failure travels the route every other produce failure travels.

**The wrapped shape is a different bug and is deliberately untouched.** A send-ack failure arrives
as `ExecutionException(InvalidPidMappingException)` from `FutureRecordMetadata.get`; `instanceof`
does not match, so it takes the generic arm - correct for the batch verdict, wrong for liveness,
because it then retries forever against the same dead producer. That spin is the field report
(confluentinc#830 / confluentinc#839, mirrored as astubbs#411) and belongs with
astubbs/parallel-consumer#410's recovery work. Unwrapping causes into the close arm would convert a
retry spin into a shutdown, which is a behaviour change that PR is already replacing.

## Instance 2 - a throwing `abortTransaction()` skipped closing the producer

`ProducerManager#close(Duration)` aborted inside `try { abortTransaction(); } finally {
releaseCommitLock(); }` and called `closeProducer(timeout)` **after** that block. The `finally`
released the lock but contained nothing, so a throwing abort escaped past `closeProducer` and leaked
the `KafkaProducer` - IO thread, sockets, buffers - while `doClose`'s finally still marked the
instance CLOSED. A host restarting PC instances leaks one producer per fenced shutdown.

`abortTransaction()` throws precisely in the states this library now deliberately creates: a fenced
producer (`ProducerFencedException` / `InvalidProducerEpochException`) or a poisoned one ("we are in
an error state" - astubbs#261 makes a terminally failed send poison the transaction on purpose).

The fix contains the whole cleanup and puts `closeProducer` in a `finally`.

## What the class sweep found - three instances of instance 2's shape, all fixed

Naming the class rather than the symptom - *a cleanup step whose throw skips a later, more important
teardown step* - turned up two siblings beyond the abort:

- **The commit-lock timeout path released a lock it never took.** `close` catches the acquire
  timeout and carries on to abort anyway (deliberately), but the `finally` then called
  `releaseCommitLock()`, which throws `IllegalStateException("Not held be me")` when the current
  thread does not hold the write lock. Same escape, same leak, reached by a different door. Fixed
  with a `commitLockHeld` flag; `acquireCommitLock` has no path that takes the write lock and then
  throws, so the flag cannot be false while the lock is held.
- **`innerDoClose`'s producer step was the one shutdown step without a guard.** Its three
  predecessors - `commitOffsetsThatAreReady`, `brokerPollSubsystem.closeAndWait`,
  `maybeCloseConsumer` - each sit in their own try/catch(WARN), from confluentinc#818 / astubbs#166.
  The producer close did not. The leak is the smaller half of the cost: unguarded, the throw reaches
  the control task's `catch (Exception e)`, which **overwrites `failureReason`** with "Error from
  poll control thread", re-runs `doClose` over already-closed subsystems, and fails the control
  future - so `close()` throws and `getFailureCause()` no longer names what actually happened.

Checked and dismissed, with where:

- **`ProducerManager#commitOffsets`** - the lock is released by `AbstractOffsetCommitter`'s own
  `finally { postCommit(); }`, and `preAcquireOffsetsToCommit()` sits **outside** that try, so a
  failed acquire never reaches the release. Correct as written.
- **The two metrics steps in `doClose`'s finally** (`deregisterMeters`, `pcMetrics.close`) - already
  guarded individually, with a comment above them explaining that an escape from a finally would
  replace the real shutdown error.
- **`RetryQueue`'s read/write lock pairs** - every `lock()` is the statement immediately before its
  `try`, so the take-then-fail-then-release shape cannot arise.
- **`BrokerPollSystem#closeAndWait`** - a wait rather than a multi-step teardown, and its one caller
  is guarded with a comment saying why the consumer close must still run.

## How each was shown red

Every fix has a unit test observed failing first. Worth recording is how instance 1's test had to be
built, because the masking above defeats the obvious one: **stub `closeOnException` to a no-op**, so
the instance stays alive and the batch's fate becomes observable. Then a failed record is
re-dispatched and nothing commits; a swallowed one is produced once, marked succeeded, and its
offset commits. Three assertions were predicted red and all three observed, one run each with the
preceding assertion removed so the next could be reached: `produceMessages` called once rather than
at least twice, offset 1 committed against an expected empty set, and the incomplete-offset count 0
rather than 1.

Instance 2's tests use the spy on the real `ProducerWrapper` that `PCModuleTestEnv` already builds,
and failed as `ProducerFencedException: fenced` and `IllegalStateException: Not held be me` escaping
`close`, and as the `KafkaException` escaping the user's `close()`.

## Pointers

- The issue: astubbs/parallel-consumer#423, which carries both holes and the PR that closes them.
- The retry spin on the wrapped shape: astubbs/parallel-consumer#411 (mirror of
  confluentinc/parallel-consumer#830).
- Recovery instead of shutdown on the produce path, and the `closeOnException` redesign:
  astubbs/parallel-consumer#410.
- The commit-failure taxonomy this feeds: astubbs/parallel-consumer#241.
