---
title: "A user's metrics registry could kill the poll thread on rebalance, and replace the real error on close"
date: 2026-08-19
category: runtime-errors
module: parallel-consumer-core
problem_type: runtime_error
component: service_object
severity: high
symptoms:
  - "A user-supplied Micrometer MeterRegistry that throws on remove() kills the broker-poll thread inside onPartitionsRevoked, on every rebalance"
  - "Every commit after that blocks until it times out, because the poll thread is the only producer of commit responses"
  - "An exception from doClose's finally replaces the real shutdown error, so close() reports a metrics failure instead of the actual cause"
  - "state never reaches CLOSED, so the state == CLOSED half of isClosedOrFailed() stays false and teardown after the throw is skipped"
root_cause: missing_validation
resolution_type: code_fix
related_components:
  - PCMetrics
  - PartitionStateManager
  - PartitionState
  - AbstractParallelEoSStreamProcessor
tags:
  - metrics
  - micrometer
  - shutdown
  - rebalance
  - third-party-code
  - never-throws-contract
  - finally-block
---

# A user's metrics registry could kill the poll thread and strand the close

## Problem

Parallel Consumer removes meters from a `MeterRegistry` in two places that must not fail:
on **partition revocation** and during **close**. When the user supplies their own registry -
which is the entire point of `ParallelConsumerOptions.meterRegistry` - every one of those removal
calls is third-party code running on a critical path, and none of it was guarded.

The exposure is real rather than theoretical because the *default* registry is PC's own. In
`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/metrics/PCMetrics.java`, the
constructor sets `isNoop` and installs a `CompositeMeterRegistry` when the caller passes `null`;
otherwise it stores the caller's instance and every teardown call goes into code PC does not own.
A push-style registry that flushes to a reporting backend on removal or on `close()` is the
ordinary trigger: the backend being unreachable at shutdown is a Tuesday, not an exotic fault.

Two distinct failure paths, both silent, neither previously covered.

**1. The revoke path - the worse one.** Meter de-registration runs on rebalance:
`PartitionState.onPartitionsRemoved` calls `deregisterMetrics()` (eight `pcMetrics.removeMeter(...)`
calls in
`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/PartitionState.java`), and
`PartitionStateManager.onPartitionsRemoved` calls `deregisterPartitionCounters`
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/PartitionStateManager.java`).
`PartitionStateManager.onPartitionsRevoked` logs and **rethrows** (`log.error("Error in
onPartitionsRevoked", e); throw e;`), and that callback runs on the broker-poll thread inside
`poll()`. Kill that thread and you have removed the only producer of commit responses, so every
later commit blocks until its timeout. That is the confluentinc#857 family's worst shape, reached
from a *reporting* concern, and it fires on **every rebalance** - not only at shutdown.

**2. The close path.** `AbstractParallelEoSStreamProcessor.doClose` does its metrics teardown in a
`finally` block: `deregisterMeters()` (which delegates to
`pcMetrics.removeMetersByPrefixAndCommonTags(USER_FUNCTION_EXECUTOR_PREFIX)`) and then
`pcMetrics.close()`, followed by `this.state = CLOSED`. An exception thrown from a `finally`
**replaces** the exception already in flight, so an unguarded registry would:

- destroy the real shutdown failure, substituting a metrics error for the actual cause;
- skip the remaining teardown, so the meters it was cleaning up leak anyway;
- never execute `this.state = CLOSED`.

The irony is on the record in the source: `doClose`'s own comment says it exists to
"ensure doClose() state transition to CLOSED by catching unhandled exceptions in subsystems during
close" (confluentinc#809). That guard wrapped `innerDoClose`. The `finally` it introduced was
itself unguarded.

## Symptoms

- On the revoke path: the broker-poll thread dies mid-`poll()` on a rebalance; commits then hang.
  This is the shape reported as `Timeout waiting for commit response PT30S`
  (`docs/inflight/bug-177-commit-response-timeout-unreproduced.md`), which lists a throwing metrics
  registry as candidate 3 and is explicit that no reported occurrence has been attributed to it -
  the mechanism matches, the attribution does not exist.
- On the close path: `close()` throws an exception whose cause is the metrics backend, not the
  shutdown problem that was actually happening. `waitForClose` rethrows the `ExecutionException`
  it gets from the control-thread future, so the substituted error is what the caller sees.
- `state` never reaches `CLOSED`. `isClosedOrFailed()` is
  `state == State.CLOSED || controlThreadFuture done-or-cancelled`, so the first half is
  permanently false - while the second half turns **true**, because `doClose` runs on the control
  thread and an escape from its `finally` completes that thread's future exceptionally.
- **So the risk runs the opposite way to the obvious guess, and the obvious guess is wrong.** Callers
  are not left waiting - they are told `true` early, by a disjunct that means "the control thread
  finished, somehow", not "the engine closed cleanly". A caller cannot tell those apart through this
  method.
- That is not hypothetical. `ManagedPCInstance.run()` in the chaos harness gates bringing up the next
  instance on `while (!parallelConsumer.isClosedOrFailed() && waitMs < 10_000 && !stopRequested)`.
  The loop exits on its **first** poll, so it waits ~0ms rather than its 10s bound, the timeout warn
  never fires, and it proceeds immediately and silently against a half-torn-down predecessor.
- Nothing goes red. Every one of these is silent.

## What Didn't Work

The failed attempts here are more useful than the fix, because each one *looked* finished.

**The guard was first put at the wrong level.** The first version wrapped
`PartitionStateManager`'s call site in a try/catch. Once the never-throws contract was placed at
the source instead, that guard could never fire - and defensive code that cannot fire is worse
than none, because it implies the contract is doubted. It was removed, and the reasoning is now
recorded *in place of it*, in the javadoc on `deregisterPartitionCounters`: "No try/catch here on
purpose: `PCMetrics#removeMeter` carries the never-throws contract".

**The `finally`-only fix was incomplete and looked complete.** `PCMetrics.close()` iterates
`this.registeredMeters.forEach(this.meterRegistry::remove)` - a direct method reference to the
user's registry that **bypasses the guarded `removeMeter`** entirely. That hole was invisible while
it stayed where it was, because its only caller (`doClose`'s `finally`) wraps the call in a
try/catch: the escape was caught one frame up, so the test passed and the guard looked total. It
surfaced only when the change was lifted onto a separate branch cut from master, where no wrapper
existed, and the test failed immediately.

> **A contract that holds only because every caller guards it is not a contract.** Moving code
> away from its callers is how a masked guard is exposed - if you want to know whether a
> never-throws promise is real, extract it and see what breaks *(auto memory [claude])*.

**The test failed twice before it passed, and both failures were the point.** The
`ExplodingRegistry` was originally armed from construction. That killed the instance long before
the close path was ever reached - which is precisely how the revoke-path exposure was discovered.
A narrower test, written to pass on the first run, would have shipped the `finally` guard and
missed the larger defect entirely. The final test arms only for the close (`armed.set(true)` right
before `closeDrainFirst()`), and the reason is written into the field's javadoc so nobody
"simplifies" it back.

**A loop-level guard needed correcting to per-meter - and that correction is on astubbs/parallel-consumer#57, not here.**
The first shape of
`removeMetersByPrefixAndCommonTags` wrapped the whole `forEach` in one try/catch. That aborts on
the first hostile meter, leaving every remaining meter in the registry *and* its id in PC's own
tracking set - which is where the confluentinc#859 heap leak lives. Wrapping a loop is not the
same as wrapping its body.

## Solution

**Guard at the source, not at the call sites.** `PCMetrics.removeMeter(Meter.Id)` and
`removeMetersByPrefixAndCommonTags(String)` now carry an explicit **never-throws** contract,
stated in javadoc that also says *why it lives there*: there are **eleven** teardown call sites -
eight in `PartitionState.deregisterMetrics`, one in
`PartitionStateManager.deregisterPartitionCounters`, two in `WorkManager` - and one missed site is
enough to reproduce the failure.

Three properties fall out of the guard:

- The registry call is wrapped, and a refusal is logged and swallowed. A leaked meter is accepted
  as the far smaller problem.
- In `removeMeter`, the id is dropped from PC's **own** tracking set whatever the registry did, so a
  failing registry cannot also grow that collection without bound. This property is `removeMeter`'s
  alone: in this checkout `removeMetersByPrefixAndCommonTags` does no untracking at all, and gains it
  on astubbs/parallel-consumer#57.
- `doClose`'s `finally` keeps its own two guards - one per step - as a last line of defence,
  because the `state = CLOSED` transition has to happen regardless of what any callee does. Each
  step is guarded *separately*: one failing must not skip the other.

Even the guard's own logging is defended. `doClose` reports through
`ThrowableUtils.logWithoutEscaping(e, () -> log.warn(...))`
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/utils/ThrowableUtils.java`),
which attaches a logging failure to the reported throwable as suppressed rather than letting it
escape - because a log call that renders user-supplied state is user code too, running before the
part that matters.

The pin is
`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/internal/MetricsTeardownCannotBreakCloseTest.java`,
which asserts three things:

1. `assertDoesNotThrow(() -> parallelConsumer.closeDrainFirst())` - a failing registry must not
   propagate out of close.
2. `parallelConsumer.isClosedOrFailed()` is true - close still reaches a terminal state.
3. **`removeAttempts.get()` is greater than zero** - the exploding registry was actually called.
   Without this the test passes against no guard at all.

### Landed in two parts - and only half of it is on master's line yet

- **astubbs/parallel-consumer#29** (`fix(core) astubbs#119`) carries the source-level guard on
  `removeMeter`, the removal of the call-site guard, `doClose`'s two `finally` guards, and the test.
- **astubbs/parallel-consumer#57** carries the completion found by the extraction: `close()`'s
  direct iteration now goes through a shared `removeQuietly(Meter.Id, String)`; `removeMeter(Meter)`
  guards `meter.getId()` as well, since a custom registry returns its own `Meter` implementations;
  `removeMetersByPrefixAndCommonTags` keeps **two** guards, an inner per-meter one via
  `removeAndUntrack` and an outer one covering enumeration (`getMeters()` is the user's code too);
  and all of it is serialised on a private `metersLock`.

Both PRs were open drafts when this was written, so **master at the time of writing had none of
this** - not a partial contract but no `catch (Exception)` anywhere in `PCMetrics`, including the
unguarded `this.registeredMeters.forEach(this.meterRegistry::remove)`. Three trees, three different
answers: master unguarded, astubbs/parallel-consumer#29 guarded at `removeMeter` and in `doClose`'s
`finally`, astubbs/parallel-consumer#57 complete. **Check which tree you are reading before assuming
the contract holds** - that is the same mistake this doc records under "What Didn't Work", where a
guard looked whole from inside the branch that had the other half.

## Why This Works

- **Eleven call sites, one guard.** The guard sits where the third-party call is made, so no future
  caller can reintroduce the defect by forgetting. The javadoc names the count and the three
  classes, so the next reader can see what the alternative would have cost.
- **The contract is stated where it is honoured, and its absence is stated where it is not.** The
  call site that *doesn't* have a try/catch explains that it deliberately doesn't, and points at
  the method that does. That is what stops the next reviewer "helpfully" restoring the guard that
  can never fire.
- **`finally` guards are about the transition, not the exception.** The two catches around
  `deregisterMeters()` and `pcMetrics.close()` exist so `this.state = CLOSED` is unconditionally
  reached. Guarding the callee is correctness; guarding the `finally` is making the state machine
  independent of the callee's correctness. Both, because the callee is not the only future
  occupant of that block.
- **Per-meter, not per-loop** - on astubbs/parallel-consumer#57, where failure of one meter costs
  exactly one meter, in the registry and in PC's tracking set. In this checkout the guard is still
  loop-level and one hostile meter still strands the rest. That matters twice: for cleanliness, and because the tracking set is
  where the confluentinc#859 heap growth lives.
- **The test cannot pass vacuously.** The non-vacuity assertion on `removeAttempts` is what makes
  this a regression test rather than a smoke test.

## Prevention

- **Guard at the source when the call-site count is high.** Eleven sites across three classes: a
  per-site guard is a guard someone will miss, and each one is also a place the contract can be
  read as optional. Put it once, at the call to the foreign code, and say in the javadoc *why it
  is there rather than at the callers*.
- **Never place a guard that cannot fire.** Once the contract is at the source, a second try/catch
  at a call site is not belt-and-braces - it signals that the contract is doubted, and the next
  reader will spread it. Replace it with a comment naming the method that carries the contract.
- **Any third-party or user-supplied code on a critical path needs an explicit never-throws
  contract.** In this library the critical paths are the broker-poll thread inside `poll()` and
  anything in a close `finally`. The question is not "is this callee likely to throw" but "can the
  caller do anything if it does" - and if the answer is no, the guard belongs there regardless of
  the callee's identity. On astubbs#57 even PC's own internal `CompositeMeterRegistry.close()` is
  guarded, for exactly that reason.
- **An exception in a `finally` replaces the one in flight - treat every `finally` that calls out
  as a diagnosis-destroying site.** Review `finally` blocks for *escapes*, not just for
  completeness. If a `finally` performs a state transition, everything before that transition in
  the same block must be individually guarded, or the transition is conditional on code you don't
  control.
- **Wrap the loop body, not the loop.** A try/catch around a `forEach` converts "one element
  failed" into "the rest never ran". If both need covering - the enumeration and the per-element
  work - use two guards and say in a comment that both are load-bearing, or someone will narrow it
  to one.
- **The test shape that pins this: assert the dangerous thing was actually attempted.** A guard
  test that never triggers the guard passes against an unguarded build. Count the calls into the
  hostile collaborator and assert the count is non-zero, with a failure message that says so.
- **Arm the fault injector at the narrowest scope that reaches the path under test - and write down
  why.** A globally-armed fault killed the instance before the target path was reached. That
  failure is *information* (it found the revoke-path exposure), but the shipped test must be scoped,
  and the scoping must carry a comment or it will be widened back by someone who reads it as timid.
- **To find out whether a never-throws contract is real, move the code away from its callers.** A
  guard that is only ever exercised through a caller that catches is untested and possibly absent.
  Cherry-pick it onto a bare branch, or write a direct unit test that calls the method with nothing
  above it. *(auto memory [claude])*
- **When a defensive fix lands in two PRs, say so in the write-up.** Half a never-throws contract
  reads exactly like a whole one from inside the branch that has the other half.

## Related

- [`revoke-path-commit-deadlock-between-poll-and-control-threads.md`](revoke-path-commit-deadlock-between-poll-and-control-threads.md) -
  the other way the same thread is lost - there it **parks** rather than dying, which is the very
  distinction bug-177 turns on. Same victim (the broker-poll thread inside `onPartitionsRevoked`)
  and the same downstream symptom (commit responses stop, waiters time out), reached from a lock cycle
  rather than from a reporting call. Adjacent, not overlapping: different root cause, different fix.
- [`../architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md`](../architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md) -
  why this seam keeps producing defects, and why the poll thread is load-bearing.
- [`../../inflight/bug-177-commit-response-timeout-unreproduced.md`](../../inflight/bug-177-commit-response-timeout-unreproduced.md) -
  this defect is the **third** candidate mechanism for `Timeout waiting for commit response PT30S`,
  and it **closes neither** astubbs/parallel-consumer#177 nor astubbs/parallel-consumer#175. It needs a
  user-supplied registry that throws; Parallel Consumer's default when none is configured is an empty
  `CompositeMeterRegistry` that cannot, and neither report says metrics were configured at all.

### Not a contradiction of "a guard must assert what it means"

[`../architecture-patterns/a-guard-must-assert-what-it-means-not-what-is-easy-to-check.md`](../architecture-patterns/a-guard-must-assert-what-it-means-not-what-is-easy-to-check.md)
argues against catch-and-log: its incident was a guard downgraded to a warning, which silently left a
consumer unclosed. **This fix deliberately swallows**, so the two read as opposite advice unless the
discriminator is stated.

The discriminator is what the failure MEANS, not how it is handled:

- **A guard failing is a bug report.** It says an invariant the code depends on is already broken, so
  swallowing it destroys the only evidence and lets the program continue on a false premise. It must
  propagate.
- **Best-effort teardown failing is a status report about something the program does not depend on.**
  Nothing downstream reads a meter. Removing one is bookkeeping for an external system, and that
  system is the user's, so the failure is theirs to see and not ours to die on.

The test: *does anything the program is about to do depend on this call having succeeded?* For
`ThreadConfinedConsumer`'s ownership check, yes - the next consumer call is unsafe without it. For
`removeMeter`, no. Same shape, opposite answers, and the answer is the reason.
