---
title: "A test asserted a null MDC as a precondition on a thread it shares with every other class in the fork - two of them leave it holding an empty map"
date: 2026-09-02
category: test-flakiness
module: parallel-consumer-core
problem_type: order_dependent_test
component: test-infrastructure / MDC
symptoms:
  - "MdcContextPropagationTest.anEmptyCallerContextIsHandledAndNothingLeaks:194 expected: null but was : {}"
  - "Fails 2 of 2 full core runs on one tree, 0 of 3 on another, with identical MDC code - CI and local disagree at the same test count"
  - "Passes every time alone, and every time with its own class on one thread"
root_cause: precondition_on_shared_thread_state_the_test_did_not_establish
tags: [mdc, logback, junit, order-dependent, fork-distribution, precondition]
related_components: [MdcPropagation, AbstractParallelEoSStreamProcessor#processWorkCompleteMailBox]
---

# A precondition on shared thread state, and the two tests that violated it

## Symptom

`MdcContextPropagationTest.anEmptyCallerContextIsHandledAndNothingLeaks` opens with

```java
// deliberately NO MDC.put here - the caller has no diagnostic context at all
assertThat(MDC.getCopyOfContextMap()).isNull();
```

and that line failed with `expected: null but was : {}` - on the JUnit thread, before Parallel
Consumer was involved at all. First sighting 2026-09-01 on astubbs#262; then 2 of 2 full core runs
on the astubbs#203 tree and the CI run of astubbs#201, while clean master, astubbs#202 and a local
run of astubbs#201 passed. Same MDC code everywhere.

## What was ruled out, and how

- **The class's own siblings, via `@AfterEach MDC.clear()`.** Master's note hypothesised that
  logback 1.6.1's `clear()` leaves an empty map. Measured directly (`jshell` against the exact
  jars): `put` then `clear()` gives **`null`**. Falsified. The class alone on one thread passes 3 of 3.
- **`MdcPropagationTest`**, the obvious suspect with the same teardown - paired with the failing
  class, one fork, one thread, alphabetical: passes.
- **`ManagedPCInstance`**, which does `MDC.remove(MDC_INSTANCE_ID)` on its caller's thread - lives in
  `src/test-integration`, and no surefire test uses it. Out of the fork.

## Mechanism

Two facts, each measured rather than read:

1. On logback 1.6.1, `MDC.remove(lastKey)` and `MDC.setContextMap(emptyMap)` leave the thread holding
   **`{}`**; only `clear()` (and a pristine thread) gives `null`.
2. `AbstractParallelEoSStreamProcessor#processWorkCompleteMailBox` wraps each completed work
   container in `MDC.put(MDC_WORK_CONTAINER_DESCRIPTOR, ...); ...; MDC.remove(...)`. On the control
   thread that is harmless. But **`ProducerManagerTest` and `TransactionalBulkCommitTest` drive that
   method directly on the JUnit runner thread**, and leave it holding `{}`.

`-Pci` runs surefire at `forkCount=1C` with `reuseForks` and `parallel-tests=false`, so the classes
in a fork share one runner thread sequentially. Whether one of those two classes precedes
`MdcContextPropagationTest` in its fork depends on how the class list splits across forks - which
moves with the total test count and with scheduling. That is the whole of the "flake": the same
tree at 594 tests fails every time, at 602 it depends on the run.

Bisect that found them: each candidate class that calls into the control loop from a test, paired
with the failing class, `-Dsurefire.forkCount=1 -Dparallel-tests=false
-Dsurefire.runOrder=reversealphabetical` (so the MDC class runs last). Four of six clean; those two
each reproduced the failure exactly once per run.

## Fix

The test now establishes its precondition instead of asserting the world into it: `clearCallersContext`
is `@BeforeEach` as well as `@AfterEach` - the shape its sibling `MdcPropagationTest` already had.
`clear()` is proven to give `null`, so the `isNull()` assertion stays and is now deterministic. Both
polluter pairings pass after the change; the full core lane on the tree that failed 2 of 2 passes.

Not done, deliberately: making the two polluters clean up after themselves. They are exercising
main code that owns its own thread in production; the MDC residue is a property of the runner thread
being borrowed, and every future test that borrows it would need the same courtesy. The test that
cares about the precondition is the one place the rule can be enforced.

Also corrected: `MdcPropagation.capture()`'s comment claimed it returns `null`, never an empty map,
for an empty context. It returns `{}` on a thread that has put-and-removed. Both readers
(`enter`, `adopt`) already treat null and empty alike, so no behaviour changed; the comment did.

## The rule

**A test may assert a precondition only on state it established.** A JUnit runner thread is shared by
every class in the fork; anything thread-local on it - MDC, `ThreadLocal`s, interrupt status - is
whatever the previous class left, and "previous class" is decided by fork distribution, which moves
with the test count. Asserting it clean without first making it clean is an order-dependent test by
construction, and it will read as a flake in exactly the runs where it matters least.
