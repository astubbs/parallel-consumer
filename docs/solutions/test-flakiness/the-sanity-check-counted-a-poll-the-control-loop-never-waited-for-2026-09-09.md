---
title: "The sanity check counted a poll the control loop never waited for"
date: 2026-09-09
category: test-flakiness
module: parallel-consumer-core
problem_type: test_failure
component: testing_framework
severity: medium
symptoms:
  - "`ParallelEoSStreamProcessorTest.processInKeyOrder` red on `[sanity check input data]`"
  - "`Actual and expected should have same size but actual size is: 0 while expected size is: 9`"
  - "Always all-or-nothing - 0 records, never 3 or 7"
  - "Needs a concurrent full suite; the class alone and the method alone stay green"
  - "Failing parameterisation varies run to run, and sibling tests in the same class fail with it"
root_cause: async_timing
resolution_type: test_fix
status: "Fixed. `551f40011` on astubbs#29, which reached master as `a6941020f` on 2026-09-02."
applies_when:
  - A test asserts on state filled by the POLL thread after waiting on control-loop cycles
  - Reading a sighting of this failure on a branch cut before 2026-09-02
  - Reviewing any `awaitForOneLoopCycle` / `awaitForSomeLoopCycles` call that precedes an assertion
related_components:
  - testing
related_prs:
  - "astubbs#29 - carries the fix (`551f40011`), merged as `a6941020f`"
  - "astubbs#264 - the sibling frontier fix in the same test, for a different assertion"
related:
  - "docs/solutions/test-flakiness/assert-the-commit-frontier-not-the-tick-path.md - the other flake in this same test, and not this one"
  - "docs/solutions/test-flakiness/a-randomised-key-draw-decided-a-batch-count-the-test-computed-from-the-record-count-2026-09-08.md - sibling shape: an expectation the test's own input decided"
  - "docs/inflight/test-untracked-ci-flakes.md - the register this row was retired from"
tags:
  - flaky-tests
  - key-ordering
  - awaitility
  - assertion-design
  - stale-resolution
---

# The sanity check counted a poll the control loop never waited for

`ParallelEoSStreamProcessorTest.processInKeyOrder` primes nine records, then asserts its own input
before testing anything:

```
java.lang.AssertionError:
[sanity check input data]
Actual and expected should have same size but actual size is: 0 while expected size is: 9
```

`polled` is filled by a `doAnswer` on `consumerSpy.poll`, which runs on the **poll** thread. The
wait in front of the assertion was `awaitForOneLoopCycle()`, which counts **control** thread loop
iterations (`blockingLoopLatchTrigger` in `AbstractParallelEoSStreamProcessorTestBase`). Nothing
orders the two. The control loop turns roughly every 100ms whether or not the poll thread has ever
been scheduled, so under CPU oversubscription the loop can turn first and the assertion reads an
empty list.

The signature is always all-or-nothing - 0 records, never a short count - because the
`MockConsumer` hands over the whole batch in one call. That is the tell that separates this from a
slow machine delivering some records: there is no partial state to see.

## Why this was hard to place, and why it is worth a document

**Two different failures share this test's name**, and conflating them cost several sessions.
The other one is a `ConditionTimeoutException` on a commit-list assertion, written up in
[`assert-the-commit-frontier-not-the-tick-path.md`](assert-the-commit-frontier-not-the-tick-path.md)
and fixed separately by astubbs#264. Same test, different phase, different mechanism, different fix.
A reader who found that document assumed this was it and stopped.

**The load story was wrong in both directions before it was right.** Early sightings were all on
loaded boxes, so "load" was recorded as the variable. A later control arm reddened *unmodified*
`master` on an idle box - three parameterisations at once, where the loaded branch arm had managed
one - which retired that reading. Load moves the rate; it is not what makes the failure possible.
The mechanism above is, and it needs no load at all, only an unlucky scheduling of two unordered
threads.

## Solution

Wait for the data instead of for a cycle count:

```java
await().untilAsserted(() ->
        assertThat(polled).as("sanity check input data").hasSameSizeAs(locks));
```

The assertion is unchanged - the same exact-size claim over the same list. Only the *precondition*
moved, from "one control-loop cycle has happened" to "the records the test seeded have arrived".
That is a strengthening, not a loosening: the old form could pass on a fast box while asserting
nothing about delivery, and the new one cannot be reached before it can be true.

## Evidence

**The original measurement**, in `551f40011`'s own body: matched-pair A/B with both arms running
simultaneously so load was controlled rather than assumed - **5 failures in 31 runs** on the arm
without the wait, **0 in 31** with it.

**The provenance check that settles the register row**, run 2026-09-09. The fix was authored on
astubbs#29's branch on 2026-08-18 and reached `master` only when that PR merged as `a6941020f` on
2026-09-02. Every recorded sighting of this failure predates that date - including the one whose
control arm reddened "unmodified master", which was at `b2e6c190d` (2026-08-27), five days before
the fix landed. There is no sighting on a tree that carries the fix.

**The control arm**, run 2026-09-09 on today's `master`: two worktrees at the same commit, differing
in that one hunk only - the `await()` wrapper removed, restoring the point assertion - running the
full `parallel-consumer-core` unit suite simultaneously, matched-pair, on a deliberately
oversubscribed 12-core box. Rates and conditions are in the commit that retired the register row;
what matters here is the design, because a single arm on one machine is a sample and not a proof,
and the 5/31-versus-0/31 pair above is the stronger measurement.

## The class, and where else it was found

**An assertion about work done by one thread must not be gated on a cycle count from another.**
Cycle counts are the test-side twin of the wall-clock tick that
[`assert-the-commit-frontier-not-the-tick-path.md`](assert-the-commit-frontier-not-the-tick-path.md)
warns about: both pin the speed of the machine while wearing a correctness assertion's clothes.

`551f40011` named the other instances rather than sweeping them into that PR, and they were checked
again on 2026-09-09:

- `ParallelEoSStreamProcessorTest` - both `assertThat(polled)` sites are now awaited (grep
  `sanity check - the records have been polled` for the second).
- `queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown` - fixed by astubbs#101, which
  replaced its cycle count with `awaitUntilTrue(gotK0::get)` plus `awaitForCommit(1)`; its comment
  records the reasoning.
- `inFlightMessagesCommittedIfProcessedDuringShutdown` - **still had `awaitForSomeLoopCycles(2)`
  standing in for "the record is in flight"**, and was caught red by the campaign above:
  `assertCommits` saw `[]`, because `close()` had no in-flight work to complete. Now waits on the
  user function actually being entered. Matched-pair on the class under heavy load, the fix as the
  only difference: red twice in fourteen runs on the unfixed arm, none in fourteen on the fixed one
  - a small n, so the fix rests on the mechanism and on the sabotage below rather than on that
  count.
- `executorThreadsInterruptedOnShutdownTimeout` - reviewed and left alone; its wait is on the latch
  the user function holds, not on a cycle count.

## The sabotage that took four attempts, and why that is the interesting part

`docs/testing-at-write-time.md` requires breaking the behaviour and watching the test fail before
trusting a changed test. For `inFlightMessagesCommittedIfProcessedDuringShutdown` that took four
main-code mutations, and **the first three left it green** - which, taken at face value, reads as
"this test asserts nothing about committing". It does not. The mutations had not reached the commit:

1. `closeDrainFirst()` switched to `DONT_DRAIN` - never ran. The test calls `close()`, which *is*
   `closeDontDrainFirst()` already, and `DONT_DRAIN` still finishes work in flight by design.
2. The close-sequence call to `commitOffsetsThatAreReady()` removed - green.
3. `commitOffsetsThatAreReady()` neutered outright - green, and this one printed a marker proving it
   ran. So the offset this test asserts is not committed by that method at all.
4. `AbstractOffsetCommitter#retrieveOffsetsAndCommit`'s call to `commitOffsets` removed - **red on
   all three parameters**, on the assertion under test.

**The marker is what separated "no effect" from "never ran"**, and it is why attempts 1 and 2 are
recorded rather than quietly dropped: without one, three green sabotages would have made a strong
and wrong case that the test was dark, and the honest conclusion from a green sabotage is *"I have
not yet shown the test still guards its property"*, never *"the property is unguarded"*. The
cheapest form is a `System.err.println` inside the mutated branch; the AGENTS.md rule it instances is
**verify your instrumentation actually reached the run**.

## Prevention

Before an assertion that reads state some *other* thread fills, ask which thread writes it. If it
is not the thread the wait is counting, the wait is unrelated to the assertion and the test is a
race. `awaitUntilTrue`, or `await().untilAsserted` on the value itself, is the fix; a larger cycle
count is not.
