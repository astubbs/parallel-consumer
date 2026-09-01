---
title: "A counting assertion is vacuous when the loop body changes the precondition it counts under"
date: 2026-08-18
category: test-flakiness
module: parallel-consumer-core
problem_type: test_design_bug
component: testing
severity: medium
root_cause: assertion_counted_a_path_the_loop_stopped_reaching_after_its_first_pass
resolution_type: test_fix_re_arm_the_precondition_each_iteration
status: "SOLVED - test-side fix on astubbs#296 (branch `fix/209-submit-to-terminated-executor`, commit 79a7b6c62), OPEN at the time of writing. Nothing enforces the rule; it is the mutation check plus this write-up."
applies_when:
  - "Writing any assertion that counts occurrences - `hasSize(n)`, `verify(times(n))`, 'logged once', 'called once'"
  - "Pinning an edge-trigger, a CAS-guarded one-shot, a rate limiter, or any 'say it once' diagnostic"
  - "Driving a state machine repeatedly from a test loop, where the code under test also writes that state"
  - "Deciding which of a change's new assertions are worth mutation-checking"
symptoms:
  - "The assertion passes, reads as strong, and survives review by reading"
  - "Deleting the guard the assertion claims to pin leaves the test green"
  - "The expected count happens to equal the number of times the path was actually reached, for an unrelated reason"
  - "A loop of N calls, but the code under test reached the interesting branch on pass 1 only"
tags:
  - vacuous-assertion
  - counting-assertion
  - mutation-testing
  - edge-trigger
  - state-machine
  - assertion-design
  - test-review
related_prs:
  - "astubbs#296 - hardening work submission against an already-closed worker pool; where all three vacuous assertions were written and found"
  - "astubbs#292 - removed the harness double-start that produced the original chaos failure astubbs#296 hardens against"
related:
  - "vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md - the await-side sibling: a condition satisfied before the system reaches its initial state. Same failure (the assertion cannot fail), different mechanism (timing window, not a state transition)"
  - "at-most-assertion-raced-the-block-it-checked-2026-08-13.md - an assertion whose window was the same length as the thing it asserted against"
  - "../workflow-issues/red-proof-old-code-new-tests-2026-08-18.md - the git traps that make a red-proof vacuously green, i.e. how the mutation check itself gets faked"
---

# A counting assertion is vacuous when the loop body changes the precondition it counts under

## Problem

Three assertions written during astubbs#296 turned out to be **vacuous** - each passed against the
very guard it claimed to pin, so deleting the guard left the test green. Two were caught by the
author while mutation-checking (one repaired, one deleted). The third survived two review rounds and
was caught by a third.

This is a finding about method, not about three careless moments: the author *was* mutation-checking
guards, and still shipped a vacuous assertion, because the check was applied selectively - to the
assertions that felt load-bearing.

## The instructive one

astubbs#296 adds an edge trigger so that a specific ERROR diagnosis is logged **once** per instance
rather than once per control-loop pass. The condition it reports is sticky - a shut-down worker pool
never comes back - so repeating the line every pass would bury the one line worth reading. The
trigger is a CAS on an `AtomicBoolean`
(`handledPoolGoneWhileStateAllowsWork` in
`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/AbstractParallelEoSStreamProcessor.java`,
`compareAndSet(false, true)` inside `onPoolGoneWhileStateAllowsWork`).

The test called `retrieveAndDistributeNewWork` three times against a shut-down pool, captured the
class logger with a Logback `ListAppender`, filtered to the pool-gone ERROR line, and asserted:

```java
.that(skipWarnings).hasSize(1);
```

It reads as airtight: three chances to log, exactly one line, therefore the edge trigger suppressed
the repeats.

**It did not test the trigger at all.** The guarded block sits behind the state gate in
`retrieveAndDistributeNewWork` -

```java
if (state == RUNNING || state == DRAINING) {
```

\- and the first call's own reaction to the dead pool is `transitionToClosing()`. So call 1 detected,
logged, and moved the instance to `CLOSING`; calls 2 and 3 failed the state gate and never reached
the guarded block. One log line was the only possible outcome **whether or not the CAS existed**.
Confirmed by deleting the CAS: still green.

The shape, stated plainly: **the loop body destroyed the precondition the loop was counting under**,
and the count came out right for a reason unrelated to the guard.

## Why review by reading could not find it

All three vacuous assertions read as strong assertions. Nothing about `hasSize(1)` over three calls
looks weak; the weakness is a two-hop inference - the code under test writes the state, and the state
gates the branch being counted - and neither hop is visible at the assertion. Two review rounds went
past this one.

**The only thing that finds it is deleting the guard and watching the test fail.**

## The repair

Re-arm the state on every iteration, and say why at the site:

```java
pc.setState(loop == 0 ? State.RUNNING : State.DRAINING);
pc.retrieveAndDistributeNewWork(userFunction, callback);
```

`DRAINING` rather than `RUNNING` after the first pass because it is the *real* re-entry route the
trigger exists for: a `close(DRAIN)` arriving after the self-close puts the state back to `DRAINING`,
which is what the guard's own comment describes. Re-arming with a state the production system could
never present would make the test pass for a reason production does not have.

With the CAS deleted, the repaired test now reports 3 log lines and fails. That was verified by
actually deleting the guard and running it, not reasoned about.

Test: `aPoolGoneUnderARunningInstanceClosesItOnceAndSaysWhy` in
`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/internal/SubmitWorkToPoolShutdownRaceTest.java`
(the filter it counts is keyed on `POOL_GONE_MESSAGE`). Both files are on
`fix/209-submit-to-terminated-executor` at 79a7b6c62; `retrieveAndDistributeNewWork` and the
`state == RUNNING || state == DRAINING` gate are already on master.

## The transferable shape

- **A counting assertion is only meaningful if the path under test is actually *reached* the expected
  number of times.** `hasSize(1)`, `verify(..., times(1))`, "logged once", "called once" all assert a
  count; none of them assert *arrival*. A state transition, an early return, an exception, or a
  short-circuit inside the loop can make the count correct for a reason that has nothing to do with
  the guard.
- **Sharpest single heuristic**: if an assertion counts occurrences of something inside a loop, check
  what the loop body does to the precondition on its first pass. If pass 1 changes the state that
  gates the counted branch, passes 2..n are decoration.
- **Suppression assertions are the high-risk family**, because "it happened once" and "it was only
  reachable once" produce identical evidence. Anything that pins an edge trigger, a one-shot CAS, a
  rate limiter, a memoisation, or a de-duplicating log is asserting a *negative* - that the extra
  occurrences did not happen - and a negative is satisfied just as well by never getting there.
- **Assert arrival separately when you can.** Counting the log line proves nothing about how often
  the branch ran; a second assertion on an observable that increments per *reached* pass turns an
  unreachable path into a failure rather than a pass.

## Prevention: mutation-check every new assertion, not just the risky-looking ones

The rule this incident produces is not "be careful with counts" - it is a change to *coverage of the
check*:

- **Delete the guard, run the test, confirm it fails, restore.** Per guard, individually, confirming
  that *exactly* its own test went red. A guard whose removal breaks three tests and a guard whose
  removal breaks none are both findings.
- **Apply it to every new assertion in the change.** Three vacuous assertions in one change is the
  evidence for this. Selectivity is what failed: the author's judgement about which assertions were
  load-bearing was itself the thing that was wrong, so it cannot be the filter that decides what gets
  checked.
- **An assertion that cannot fail is worse than none**, because it is counted as coverage - by the
  next reader, by the PR description, and by whoever later decides the area is well tested.
- Beware faking the check itself: a stash/checkout round-trip can run the *old* tests against the
  *old* code and report a comfortable green - see
  [`../workflow-issues/red-proof-old-code-new-tests-2026-08-18.md`](../workflow-issues/red-proof-old-code-new-tests-2026-08-18.md).
