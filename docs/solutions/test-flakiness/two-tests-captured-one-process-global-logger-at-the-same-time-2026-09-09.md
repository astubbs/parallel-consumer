---
title: "Two tests captured one process-global logger at the same time"
date: 2026-09-09
category: test-flakiness
module: parallel-consumer-core
problem_type: test_failure
component: testing_framework
severity: medium
symptoms:
  - "`AmbientProbeExtensionTest` headroom methods red with `iterable.size() expected 1 but was 2`"
  - "The extra element is a `PC-DEADLINE-HEADROOM` line carrying the SIBLING test's outcome"
  - "`headroomIsSilentWithoutADeadlineAndWithoutAMeasurement` sees a line it never caused"
  - "Fails MORE when run as a class alone than inside a full suite"
  - "Never red on CI"
root_cause: shared_state
resolution_type: test_fix
status: "Fixed - the three capturing methods now share a `@ResourceLock`."
applies_when:
  - Two test methods in one class open `LogCapture` on the same logger
  - A test asserts on the exact set of lines a capture saw
related_components:
  - testing
related:
  - "docs/inflight/test-untracked-ci-flakes.md - the register this row was retired from"
tags:
  - flaky-tests
  - test-isolation
  - logging
  - shared-state
---

# Two tests captured one process-global logger at the same time

`LogCapture.of(SomeClass.class)` attaches an appender to the logger for that class. A logger is
**process-global**, not test-scoped, so while two test methods are inside their `try` at once, each
one's capture receives *both* tests' lines:

```
value of    : iterable.size()
expected    : 1
but was     : 2
iterable was: [PC-DEADLINE-HEADROOM ... outcome=PASSED,
               PC-DEADLINE-HEADROOM ... outcome=FAILED]
```

The `outcome=FAILED` line belongs to the sibling test. Each method's own assertion is correct; what
was missing is that only one of them may hold the capture at a time.

## Why it looked backwards

**It failed more when run alone than in a full suite** - which is the wrong direction for ordinary
contention, and is the tell for this mechanism. `parallel-consumer-core` sets
`junit.jupiter.execution.parallel.mode.default=concurrent` and leaves `mode.classes.default` at
JUnit's `same_thread`, so *methods within a class* run in parallel and *classes* do not. Running the
class alone puts the sibling methods side by side with nothing between them; a full suite
interleaves other work and sometimes separates them.

**It is never red on CI**, which is why it survived. The `ci` profile sets `parallel-tests=false`,
so JUnit method parallelism is off there and the two methods cannot overlap. Only local runs are
exposed - which is worse rather than better, because it is local runs a developer reads.

## Solution

A shared `@ResourceLock` on the three methods that capture this logger:

```java
private static final String HEADROOM_LOG_CAPTURE_LOCK = "ambient-probe-headroom-log-capture";
```

A lock rather than `@Execution(SAME_THREAD)` on the class, which is what the two other classes with
this shape reach for: those capture in most of their methods, this one in three of many, and
serialising the rest buys nothing. The class already had the same pattern for a different global
(`ENVIRONMENT_DUMP_LOCK`), so this is the established local idiom rather than a new one.

## Evidence

Control arm - `origin/master`, unmodified, `AmbientProbeExtensionTest` alone: **red 4 of 4**, 1 to 3
failures each. Treatment - the same commit with only those three annotations added: **green 6 of
6**. Same box, same command, one term changed.

**Sabotaged before being trusted**, per `docs/testing-at-write-time.md`: with the fix in place,
removing `reportDeadlineHeadroom(context, "PASSED")` from `AmbientProbeExtension.testSuccessful`
turned the run red with exactly one failure, `headroomIsReportedOnAPassingTestToo`. The lock
isolates the tests without blunting them.

## The class, swept

**Two test methods that capture the same logger concurrently.** Every `LogCapture.of(` call site was
grouped by (test class, captured logger) and checked. Only `AmbientProbeExtensionTest` was exposed;
the rest are dismissed for stated reasons:

- `ConsumerOffsetCommitterAsyncFailureLoggingTest` - three sites on `ConsumerOffsetCommitter`, but
  the class carries `@Execution(ExecutionMode.SAME_THREAD)`.
- `LoadFactorCeilingReportingTest` - two sites on `AbstractParallelEoSStreamProcessor`, and
  `@Execution(SAME_THREAD)`; its own comment already records that `@Isolated` alone would not have
  been enough.
- `RevokeOnTheControlThreadTest`, `UserFunctionFailureLoggingTest`, `RemovedPartitionStateTest`, and
  `AmbientProbeExtensionTest`'s single `ProgressProbe` capture - **one capturing method each**, so
  no sibling exists to collide with. Cross-class collision is excluded separately by
  `mode.classes.default` being `same_thread`.

## Prevention

When a test captures a logger and asserts on the exact set of lines it saw, ask what else in the
same class captures the same logger. If anything does, they need a shared `@ResourceLock` (or the
class needs `@Execution(SAME_THREAD)`) - the capture is global and the assertion is not.
