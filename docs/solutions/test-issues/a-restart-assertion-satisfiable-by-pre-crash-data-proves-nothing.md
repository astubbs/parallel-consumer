---
title: "A restart assertion satisfiable by pre-crash data proves nothing"
date: 2026-08-10
category: test-issues
module: parallel-consumer-streams
problem_type: best_practice
component: testing_framework
severity: high
applies_when:
  - Writing a crash, kill, restart, failover or rebalance test that asserts on what the second run did
  - Asserting on records, rows or files that an earlier phase of the same test already produced durably
  - Testing any transition where the before-state and the after-state are the same shape of data
  - Reviewing a test that reads a topic, table or directory that both phases write to
  - Deciding whether a red-then-green development history is enough evidence that a test can fail
symptoms:
  - Test is green whether or not the mechanism under test is present
  - The assertion text names the transition but the reader does nothing to exclude the pre-transition state
  - "A phase-2 reader consumes from the earliest offset with a fresh consumer group"
  - Deleting the second phase entirely would leave the test passing
tags:
  - vacuous-assertion
  - crash-restart
  - integration-tests
  - kafka
  - test-design
  - red-then-green
related_components:
  - development_workflow
  - documentation
---

# A restart assertion satisfiable by pre-crash data proves nothing

## Context

In astubbs/parallel-consumer#271 (issue astubbs#255), two integration tests in
`parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/CommitFrontierCrashRestartTest.java`
existed to prove requirement R10: kill an instance mid-run, restart it, and the record that was in
flight at the crash is redelivered and reprocessed. That is the whole point of committing the
frontier rather than the consumer position, and it is the property the plan's exit criteria demand a
kill-restart test demonstrate
(`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:788` for U9, `:1439` for the criterion).

Both tests drained the output topic after the restart with a fresh consumer group reading from the
earliest offset. `KafkaClientUtils` defaults its consumers to
`OffsetResetStrategy.EARLIEST`
(`parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/utils/KafkaClientUtils.java:87`),
so a new group subscribing to the output topic re-reads the entire topic from offset zero. The
records the FIRST phase had already written durably were sitting right there. The assertion
`outputs.contains(BLOCKER_VALUE)` was satisfied by phase-1 output.

The restart could have been a complete no-op and both tests would still have been green. An
independent reviewer found this; the author, who had written the tests to prove exactly this
property, did not.

## Guidance

**Name the class first: a test that asserts on a state BOTH the pass path and the fail path can
produce is vacuous.** It does not matter how strongly worded the assertion is or how specific the
value it checks. If the failing implementation also reaches that state, the assertion is a
tautology dressed as a proof.

**The tell is structural, and you can spot it without understanding the system.** Read the
assertion and ask: does anything in the reader exclude the "before" state? Not the assertion, the
*reader*. In a before/after test the reader is the thing that decides which data is even eligible
to satisfy the assertion. If the answer is "the assertion happens to look at the right thing", the
test is vacuous, because "happens to" is a property of today's code, not of the test.

**Fix by making the wrong answer unreachable, not by strengthening the assertion.** The tempting
repair is to assert harder: count the records, check they are duplicates, compare timestamps. All
of that leaves a reader that can still see pre-crash data, so the test's correctness now depends on
the assertion staying careful. The next person who edits it will not know that, and the test
silently returns to vacuous. Instead:

1. **Capture the boundary.** At the moment of the crash, record the position of the output: the end
   offset, the max row id, the file count, the log cursor.
2. **Scope the reader to that boundary.** Read only what appears after it. Do not subscribe and
   filter; assign and seek, so the reader is *structurally incapable* of returning the earlier
   state.
3. **Then assert whatever you like.** With the reader scoped, even a weak assertion is real
   evidence, because the only records that can reach it are ones the phase under test produced.

**Red-then-green during development is necessary and not sufficient.** A vacuous test still goes
red before the feature exists, which is what makes it feel proven. It went red for a *different*
reason than the one claimed, most often because the first phase did not run either. The extra check
is a control arm on the test itself: break ONLY the mechanism under test, leave everything else
working, and confirm the test still fails. If phase 1 still produces its output and the test stays
green, the test is not testing phase 2.

**This class is not about crashes.** It covers any test of a transition where the before-state and
the after-state are the same shape of data:

- a cache-invalidation test asserting on a value the stale cache also returns
- a migration test asserting on rows the old schema also produced
- a retry test asserting on a result the first attempt also gives
- a failover test asserting on a response the original node could still have served
- a rebalance test asserting on progress the pre-rebalance assignment already made

In every case the same question applies: what did the reader do to exclude the before-state?

## Why This Matters

The failure mode is worse than an untested feature, because it is an untested feature that reports
itself as tested. The test's name, its Javadoc, the requirement it is linked to and the plan entry
that cites it all say the crash-safety property is proven. A future change that reopens the
data-loss window will not turn this test red. Nobody will look again, because the coverage is
already there.

The specific property at stake here is silent data loss: a commit that covers a record still in
flight means a restart never redelivers it, and there is no exception, no warning and no gap in the
log to say it happened. A vacuous test for that property is a false negative on the one signal that
was supposed to catch it.

The cost of the fix is one metadata call between phases. The cost of not fixing it is a permanent
green light on a property nobody is checking.

## When to Apply

- Whenever a test has phases and the later phase writes to the same place as the earlier one.
- Whenever a test reads a Kafka topic, a table, a directory or a log that persists across the
  boundary event it is testing.
- Whenever an assertion mentions a transition ("after the restart", "once invalidated", "on the
  retry") and the reader it asserts over does not.
- Before merging any test whose only evidence of validity is that it was red before the feature
  landed.
- During review of someone else's before/after test, where this defect is far easier to see than in
  your own, since you do not know what the test was *meant* to do.

## Examples

### The defect: an earliest-reading drain in a crash-restart proof

Both restart phases drained the output topic with a fresh group and no seek. Because
`KafkaClientUtils` sets `auto.offset.reset=earliest` (`KafkaClientUtils.java:87`), the drain re-read
phase 1's durable output from offset 0 and the `contains(BLOCKER_VALUE)` assertion was satisfied
before the restarted instance produced anything at all.

The still-legitimate use of that same pattern survives in the class: `awaitOutputs`
(`CommitFrontierCrashRestartTest.java:353-364`) subscribes from earliest on purpose, because it is
waiting for phase 1's own output during phase 1. The pattern is not wrong; using it across a
boundary is.

### The fix: capture the boundary, then seek past it

Two helpers, each carrying its own reasoning so the next editor cannot remove them by accident.

`outputEndOffset` (`CommitFrontierCrashRestartTest.java:262-269`) captures the output position via
the shared AdminClient, with the Javadoc at `:256-261` stating the hazard outright:

```
The output topic's current end offset - captured between phases so the next phase's reader can be
scoped to records produced AFTER this point. Without it, an earliest-reading consumer re-reads the
previous phase's durable outputs and the restart assertions pass on evidence the restart never
produced (U9 review findings on this class - the vacuous-restart-assert defect).
```

`drainFrom` (`CommitFrontierCrashRestartTest.java:275-292`) is the reader that cannot see the past,
and its Javadoc at `:271-274` says why the mechanism is `assign` plus `seek` rather than a filter:

```
Reads only records at or after fromOffset - assign+seek, never subscribe-from-earliest, so
the caller's assertions can only be satisfied by records the phase under test itself produced.
```

The implementation is two lines of intent:

```java
consumer.assign(UniLists.of(outputPartition));
consumer.seek(outputPartition, fromOffset);
```

Note what is NOT there: no filtering of already-seen values, no de-duplication, no comparison
against a phase-1 snapshot. The consumer is never positioned anywhere it could read old data from.

### The call sites: the boundary is captured at the crash, not later

In `killRestartLosesNothing`, the end offset is taken *before* the restart begins
(`CommitFrontierCrashRestartTest.java:178`), then handed to the scoped drain (`:182`), with the
comment at `:173-176` recording that every assertion below is about what the restart did. In
`stockRestartOnPcCommittedGroupDegradesGracefully` the same pair brackets the phase boundary
(`:232` and `:245`), so the `contains(BLOCKER_VALUE)` there can only be satisfied by records stock
Kafka Streams produced after taking the group over.

Ordering matters: capture the position at the crash instant. Capturing it after the restart has
started admits a race where the restart's own early output lands below the boundary and is
excluded, which turns a vacuous test into a flaky one.

### The check that would have caught it earlier

Ask of the original test: if `startParkableTopology` were simply never called for run 2, would the
test still pass? It would. `drainFrom` makes that question impossible to answer wrongly, because
with the second run absent there are no records at or after `preRestartEnd` and the drain times
out.

That is the general form of the control arm: delete or disable ONLY the mechanism under test and
confirm red. It is a different question from "was it red before the feature landed", and it is the
one that distinguishes a proof from a coincidence.

## Related

- [Fresh work needs an independent reviewer, and the tail is what momentum skips](../best-practices/fresh-work-needs-independent-review.md) -
  this defect was one of four findings from that review pass, and the author had already re-read
  their own diff. Section "The vacuous assertions: passing on pre-crash data" covers the same fix
  from the review-process angle.
- [A control arm must vary exactly one term](../best-practices/control-arms-vary-exactly-one-term.md) -
  the discipline behind "break only the mechanism under test and confirm the test still fails".
- [Await conditions that are vacuously true before the system reaches its initial state](../test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md) -
  a sibling in the same family with a different mechanism. There, an emptiness condition is
  satisfied *before* the system starts; here, an assertion is satisfied by data from *before* the
  boundary. Both are green runs that measure nothing, and both are fixed by making the vacuous
  window unreachable rather than by tuning the check.
- [A high-water mark cannot express out-of-order completion](../architecture-patterns/a-high-water-mark-cannot-express-out-of-order-completion.md) -
  the property these tests exist to prove, including why the crash must be a real abort
  (`:214` records the scoped-reader requirement as one of three things that make it a proof).
- `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md` - U9 (`:788`) is the unit that introduced
  the test; exit criterion 9 (`:1439`) is the claim the test has to earn.
- `docs/plans/2026-08-08-002-ks-on-pc-spike-result.md:217` - section 5.2, "the obvious assertion is
  the vacuous one", an independent instance of the same trap found earlier in the same spike.
- astubbs/parallel-consumer#271, issue astubbs#255 - the PR and issue this was learned on.
