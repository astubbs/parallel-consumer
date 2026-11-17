---
title: "The only producer of commit responses died, so every waiter reported a timeout that described nothing"
date: 2026-09-18
category: logic-errors
module: parallel-consumer-core/internal
problem_type: logic_error
component: service_object
severity: high
symptoms:
  - "InternalRuntimeException: Timeout waiting for commit response PT30S to request ConsumerOffsetCommitter.CommitRequest(...) - in production, with no rebalance or broker event near it"
  - "Processing stops for many minutes, not for the timeout: the reporter's last commit response was 17 minutes before the waiter that gave up"
  - "The duration in the message (PT30S) was not the budget that elapsed"
root_cause: logic_error
resolution_type: code_fix
related_components:
  - BrokerPollSystem
  - ConsumerOffsetCommitter
  - PollThreadStallDiagnosis
tags:
  - commit-response-timeout
  - poll-thread-death
  - upstream-833
  - upstream-809
  - issue-177
  - issue-175
---

# The only producer of commit responses died, so every waiter reported a timeout that described nothing

Retired from `docs/inflight/upstream-tell-809-833-the-hang-is-fixed.md` on 2026-09-18, once its three
tellings were done. The note's last full version is `git show 821c6f36b:docs/inflight/upstream-tell-809-833-the-hang-is-fixed.md`.
The upstream reports are confluentinc#833 (mirror astubbs#177, closed as fixed) and confluentinc#809
(mirror astubbs#175, still open; the soak that re-ran its shape on v0.6.0.0 is astubbs#518 and
`docs/inflight/bug-177-commit-response-timeout-unreproduced.md` carries the findings).

## Problem

In the consumer-commit modes, `ConsumerOffsetCommitter` runs a request/response seam between the
control thread and the broker-poll thread: the control thread enqueues a commit request and waits on
`commitResponseQueue`; the broker-poll thread is the **only** producer of responses. If that thread
dies, every later waiter blocks for the full timeout and then throws "Timeout waiting for commit
response", a message whose cause looks nothing like it. `ConsumerOffsetCommitter`'s own javadoc,
added with astubbs#100, names exactly this.

confluentinc#833's forensics fit the mechanism precisely: the last commit response added at 21:16:54,
a waiter still blocked at 21:33:39, `pc_processed_records_total` flat across the window. A 30-second
timeout expiring does not explain a 17-minute gap; the only producer of responses being dead does.

## What Didn't Work

**Fixing the exceptions that kill the thread one at a time.** astubbs#100 caught a mid-rebalance
`RebalanceInProgressException` that had killed the broker-poll thread permanently, and astubbs#108
removed a second route. The note that became this write-up first read both reports as "plausibly
already fixed by astubbs#100". That inference was wrong: broker-down, offset-encoding and
authorization failures still produced the same symptom, and, as the fix commit says in its own words,
fixing exceptions one at a time was never going to close the class.

**Waiting.** A waiter cannot learn of the producer's death by waiting; waiting is precisely what does
not work.

## Solution

The four `astubbs#177` commits of 2026-08-19 close the class by publishing the death rather than
preventing it:

- `fe45fddfa` - `BrokerPollSystem`'s exceptional exit calls `ConsumerOffsetCommitter#notifyPollerDied`
  before unwinding. A waiter is released at that moment with the poller's own exception as the cause,
  and later commits fail fast on the recorded death instead of each waiting out the timeout.
- `c31829fe6` - `commitSync`'s budgets: four defects, including a retry budget captured inside the
  retry loop so every attempt reset it (measured at 51 attempts against a 500ms budget, PC neither
  committing nor failing).
- `e9fc445ac` and `2b5d0ec86` carry the test and a sighting.
- Later, `d9a9a4408` added `PollThreadStallDiagnosis`: when the timeout does fire, it reads the thread
  at that moment and states DEADLOCK / BLOCKED / WAITING / RUNNABLE / INCONCLUSIVE, so a recurrence
  arrives with its own diagnosis attached.

Shipped in v0.6.0.0.

## The message lied about its own budget

`commitAndWait` waited on `commitTimeout` (`offsetCommitTimeout`) while interpolating the unrelated
`AbstractParallelEoSStreamProcessor.DEFAULT_TIMEOUT` (PT30S) into the message, so every occurrence
reported a duration that was not the one that elapsed. Both reporters quote `PT30S` and reason from
it, overstating the shipped default threefold. Fixed in `fe45fddfa`; the throw site carries a comment
(grep `report the timeout actually waited` in `ConsumerOffsetCommitter`). First noted in
`docs/plans/2026-08-01-001-investigate-chaos-w4-red-report.md` and not acted on until then.

## The `dirty` asymmetry, for anyone reproducing this

Only a success marks a partition dirty, so a workload where nothing succeeds attempts no commits at
all, and a green "no timeout" run cannot tell "no timeout occurred" from "no commit was attempted".
confluentinc#833's 50%-failure workload touches this; it is why the soak in
`CommitResponseTimeoutSoakIT` has to prove the commit path was alive before its verdict means
anything. Settled for confluentinc#833; for confluentinc#809 the close path was read separately
(`docs/inflight/upstream-175-sporadic-commit-timeouts.md`), since it is a different entry point.

## Prevention

A role held by one thread needs a way for that thread to say it has stopped; the waiter must be
told, never left to infer it from silence. The same shape, on the close path, is
[`a-duty-assigned-by-role-is-unassigned-when-the-role-holder-dies-2026-09-08.md`](a-duty-assigned-by-role-is-unassigned-when-the-role-holder-dies-2026-09-08.md).

## Who was told

- confluentinc#833: https://github.com/confluentinc/parallel-consumer/issues/833#issuecomment-5723466465
- confluentinc#809: https://github.com/confluentinc/parallel-consumer/issues/809#issuecomment-5723607583
- astubbs#44's summary corrected: astubbs#100 removed one route; astubbs#204 closed the class.
