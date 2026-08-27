# PC runs user code where its own bookkeeping cannot survive a throw - what is still open

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->

**The defect class, not the PR.** It started as a `ConcurrentModificationException` from concurrent
listener registration and became a class: PC invokes user-supplied code - a rebalance listener, a
`retryDelayProvider`, an `onError`, and the logging binding itself when it renders a user throwable -
at points where its own bookkeeping must still complete. A throw there does not surface as an error;
it leaves a record neither in flight nor completed, which is a silent stall.

<!-- post-merge: checked-begin -->
Most of the class was closed by astubbs/parallel-consumer#267. This note is what that work did not
finish, and it outlives the PR.
<!-- post-merge: checked-end -->

## Assume the sweep is incomplete

**No review round on that PR came back clean, and more than one found the same class at a sibling
site the round before it had missed.** The reason is recorded in
[`../solutions/best-practices/a-guard-outlives-the-claim-that-motivated-it.md`](../solutions/best-practices/a-guard-outlives-the-claim-that-motivated-it.md):
sweeping by MODULE keeps finding the implementations that look alike. Reactor and Mutiny answer every
batch-shaped grep; vert.x handles one container per failure and falls out of it, which is why it was
the last engine fixed. Enumerate by the shape of the hazard - *user code running before bookkeeping
that must not be skipped* - not by the list of places you expect it.

## Still open

<!-- post-merge: checked-begin -->
- **`PCMetrics` is astubbs#57's**, deliberately not fixed in the astubbs#267 work. Its meter
  registration has the same cross-thread shape.
<!-- post-merge: checked-end -->
- **What recovers an un-mailboxed container** -
  [`core-unmailboxed-container-recovery.md`](core-unmailboxed-container-recovery.md) owns that
  question. It is narrower since the operator ruled that a failure to mailbox is terminal and PC now
  shuts down rather than continuing, but the question of what the *recovery* mechanism is, if any,
  is unanswered - and the answer decides whether the per-container guards are a latency fix or a
  stall fix.

## The asymmetry to preserve

Two adjacent catches in the same handler are treated differently on purpose, and a future tidy-up
will want to make them consistent:

- **`onUserFunctionFailure` throwing is logged and survivable.** What throws is USER code, and making
  a user callback able to stop the consumer is the whole defect class above.
- **`addToMailbox` throwing is TERMINAL.** That is PC's own bookkeeping, so a throw is our bug, and
  continuing means possibly committing past work that was never done. Operator ruling: a silent skip
  is not a state PC may keep running in.

Grep `failFatallyOnUnmailboxableRecord` for the escalation and why it signals rather than throwing or
blocking.
