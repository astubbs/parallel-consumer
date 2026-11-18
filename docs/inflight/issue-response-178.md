# Draft response to astubbs#178 (confluentinc#843) - the rebalance-gated route is fixed

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->

**Not posted.** Post only on explicit instruction, and delete this file when it is posted - never when
the PR that wrote it lands. The issue is OPEN, `wait for info`, and its thread already establishes two
things this draft must not repeat: that the reported no-rebalance race is not reachable by inspection,
and that the wait-for-info route is permanently closed (the report is against an unmaintained
repository and the richer logs will not arrive), with the no-disturbance chaos scenario tracked in
`test-no-disturbance-duplicate-scenario.md`.

What the thread does NOT yet say is that the one route in the engine that produces the reported
symptom - one key on two threads in one instance - has now been ruled a defect and fixed. The
mechanism and the evidence are in
[`a-revoke-sweep-freed-a-key-whose-worker-was-still-running-2026-09-18.md`](../solutions/logic-errors/a-revoke-sweep-freed-a-key-whose-worker-was-still-running-2026-09-18.md);
the draft below is the user-facing half of that.

---

## Draft

An update, because one thing has changed since the comments above: there IS a route to what you
described, and it is fixed.

**The route needs a rebalance, which you said you did not have** - so this may not be your case, and
the earlier reasoning about your exact scenario still stands. But it is the only mechanism in the
engine that runs one key on two threads inside one instance, and it went like this:

1. A worker is inside your function for a record of key K.
2. The partition is revoked and immediately handed back to the same instance (any group membership
   change under the eager assignor does this to every partition).
3. Parallel Consumer does not interrupt a running worker on revoke - deliberately, because waiting
   inside the rebalance callback is what causes the poll-interval deadlocks in confluentinc#857. But
   the revoke's cleanup removed the running record from its ordering queue on the grounds that it
   belonged to a superseded assignment, without asking whether it was still executing.
4. The offset was never committed, so it is re-delivered. Its queue no longer holds the running
   record, so the re-delivery is handed to a second worker at once. Your function runs twice, for the
   same key, concurrently - about a rebalance's width apart, which is compatible with the 70ms gap in
   your log.

Parallel Consumer's own bookkeeping never noticed, because the first worker's result is discarded as
belonging to the old assignment. Only your counter could see it.

**The fix** makes the re-delivered record wait for the running one to finish before it is handed out,
exactly as it would have waited had there been no rebalance - so the ordering promise holds across a
revoke-and-reassign, not only between them. The chaos suite's per-key ledger has been widened to
detect the same shape under real churn.

If your logs ever show the two pickups with a consumer-group rebalance in the minute before them, this
was it. If they show none, the no-disturbance scenario mentioned above remains the way to chase it.
