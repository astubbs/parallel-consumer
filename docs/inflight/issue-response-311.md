# Draft response to astubbs#311

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->

Not posted. Post only on explicit instruction, and delete this file when it is posted - not when
its PR merges. It covers the **validation half only**; the issue stays open for the arithmetic half.

---

The second defect in this issue - `batchSize` being unvalidated - is fixed on master.

`ParallelConsumerOptions.validate()` now rejects a batch size below one, and null, with an
`IllegalArgumentException` naming the option, the value and the bound. That closes all three shapes
the issue describes: the silent one (a default configuration that starts cleanly and processes
nothing forever), the `ArithmeticException` at construction when `messageBufferSize` is set, and the
NPE on null. The bound runs inside `validateConfiguration()`, which the constructor calls before the
load factor is initialised, so it pre-empts the division rather than sitting beside it.

This is a user-visible behaviour change: a caller passing `batchSize(0)` used to get a process that
started and did nothing, and now gets an exception at construction. Nothing changes for one or more.

**The first and larger defect in this issue - `calculateQuantityToRequest` using `target - modulo`
where it means `batchSize - modulo`, so a batching configuration settles at roughly twice its
configured in-flight target - is still open.** It is throughput-only with no data-safety
consequence, which is why it is scheduled after v6 rather than with the validation bound.
