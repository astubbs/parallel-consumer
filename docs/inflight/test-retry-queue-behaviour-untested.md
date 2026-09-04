# `RetryQueue`: the behaviour still untested

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->


The class carries a uniqueness/ordering invariant across **two** collections - `unique`, keyed by
topic/partition/offset, and `sorted`, ordered by retry-due time - maintained only by `add`, `remove`,
`tryRemove` and `removeAll`. It sits on the retry path, so a defect here shows up as *records retried
late or never* rather than as an exception, and the counters it feeds gate the broker poller. A wrong
answer is quiet, which is why the gaps are worth naming rather than left to a code reader.

Two are left:

- **`add` is last-write-wins with a CHANGED retry-due time.** It does `unique.put`, then
  `sorted.remove` and `sorted.put`; nothing asserts what happens when the same offset is added twice
  with *different* due times - which is the normal shape after a retry is scheduled. The
  highest-value gap left.
- **The two-map invariant on the paths `RetryQueueLincheckTest` does not reach** - `clear` in
  particular, which has no production caller at all and so is exercised by nothing.

Where the rest went, so nobody re-derives it here: shard/queue consistency under contention and the
whole revoke path are
[`../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md`](../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md),
`ShardManager.removeWorkFromShardFor`'s javadoc owns how long an abandoned pair waits before it is
retired, and `RetryQueueTest`, `RetryQueueRebalancePathTest` and `RetryQueueLincheckTest` are the
coverage that closed them.

Surfaced while reviewing astubbs/parallel-consumer#31.
