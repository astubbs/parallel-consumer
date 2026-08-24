# Collections shared across the poll/control boundary, unsynchronised - fixed only on a branch

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->


**Live on master.** `PCMetrics` keeps `private List<Meter.Id> registeredMeters = new ArrayList<>()`,
appended from every registration path and, in `close()`, walked with
`this.registeredMeters.forEach(this.meterRegistry::remove)` and then cleared. Registration happens
per partition inside a rebalance callback, so a walk racing an add throws
`ConcurrentModificationException`. `RemovedPartitionState` is the same class of mistake at wider
scope: `private static final SortedSet<Long> READ_ONLY_EMPTY_SET = new TreeSet<>()` is a mutable set
shared by every PC instance in the JVM.

**Why it is filed as a stall rather than a lost metric.** The exception propagates into `doClose`'s
`finally` and skips the `state = CLOSED` transition on the next line - the transition that block
exists to guarantee. The consumer is then stuck short of closed and the group waits out its session
timeout instead of getting a prompt departure. A cleanup failure surfacing as a stall in an
unrelated subsystem is the kind nobody traces back.

**A fix exists and is not proposed anywhere.** Branch `fix/concurrent-collection-sweep` sweeps six
such sites as a follow-up to astubbs#267, with a reproduction and a deterministic regression test,
and then fixes the leak the first fix created - walk-then-clear silently discarded meters registered
*during* the walk, so `close()` now drains. Read `git log master..fix/concurrent-collection-sweep`;
the bodies carry the whole diagnosis. **It has no PR**, which is the part no command will tell you.

**The undecided part is sequencing, not the fix.** It touches partition-state files astubbs#57 also
changes (`PartitionState.java`, `PartitionStateManager.java`, `ShardManager.java` - `gh pr diff 57`
is the current list), and astubbs#57 already has a merge position in the ordering
recorded in [`pr-323-docs-outstanding.md`](pr-323-docs-outstanding.md). Whoever opens the PR decides
whether it goes before or after, or folds into astubbs#267.

## Delete when

The sweep merges, or the defect is fixed some other way.
