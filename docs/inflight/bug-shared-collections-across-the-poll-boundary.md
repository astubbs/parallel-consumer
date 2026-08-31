# Collections shared across the poll/control boundary, unsynchronised - fixed only on a branch

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->


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

<!-- post-merge: checked-begin -->
**A fix exists and is not proposed anywhere.** Branch `fix/concurrent-collection-sweep` sweeps six
such sites as a follow-up to astubbs#267, with a reproduction and a deterministic regression test,
and then fixes the leak the first fix created - walk-then-clear silently discarded meters registered
*during* the walk, so `close()` now drains. Read `git log master..fix/concurrent-collection-sweep`;
the bodies carry the whole diagnosis. **It has no PR**, which is the part no command will tell you.
<!-- post-merge: checked-end -->

<!-- post-merge: checked-begin -->
**The undecided part is sequencing, not the fix.** It touches files astubbs#57 also changes -
`PCMetrics.java` (where astubbs#57 converted `registeredMeters` to a `LinkedHashSet` under
`metersLock`, so that branch's hunk there is likely redundant rather than merely conflicting),
plus `PartitionStateManager.java` and `ShardManager.java`. `gh pr diff 57` is the definitive list.
`PartitionState.java` moved out of astubbs#57 with the confluentinc#893 carry on 2026-08-24, so that
file collides with astubbs#337 (`fix/121-offset-accuracy-on-assignment`) instead. The merge ordering
that placed astubbs#57 is in
[`pr-blockers-and-collisions.md`](pr-blockers-and-collisions.md), which took it over from
astubbs#323's own note when that PR merged. Whoever opens the PR decides
whether it goes before or after astubbs#57. It builds on astubbs#267's guards, so it cannot land
ahead of that one.
<!-- post-merge: checked-end -->

## Delete when

The sweep merges, or the defect is fixed some other way.
