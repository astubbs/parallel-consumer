# Collections shared across the poll/control boundary, unsynchronised

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-state: closed - both named defects are fixed on master: `PCMetrics.registeredMeters` in 758744b30 (astubbs#120 / astubbs#57) and `RemovedPartitionState.READ_ONLY_EMPTY_SET` in 6aae29989 (astubbs#267). Kept because three notes cite it -->
<!-- inflight-vetted: 2026-09-08 - applied: closed on the owner's ruling, headline corrected from "Live on master", and the branch section reduced to what is still true of it; checked: `PCMetrics.registeredMeters` is a final `LinkedHashSet` with every add, walk and remove under `@Synchronized("metersLock")` and `close()` walking it via the never-throwing `removeQuietly`, and `RemovedPartitionState.READ_ONLY_EMPTY_SET` is `Collections.emptySortedSet()`; `fix/concurrent-collection-sweep` still exists on origin with no PR -->
<!-- inflight-labels: concurrency -->


**Both defects are fixed on master; this note is kept only because other notes cite it.** It
recorded the pair below, and the mechanism is still worth reading - a cleanup failure surfacing as a
stall in an unrelated subsystem is the kind nobody traces back.

`PCMetrics` kept `private List<Meter.Id> registeredMeters = new ArrayList<>()`, appended from every
registration path and, in `close()`, walked with
`this.registeredMeters.forEach(this.meterRegistry::remove)` and then cleared. Registration happens
per partition inside a rebalance callback, so a walk racing an add threw
`ConcurrentModificationException`. `RemovedPartitionState` was the same class of mistake at wider
scope: `private static final SortedSet<Long> READ_ONLY_EMPTY_SET = new TreeSet<>()` is a mutable set
shared by every PC instance in the JVM.

**Why it was filed as a stall rather than a lost metric.** The exception propagated into `doClose`'s
`finally` and skipped the `state = CLOSED` transition on the next line - the transition that block
exists to guarantee. The consumer was then stuck short of closed and the group waited out its
session timeout instead of getting a prompt departure.

## What is left of the branch

Branch `fix/concurrent-collection-sweep` swept six such sites as a follow-up to astubbs#267, with a
reproduction and a deterministic regression test. **It has no PR**, which is the part no command will
tell you, and it is far enough behind master that its remaining sites want re-deriving against HEAD
rather than merging - the two above are already fixed there by other means. If that re-derivation is
worth doing it is its own `branch-` note; nothing here tracks it.
