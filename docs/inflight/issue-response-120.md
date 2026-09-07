# Draft response to astubbs#120 - posted by the pre-release sweep, not by this PR

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- post-merge: exempt-file - a drafted issue reply, held until the pre-release sweep posts it.
     It deliberately outlives the PR that wrote it, so it cannot be written in post-merge terms. -->

Written while the context is here, per `docs/inflight/AGENTS.md` - the agents who did the work hold
the best context at merge time, and by release time it has to be re-mined from commit logs.

**Not posted, and it survives this PR.** It is deleted when it is posted and not before; the sweep in
[`docs/releasing.md`](../releasing.md), "Post the drafted issue responses before you freeze the
section", is what consumes it, so all of these go out together with one view of what shipped.

---

Fixed in astubbs/parallel-consumer#57.

`registeredMeters` is now a `LinkedHashSet`, pruned on removal - your own suggested fix - and
`PartitionStateManager` caches its `OffsetMapCodecManager` rather than creating a throwaway per
partition assignment, which removed the second registration site. Regression coverage is
`PCMetrics859Test`. With confluentinc#892 already merged upstream for the commit path, the
unbounded-growth report should be fully closed out.

**Two things came out of it that the report did not ask for, and both are worth knowing if you are
running this in production.**

**The same field had a second defect: it was written from two threads with no lock.** A Lincheck
harness aimed at something else reproduced `ArrayIndexOutOfBoundsException` out of `ArrayList.add`,
from two concurrent `createOffsetAndMetadata` calls - the classic torn-grow signature, where both
threads read the same size, one grows the backing array and the other writes past the array it had
already loaded. **The exception is the lucky outcome**; the ordinary one is a silently dropped or
duplicated `Meter.Id`, which then makes deregistration miss a meter on partition revocation. So the
collection change and the `metersLock` that now guards every mutation of it are one fix rather than
two: the leak and the race shared a field. If you saw meters surviving a rebalance that should have
gone, that is the likely reason.

**Metrics teardown could take down consumption.** Meter removal runs on the path that closes the
instance, and an exception from the registry there propagated into the caller - which is the poll
thread. A metrics backend having a bad day could fail a shutdown or stop consumption outright.
Teardown never throws now.

Independent corroboration that the leak half is genuinely closed, from a checker nobody pointed at
it: the repository's Infer ratchet records known findings by identity, and four
`THREAD_SAFETY_VIOLATION`s on the `PCMetrics` registration methods stopped firing on this change and
were retired from the set.
