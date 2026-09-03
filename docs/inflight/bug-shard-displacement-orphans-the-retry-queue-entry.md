# `addWorkContainer`'s displacement branch orphans the displaced container's retry-queue entry

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->

**Found by the defect-class sweep on the re-queue orphan window**, which is fixed and written up in
[`docs/solutions/runtime-errors/retry-queue-orphan-window-between-the-requeue-check-and-the-add.md`](../solutions/runtime-errors/retry-queue-orphan-window-between-the-requeue-check-and-the-add.md).
Same class - **a container leaves a shard without its paired retry-queue removal** - different site,
and not fixed by that work.

## The gap

`ProcessingShard.addWorkContainer` replaces a resident it has decided is stale. On the branch where
`workMap.put` returns a displaced container it retires it from the population and gives back its
selection claim - anchor `A real replacement after all` - but it does not remove it from the retry
queue, and **it cannot**: `ProcessingShard` holds no reference to the `RetryQueue`. The queue is
passed in as a parameter to `getWorkIfAvailable` and nowhere else.

So if the displaced container had previously failed and was parked for retry, its queue entry is left
behind with the container resident in no shard - the same queue-only orphan, with the same
consequence: nothing can ever remove it, and once its retry delay elapses
`isRecordsAwaitingProcessing()` reads true forever, holding a draining close open to its timeout.

## Evidence, and what it does not cover

**The pairing gap is demonstrated, not merely read.** A scratch probe planted a stale resident that
was also in the retry queue, drove a fresh record through `ShardManager.addWorkContainer` at the same
offset, and asserted the queue afterwards: the displacement happened and the queue entry survived it.
The probe was deleted after the run rather than kept - it asserts a defect rather than a contract, so
it belongs with the fix, not before it.

**Production reachability is NOT established.** The probe plants its stale resident white-box. For
this to bite in production a *failed, retry-parked* container must still be resident in its shard,
already stale, when a fresh record arrives at the same offset - i.e. the revoke sweep and both stale
sweeps must all have missed it in the interval. That is the open question and it decides the urgency:
demonstrate it end to end before deciding this is worth the fix, because the answer may be that
nothing can reach the branch with a queue-resident container.

## What a fix has to answer

The shard cannot remove from a queue it has no handle on, so the fix is a design choice, not a line:

- give `ProcessingShard` the `RetryQueue` it is currently handed per-call - the smallest change, and
  the one that makes the pairing enforceable in the class that owns every departure (`retire`);
- or return the displaced container to `ShardManager.addWorkContainer` and pair the removal there,
  which keeps the shard ignorant of the queue but adds a second site that has to remember;
- or accept it and prove it unreachable, recording the discriminator on `addWorkContainer`.

Whichever is chosen, the same ordering caution applies as at `ShardManager.onFailure`: a residency or
membership test *before* the mutation is a check-then-act. See the solutions write-up for the
add-then-confirm shape that closes rather than narrows.

**`ProcessingShard.retire`'s javadoc is where the pairing invariant is stated**, and it currently
covers only the population and the selection claim - if the queue becomes the shard's business, that
javadoc is the place the third half goes.
