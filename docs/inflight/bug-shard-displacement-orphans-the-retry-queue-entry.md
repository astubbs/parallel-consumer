# `addWorkContainer`'s displacement branch orphans the displaced container's retry-queue entry

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
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
passed in as a parameter to `getWorkIfAvailable` and nowhere else - on
astubbs/parallel-consumer#431's branch it is also handed to `removeStaleWorkContainersFromShard`,
so that clause goes stale when astubbs#431 lands; astubbs#431 owns the rest of what that
changes.

So if the displaced container had previously failed and was parked for retry, its queue entry is left
behind with the container resident in no shard. That is the same pairing gap - but **NOT the same
consequence, and the first version of this note said it was.**

**The entry is not permanent.** `RetryQueue` keys by topic, partition and offset alone
(`WorkContainerKey.of`), never by container identity, and `ShardManager.onSuccess` removes by that
key **unconditionally**, before it touches any shard. The container that displaced the stale one
carries the same three coordinates, so the replacement's own first terminal event clears the entry:
success removes it; failure re-adds the same key, and `RetryQueue.add` replaces the existing entry
rather than duplicating it, so what is left is an ordinary retry entry; a revoke or stale sweep that
finds the replacement in the shard removes it by that key too. The window is bounded by the
replacement's lifecycle, not by the instance's, so nothing here holds a draining close open.

**What IS wrong inside that window is the figure.** The surviving entry carries the *displaced*
container's retry-due time, so the ready-to-retry count and `RetryQueue.getLowestRetryTime` read one
entry high until the replacement reaches that terminal event. A briefly wrong reading, not work that
can never leave - which is why the impact tag above is `misdirection` rather than `stall`.

This over-claim is the same shape as the one corrected at `WorkManager.onFailureResult` by the work
that found this gap: a bounded, self-clearing cost written up as a permanent one.

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

## Update 2026-09-08 - the entry is now collected, and the fix question narrows

`ShardManager.purgeDepartedRetryEntries()` collects every retry-queue entry whose container is
resident in no shard, once per control-loop pass, on the controller thread. **A displaced container
is resident in no shard from the moment its replacement takes its offset** - residency is reference
identity - so the surviving entry this note is about is now collected on the next pass rather than
waiting on the replacement's own terminal event.

That does not close the note, and the difference is worth keeping straight:

- **The FIGURE this note names as the actual harm is now bounded by one control-loop tick** rather
  than by the replacement's lifecycle. That is a strictly smaller window, on the same misdirection.
- **The pairing gap itself is unchanged.** `ProcessingShard.addWorkContainer` still cannot remove
  from a queue it holds no reference to, and the three design options below are still the options.
  What has changed is the cost of doing nothing, which was already "bounded misdirection" and is now
  a tick of it.
- **The clause about astubbs/parallel-consumer#431's branch is dead.** That PR is superseded, not
  merged; `removeStaleWorkContainersFromShard` never took the queue, and no rebalance-path code
  touches the queue at all now. Both designs:
  [`../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md`](../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md).

**Production reachability is still not established**, which is the open question this note names and
the purge does not answer.
