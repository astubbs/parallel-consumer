# `addWorkContainer`'s displacement branch orphans the displaced container's retry-queue entry

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-labels: concurrency -->
<!-- inflight-vetted: 2026-09-08 - PROPOSED closed - unreachable: the open question below is now settled and the proof is `docs/solutions/logic-errors/the-shard-displacement-orphan-is-unreachable-and-the-guard-is-outside-the-class-2026-09-08.md`, with `ShardDisplacementOrphanReachabilityTest` (3 arms, ablation matrix run). Checked: `RetryQueue.add`'s one production caller; `couldBeTakenAsWork`s stale refusal; the three staleness transitions and which carry a sweep; no `seek` in main; `ShardKey.KeyOrderedKey`s partition scoping. Impact is `misdirection`, so the state change is the owner's -->

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

**Production reachability IS now established, and the answer is UNREACHABLE** (2026-09-08). The
proof, its control arms and its ablation matrix are in
[`docs/solutions/logic-errors/the-shard-displacement-orphan-is-unreachable-and-the-guard-is-outside-the-class-2026-09-08.md`](../solutions/logic-errors/the-shard-displacement-orphan-is-unreachable-and-the-guard-is-outside-the-class-2026-09-08.md),
which **owns this question**; `ShardDisplacementOrphanReachabilityTest` is the durable form. In one
line: a container can only enter the retry queue while it is *not* stale (`couldBeTakenAsWork`
refuses a stale one, so it can never be selected, fail, or be re-queued), and the only transitions
that then make it stale either carry their own paired sweep on the same thread inside the rebalance
callback, or - the fence - cannot be followed by a second container at the same offset, because a
duplicate offset needs a re-assignment and that is the swept path.

**The last leg of that argument is not in this engine**, which is why the write-up exists rather than
a one-line dismissal: it rests on the consumer's fetch position never going backwards within an
assignment generation. Any in-generation replay of an already-registered offset - a `seek`, an
offset-reset or truncation replay - makes the displacement branch orphan an entry immediately, and
nothing goes red for it.

## What a fix has to answer

The shard cannot remove from a queue it has no handle on, so the fix is a design choice, not a line:

- give `ProcessingShard` the `RetryQueue` it is currently handed per-call - the smallest change, and
  the one that makes the pairing enforceable in the class that owns every departure (`retire`);
- or return the displaced container to `ShardManager.addWorkContainer` and pair the removal there,
  which keeps the shard ignorant of the queue but adds a second site that has to remember;
- or accept it and prove it unreachable, recording the discriminator on `addWorkContainer` -
  **this is the option taken, 2026-09-08**: the discriminator is on the branch, the proof is in
  `docs/solutions/`, and the first two options are left here because they are what a *fix* would
  cost if the last leg of that proof ever stops holding.

Whichever is chosen, the same ordering caution applies as at `ShardManager.onFailure`: a residency or
membership test *before* the mutation is a check-then-act. See the solutions write-up for the
add-then-confirm shape that closes rather than narrows.

**`ProcessingShard.retire`'s javadoc is where the pairing invariant is stated**, and it currently
covers only the population and the selection claim - if the queue becomes the shard's business, that
javadoc is the place the third half goes.
