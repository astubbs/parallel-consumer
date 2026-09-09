# `addWorkContainer`'s displacement branch orphans the displaced container's retry-queue entry

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-labels: concurrency -->
<!-- inflight-vetted: 2026-09-08 - PROPOSED closed - unreachable AND bounded: the two open halves are both answered, in the last two sections. astubbs#481s purge bounds the harm to one control-loop tick, and the reachability question that section leaves open is answered UNREACHABLE by `docs/solutions/logic-errors/the-shard-displacement-orphan-is-unreachable-and-the-guard-is-outside-the-class-2026-09-08.md` plus `ShardDisplacementOrphanReachabilityTest` (3 arms, ablation matrix). Checked against the post-astubbs#481 tree: `RetryQueue.add`s one production caller; `couldBeTakenAsWork`s stale refusal; the three staleness transitions, each shard removal being what the argument needs; no `seek` in main; `ShardKey.KeyOrderedKey`s partition scoping. Impact is `misdirection`, so the state change is the owner's -->

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
verdict, its argument and its evidence are stated **once**, in this note's last section -
<!-- post-merge: checked -->
`Answered, same day, by astubbs/parallel-consumer#483` - and not here, because it has to be read
against the purge introduced in the section before it. The scratch probe above is what it
replaces: a kept, control-armed test now stands where a deleted one-off did.

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
- **The clause about astubbs/parallel-consumer#431's branch is dead, and so is the vet marker's
  reading of it.** That PR is CLOSED as superseded, never merged; `removeStaleWorkContainersFromShard`
  never took the queue, and no rebalance-path code touches the queue at all now. The 2026-09-07 stamp
  this paragraph was written against said the clause "has not gone stale" *because* that PR was open -
  true on its date, and the reason it is corrected here rather than edited there. That stamp has since
  been superseded by the one at the top, which the tag gate allows only one of; this sentence is the
  only surviving record of what it said. Both designs:
  [`../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md`](../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md).

**Production reachability is still not established**, which is the open question this note names and
the purge does not answer.

<!-- post-merge: checked -->
### Answered, same day, by astubbs/parallel-consumer#483

**UNREACHABLE.** Proof, control arms and ablation matrix:
[`../solutions/logic-errors/the-shard-displacement-orphan-is-unreachable-and-the-guard-is-outside-the-class-2026-09-08.md`](../solutions/logic-errors/the-shard-displacement-orphan-is-unreachable-and-the-guard-is-outside-the-class-2026-09-08.md),
which **owns the question**; `ShardDisplacementOrphanReachabilityTest` is the durable form. The two
results are independent and both worth keeping: **the purge bounds the harm whatever happens, and
the proof says the case does not arise**, so the purge is a genuine backstop here rather than the
thing standing between the displacement branch in `addWorkContainer` and an orphan.

**The proof turns on RESIDENCE, not on the queue, so the purge does not move it.** A container
enters the retry queue only while it is not stale (`couldBeTakenAsWork` refuses a stale one, so it is
never selected, never fails and never re-queues) and, since astubbs/parallel-consumer#437, only
while it is resident. Only three transitions can then make it stale: the removed-state swap and the
`putAll` in `PartitionStateManager`, each immediately followed on the same thread by the sweep that
takes the container **out of its shard** - which is all that is needed, because a container the
sweep removed is not a resident and there is nothing left to displace - and `fenceForRevocation`,
which sweeps nothing but cannot be followed by a second container at the same offset, since a
duplicate offset needs a re-assignment and that is the swept path.

**What the two results say together about the fix question above.** The harm is now a tick of
misdirection in a case that cannot arise, so neither of the first two design options is worth paying
for today. What is worth knowing is the last leg: it rests on the consumer's fetch position never
going backwards within an assignment generation, which is a property of the Kafka consumer and not
of this engine. An in-generation replay of an already-registered offset - a `seek`, an offset-reset
or truncation replay - makes the displacement branch orphan an entry immediately, and nothing goes
red for it; the purge would then collect it a tick later, which is precisely the difference this
section makes.

## Update 2026-09-09 - the other half of astubbs#483's defect-class sweep is now fixed

**This note's own state is unchanged**, and the `PROPOSED closed` marker at the top stands as it
was: nothing below touches the displacement branch, its orphan, or the reachability argument. What
changed is the second item astubbs/parallel-consumer#483 reported alongside them and did not fix -
`ShardManager.removeWorkFromShardFor`, the revoke and lost path's `removeWorkAtOffset`, and the last
unconditional by-key shard removal in main.

It is now conditional on the container the revoked record was registered as
(`ProcessingShard.removeWorkForRevokedRecord`), which is the same shape
astubbs/parallel-consumer#468 gave the stale sweep, arrived at from the other side: the caller here
holds a `ConsumerRecord` and not a container, so what it names is the *registration*. The write-up
that owns the class carries the mechanism, the two-legged guard and why the middle option was wrong:
[`../solutions/logic-errors/a-by-key-removal-cannot-say-which-container-it-meant-2026-09-07.md`](../solutions/logic-errors/a-by-key-removal-cannot-say-which-container-it-meant-2026-09-07.md),
"Update 2026-09-09 - the second site".

**Recorded here because it is a claim about this note's neighbourhood, not about this note.** The
sweep's four items were reported as one list on one PR, so a reader arriving at the displacement
orphan is the reader most likely to want to know which of the other three moved. Two remain as
astubbs#483 left them, both deliberately: `ProcessingShard.onSuccess`'s by-key removal (a non-stale
resident is never displaced, and the third staleness checkpoint in `WorkManager.handleFutureResult`
stops a stale result reaching it - re-verified 2026-09-09), and `RetryQueue.remove`/`removeAll`
being by coordinates at every call site, which is the queue's keying model rather than a removal
that forgot to name its target.
