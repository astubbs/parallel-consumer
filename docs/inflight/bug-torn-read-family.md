# The torn-read family: the hunt's candidates, now settled, and the fixes they await

<!-- inflight-type: bug -->
<!-- inflight-impact: data-loss -->

**The family, stated precisely:** multiple reads of moving shared state within one logical operation,
combined as though they were one consistent snapshot. Confirmed instances so far, both silent-loss
capable and both fixed: the commit base/payload tear in `createOffsetAndMetadata`
(confluentinc#894, carried by astubbs#337) and the encoder snapshot/range tear in
`encodeOffsetsCompressed`
<!-- post-merge: checked-begin -->
(astubbs#344, whose defect-class sweep produced this note).
<!-- post-merge: checked-end -->

A three-way parallel audit of the commit path, the state managers, and shards/retry/metrics traced
every candidate thread-by-thread. Most were dismissed, and the dismissals fall into five shapes:
safe-direction orderings, mailbox-confined single-threading verified in source, lock-guarded pairs,
broker-backstopped combinations, and metrics-only consumers. **Re-derive rather than trust a tally
here** - the audit's own totals were a point-in-time measurement whose report artifacts are not
durable, and they stop being true as candidates are fixed or the lens widens. The live shape is
`grep -rn "getOffsetHighestSucceeded\|getOffsetHighestSeen" parallel-consumer-core/src/main/java`
read against the five dismissal shapes above. Three candidates survived, and **all three are now
settled with control arms** (2026-08-25): two reproduced deterministically, one refuted as independently reachable and
reclassified as a downstream stage of another. The armed red reproductions live on
`test/torn-read-candidates-reproduction` and become the fix branches' regression tests.

## Candidate 1 - bootstrap-reset tear: REFUTED as independent - it is candidate 3's downstream stage

**Settled 2026-08-25, and the dossier below had it wrong in a specific way.** The tear is real -
forced open, a mid-window read commits offset 101 with no payload on a partition the broker reset
to 5, cancelling the replay - but it is **fenced**: the commit path collects only `dirty` states,
`setDirty()` has exactly one call site (`onSuccess`), and a bootstrap-phase state cannot have had an
epoch-matched completion. The one production route that dirties a bootstrap-phase state is candidate
3's success-path tear, so fixing candidate 3 closes candidate 1's only door. Harden the reset-write
ordering later, with the planned racing-double unification. Original analysis kept below for the
record:

<!-- post-merge: checked-begin -->
**A review of astubbs#344 found that the encoder's single-sample fix changes the SHAPE of this
candidate's window, and the direction is not the reassuring one.** Under the old two-read code, a
reset landing mid-encode was observed by the second read: the range top came back lowered, so the
payload was narrow and the interleaving was safe. Under the single-sample fix the high-water mark is
captured first, so a reset that then replaces `incompleteOffsets` and lowers
`offsetHighestSucceeded` leaves the *old, high* bound paired with the *new, empty* map - and every
offset in that stale range encodes as complete, cancelling the broker-mandated replay. So the fix is
strictly better against the completion seam it was written for, and strictly worse against this one.

**The seam is UNREACHABLE, not merely fenced - and the argument is structural, so it does not depend
on any other PR landing.** The first reading of this was the weaker "the dirty gate happens to be
shut"; a second review found the stronger one, and it was verified against the source rather than
accepted:

1. `bootstrapPhase` has exactly **one** write site in `PartitionState` - the first line of
   `maybeTruncateBelowOrAbove`, which flips it `true`→`false` and otherwise returns early. The
   reset branch can therefore execute at most once per instance, on the first call.
2. That call is reached from `maybeRegisterNewPollBatchAsWork`, which invokes
   `maybeTruncateOrPruneTrackedOffsets` **before** its `addNewIncompleteRecord` loop - so the reset
   strictly precedes any offset being registered on that instance.
3. `dirty` can only be set by `onSuccess`, which can only fire for an offset registered in step 2.
4. Reassignment cannot reopen the window: `PartitionStateManager.onPartitionsAssigned` always builds
   new instances via `loadPartitionStateForAssignment` and `putAll`s them over the map entry, and
   revocation substitutes the `RemovedPartitionState` singleton rather than mutating the old object,
   so no instance can pair a stale `dirty == true` with a reinstated `bootstrapPhase == true`.

The reset window and the dirty-encode window are therefore **temporally disjoint on any given
instance**. That is why the encoder's single-sample read is safe here without tying the snapshot and
the bound to one state generation, and why this does not wait on astubbs#346 - the earlier reading
made the safety contingent on candidate 3's fix, and it is not.
<!-- post-merge: checked-end -->

`PartitionState.maybeTruncateBelowOrAbove` (reset-below arm) calls `initStateFromOffsetData`, which
performs three separate plain writes - `offsetHighestSeen = -1`, `incompleteOffsets = new map`,
`offsetHighestSucceeded = -1`; none volatile - while the broker-poll commit path can read across
them (always concurrent in the default `PERIODIC_CONSUMER_ASYNCHRONOUS` mode). The committer can
combine an old-map offset with a new-map (empty) incompletes payload, re-asserting a pre-reset
offset and cancelling a broker-mandated replay. Trigger is rare - a bootstrap reset-below event -
but the consequence is the wrong-commit family. The seam is injectable the same way
`RacingEncodeWindowState` injects.

## Candidate 2 - `ShardManager.removeWorkFromShardFor`: FIXED, landed by astubbs#345

**Settled 2026-08-25: deterministic, 4/4** - the real production shard removal fired between the two
map reads throws `NullPointerException` out of the revoke sweep; in production, out of the
`ConsumerRebalanceListener` into `consumer.poll`. Control: the same removal landing before the sweep
takes the confluentinc#757-guarded branch gracefully. The fix is the one-line single-read
`getShard(key)` idiom the rest of the file already uses, and astubbs#345 landed it. Original
analysis kept for the record:

`containsKey` followed by `get`, the result dereferenced unconditionally. Broker-poll (revoke sweep
via `PartitionState.onPartitionsRemoved`) races control (`onSuccess` → `removeShardIfEmpty`, which
removes empty shards under KEY ordering). A hit between the two map reads throws NPE out of the
`ConsumerRebalanceListener` into `consumer.poll` - the poller-death family. KEY ordering only,
narrow window, hot path. Every other access in the file already uses the single-read
`getShard(key)` Optional idiom; this is the one remaining pair.

## Candidate 3 - `WorkManager.handleFutureResult`: REPRODUCED, both harms - fix before the release

**Settled 2026-08-25: deterministic, 3/3, and the worst of the three.** The failure path re-queues a
stale container into `retryQueue` where nothing can remove it - `workIsWaitingToBeProcessed()` then
reads true forever with nothing assigned, a confluentinc#857-family stall signature. The success
path trips `PartitionState.onSuccess`'s assert under `-ea`; without `-ea` it silently dirties a
bootstrap-phase state, which is what opens candidate 1's gate. The no-data-loss claim below is
verified for consumer-commit modes; transactional mode was not traced. Original analysis:

Checkpoint 3's epoch check and the subsequent acting reads are two separate `partitionStates.get(tp)`
lookups. Control passes the check, broker-poll completes a full revoke(+reassign) in the gap, control
acts on the swapped state. Worst traced harms: the failure path re-adds a stale container to
`retryQueue` that nothing removes (permanent orphan → near-zero control-loop block times, CPU churn,
counter drift); the success path can trip `PartitionState.onSuccess`'s `assert` under `-ea`. No
data-loss route found after tracing all sub-cases; it contradicts checkpoint 3's documented
guarantee.

## Related, tracked between branches - the seam re-hook, now DISCHARGED

<!-- post-merge: checked-begin -->
The fixed encoder no longer calls `getIncompleteOffsetsBelowHighestSucceeded()` - the exact seam
astubbs#337's `RacingCommitCycleState` overrode to inject its race. This note predicted that whichever
of the two landed second would inherit the re-hook. astubbs#337 landed first, so astubbs#344 did it:
the double now overrides the bounded `getIncompleteOffsetsBelow(long)`, which catches **both** entry
points because the no-arg convenience method delegates through it in the base class.

**The prediction held, and so did the guard.** Taking master produced exactly seven failures - all
three `PartitionStateCommitEncodeShift894Test` cases and all four
`PartitionStateCommitShiftCompounding894Test` cases - each failing at its fired-assertion with a
message naming the cause and the remedy. Worth recording that the compounding test *did* have a
fired-assertion by then: this note previously said it did not and could go silently green, which was
true when written and had been fixed on astubbs#337's side before it merged. Had it not been, the
seven-failure signal would have been three, and the other four would have passed while proving
nothing.

The unification of the two near-clone racing doubles is still not done, and is still the right home
for this: `RacingCommitCycleState` and `RacingEncodeWindowState` now differ only in which offset they
race and what they record.
<!-- post-merge: checked-end -->

## Also surfaced, out of family - now tracked separately

The hunt turned up four defects that are **not** members of this family. They have their own owners
and lifecycles, so they are no longer recorded here: this dossier is deleted once the family's work
closes, which would have taken four still-open findings with it, and a paragraph inside another note
cannot be found by anyone listing this directory.

- [`bug-async-commit-marked-successful-before-broker-ack.md`](bug-async-commit-marked-successful-before-broker-ack.md)
- [`bug-unsynchronised-cross-thread-counter-maps.md`](bug-unsynchronised-cross-thread-counter-maps.md)
- [`bug-reset-offset-map-npe-on-partial-assignment.md`](bug-reset-offset-map-npe-on-partial-assignment.md)
- [`bug-brokerpollsystem-pause-api-is-racy-and-uncalled.md`](bug-brokerpollsystem-pause-api-is-racy-and-uncalled.md)

## No shipped static analysis can see this family - verified empirically, not assumed

Checked 2026-08-25, because the obvious question is "why does SpotBugs not catch these". SpotBugs
4.10.3 at `effort=Max, threshold=Medium` (the repo's own configuration) reports **nothing** relevant
on the unfixed code - only `EI_EXPOSE_REP2` noise. The nominally-relevant detector,
`AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION`, also produced zero findings on a purpose-built
textbook probe: a `containsKey`-then-`get` pair on a concretely-typed `ConcurrentHashMap`, the
cleanest possible instance of the shape. So it is not a configuration or static-typing issue - the
detector simply does not catch it in this version. Two further points close the question:

- Even a firing detector would have been masked: the defects predate the fork, so the baseline job
  would have recorded them as pre-existing. A baseline is drift protection, not bug discovery.
- ArchUnit cannot see the family in principle - it checks structure, not dataflow, and "two reads
  combined as one snapshot" is invisible to it. What it CAN enforce is the access idiom (e.g. all
  `processingShards` access through `getShard`), which is convention enforcement, not detection.

What can detect the family: the racing-double seam tests (deterministic, per known seam), and
scheduler-controlled concurrency testing (Lincheck and jcstress, both now adopted - open items in
[`test-lincheck-lane-open-items.md`](test-lincheck-lane-open-items.md) and
[`test-jcstress-probe-module-open-items.md`](test-jcstress-probe-module-open-items.md)), which is the
only tool class that finds UNKNOWN interleavings. Neither is pointed at this family's classes yet, so
the hunt below remains this repo's only working detector for it.

## Next iteration, so it is not forgotten

One hunt pass found four actionable items (these three plus the cross-branch seam above) and this
family keeps composing - candidate 3 feeding candidate 1 was invisible until both were settled. Once
the two fixes land and the current PR set merges, **run another hunt iteration**: same lens, fresh
eyes, including the out-of-family stragglers above and whatever the fixes themselves change.

## Which merge closes which section

The candidates above are settled but their sections stay open until the fixes land, and the mapping is
not derivable from this note alone:

- **astubbs#345** - merged; candidate 2 closed.
<!-- post-merge: checked-begin -->
- **astubbs#346** - merged; candidates 3 and 1 both close (1 is 3's downstream stage, so 3's fix
  shuts its only door). Their sections stay, as candidate 2's did when astubbs#345 landed - this
  note is the hunt's durable summary, and it remains open for the out-of-family stragglers, the
  astubbs#57 trigger below, and the next iteration.
<!-- post-merge: checked-end -->
<!-- post-merge: checked-begin -->
- **astubbs#337 and astubbs#344** - astubbs#337 landed first, so astubbs#344 carried the seam re-hook
  described under "Related" above. Discharged; it is not owed again.
<!-- post-merge: checked-end -->
<!-- post-merge: checked-begin -->
- **astubbs#57** - the unprompted-Lincheck-find note that arrived on astubbs#347's branch,
  `bug-pcmetrics-registered-meters-is-a-plain-arraylist.md`, is deleted in that PR, as its own last
  section instructed. Discharged, and not simply dropped: the reproduction moved into
  `PCMetrics859Test`'s class javadoc as the reason `metersLock` exists, and the half astubbs#57 does
  not fix - the plain `HashMap` counter maps in `WorkManager` and `PartitionStateManager` - has its
  own note, [`bug-metrics-counter-maps-are-plain-hashmaps.md`](bug-metrics-counter-maps-are-plain-hashmaps.md).
<!-- post-merge: checked-end -->

## Closing this note

Each candidate closes by reproduction-plus-fix or by a demonstrated refutation with a control arm -
a worked argument is not enough in either direction; that is the lesson the two confirmed instances
taught twice. The dismissal write-ups (every candidate, with call paths) live in the hunt agents'
report artifacts from 2026-08-24; the durable summary is this note.
