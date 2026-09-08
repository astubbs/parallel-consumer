# The torn-read family: what the hunt left open

<!-- inflight-type: bug -->
<!-- inflight-impact: data-loss -->
<!-- inflight-vetted: 2026-09-08 - applied: shrunk to the racing-double unification, the next hunt iteration and the out-of-family pointers; the three candidate dossiers and the merge-mapping section are removed as a FIXED narrative, with a history pointer to their full text; checked: candidate 2's single-read `getShard(shardKey)` is in `ShardManager.removeWorkFromShardFor` with the tear named in its comment, candidate 3's single resolution is in `WorkManager.handleFutureResult`, the counter maps are `ConcurrentHashMap`, and `RacingCommitCycleState` and `RacingEncodeWindowState` both still exist unmerged -->

**The family, stated precisely:** multiple reads of moving shared state within one logical operation,
combined as though they were one consistent snapshot. Confirmed instances so far, both silent-loss
capable and both fixed: the commit base/payload tear in `createOffsetAndMetadata`
(confluentinc#894, carried by astubbs#337) and the encoder snapshot/range tear in
`encodeOffsetsCompressed` (astubbs#344, whose defect-class sweep produced this note).

A three-way parallel audit of the commit path, the state managers, and shards/retry/metrics traced
every candidate thread-by-thread. Most were dismissed, and the dismissals fall into five shapes:
safe-direction orderings, mailbox-confined single-threading verified in source, lock-guarded pairs,
broker-backstopped combinations, and metrics-only consumers. **Re-derive rather than trust a tally
here** - the audit's own totals were a point-in-time measurement whose report artifacts are not
durable, and they stop being true as candidates are fixed or the lens widens. The live shape is
`grep -rn "getOffsetHighestSucceeded\|getOffsetHighestSeen" parallel-consumer-core/src/main/java`
read against the five dismissal shapes above.

## The three candidates are settled and their fixes have landed

All three were settled with control arms on 2026-08-25 and are fixed in the tree: the
`ShardManager.removeWorkFromShardFor` `containsKey`-then-`get` pair is now the single-read
`getShard(key)` idiom the rest of that file uses (astubbs#345), `WorkManager.handleFutureResult`
resolves the partition state once (astubbs#346), and the bootstrap-reset tear was refuted as an
independent candidate - it is candidate 3's downstream stage, and closing that shut its only door.

**The one part of those dossiers worth carrying forward** is why the encoder's single-sample read is
safe against a bootstrap reset without tying the snapshot and the bound to one state generation:
`bootstrapPhase` has exactly one write site, on the first line of
`PartitionState.maybeTruncateBelowOrAbove`, reached from `maybeRegisterNewPollBatchAsWork` *before*
its `addNewIncompleteRecord` loop; `dirty` can only be set by `onSuccess`, which needs an offset
registered by that loop; and reassignment always builds a fresh instance rather than reopening the
old one. The reset window and the dirty-encode window are therefore temporally disjoint on any given
instance. The full dossiers - each candidate's call paths, harms, control arms and the refutation
argument in four steps - are at
`git show f318e9434:docs/inflight/bug-torn-read-family.md`.

## Still open: the racing doubles are two near-clones

`RacingCommitCycleState` and `RacingEncodeWindowState` now differ only in which offset they race and
what they record. Unifying them is not done, and this is its home. astubbs#344 carried the seam
re-hook onto the bounded `getIncompleteOffsetsBelow(long)`, which catches both entry points because
the no-arg convenience method delegates through it in the base class; that obligation is discharged
and is not owed again.

## Still open: the next hunt iteration

One hunt pass found four actionable items and this family keeps composing - candidate 3 feeding
candidate 1 was invisible until both were settled. Now that the fixes have landed, **run another hunt
iteration**: same lens, fresh eyes, including the out-of-family stragglers below and whatever the
fixes themselves changed.

## Also surfaced, out of family - tracked separately

The hunt turned up defects that are **not** members of this family. They have their own owners and
lifecycles, so they are not recorded here:

- [`bug-async-commit-marked-successful-before-broker-ack.md`](bug-async-commit-marked-successful-before-broker-ack.md)
- [`bug-brokerpollsystem-pause-api-is-racy-and-uncalled.md`](bug-brokerpollsystem-pause-api-is-racy-and-uncalled.md)
- The unsynchronised cross-thread counter maps - **closed**; all four are now concurrent, and what
  the hunt's sighting did and did not establish is in
  [`../solutions/logic-errors/the-metrics-counter-maps-were-plain-hashmaps-2026-09-05.md`](../solutions/logic-errors/the-metrics-counter-maps-were-plain-hashmaps-2026-09-05.md).

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
the hunt above remains this repo's only working detector for it.

## Closing this note

Each candidate closes by reproduction-plus-fix or by a demonstrated refutation with a control arm -
a worked argument is not enough in either direction; that is the lesson the two confirmed instances
taught twice. The dismissal write-ups (every candidate, with call paths) live in the hunt agents'
report artifacts from 2026-08-24; the durable summary is this note.
