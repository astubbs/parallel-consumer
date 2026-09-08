# "Truncating state" warns when there is no state to truncate

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-vetted: 2026-09-08 - shrunk to the one live case. The below-expected branch is refuted: read `ClassicKafkaConsumer.updateAssignmentMetadataIfNeeded` and `ConsumerCoordinator.initWithCommittedOffsetsIfNeeded` in kafka-clients 3.9.2 - the fetcher's position read follows the rebalance listener, so the named race yields the ABOVE branch - and `PartitionStateBootstrapTruncation162Test` round-trips four commit shapes with the expectation equal to the committed offset every time. The absent-commit-data warning is unchanged at HEAD: `expectedBootstrapRecordOffset = getOffsetToCommit()` still never asks whether commit data existed, and nothing asserts a no-commit-data partition starts quietly -->

[astubbs#162](https://github.com/astubbs/parallel-consumer/issues/162), mirroring
[confluentinc issue #546](https://github.com/confluentinc/parallel-consumer/issues/546). That thread
holds three distinct defects behind one warning string. One is fixed here by inheritance; the other
two are live at HEAD and neither has a test.

## Fixed: the RunLength "expected 1" decode

`857c384af` (confluentinc#563, upstream 0.5.2.6) is in this fork's history, and its guard is
`long highestSeenOffset = (baseOffset > 0)` in `offsets/OffsetRunLength.java`, with a comment naming
confluentinc#546. A no-progress commit used to decode back as base offset 0, so bootstrap expected
offset 1 and truncated. That form of the message cannot occur now.

## Live: absent commit data reports as "expected 0 from loaded commit data"

`maybeTruncateBelowOrAbove` in `state/PartitionState.java` takes its expectation from
`long expectedBootstrapRecordOffset = getOffsetToCommit()` and never asks whether commit data
existed. When it did not, the partition came from the `PartitionState<K, V> defaultEntry` branch in
`offsets/OffsetMapCodecManager.java`, so `offsetHighestSucceeded` is `KAFKA_OFFSET_ABSENCE` and the
expectation computes to 0. The comparison is strict (`bootstrapPolledOffset >
expectedBootstrapRecordOffset`), so a first poll at offset 0 does not qualify - but a first poll at
any offset above 0 takes the above-expected branch and logs `Truncating state - removing records
lower than`, while pruning nothing, because the incompletes map is empty. The message is false in both halves: no commit data was loaded, and no state was
removed.

That is the shape reported upstream on **0.5.2.7**, after the RunLength fix shipped, alongside a
broker CLI screenshot showing an empty CURRENT-OFFSET for exactly those partitions. Established by
reading the code, not by running it.

The same default entry is reached from `catch (OffsetDecodingError offsetDecodingError)` in the same
file, so the foreign-metadata recovery landed by astubbs#217 routes its partitions into this false
warning rather than the crash it replaced.

## Refuted: the below-expected branch is not a defect PC can reach on its own

The thread's second warning, `Bootstrap polled offset has been reset to an earlier offset`, was never
explained upstream. The hypothesis this note carried - that PC's own `consumer.committed()` in
`loadPartitionStateForAssignment` races the consumer's resolution of the fetch position for a newly
assigned partition, so a commit landing between the two reads leaves PC one commit ahead - is
**wrong, and wrong in a way that names the other branch**. It is not a race at all:
`ClassicKafkaConsumer.updateAssignmentMetadataIfNeeded` runs `coordinator.poll` (which invokes the
rebalance listener, and so PC's `committed()`) and only afterwards `updateFetchPositions`, whose
`ConsumerCoordinator.initWithCommittedOffsetsIfNeeded` is what gives an initializing partition its
position. PC's read strictly precedes the fetcher's, so a commit landing between them is the one the
**fetcher** sees: the polled offset comes out at or above PC's expectation, which is the
above-expected branch. Read against kafka-clients 3.9.2, the version this tree builds on.

That leaves one route to the branch that belongs to PC rather than to the broker: a commit whose
encoded payload decodes to an expectation **above** the offset that commit was filed under. That is
the defect astubbs#337 closed, and it is now pinned - `PartitionStateBootstrapTruncation162Test`
round-trips a commit PC wrote back through `MockConsumer`, `WorkManager#onPartitionsAssigned` and the
decoders, over four shapes PC can genuinely commit, and asserts the bootstrap expectation equals the
committed offset exactly. Negative control: dropping the payload's lowest incomplete on decode flips
the two shapes that carry a payload, at gaps of 4 and 10.

With that invariant held, every remaining way to reach the branch is the broker handing back a
position below the committed offset - an offset reset, a manual rewind, another member committing
lower, a stale `OffsetFetch` across a coordinator failover - and there the branch is doing the right
thing. The same test pins what it costs when it does fire, as a controlled pair against the
above-expected branch: every loaded incomplete is discarded, including ones above the poll batch that
the batch does not re-register, and `offsetHighestSucceeded` resets, so records the previous owner
completed and committed are processed again. **Duplicate processing, and structurally not loss** -
the expectation the branch fires against IS the lowest tracked incomplete, so nothing tracked can lie
below the polled offset the fetcher is about to read from.

## What is still untested is the false warning, not the branches

`PartitionStateBootstrapTruncation162Test` now covers both branches and the round trip above;
`PartitionStateCommittedOffsetIT` covers deliberate truncation - compaction, committed offset moved
higher or lower. What still has no test is the case at the top of this note: **a partition with no
commit data is never asserted to start quietly**, in either the first-offset-0 or the
first-offset-above-0 shape.

<!-- post-merge: checked-begin -->
astubbs#106 (stop walking every offset), astubbs#306 (encoding density) and astubbs#207
(`invalidOffsetMetadataPolicy` reachability) all touch this area and address neither case. The mirror
body implies astubbs#106 might; it does not - re-confirmed against its merged tree, which changes only
the offsets/ encoders and leaves `PartitionState#maybeTruncateBelowOrAbove` untouched.
<!-- post-merge: checked-end -->

## Draft replacement for the mirror's `## Fork status` (NOT posted)

Text a maintainer can paste over the existing section, which is stale in the ways above. Fully
qualified because it is destined for GitHub, where `astubbs#NN` renders as plain text.

> **Partially fixed.** confluentinc/parallel-consumer#563, which upstream shipped in 0.5.2.6, is in
> this fork by inheritance (`857c384af`); its guard sits in `OffsetRunLength` and it ends the
> "expected 1" form of the warning.
>
> It does not close this issue. The same warning was reported upstream on **0.5.2.7**, after that fix
> shipped, in a different form - "expected 0 from loaded commit data", on partitions the broker shows
> with no committed offset at all. That is a second defect, still present here:
> `PartitionState#maybeTruncateBelowOrAbove` never checks whether commit data existed, so absence
> computes to an expectation of 0 and the warning fires having truncated nothing.
>
> A third path, the "reset to an earlier offset" branch, was never diagnosed upstream. It is **not**
> a defect this fork can reach on its own: the client resolves a newly assigned partition's fetch
> position after the rebalance listener runs, so PC's own committed-offset read can never be the
> newer of the two, and a commit PC writes decodes back to exactly the offset it was filed under. It
> now fires only on a genuine rewind by the broker, where replaying - not losing - is the right
> answer.
>
<!-- post-merge: checked-begin -->
> Both bootstrap branches, and the invariant that keeps the third path shut, are now covered by
> `PartitionStateBootstrapTruncation162Test`; `PartitionStateCommittedOffsetIT` covers deliberate
> truncation. The second defect above - the false warning on a partition with no commit data - is the
> one that still has no test. The open offset-encoding work (astubbs/parallel-consumer#106,
> astubbs/parallel-consumer#306, astubbs/parallel-consumer#207) addresses none of it.
<!-- post-merge: checked-end -->

## The decision to make

Whether absent commit data should warn at all. It is the normal state of a new group or an expired
offset, so the honest handling is a distinct message at a lower level and no truncation branch - but
that is a behaviour change to a log line operators alert on.

## Delete when

The false warning on a partition with no commit data is closed, or the maintainer rules it acceptable
as it stands. The reset-to-earlier branch no longer gates this note - it is refuted above and guarded
by a test.
