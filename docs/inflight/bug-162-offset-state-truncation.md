# "Truncating state" warns when there is no state to truncate

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

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

## Live and never diagnosed: the below-expected branch resets and replays

The thread's second warning, `Bootstrap polled offset has been reset to an earlier offset`, survived
the 0.5.2.6 snapshot for the original reporter and was never explained upstream. Its branch discards
all loaded state and replays, so the cost is duplicate processing, not loss. Reported gaps were small
and were seen on already-running instances taking on new partitions, which points at handoff rather
than retention.

**Untested hypothesis, stated so it can be refuted:** PC issues its own `consumer.committed()` in
`loadPartitionStateForAssignment`, separate from the consumer's own position resolution for a newly
assigned partition. If the previous owner's commit lands between those two reads, PC's expectation is
one commit newer than the position the fetcher starts from. The control arm is a test that delays the
old owner's commit past assignment, predicting the warning appears in that arm only.

## Neither case is tested, and the open offset PRs do not touch them

`PartitionStateCommittedOffsetIT` covers deliberate truncation only - compaction, committed offset
moved higher or lower, no reset policy on startup. No test in the surefire or failsafe suites references the
warning or `maybeTruncateBelowOrAbove` - the other references are javadoc in
`state/PartitionStateManager.java` and `state/PartitionState.java`, plus a jcstress probe in
`jcstress-poc/` that models the same branch but is not part of the test suite - so a partition with
no commit data is never asserted to start quietly. A test wants both cases: first offset 0, and first offset
above 0.

astubbs#106 (stop walking every offset), astubbs#306 (encoding density) and astubbs#207
(`invalidOffsetMetadataPolicy` reachability) all touch this area and address neither case. The mirror
body implies astubbs#106 might; it does not.

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
> A third path, the "reset to an earlier offset" branch, was never diagnosed upstream and is also
> still open. It causes replay, not loss.
>
> `PartitionStateCommittedOffsetIT` covers deliberate truncation only; neither remaining case is
> tested. The open offset-encoding work (astubbs/parallel-consumer#106,
> astubbs/parallel-consumer#306, astubbs/parallel-consumer#207) addresses none of it.

## The decision to make

Whether absent commit data should warn at all. It is the normal state of a new group or an expired
offset, so the honest handling is a distinct message at a lower level and no truncation branch - but
that is a behaviour change to a log line operators alert on.

## Delete when

Both live cases are closed, or the maintainer rules the warning acceptable as it stands.
