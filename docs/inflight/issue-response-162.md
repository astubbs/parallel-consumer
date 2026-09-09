# Draft replacement for the `## Fork status` section of astubbs#162 - posted by the pre-release sweep, not by this PR

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- inflight-state: deferred - until the pre-release sweep, or an explicit instruction to post -->
<!-- post-merge: exempt-file - a drafted issue reply, held until the sweep posts it. It deliberately
     outlives the PR that wrote it, so it cannot be written in post-merge terms. -->

Not posted. Post only on explicit instruction; delete this file when it is posted, not when its PR
merges.

astubbs#162 is the fork mirror of confluentinc/parallel-consumer#546 (lennehendrickx, 2023-02, 22
comments upstream). This text **replaces the mirror's existing `## Fork status` section**, which
predates every one of the three findings below and says the fork position is "not confirmed". The
issue can be closed once this is posted: all three defects behind that one warning string are
settled. Its `partially-fixed-in/0.5.2.6` label is still accurate as history; the `0.6.0.0` label is
where the remaining fix ships.

Fully qualified issue and PR references throughout, because this is destined for GitHub, where
`astubbs#NN` renders as plain text.

---

## Fork status

**Fixed here, in three parts - the thread holds three distinct defects behind one warning string,
and separating them is why it outlived its own fix.**

**1. The `expected 1` form - fixed upstream, and already in this fork.**
confluentinc/parallel-consumer#563, which upstream shipped in 0.5.2.6, is here by inheritance
(`857c384af`); its guard sits in `OffsetRunLength`. A no-progress commit used to decode back as base
offset 0, so bootstrap expected offset 1 and truncated. The original reporter confirmed that form of
the message stopped.

**2. The `expected 0 from loaded commit data` form - a second defect, and the one this thread never
got an answer for. Fixed in astubbs/parallel-consumer#PR.**

It was reported here on **0.5.2.7**, after the fix above shipped, with a broker CLI screenshot
showing an empty `CURRENT-OFFSET` for exactly the partitions that warned. That screenshot is the
diagnosis: those partitions had **no committed offset at all**.
`PartitionState#maybeTruncateBelowOrAbove` never asked whether commit data existed. With none, the
partition is built from the offset codec's default entry, its highest-succeeded offset is the
`-1` absence sentinel, and the bootstrap expectation computes to `0` - so any first poll above
offset 0 took the truncation branch, logged `Truncating state - removing records lower than ...`,
and removed nothing, because nothing had been loaded to remove. The line was false in both halves,
and it fired for a **new consumer group starting normally**, or one whose committed offset had aged
out of the offsets topic.

It now takes no truncation branch and says what actually happened, at INFO:

> No committed offset for partition 34 of topic data.input.topic - starting from the first polled
> offset 55674910. Nothing was loaded, so nothing is being removed. Expected for a new consumer
> group, or where the group's committed offset has aged out of the offsets topic.

The WARN is unchanged for the genuine cases - a committed offset the broker no longer has records
for, compaction, a raised committed offset - so an operator alerting on `Truncating state` keeps
exactly the alert they had, minus the false firings.

Worth stating because it looks like an easy shortcut and is not: the fix does **not** test the `-1`
sentinel. A real commit filed at offset 0 with no offset map decodes to that same sentinel and the
same expectation of 0, with commit data that genuinely existed - so testing the sentinel would
silence the true warning for exactly the partition most likely to be caught by retention. The
absence is instead recorded where it is known, when the partition state is constructed.

**3. `Bootstrap polled offset has been reset to an earlier offset` - not a defect Parallel Consumer
can reach on its own.** Settled in astubbs/parallel-consumer#484. This was never explained upstream,
and the plausible-looking hypothesis - that PC's own `consumer.committed()` on assignment races the
consumer's resolution of the fetch position - is refuted by the client's ordering:
`updateAssignmentMetadataIfNeeded` runs `coordinator.poll` (and therefore the rebalance listener,
and therefore PC's read) *before* `updateFetchPositions`, so PC's read strictly precedes the
fetcher's and a commit landing between them produces the *other* branch. A commit PC writes also
decodes back to exactly the offset it was filed under, pinned over four commit shapes by
`PartitionStateBootstrapTruncation162Test`. Every remaining way into that branch is the broker
handing back a position below the committed offset - an offset reset, a manual rewind, another
member committing lower, a stale `OffsetFetch` across a coordinator failover - and there replaying
is the correct response, not a bug. It costs duplicate processing, never loss.

**Tests.** `PartitionStateAbsentCommitData162Test` covers (2), including the assignment path with a
consumer reporting no committed offset, and a control arm proving the sentinel shortcut would be
wrong. `PartitionStateBootstrapTruncation162Test` covers (3) and both truncation branches;
`PartitionStateCommittedOffsetIT` covers deliberate truncation - compaction and a committed offset
moved either way.

**One correction to the earlier fork note**, in case anyone read it: unreadable commit metadata (the
Kafka Streams / foreign-metadata case, astubbs/parallel-consumer#217) does **not** land on the
no-commit-data path. Under the default `IGNORE` policy it keeps the committed offset and bootstraps
with a real expectation, so the truncation branches remain correct for it.

This fork is where Parallel Consumer continues; the Confluent-hosted repository is no longer
maintained and its README says so.
