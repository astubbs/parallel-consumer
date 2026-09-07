# Draft response to astubbs#168 (confluentinc#629) - the line carried the identifiers and 4KB per partition with them

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->

**Not posted.** Post only on explicit instruction, and delete this file when it is posted - never
when the PR that wrote it lands.

**What the issue currently says, and why it needs replacing rather than adding to:** its *Fork
status* section cites the line by `ConsumerOffsetCommitter:103`, a line number that has already
moved, quotes the old format string, and states "No equivalent gap on the sync path". The last of
those is wrong - see the second half of the draft - and the first two are stale. This draft is a
replacement for that section, not a comment beneath it.

---

## Draft

**Implemented, and since tightened.** The identifiers you asked for were added upstream by
`add info of commiting offsets in error log when it failed to commit (#850)` and carried into this
fork, so the line already named every topic, partition and offset in the failed commit. What came
with them was each entry's `metadata`: Parallel Consumer's own base64-encoded offset map, up to
`OffsetMapCodecManager.DefaultMaxMetadataSize` (4096) characters **per partition**. On a large
assignment that grew the line to partitions x 4KB, and log tooling truncated it on exactly the
occasion an operator needed it whole - so the identifiers were on the line but not always in the
log.

The line now reads:

```
ERROR Error committing offsets: 2 partitions: orders-0: offset 1000, 812 chars of metadata; orders-1: offset 5, no metadata, exception: ...
DEBUG Failed commit in full: {orders-0=OffsetAndMetadata{...}, ...}
```

Every partition and its offset is still named - deliberately with no cap, because they are what this
issue asked for and a commit map has one entry per partition. Only the metadata is reduced, to its
length, which is itself the diagnostic when a commit is rejected for its size. The unabridged map is
one level down at `DEBUG`, where it has to be asked for.

**One correction to what this issue previously said.** It claimed there was no equivalent gap on the
sync path. There is: in `PERIODIC_CONSUMER_SYNC`, a `RebalanceInProgressException` or a
`CommitFailedException` is caught and logged as `Offset commit deferred (postponed, not dropped)`
with no partition and no offset at all. Your report was in the asynchronous mode, so the fixed line
is the one your scenario hits - but the sync equivalent is a real gap, and it is tracked separately
rather than left implied.

**On the "infinite rebalance" itself:** a `RebalanceInProgressException` from the commit callback is
Kafka saying "not yet", and Parallel Consumer defers rather than dropping the commit - the offsets
stay dirty and are re-committed on the next cycle. If you still see a group that never settles under
`CooperativeStickyAssignor`, that is a separate defect from the logging one and worth its own issue
with the improved line's output in it.
