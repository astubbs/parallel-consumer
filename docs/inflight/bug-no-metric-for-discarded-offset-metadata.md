# A discarded offset map is logged but not counted

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- post-merge: checked-begin -->

When `invalidOffsetMetadataPolicy` is `IGNORE` - **the default since astubbs#207** - PC discards an
unreadable offset map and resumes from the committed offset. `EncodedOffsetPair#handleUnreadableMetadata`
logs a `log.warn` carrying the partition, the base offset and the specific reason, so it is not silent.

**But nothing counts it.** `grep -rn handleUnreadableMetadata parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/metrics/`
returns nothing, and `PCMetrics` has no counter for the event. An operator who watches dashboards
rather than grepping logs cannot see that it happened at all.

**Why the default change makes this matter more than it did.** While the default was `FAIL`, an
unreadable payload announced itself by stopping the application - impossible to miss. Now the same
event is a warn line in a log that, on a busy consumer, nobody is tailing. The thing being hidden is
not cosmetic: discarding the map replays every record that completed but was not committed, so the
absence of a counter means duplicate processing with no signal an operator can alert on.

Raised in review of astubbs#207 as a follow-up rather than a blocker, and it is genuinely separable -
the counter belongs with the rest of `PCMetrics`, not in the offsets decode path.

<!-- post-merge: checked-end -->

## 2026-09-07: the WRITE side is counted now, the read side is not

The opaque-rider work for astubbs#255 gave `PartitionState`'s commit path four per-partition meters,
so the "discarded and only logged" class this note names is now covered on the side where PC throws
away a payload it was about to write:

- `pc.offsets.payload.stripped` - the offset map was itself too large for the metadata limit, so a
  bare offset was committed. That is the same event as `stripPayloadForSize`'s warn line, and as the
  `NoEncodingPossibleException` arm's, both of which were previously log-only in exactly the way this
  note describes.
- `pc.offsets.rider.dropped` - an embedder's rider was shed for size. Same class of silently
  discarded metadata, arriving with the feature that made it possible.

**What stays open is the READ side, which is what this note is about.** Under `IGNORE`,
`EncodedOffsetPair#handleUnreadableMetadata` still discards an unreadable payload on assignment with
nothing but a warn line, and the write-side meters cannot stand in for it: they are recorded during a
commit, on a partition this member owns, and a discard on assignment never reaches that code at all.
The duplicate-processing consequence in the paragraphs above is unchanged.
