# A discarded offset map is logged but not counted

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->
<!-- inflight-vetted: 2026-09-07 - `ParallelConsumerOptions.invalidOffsetMetadataPolicy` still defaults to `IGNORE`, `EncodedOffsetPair.handleUnreadableMetadata` still only `log.warn`s the discard, and `handleUnreadableMetadata` still appears nowhere under `metrics/`; `PCMetricsDef` still carries no counter for a discarded map (`METADATA_SPACE_USED` is the only metadata-named entry). astubbs#207 is MERGED, which is what made the default `IGNORE` - it is the notes premise, not its fix -->
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
