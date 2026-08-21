# A compacted offset on a poll-batch boundary is never pruned, and pins the commit

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->

Found 2026-08-21 while verifying, for a competitor comparison, that PC is robust to non-contiguous
offsets. **It is** - see [`next-llingr-questions-and-answers.md`](next-llingr-questions-and-answers.md)
- but the reconciliation mechanism has a narrow hole. **Not observed in the wild; found by reading.**

## The defect

`PartitionState.maybeTruncateOrPruneTrackedOffsets(...)` removes tracked incomplete offsets that the
broker will never deliver again - typically compacted away between one assignment and the next. It
does so per poll batch, bounded by that batch's own range:

```java
var trackedIncompletesWithinPolledBatch =
    incompleteOffsets.keySet().subSet(offsetOfLowestRecord, true, offsetOfHighestRecord, true);
```

An offset escapes pruning when **all four** hold:

1. it is tracked as incomplete, decoded from committed metadata on assignment;
2. it has since been compacted away, so it will never be polled again;
3. it is **not** the lowest incomplete - the lowest is handled by the bootstrap truncate
   (`maybeTruncateBelowOrAbove`, `confluentinc#409`); and
4. it sits **exactly between** the highest record of one poll batch and the lowest of the next, so it
   is outside both `subSet` windows.

## Worked example

Incompletes `{100, 500}`, committed offset 100, offset 500 compacted away.

- Bootstrap poll returns `100..499`. The first polled offset is not above the expected commit offset,
  so `maybeTruncateBelowOrAbove` does nothing. `subSet(100, 499)` does not contain 500.
- Next poll returns `501..600`. `subSet(501, 600)` does not contain 500.
- 500 is now a permanent phantom. Once 100 completes,
  `getOffsetHighestSequentialSucceeded()` returns 499 forever and **the committed offset pins at
  500** for as long as the partition stays assigned.

## Severity: low, and it self-heals

- **A commit stall, not loss and not redelivery.** Processing continues normally; only the committed
  offset stops advancing.
- **No back-pressure.** One incomplete run-length-encodes to a few bytes, so the payload never
  approaches the metadata limit and nothing throttles.
- **Self-healing.** On the next rebalance or restart the bootstrap poll starts at 500, receives 501,
  and `maybeTruncateBelowOrAbove` prunes it.
- **The cost while it lasts** is a growing duplicate window: a crash replays everything back to the
  pinned offset.
- **Probability is low** - the hole must land exactly on a fetch-response boundary, which depends on
  `max.partition.fetch.bytes` and post-compaction batch layout.

## Suggested fix, and the test that is missing

Widen the prune's lower bound so consecutive batches cannot leave a hole between them: track the
previous batch's high-water mark (or the last-pruned position) per partition and prune from there
rather than from the current batch's own lowest record.

`PartitionStateCommittedOffsetTest.compactedTopic` covers phantoms **inside** a batch range, and
`bootstrapPollOffsetHigherDueToRetentionOrCompaction` covers the **above-the-range** case. **Neither
covers the exact-boundary case**, which is why this was not caught. A regression test should construct
two consecutive batches with a tracked-but-compacted offset in the seam.

## Related

- `confluentinc#409` / `docs/features/compacted-topic-offset-recovery.yaml` - the compaction support
  this refines, `production-use` since 0.5.2.4.
- `confluentinc#329` - the sibling mechanism for control-record gaps
  (`OffsetSimultaneousEncoder.maybeRaiseOffsetHighestSucceeded`).
- `TransactionMarkersTest` carries an open question in its javadoc - *"todo can these gaps also be
  created by log compaction? If so, is the solution the same?"* - which this investigation answers:
  yes to both, handled by different mechanisms, and this is the seam between them.
