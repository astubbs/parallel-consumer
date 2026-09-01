# `invalidOffsetMetadataPolicy(FAIL)` stops the consumer by escaping Kafka's rebalance callback

<!-- inflight-type: task -->
<!-- inflight-impact: reliability -->
<!-- post-merge: checked-begin -->

`EncodedOffsetPair#handleUnreadableMetadata` throws under `FAIL`. What it throws is an
`InternalException` subclass smuggled out with `@SneakyThrows`, and deliberately **not** an
`OffsetDecodingError` - because `OffsetMapCodecManager#loadPartitionStateForAssignment` catches that
type unconditionally and drops the offset map, which is precisely the recovery `FAIL` exists to refuse.

So the exception propagates out of `onPartitionsAssigned`, and Kafka wraps it as
`User rebalance callback throws an error` with the real cause nested.

**Stopping is correct - the surfacing is not.** That opaque failure is the symptom astubbs#118 was filed
about, and a user who deliberately selected `FAIL` deserves a clear stop rather than the same trace the
bug report carried. A deliberate stop should go through PC's own fatal-error path, naming the partition,
the magic byte or structural problem, and the option that caused it.

**Not urgent, because it is now opt-in.** astubbs#207 moved the default to `IGNORE`, so nobody reaches
this path without asking for it. It is recorded rather than fixed there because a clean fatal exit
reaches beyond the offsets package into the engine's error handling.

<!-- post-merge: checked-end -->
