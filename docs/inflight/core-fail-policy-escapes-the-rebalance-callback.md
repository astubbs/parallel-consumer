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

## astubbs#29 named the mechanism, and it is worse than opaque surfacing

Inherited when astubbs#29 merged, and it argues against reading this note as cosmetic.
`PartitionStateManager#deregisterPartitionCounters` now carries a javadoc contract for the *revoke*
side of exactly this hazard - grep it for `never-throws contract`. Its reasoning: the callback runs on
the broker-poll thread inside `poll()`, so an exception escaping it **kills the poll thread**, which is
the only producer of commit responses, so every later commit blocks until it times out. It names that
as the confluentinc#857 family's worst failure shape.

`onPartitionsAssigned` is the same thread and the same wrapper, so the mechanism carries across - which
turns the open question here from *"is the message clear?"* into *"is a `FAIL` stop a stop at all, or a
hang wearing one?"*. **Untested either way**, and it is the thing to establish first: drive an
unreadable payload under an explicit `FAIL` and observe whether PC reaches a terminal state or sits
until the commit timeout. If it hangs, this stops being a task about surfacing.

**Still opt-in, so still not urgent.** astubbs#207 moved the default to `IGNORE`, so nobody reaches this
path without asking for it. It is recorded rather than fixed there because a clean fatal exit reaches
beyond the offsets package into the engine's error handling.

<!-- post-merge: checked-end -->
