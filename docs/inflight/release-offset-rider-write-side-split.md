# Whether the rider's write side ships with its read side, or one minor later

<!-- inflight-type: task -->
<!-- inflight-impact: release-gate -->

The opaque rider (astubbs#255) has two halves with different compatibility floors. The **read side** - a build that knows the `RiderEnvelope` magic byte and can
decode past it - is what every group member must run before any member may write a rider. The
**write side** - `ParallelConsumerOptions.riderSupplier` - is what produces the payload an older
reader cannot survive: every released Parallel Consumer throws from inside the rebalance callback on
an unknown magic byte, before any policy is consulted, and the metadata is durable in
`__consumer_offsets`, so one such member crash-loops until the group's offsets are rewritten.

Both halves are unreleased, so 0.6.0.0 would be the first release with either, and the floor cannot
be met by any earlier version.

## The decision

- **Split.** 0.6.0.0 ships the envelope decoder and `OffsetMapCodecManager.decodeRider`; the
  `riderSupplier` option ships in the following minor. A rollback from the release that first
  writes riders then lands on one that already reads them, and the floor is satisfied by the
  previous release rather than by the operator's diligence.
- **Together.** 0.6.0.0 ships both and relies on the opt-in, the one-time `INFO` line naming the
  requirement, and the documented recovery
  (`kafka-consumer-groups --reset-offsets --to-current`).

**The decision owner is the release, not the implementation** - nothing in the rider's code changes
either way. What moves is `availability.target_release` in `docs/features/offset-metadata-rider.yaml`
(currently `0.6.0.0` for both halves, with the split recorded under `release_assumptions`), and the
plan's `Open Questions` in `docs/plans/2026-09-05-001-feat-offset-metadata-rider-plan.md` carry
the cost of each side.

## Delete when

The release that first carries `riderSupplier` is cut, and the feature record's `target_release`
says which release that was.
