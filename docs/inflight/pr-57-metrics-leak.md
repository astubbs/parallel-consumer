# astubbs#57 - PCMetrics leak (confluentinc#859) + cherry-picks

<!-- inflight-type: bug -->
<!-- inflight-impact: crash -->


Fixes duplicate Micrometer meter re-registration on assignment/revocation, and bundles the
`confluentinc#893` (offset accuracy on assignment) and `confluentinc#905` (max-queued-records-per-shard
metric) cherry-picks into one PR instead of a 3-deep stack, superseding the old closed stack
(astubbs#42 → astubbs#43 → astubbs#45).

Owns `PCMetrics.java`, `PCMetricsDef.java`, `PartitionState.java`, `PartitionStateManager.java`,
`ShardManager.java` - which is why astubbs#51 and anything touching partition state sequences after it.

## Open on this PR

- **Stacked on astubbs#325.** The base branch is `test/chaos-instrumentation`, not `master`, so this
  PR's diff shows only its own 32 files. **Merging it while the base is that branch would merge into
  astubbs#325 rather than master.** GitHub retargets when astubbs#325's branch is deleted on merge -
  confirm it happened rather than assuming.
- **A decision is owed before merge: the `SHARDS_MAX_SIZE` gauge doubles a known-O(n) scrape.**
  `ProcessingShard.entries` is a `ConcurrentSkipListMap`, whose `size()` traverses, so
  `getCountOfWorkTracked()` is O(n). Master already pays that once for `SHARDS_SIZE`; the
  confluentinc#905 cherry-pick here adds a second gauge doing the same walk, and Micrometer pulls
  each gauge independently so they cannot share a scan. Cost is O(total queued records) per scrape,
  twice. The `TODO(refactor)` in `ShardManager` carries the cheap fix - an O(1) counter on
  `ProcessingShard`, maintained where `availableWorkContainerCnt` already is. Ship the gauge and take
  the doubling, or land the counter first?
- **No human LGTM.** Green CI is not approval.
- **The teardown never-throws contract grew after the leak fix** and has its own open follow-up in
  [`core-pcmetrics-lock-held-across-registry-calls.md`](core-pcmetrics-lock-held-across-registry-calls.md).
- **`pr57-pre-recut` tag is local-only** - it marks the pre-recut history and has never been pushed.
