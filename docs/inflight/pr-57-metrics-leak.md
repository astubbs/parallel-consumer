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

- **No human LGTM.** Green CI is not approval.
- **The teardown never-throws contract grew after the leak fix** and has its own open follow-up in
  [`core-pcmetrics-lock-held-across-registry-calls.md`](core-pcmetrics-lock-held-across-registry-calls.md).
- **`pr57-pre-recut` tag is local-only** - it marks the pre-recut history and has never been pushed.
