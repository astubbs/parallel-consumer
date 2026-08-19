# astubbs#57 - PCMetrics leak (confluentinc#859) + cherry-picks

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->


Fixes duplicate Micrometer meter re-registration on assignment/revocation, and bundles the
`confluentinc#893` (offset accuracy on assignment) and `confluentinc#905` (max-queued-records-per-shard
metric) cherry-picks into one PR instead of a 3-deep stack, superseding the old closed stack
(astubbs#42 → astubbs#43 → astubbs#45).

Owns `PCMetrics.java`, `PCMetricsDef.java`, `PartitionState.java`, `PartitionStateManager.java`,
`ShardManager.java` - which is why astubbs#51 and anything touching partition state sequences after it.
