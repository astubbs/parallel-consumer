# #57 - PCMetrics leak (upstream #859) + cherry-picks

Fixes duplicate Micrometer meter re-registration on assignment/revocation, and bundles the
`upstream #893` (offset accuracy on assignment) and `upstream #905` (max-queued-records-per-shard
metric) cherry-picks into one PR instead of a 3-deep stack, superseding the old closed stack
(#42 → #43 → #45).

Owns `PCMetrics.java`, `PCMetricsDef.java`, `PartitionState.java`, `PartitionStateManager.java`,
`ShardManager.java` - which is why #51 and anything touching partition state sequences after it.
