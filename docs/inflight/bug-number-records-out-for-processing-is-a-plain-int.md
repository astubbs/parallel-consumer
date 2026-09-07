# Bug: `numberRecordsOutForProcessing` is a plain `int`, and it is the last counter of its family

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-labels: concurrency -->

`WorkManager.numberRecordsOutForProcessing` is a plain `int`, mutated on several controller-thread paths and
read without synchronisation. It is what is left of the *two drifting counters feeding one throttle decision*
shape, after both of the others were dissolved:

- the shard population is now derived by conservation (`ShardManager.getWorkableRecords`, `RecordPopulation`),
  and the load gate reads that instead of a maintained total;
- the shard's selection count is now owned by a compare-and-set on each container's claim
  (`ProcessingShard.includeInSelection` / `excludeFromSelection`, `WorkContainer.claimSelection`), so no site
  infers whether it already spent a unit, and there is no clamp to absorb the sites that got it wrong.

This one got neither treatment. It no longer gates record intake, but it still drives `hasWorkInFlight()`,
`isWorkInFlightMeetingTarget()` and the `INFLIGHT_RECORDS` metric, so drift here misreports in-flight work and
can hold a target-based decision open or closed.

## Why it is recorded rather than fixed

The two fixes above each replaced a *maintained* number with something conserved or owned, and both had a
natural owner to hang the accounting on - the map for one, the container for the other. In-flight has one too
(`WorkContainer`'s `Execution` state), which is what makes deriving this plausible rather than speculative:
`getCountWorkInFlight()` already answers it by scanning, so the question is whether a per-shard or per-manager
owned count is worth the change over a scan that is only read per control loop.

Measure before replacing it. `ProcessingShard.getCountWorkInFlight()` summed across shards is the independent
truth to hold it against, the same way `ShardManager.countRecordsInShardsByScan()` and
`ProcessingShard.countSelectionClaimedByScan()` are used for the two that were fixed - replacing state that
turns out never to disagree is a refactor, and replacing state that does is a fix.
