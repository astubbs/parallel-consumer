# Key ordering across a rebalance - the one route to "same record, two threads at once"

<!-- inflight-type: task -->
<!-- inflight-impact: reliability -->
<!-- inflight-labels: concurrency -->

Triage of [astubbs#178](https://github.com/astubbs/parallel-consumer/issues/178), the fork mirror of
[confluentinc issue #843](https://github.com/confluentinc/parallel-consumer/issues/843): one record
run by two `pc-pool-1-thread-*` threads in one pod, 70ms apart, on 0.5.3.0.

## confluentinc#909 is NOT the cause - prediction refuted

`src/docs/development/upstream-pr-analysis.adoc` carries "PR #909 likely root cause" for <!-- issue-refs: exempt -->
confluentinc#843, unverified since it was written. It is wrong on both terms:

- **Wrong consequence.** The confluentinc#909 defect *drops* the fresh container and wedges the
  offset - anchor `already exists in shard queue, dropping record` in `ProcessingShard.java`. Loss and
  stall, not duplication. astubbs#322's reproduction counts lost records, never a duplicate. <!-- post-merge: checked - names the reproduction by the PR that added it, so it holds once that PR is on master -->
- **Wrong precondition.** It needs a rebalance inside the registration loop. The reporter states there
  was none, no retry and no load peak.

Same for confluentinc#893, which is offset accuracy on assignment - also rebalance-gated.

## The only inspected route that produces the symptom

Not the reporter's scenario, but it is the one mechanism in the tree that puts the same key on two
worker threads at once *within one instance*, and nothing names it yet:

1. A container is taken as work (`onQueueingForExecution` in `WorkContainer.java`) and stays resident
   in its shard; a worker is inside the user function.
2. A revoke bumps the epoch and sweeps - anchor `removeStaleWorkContainersFromShard` in
   `ProcessingShard.java`, reached via `ShardManager.removeStaleContainers()`, which
   `PartitionStateManager.java` calls on both assign and remove. (Do not anchor on the comment
   `remove stale work containers after partition epoch changed` - it occurs twice in that file,
   so it does not identify a site.) That method tests staleness only and **never asks whether the container is in flight**. The worker keeps
   running: `onPartitionsRevoked` in `AbstractParallelEoSStreamProcessor.java` commits and truncates,
   it does not drain.
3. The partition comes back to the same consumer. The offset was never committed, so it re-delivers,
   the shard slot is now free, and the fresh container is takeable - the key-ordering restriction only
   consults `entries`, and the old container is no longer in it.
4. Both run. `handleFutureResult` discards the old result (anchor `Dropping work from revoked
   partition` in `WorkManager.java`), so PC's *bookkeeping* stays correct while the user function has
   already executed twice, concurrently, on one key.

The sweep route is in 0.5.3.0 (`git tag --contains bab2c6b7a`), so it is not fork-introduced.
astubbs#31 added a second way into the same state: the `Replacing stale entry (epoch {})` path frees
the slot even when the sweep missed it.

**README promises "strong ordering by key".** Whether an undrained old-epoch delivery is a violation
or legitimate at-least-once is a maintainer call, and it is the blocker here.
`KeyOrderLedger.java`'s javadoc reaches the same fork - it says choosing the bound on how long an
old-epoch delivery may still run before it counts as a violation is "the whole job" (grep
`picking that number is the whole job`).

## Next step, and what it is not

The detector is an **analysis**, not instrumentation. `KeyOrderLedger` already asserts
`LEDGER_KEY_CONCURRENCY`, but its window key includes the epoch (`"|e" + epoch`), so a cross-epoch
overlap falls into two windows and passes. Every `Delivery` already carries what a cross-epoch check
needs and the full history is retained - see the "Recorded but not yet analysed" section of
`docs/testing.md`. `ChaosRevokeUnderWorkKeyOrderIT` is the scenario cell to run it under. Cost: the
comparison is small, the bound is the decision above.

**That still would not settle confluentinc#843**, which had no rebalance. Nothing was ever supplied
that could: the reporter never logged partition or offset at pickup, said detailed logging was added,
and has not returned since 2025-03. Their own uniqueness key is `groupId`+`taskId` while the Kafka key
is the item id, so two records at different offsets carrying one logical task remain unexcluded.
Cannot reproduce - not "did not happen".

## The information request, ready to post

The issue is `wait for info` and the ask was never made precisely, so here it is. Post to
`astubbs/parallel-consumer#178`, and cross-post to confluentinc#843 only if the reporter is still
watching there. Not posted by triage.

> Re-reading this with fresh eyes, the analysis above still holds - work selection is single-threaded
> and the in-flight flag is written and read by that one thread - so a duplicate dispatch with no
> rebalance is not reachable by reading the code. There is exactly one route we can find that puts one
> key on two threads in a single instance, and it needs a rebalance. Four things would separate them,
> and the detailed logging you added should already carry the first two:
>
> 1. **Partition and offset for both pickups.** If both threads logged the same partition and offset,
>    it is one record dispatched twice and we have a real bug. If they differ, it is two records
>    carrying one logical task, and the trail moves to your producer.
> 2. **Consumer-group events for that pod in a wider window** - the minutes either side, not just the
>    second the two log lines span. `Revoke previously assigned partitions`,
>    `Successfully joined group with generation N`, or a heartbeat failure is enough. A rebalance
>    shortly *before* the first pickup would fit the mechanism we found; a rebalance inside the 70ms
>    gap would not.
> 3. **Your key vs your identity.** You partition by item id but identify a task by `groupId`+`taskId`,
>    so two records with different keys can carry one task and key ordering would not serialise them.
>    Worth confirming the two log lines came from the same key.
> 4. **The exact Parallel Consumer version**, and, if you can reproduce, DEBUG on
>    `io.confluent.parallelconsumer.state` (`bz.stub.parallelconsumer.state` on this fork). The lines
>    that matter are the stale-container removals and `Replacing stale entry`.
>
> If (1) shows the same partition and offset with no rebalance anywhere near, that is a new mechanism
> and we will build a reproduction for it directly.

If nothing arrives, do not close as "not a bug" - close as **not actionable without a reproduction**,
citing the two exclusions above. The mechanism this note records is separate and stays open either
way.
