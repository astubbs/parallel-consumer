# The Kafka Streams task lifecycle under PC dispatch is untested, and diverges in six known ways

For `parallel-consumer-streams` (astubbs#255), and **relevant to the Connect module too** - it wraps a
task whose lifecycle the framework drives the same way, so most of these questions transfer.

Recorded here rather than only in the plan because a downstream branch scans this directory and will
not read another branch's plan. Planned as U10 (taken together with pile B, the close/suspend/recycle
failures in Kafka's own suite).

## What is actually proven today

**One partition, one task, one instance.** Every integration proof this module has runs that shape.
Multi-task, multi-instance and rebalance behaviour are *unexercised* - not "imperfect", untested. That
is the honest headline limit, and it is bigger than the feature list.

## The six divergences

1. **The suspend-drain pump runs after every flow's final commit.** In 3.9.2, revocation and clean close
   commit *before* `suspend()`, so the records the drain processes produce output that post-dates the
   final commit: **duplicates on routine rebalances**, and recycled tasks force-dirtied when a backlog
   exists. The design call is whether the PC path commits the post-drain frontier itself, or the drain
   moves ahead of the commit in the teardown order.
2. **A revived task keeps its closed dispatcher.** ~~Silent permanent stall~~ - **now fails loudly**:
   patched `StreamTask.revive()` throws rather than accepting records into a dispatcher that will never
   hand them out. Recreating the dispatcher so revival actually works is still open.
3. **`prepareRecycle()` never closes the dispatcher.** Active/standby recycling leaks the registry
   entry, the worker pool, and the WorkManager's partition state. Dormant only because no test
   configures standby replicas.
4. **A timed-out drain falls through to `closeTopology()`** with workers still inside the chain.
5. **`updateInputPartitions()` never reaches the dispatcher** - stale partition set under cooperative
   rebalancing.
6. **`onOffsetCommitSuccess` has no epoch guard.** A stale ack could clear a reassigned partition's
   dirty flag. Not reachable through today's one-dispatcher-per-task wiring; becomes reachable if that
   changes.

## What this blocks, and what it does not

**Not the technical preview.** The preview's contract is at-least-once inside a stated envelope, and
rebalance duplicates sit inside that contract. What the preview owes is **disclosure** - saying plainly
that rebalance is unexercised.

**Production, yes.** None of these should reach someone running this for real.

## Delete when

The multi-instance test exists and each of the six is either fixed with a test or re-recorded with
evidence that it does not bite.
