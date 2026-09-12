# The public surface hands out live engine objects, and nobody has decided whether it should

<!-- inflight-type: task -->
<!-- inflight-impact: reliability -->
<!-- post-merge: checked - every mention of astubbs#506 here is past tense about what that PR did to two accessors, which stays true once it lands; the note is about the surface, not about the PR -->
<!-- inflight-state: deferred - needs a product decision: does this fork's published surface return live engine objects or projections of them? Two accessors widened by astubbs#506 are blocked on the same answer, and neither is urgent because the exposure predates them -->

<!-- post-merge: checked-begin - what astubbs#506 did is history once it lands; the exposure it widened is what this note is about -->
A user who can reach a `WorkManager` or a `PartitionState` can call the engine's own mutators on it.
This is **not new**, and astubbs#506 did not open it - it widened it twice, and the fluent API's
`ConsumerHandle.parkedContainers()` hands one of the two lists to end users, which is where the
question stops being theoretical.
<!-- post-merge: checked-end -->

## What is reachable, and what it would do

Grep the mutators rather than trusting this list, which is a snapshot:
`grep -n 'public void' parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/state/PartitionState.java`
and the same for `WorkContainer.java`.

- **`PartitionStateManager.getAssignedPartitions()`** returns an unmodifiable map whose **values are
  live**. `PartitionState.fenceForRevocation()` is public on them, and calling it would make the
  engine treat every record on that partition as stale.
- **`ShardManager.getParkedWorkContainers(boolean)` / `WorkManager.getParkedWorkContainers(boolean)`**
  return live `WorkContainer`s. `setFuture`, `endFlight`, `onUserFunctionSuccess` and
  `onUserFunctionFailure` are all public on them, so a caller can record a verdict the user function
  never gave.

<!-- post-merge: checked-begin - a statement about what a past review concluded and what a landed PR changed; neither moves on merge -->
**The exposure is pre-existing.** The same live `PartitionState` was already reachable through the
public `getPartitionState(TopicPartition)` on the same public class, and live `WorkContainer`s through
pre-existing public `WorkManager` methods, all of it through the public `getWm()`. Widening two
accessors changed the blast radius, not the boundary - which is why the defect astubbs#506's review
actually charged was a false javadoc claim, corrected in place, and not these methods.
<!-- post-merge: checked-end -->

## The decision

One answer settles both, and it is a boundary choice rather than a defect to patch:

1. **Live objects, documented.** Say on each accessor that the values are the engine's own and are to
   be read, not called. Cheapest, and consistent with what the surface already does. Costs nothing at
   runtime and relies entirely on the user reading the javadoc.
2. **Projections.** A `ParkedRecord`-shaped read-only view per returned element. Makes the contract
   structural instead of advisory, and is the shape the fluent API already prefers for what it hands
   users.

**One measurement exists and does not answer this.** `PartitionState.countParkedNow()`'s javadoc
records that building a `ParkedRecord` per parked record *per gauge per scrape* cost a full key
deserialisation each and was rejected on that basis. **That is a different question**: a projection
built once per user query is not a projection built twice per partition per scrape. Do not cite the
gauge measurement as evidence against option 2.

## Why it is deferred rather than done

Nothing is currently mis-behaving: every in-tree caller of the two widened accessors is inside the
class that owns them, so today this is a surface the engine offers and nobody misuses. The cost of
deciding late is that a projection is a breaking change once users hold the live type, and
`docs/refactoring.md`'s breaking-change gate is open only while `0.6.0.0` is unreleased - so the
cheap window is this release line.

## Not the same question as astubbs#139

[`core-139-public-api-thread-safety-contract.md`](core-139-public-api-thread-safety-contract.md) asks
**which thread** may call a published method; this asks **what the method hands back**. They meet on
one method - a live object handed to a user's thread is both questions at once - and its definition of
done (a per-method javadoc sentence naming the callers) is the mechanism option 1 would use. Answer
them together if both are picked up, but neither subsumes the other.

## Delete when

The decision is made and applied to both accessors, or the surface is deliberately kept as-is and each
accessor says so in its javadoc.
