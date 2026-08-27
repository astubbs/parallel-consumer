# `getEpochOfPartition` is documented nullable and is unboxed into a `long` on assignment

<!-- inflight-type: bug -->
<!-- inflight-impact: crash -->

`PartitionStateManager.getEpochOfPartition` returns `Long` and its javadoc says **"or null if not
yet assigned"**. `OffsetMapCodecManager` takes that value and passes it straight to
`PartitionState`'s constructor, whose first parameter is a primitive `long`. **Two sites, both
inside `loadPartitionStateForAssignment`** - grep `getEpochOfPartition` in that file:

```java
// decodePartitionState - a partition that HAS commit history
var epoch = module.workManager().getPm().getEpochOfPartition(tp);
return new PartitionState<>(epoch, module, tp, incompletes);

// the default-entry lambda - a partition with NO commit history
var epoch = psm.getEpochOfPartition(topicPartition);
PartitionState<K, V> defaultEntry = new PartitionState<>(epoch, module, topicPartition, HighestOffsetAndIncompletes.of());
```

The auto-unbox is the dereference. A null epoch throws `NullPointerException` on the rebalance path,
while building partition state.

**The second site is the more interesting one and Infer never named it.** It runs for partitions
with no commit history - a fresh assignment - which is exactly the state "not yet assigned"
describes, so if either site can see a null epoch it is that one. It is not new: it predates the
`io.confluent` -> `bz.stub` rename. A fix has to cover both, and a fix that only covers the reported
one would leave the likelier path open while retiring the finding that pointed at the area.

## Why this has its own note now, and what silences it is NOT yet known

**Infer stopped reporting it, and nothing about the dereference changed.** The identity
`NULLPTR_DEREFERENCE OffsetMapCodecManager.lambda$loadPartitionStateForAssignment$2` has been in
`config/infer-known-findings.txt` since the ratchet was written (`d8d333972`). It fires on master. It
<!-- post-merge: checked -->
did not fire on astubbs#57, whose main-code diff is `PCMetrics`, `PCMetricsDef`,
`PartitionStateManager` and `ShardManager` - none of which is this dereference, its javadoc, or
`PartitionState`'s constructor.

<!-- post-merge: checked-begin -->
**A first explanation was proposed and then falsified, which is why the mechanism is stated as open
rather than guessed at again.** astubbs#57 briefly removed `@Setter` from `PCModule.workManager`, and
that looked like the cause: with the setter gone, Infer could no longer construct a path where
`module.workManager()` returns null. The removal was reverted at merge prep for an unrelated reason -
master's `d702e2ca2` added a test that calls the setter - and the lane stayed **green with the
identity still absent**. Setter present, finding still not reported. The explanation was wrong.

What remains is the caching change in `PartitionStateManager`: a throwaway
`new OffsetMapCodecManager<>(module)` per assignment became one field initialised in the constructor.
That is the only other edit on the branch that touches this call chain, so it is the candidate - but
it has not been demonstrated, and Infer is not installed locally here to demonstrate it.
<!-- post-merge: checked-end -->

**The defect claim does not depend on any of that.** It is read off the source above: the javadoc
says nullable, the constructor takes a primitive, and the unbox is the dereference. Whatever changed
the analyser's reach, the code did not change, and a green Infer lane on a branch that carries this
change is reporting reach rather than correctness.

The ratchet has no state for "silenced" - `bin/infer-test.sh` reads "no longer fires" as "somebody
fixed it and did not ratchet" - so the only way to a green lane was to retire the line. It was
retired alongside four `PCMetrics` identities that genuinely were fixed, which is exactly how a
silenced finding gets mistaken for a fixed one. This note is what stops that.

## What it needs

A decision, not a patch, which is why it is not simply fixed here: null means *not yet assigned*, so
`decodePartitionState` has to choose what to do with a partition being decoded before its epoch
exists. That is the same shape as the open question in
[`core-stale-arrival-guard-needs-a-null-safety-decision.md`](core-stale-arrival-guard-needs-a-null-safety-decision.md)
- fail open or fail closed - on the most safety-critical path in the library, and it should not be
answered inside a metrics PR.

**Whoever fixes it should also decide whether the ratchet needs a third state.** "Fixed" and "new"
are the only two it has, and this finding was neither.

## Delete when

The unbox is guarded or the contract is narrowed to non-null, with a test that fails against the
current code. Re-adding the ratchet line does **not** close it: Infer cannot currently reach the
dereference, so the line would be an identity that never fires - the failure one way round instead
of the other.
