# What the mutation lane found in PartitionState, OffsetMapCodecManager and RemovedPartitionState

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

The first two real runs of the PR-scoped mutation lane (after astubbs#371 fixed it) scored
astubbs#344's changed classes and reported four survivors, 88% test strength. This note is that
finding worked out, plus a wider local sweep of the same three classes.

**Reproduce rather than trust the numbers here** - they are a point-in-time measurement and the
report artifacts are not durable
([`ci-mutation-survivor-list-is-unreadable.md`](ci-mutation-survivor-list-is-unreadable.md) is why
that is currently expensive):

```bash
./mvnw --batch-mode -Pci test-compile org.pitest:pitest-maven:mutationCoverage \
  -Djacoco.skip=true \
  -DtargetClasses='bz.stub.parallelconsumer.offsets.OffsetMapCodecManager,bz.stub.parallelconsumer.offsets.OffsetMapCodecManager$*,bz.stub.parallelconsumer.state.PartitionState,bz.stub.parallelconsumer.state.PartitionState$*,bz.stub.parallelconsumer.state.RemovedPartitionState,bz.stub.parallelconsumer.state.RemovedPartitionState$*' \
  -DtargetTests='bz.stub.parallelconsumer.*' \
  -DexcludedTestClasses='bz.stub.parallelconsumer.integrationTests.*,bz.stub.parallelconsumer.state.*Lincheck*' \
  -DoutputFormats=XML,HTML -Dthreads=4 -pl parallel-consumer-core -am
```
then parse PIT's generated `mutations.xml` - it lands under the core module's `target/pit-reports/`
directory - for `status='SURVIVED'` and `status='NO_COVERAGE'`. The lane's own scope is narrower than this sweep -
it mutates changed classes in decidable packages only - so the CI figure and the sweep figure are
not comparable, and neither is wrong.

## The one on astubbs#344's own new code, and why it is probably NOT a test gap

`PartitionState#getIncompleteOffsetsBelow` - **changed conditional boundary** on the filter
`x -> x < highestSucceededBound`, mutated to `<=`. It survives: nothing distinguishes the two.

**Read this as an equivalent mutant, not a missing assertion.** The bound is a sample of
`offsetHighestSucceeded`, and an offset that has succeeded is by construction absent from
`incompleteOffsets` - `onSuccess` removes it before raising the mark. So `<` and `<=` cannot differ
while that invariant holds, and PIT has no way to know the invariant exists.

The useful response is therefore **not** a boundary test, which would pin an accident. If it is
worth pinning at all, pin the invariant - that the high-water offset is never a member of the
incomplete set - which is the thing actually keeping the two forms equivalent. Recorded here rather
than acted on, because writing a test to kill an equivalent mutant is how a suite acquires
assertions that describe the implementation instead of the contract.

## The rest on the changed methods: metrics only

`OffsetMapCodecManager#encodeOffsetsCompressed` - two survivors, both **removed call to
`Counter::increment`** (the forced-codec branch and the pack-smallest branch). Deleting a metrics
increment is undetected. Real, low value: these feed dashboards, not correctness. Worth knowing that
the encoder's metrics are unasserted; not worth a test on its own.

## The wider sweep: where the actual gaps are, and they predate astubbs#344

The full-class sweep over the same three classes found substantially more, concentrated in code this
PR never touched. Grouped by shape:

- **Metrics teardown is entirely unasserted.** Every `PCMetrics::removeMeter` call in
  `PartitionState#deregisterMetrics` survives removal, as does the `deregisterMetrics` call itself
  from `onPartitionsRemoved`. Nothing notices if partition metrics are never deregistered - which is
  the leak shape astubbs#57 is about, on a different meter.
- **Back-pressure boundaries.** `updateBlockFromEncodingResult` and `tryToEncodeOffsets` survive
  conditional-boundary changes, `setAllowedMoreRecords` call removal, and having their
  double division replaced with multiplication - the payload-ratio arithmetic is unpinned.
- **State predicates returning a constant.** `isBlocked`, `isBlockingProgress` and
  `RemovedPartitionState`'s `isRemoved` / `isPartitionRemovedOrNeverAssigned` /
  `getOffsetHighestSucceeded` all survive being replaced with a fixed return.
- **No coverage at all.** `couldBeTakenAsWork` (both arms), `hasIncompleteOffsets`, and several
  `RemovedPartitionState` no-op accessors are never reached by the mutated-class test set.

The `RemovedPartitionState` entries are the least alarming: astubbs#344 added
`RemovedPartitionStateTest`, which pins the incomplete-offsets contract specifically, and the
remaining accessors are one-line constant returns whose "contract" is that they return that
constant - close to equivalent-mutant territory again.

**The honest summary is that this class's correctness paths are well covered and its metrics,
back-pressure arithmetic and predicate surface are not.** That is a coherent piece of work, and a
better use of the lane than chasing individual survivors.
