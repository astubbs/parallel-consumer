# `PCMetrics.registeredMeters` is a plain `ArrayList`, written from both threads

<!-- inflight-type: bug -->
<!-- inflight-impact: crash -->

## Already known, already being fixed - astubbs/parallel-consumer#57

The collection is not a new discovery. astubbs/parallel-consumer#120 (open, the fork mirror of
confluentinc#859, "Memory leak in PCMetrics class") has reported the same field as a leak since
before this branch existed, and astubbs/parallel-consumer#57 (open) carries the fix: `registeredMeters`
becomes a `LinkedHashSet`, and every one of the nine mutation sites moves under a private
`metersLock` monitor rather than the `synchronized(this)` a review rejected. That closes the race
described below as well as the leak - the two share one field and one fix.

**What IS new here is the evidence, not the defect.** The leak report reasoned about growth; what the
Lincheck harness produced is a reproduced `ArrayIndexOutOfBoundsException` out of `ArrayList.add` on
the commit path, with the interleaving that caused it. So this note's job is to hold the race
evidence until astubbs/parallel-consumer#57 lands - not to claim an independent finding.

`PCMetrics` keeps every meter it registers in `private List<Meter.Id> registeredMeters = new
ArrayList<>()`, and adds to it from `getCounterFromMetricDef`, `getTimerFromMetricDef` and their
siblings. Those are called on the **commit path** - `OffsetMapCodecManager.getCounterMeterForEncoding`
runs once per encode - and the commit path runs on the control thread or the broker-poll thread
depending on commit mode. Two threads growing an `ArrayList` is a data race with no lock and no
concurrent collection anywhere in it.

## Reproduced, not reasoned

```
java.lang.ArrayIndexOutOfBoundsException: Index 11 out of bounds for length 10
	at java.util.ArrayList.add(ArrayList.java:455)
	at bz.stub.parallelconsumer.metrics.PCMetrics.getCounterFromMetricDef
	at bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.getCounterMeterForEncoding
	at bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.encodeOffsetsCompressed
	at bz.stub.parallelconsumer.state.PartitionState.tryToEncodeOffsets
	at bz.stub.parallelconsumer.state.PartitionState.createOffsetAndMetadata
```

Two concurrent `createOffsetAndMetadata` calls, no injected seam. `ArrayIndexOutOfBoundsException` out
of `ArrayList.add` is the classic torn-grow signature: both threads read the same `size`, one grows the
backing array, the other writes past the array it had already loaded. The exception is the *lucky*
outcome - the ordinary one is a silently dropped or duplicated `Meter.Id`, which then makes
`removeMeter`/deregistration miss a meter on partition revocation.

## A second sighting, from a different path

Two concurrent rebalances - `onPartitionsRevoked` then `onPartitionsAssigned`, both threads - also throw
`ArrayIndexOutOfBoundsException`, out of the counter maps those callbacks register and deregister
(`slowWorkCounters` in `PartitionStateManager`, `succeededRecordsCounters` / `failedRecordsCounters` in
`WorkManager`, all plain `HashMap`s, plus `registeredMeters` underneath them). That report carried no
stack, so which collection tore is not established - the class of defect is.

**That particular scenario is not reachable in production** and is not the argument for fixing this:
`ConsumerRebalanceListener` callbacks are invoked by the consumer from inside one `poll`, so two
rebalances never overlap. It is recorded because it is a second, independent demonstration that the
metrics collections on these paths are unguarded, and the FIRST sighting above - two commits - is on a
path that genuinely does run on two threads.

## Provenance, and why it is not in the torn-read dossier

Found by the Lincheck proof of concept
(`docs/plans/2026-08-25-001-test-lincheck-poc-plan.md`), from a harness aimed at the commit-path
torn reads. It is **not** one of the four the hunt had already named, and it is not a torn read: it is
plain unsynchronised mutation. It is the PoC's evidence that the tool finds things nobody pointed it
at.

Adjacent but distinct from the dossier's "unsynchronized cross-thread `HashMap`s" straggler
(`slowWorkCounters`, `succeededRecordsCounters`, `failedRecordsCounters`) - those are in
`WorkManager`/`PartitionStateManager`; this one is `PCMetrics`' own registry, and it is on a hotter
path.

## Delete when

astubbs/parallel-consumer#57 lands, which guards every mutation of `registeredMeters` behind
`metersLock`. Delete this note in that PR, and carry the reproduced stack above into it as the
regression test's motivation.

The `HashMap` stragglers are NOT covered by it: `gh pr view 57 -R astubbs/parallel-consumer --json
files` shows `WorkManager.java` untouched, and its `PartitionStateManager.java` edit is about
re-registration rather than about making `slowWorkCounters` concurrent. They need their own pass -
same defect class, different fields.
