# `PCMetrics.registeredMeters` is a plain `ArrayList`, written from both threads

<!-- inflight-type: bug -->
<!-- inflight-impact: crash -->

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

`registeredMeters` is a concurrent collection (or its mutation is guarded), with a test that fails
against the plain `ArrayList`. Worth doing in the same pass as the `HashMap` stragglers - same class,
same file neighbourhood, one PR.
