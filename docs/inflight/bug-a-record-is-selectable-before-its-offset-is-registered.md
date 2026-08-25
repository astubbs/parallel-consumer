# A record becomes selectable before its offset is in `incompleteOffsets`

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

Recorded 2026-08-22 on `research/market-analysis-recut`. **Latent, not observed.** It is the second
instance of the defect class that produced the direct-pull double delivery, and it is left open here
because closing that one did not close this one.

`PartitionState#maybeRegisterNewPollBatchAsWork` does, in this order:

```java
getShardManager().addWorkContainer(epochOfInboundRecords, aRecord);
addNewIncompleteRecord(aRecord);
```

so a record is visible to shard scanners **before** its offset is in `incompleteOffsets`. Under the
direct-pull engine those scanners are worker threads, not the control loop.

**Why it is closed today, and why that is not a guarantee.** Registration and completion both run on
the control thread, so a worker cannot get a verdict back while registration is in progress. Nothing
in the code states that invariant, nothing tests it, and the engine that broke every other
single-threaded assumption in selection did not break this one only because it does not change who
*registers* work.

**It is recorded because of what it would look like.** It is the one shape that fires
`PartitionState#onSuccess`'s `assert (removedFromIncompletes)` with **no double delivery at all** -
so anyone meeting that assert again, and correctly ruling out the claim (now a single atomic
transition, and covered by `WorkClaimStateMachineTest`), needs this on the list rather than in front
of them for the first time.

## What settling it looks like

- **Swap the two lines**, so the offset is registered before the record is reachable. Cheap, and the
  reverse order has no reason to be preferred - but establish first whether anything depends on the
  container existing before the offset does.
- **Or state the invariant and test it**: registration is control-thread-only. A test that drives
  registration from a second thread while pullers scan would fail today, which is the honest way to
  find out whether the ordering matters.

**Do not treat the assert as proof of a double delivery.** That inference was correct for the
2026-08-22 sightings - every one carried `deliveryCount == 2` - but it is the *evidence* that made it
correct, not the assert.
