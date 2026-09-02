# The fix for confluentinc#830 catches a shape the reported failure never takes

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->


`ParallelEoSStreamProcessor` catches `InvalidPidMappingException` around the produce-and-ack block and
calls `closeOnException`. That catch was `confluentinc#839`, written to end the infinite retry loop
reported in astubbs#411 (`confluentinc#830`). **It cannot fire for that report**, so the loop is
plausibly still live on master.

## Why it cannot fire

`FutureRecordMetadata.valueOrError` raises `new ExecutionException(exception)`, so the ack wait's
`get` can only ever surface an `ExecutionException`. The typed catch does not match it. Control falls
through to the generic handler that wraps it as
`PCInternalRuntimeException("Error while waiting for produce results", e)` — which is **verbatim the
stack trace in the upstream report**, from a build that already carried the fix.

From there it is an ordinary user-function failure: the record is marked failed and re-dispatched onto
the same producer, which is still invalid. That is the reported spin.

The typed catch fires only when `ProducerManager.produceMessages` throws *synchronously*. That is what
`ParallelEoSStreamProcessorTest.closePCWhenInvalidPidMappingException` mocks
(`when(producerManager.produceMessages(any())).thenThrow(...)`), which is why the test passes without
covering the path the reporter hit.

## What is not yet established

- **Whether the spin is reachable in practice today**, as opposed to on the reporter's 0.5.2.8. The
  reasoning above is from the current tree and the kafka-clients 3.9.2 sources; nobody has reproduced
  it. A reproduction wants a producer whose send future completes exceptionally — a `MockProducer`
  with `errorNext`, or a broker-side producer-id expiry.
- **How wide the class is.** `InvalidPidMappingException` is the reported instance; every transactional
  condition arriving from a send future has the same shape, and `UnknownProducerIdException` — what a
  partition leader returns once producer state expires from inactivity, the reporter's trigger — is the
  same story.

## Relationship to astubbs#225

**Not fixed by it.** The recovery work
(`docs/plans/2026-09-02-001-feat-recoverable-producer-fencing-plan.md`) requires detection
to unwrap before matching, but only on the path where PC builds its own producer. Every user today
supplies a `Producer` instance, which that plan deprecates and explicitly leaves on current behaviour —
so the reporter's own configuration keeps this defect until they migrate. The plan names the exclusion
in its Scope Boundaries rather than absorbing it, which is why this note exists.

Two ways out, and the choice is a product one:

1. Extend the unwrapping to the producer-instance path, so the condition at least reaches the terminal
   close `confluentinc#839` intended. Small, and makes the deprecated path honest.
2. Leave it, and let migration be the fix. Cheaper, but ships a known spin in the deprecated path for
   as long as that path exists — which the queued removal in `docs/refactoring.md` bounds but does not
   end soon.

## Do not re-derive

The `confluentinc#839` catch *looks* correct on inspection, and its test is green. Three review rounds
on that plan each re-examined this area; the discovery came from reading
`FutureRecordMetadata.valueOrError` against the report's stack trace, not from the PC code alone.
Anyone auditing the produce path should start there rather than at the catch.
