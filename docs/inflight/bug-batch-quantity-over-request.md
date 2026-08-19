# Batching requests a full extra in-flight target of work

<!-- inflight-type: bug -->
<!-- inflight-impact: throughput -->


Tracked as astubbs#311. Gated on `isUsingBatching()`, so **a default configuration (`batchSize = 1`)
is unaffected** - but every configuration that actually uses batching is affected, continuously.
Surfaced by the batching ideation pass (`next-batching-enhancements.md`) and confirmed by reading
`master`. The `batchSize` validation gap this note
originally carried is now [`bug-unvalidated-batchsize.md`](bug-unvalidated-batchsize.md), tracked
under the same issue; that one needs a caller to pass a bad value, this one is the defect the
library inflicts on a correct configuration.

## The arithmetic is wrong, and it fires almost always

`AbstractParallelEoSStreamProcessor.calculateQuantityToRequest`:

```java
int modulo = delta % batchSize;
if (modulo > 0) {
    int extraToFillBatch = target - modulo;   // should be batchSize - modulo
    delta = delta + extraToFillBatch;
}
```

The variable name states the intent the arithmetic does not implement. `target` is
`maxConcurrency * batchSize * loadFactor`, hence always an exact multiple of `batchSize`, so
`modulo` is zero only when `current` happens to be a multiple of `batchSize` too - and records
complete individually, so in steady state it is not. With `maxConcurrency=16, batchSize=10`
(`target=160`) the intended extra is at most 9 and the actual extra is about 155. **In-flight
settles at roughly 2x the configured target.**

Nothing downstream clamps it: `delta` reaches `ShardManager.getWorkIfAvailable`, whose
`workFromAllShards.size() < requestedMaxWorkToRetrieve` loop treats it as a ceiling.

## The second-order effect is the one worth remembering

`lastWorkRequestWasFulfilled = gotWorkCount >= delta`, and an inflated `delta` makes it false. It
gates the adaptive load factor in `checkPipelinePressure`:

```java
if (isPoolQueueLow() && lastWorkRequestWasFulfilled) {
```

So under batching the load factor stops stepping up **precisely when the pool queue is low** - the
inflation meant to fill batches suppresses the adaptation meant to fill the pool. Silent; TRACE only.

No data-safety consequence - no loss, no duplication beyond normal at-least-once - which is the
likely reason it has gone unremarked. The costs are memory held, a larger incomplete-offset map
reaching the 4096-byte commit-metadata cap sooner than configured (the surface astubbs#192
measures), and more in-flight work abandoned on rebalance.

## Independently verified

The automated review on astubbs#312 checked the claim from a fresh context against `master` and
confirmed each step: the wrong operand, `target` always being a multiple of `batchSize` via
`getTargetAmountOfRecordsInFlight() = maxConcurrency * batchSize`, and the
`lastWorkRequestWasFulfilled` gate. It recorded the 2x figure as "correct arithmetic, not an
approximation".

## Other instances of the class - searched, one found, and it is clean

The class is *rounding up to a multiple using the wrong operand*. Every modulo in main code was
searched, untruncated: eight raw hits, of which one other is real arithmetic.
`PCModule.initDynamicLoadFactor()` computes `a / b + (a % b == 0 ? 0 : 1)`, a correct ceiling
division over a single pair of operands - not the wrong-third-variable shape. So the defect is a
single site, not a pattern. (That same expression divides by a value `batchSize = 0` zeroes, which
is why it appears in the sibling note; that is a different defect.)

## What was checked and ruled out

confluentinc#551 ("Batching not working as expected") and confluentinc#373 (closed, "batchSize(100)
but only getting from 1 to 3 messages") are both **under-filled batches** - the opposite symptom -
and neither touches request quantity. No note on `master` records this defect; the two existing
mentions of `calculateQuantityToRequest` in `docs/solutions/` use it as an arithmetic reference in
flakiness write-ups.

## Delete when

astubbs#311 is fixed - `batchSize - modulo`, with a test. The method has **no test coverage at all**
today: it appears only in its definition, its one call site, and a comment in `ExternalEngine`, so
the fix has to bring its own first test.
