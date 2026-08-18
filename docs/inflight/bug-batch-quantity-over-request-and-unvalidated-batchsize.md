# Batching requests a full extra in-flight target, and `batchSize` is unvalidated

Tracked as astubbs#311. Both defects sit in `calculateQuantityToRequest` and the config surface it
reads, and both are gated on `isUsingBatching()` - **a default configuration (`batchSize = 1`) is
unaffected**. Found while extracting stranded ideation notes; the batching ideation pass
(`next-batching-enhancements.md`) surfaced the first one, and this note records what reading
`master` then confirmed.

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

## `batchSize = 0` is a silent no-op consumer

`ParallelConsumerOptions.validate()` bounds nothing. At `batchSize = 0` there is no divide-by-zero,
because `isUsingBatching()` is `getBatchSize() > 1`; instead `getTargetAmountOfRecordsInFlight()`
returns `maxConcurrency * 0` = 0, `delta` is never positive, and the consumer starts cleanly and
processes nothing forever. Negatives behave the same.

## What was checked and ruled out

confluentinc#551 ("Batching not working as expected") and confluentinc#373 (closed, "batchSize(100)
but only getting from 1 to 3 messages") are both **under-filled batches** - the opposite symptom -
and neither touches request quantity or config validation. No note on `master` records either
defect; the two existing mentions of `calculateQuantityToRequest` in `docs/solutions/` use it as an
arithmetic reference in flakiness write-ups.

## Delete when

astubbs#311 is fixed - `batchSize - modulo`, a `validate()` bound rejecting `< 1`, and tests. The
method has **no test coverage at all** today: it appears only in its definition, its one call site,
and a comment in `ExternalEngine`.
