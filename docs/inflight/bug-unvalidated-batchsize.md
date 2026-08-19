# `batchSize` is unvalidated, and zero fails in two different ways

<!-- inflight-class: config-lie -->


Tracked as astubbs#311, alongside the arithmetic defect in
[`bug-batch-quantity-over-request.md`](bug-batch-quantity-over-request.md) - same config field, same
one-PR fix, so one issue covers both. Kept as its own note because this directory is one item per
file, and this is a different kind of defect: not wrong arithmetic, but **absent input validation**.

Lower priority than its sibling. That one is a defect the library inflicts on a correct
configuration; this one needs a caller to pass a bad value.

## Nothing bounds it

`ParallelConsumerOptions.validate()` checks a non-null consumer and transaction-mode consistency.
`batchSize` is `@Builder.Default private final Integer batchSize = 1` on a `@Builder(toBuilder = true)`
class with no `@NonNull` and no bounds annotation, and every reference to `batchSize` in main code
across all modules is a read or javadoc. So `.batchSize(0)` is reachable through the public,
`@InterfaceStability.Evolving` builder, as are negatives and - since the field is boxed - `null`.

## Zero fails in two different ways, and neither is a clear error

**Silently, on a default configuration.** `isUsingBatching()` is `getBatchSize() > 1`, so it is false
at zero and `calculateQuantityToRequest` never divides. Instead `getTargetAmountOfRecordsInFlight()`
returns `maxConcurrency * 0` = 0, so `delta` is never positive and no work is ever requested. The
consumer starts cleanly, logs nothing unusual, and processes nothing forever. Negatives behave the
same way.

**Loudly, but only if a buffer size is set.** `PCModule.initDynamicLoadFactor()` divides
`messageBufferSize` by the same zeroed `getTargetAmountOfRecordsInFlight()`, throwing
`ArithmeticException` at construction. It is guarded by `if (messageBufferSize > 0)`, and
`messageBufferSize` carries no `@Builder.Default`, so the default path takes the `else` branch and
never divides.

`null` is a third shape: NPE unboxing at `getBatchSize() > 1`.

So the same misconfiguration is silent, fatal-at-construction, or an NPE depending on unrelated
settings - which is the argument for validating rather than for documenting.

## Delete when

`validate()` rejects `batchSize < 1` (and null), with a test. One bound closes all three shapes.
