# `PCModule.admissionController()` can construct two controllers

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`PCModule.admissionController()` is an unsynchronized lazy initialiser on a non-volatile field:

```java
public AdmissionController admissionController() {
    if (admissionController == null) {
        admissionController = initAdmissionController();
    }
    return admissionController;
}
```

Every other accessor on that module has the same shape, and for most of them it is safe because only the
control thread ever touches them. This one is not: it is reached from **at least three threads**, and on
the default configuration nothing forces it during construction, so the first touch happens at runtime
from whichever thread gets there first.

- the **control thread**, every loop pass (`tickAdmissionController`, `sampleAdmissionInFlight`)
- the **broker-poll thread**, on every rebalance callback (`onPartitionsRevoked` / `onPartitionsAssigned` /
  `onPartitionsLost` in `AbstractParallelEoSStreamProcessor`)
- **user or test code** holding the module, which is a documented seam

`initDynamicLoadFactor` forces the controller during processor construction **only** when
`messageBufferSize > 0`; on the default path (`messageBufferSize == 0`) the `enforceRequested` branch
returns without touching it. So the field is genuinely null when the engine starts.

## Why it matters, and why it is `misdirection`

Two threads racing the first touch both see null and both construct. One wins the field; whichever caller
cached the other keeps a live-looking controller that **nothing ever ticks**. It does not throw, does not
log, and reports a perfectly plausible target - its seed, forever.

The split is worse than a duplicate object. Rebalance callbacks and the tick arrive on different threads,
so the losing instance can be the one receiving `onPartitionsAssigned` while the winning one is the one
making decisions: the R13 rebalance freeze would then never fire on the controller that is actually
steering admission. The `pc.admission.*` meters would also be registered twice.

## How it was found

Writing `AdaptiveConcurrencyEnforceIT` (astubbs#333 follow-up). The test took its controller reference
from the module immediately after `poll()` - i.e. racing the control loop's first pass - and then watched
a target that sat at its seed of 2 for the full ninety-second await, while the log printed a complete
`2 -> 12` ramp from the other instance in the same JVM, one line at a time.

That is the whole diagnostic value here: the failure mode looks exactly like *the feature does not work*.

## Options

1. `synchronized` on the accessor (cheapest; the call is once per loop pass, uncontended).
2. Force it in `AbstractParallelEoSStreamProcessor`'s constructor, next to `module.pcMetrics()` and
   `module.dynamicExtraLoadFactor()` - single-threaded there, and it matches how those two are already
   pinned. Note `initAdmissionController()` calls `pcMetrics()`, so the ordering constraint is already
   satisfied at that point.
3. Audit the rest of `PCModule` for accessors reachable off the control thread and fix the class of
   defect rather than this instance. `workManager()`, `consumerManager()` and `brokerPollSystem()` are
   the ones worth checking - they are touched from more than one thread once the engine is running.

Not fixed here: `AdaptiveConcurrencyEnforceIT` takes its reference **before** `poll()` starts any thread,
which is deterministic, and carries an `isSameInstanceAs` guard that fails loudly if the module's field
ever stops holding the instance the test is asserting on.
