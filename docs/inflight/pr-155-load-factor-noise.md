# #155 (upstream #402) - "Max loading factor steps reached" log noise

Branch `fix/155-load-factor-noise`. Fixes the log spam half of #155; the stall half of that report was
already fixed (upstream #547/#606 plus the #857 family - see `bug-857-family.md`), and nothing here
touches it.

## The mechanism, as confirmed in the code

`AbstractParallelEoSStreamProcessor#checkPipelinePressure()` runs on **every** control loop pass and
logged `WARN isPoolQueueLow(): Max loading factor steps reached: {}/{}` with no rate limiting whenever
`DynamicLoadFactor#isMaxReached()` held. Two configurations reach that state, and both spam:

- **Dynamic factor at its cap** (the reported `100/100`). Once the queue stays below target for long
  enough to step 2 -> 100, the condition is permanent and the line repeats for the life of the process.
- **Fixed factor** - `PCModule#initDynamicLoadFactor()` builds `DynamicLoadFactor(n, n)` when
  `messageBufferSize` is set, so `isMaxReached()` is true from construction and the WARN fires from the
  very first pass. That is what the README's own PARTITION-mode tuning advice tells people to do.

Reproduced in `LoadFactorCeilingReportingTest`: 500 control loop passes produced 500 warnings in each
case before the fix.

## What was decided, and why

- **The buffering behaviour is untouched.** Only the reporting changed - the factor, the queue target
  and the step-up rules are exactly as before.
- **A fixed factor reports at DEBUG, not WARN.** `DynamicLoadFactor#isStaticFactor()` is true when the
  factor starts at its own ceiling (`messageBufferSize`, or `initialLoadFactor == maximumLoadFactor`).
  There is nothing to step to and nothing wrong, so there is nothing to warn about.
- **A dynamic factor at its cap still WARNs** - it says the in-flight target will not grow further,
  which a user may want to act on - but rate limited to once per 30s (`RateLimiter`, as already used by
  `BrokerPollSystem` and `ProcessingShard`), and reworded so it reads as saturation rather than failure.
  Deliberately NOT demoted: weakening a real signal to fix a volume problem would be the wrong trade.
- **The numbers in the message name what the code actually compared.** Review caught a first cut that
  printed `getQueueTargetLoaded()` (target x factor) as the threshold the queue was "below", when the
  branch was entered by `isPoolQueueLow()` comparing against the un-multiplied `getPoolLoadTarget()` -
  `0 queued vs 1008` where the real test was `0 vs 16`. Both numbers are useful and both are now
  reported, each labelled: the pool queue target is what the queue was measured against, the loaded
  target is the in-flight buffer the factor scales. `LoadFactorCeilingReportingTest` now asserts on the
  values, which is what let the mismatch through in the first place.

## Left open

`DynamicLoadFactor`'s warm-up and cool-down are hard-coded `Duration` fields with no seam, so reaching
the dynamic ceiling for real costs one cool-down per step (minutes for 2 -> 100). The test therefore
asserts the terminal state via a subclass instead of stepping there. Making those injectable (via
`PCModule`) would let the stepping schedule itself be tested; nobody has needed it yet.
