# Bug: the "Max loading factor steps reached" warning, and the static-buffer bug beside it

<!-- inflight-type: bug -->
<!-- inflight-impact: correctness -->
<!-- inflight-labels: release-note -->

Opened 2026-08-21. **Tracked as astubbs#155**, whose title - *"confluentinc#402: Max loading factor
steps reached: 100/100"* - is misleading about what is actually open. Read the issue body, not the
title: this note exists so that does not have to happen twice.

## What astubbs#155 actually says, having read it

**Three separate things are tangled under one title, and only two are open.**

1. **The stall the original reporter hit is FIXED.** Cause was stale work leaking
   `WorkManager#numberRecordsOutForProcessing`, fixed upstream by confluentinc#547 and #606, with
   another reporter on the thread confirming it stopped recurring. The fork has since fixed three
   further stall causes in the confluentinc#857 family. **The warning was never the stall.**
2. **The warning itself is open, and is noise.** It logs at WARN with no rate limiting from
   `checkPipelinePressure()`, which runs every control-loop pass.
3. **A bug beside it is open and was never separately reported** - see below.

**And the answer to the original question**, which is worth repeating wherever this message is
discussed: **it is a saturation signal, not an error.** It means PC has scaled its in-flight target to
the configured ceiling and will not ask for more. On its own it does not explain a stall.

## The defects, verified present in the code 2026-08-21

**Unlimited WARN** - `AbstractParallelEoSStreamProcessor.checkPipelinePressure()`:

```java
} else if (dynamicExtraLoadFactor.isMaxReached()) {
    log.warn("isPoolQueueLow(): Max loading factor steps reached: {}/{}", ...);
}
```

`RateLimiter` already exists in this codebase and is used for exactly this purpose in
`BrokerPollSystem` and `ProcessingShard`. It is simply not used here.

**Static buffer makes it permanent** - `PCModule.initDynamicLoadFactor()`:

```java
if (options().getMessageBufferSize() > 0) {
    int staticLoadFactor = (...);
    return new DynamicLoadFactor(staticLoadFactor, staticLoadFactor);   // initial == maximum
}
```

**`isMaxReached()` is true from startup.** Anyone who sets `messageBufferSize` - which the README
recommends for buffer tuning - gets this WARN on every control-loop pass, forever, reporting
saturation that is just their own configuration working as asked.

**Proposed for 0.6.0.0**: rate-limit the line, demote it to debug when the factor is static, and
reword it so it does not read as a failure.

## Why this is worth more than a logging tidy-up

**This is the visible face of the load-factor system**, and it is the reason that system has a
reputation for being broken. It is not broken in the way the title suggests - it is *noisy*, and one
construction bug makes it permanently noisy for exactly the users who followed the tuning advice.

**Fixing it separates two questions that keep getting conflated:** whether the pressure system
misbehaves (largely no - see [`perf-hypothesis-register.md`](perf-hypothesis-register.md), where the
load-factor buffer was tested and refuted as a throughput cause), and whether the pressure system
should exist at all (a live architectural question - see
[`parked-2022-central-queue-rework.md`](parked-2022-central-queue-rework.md)).

**A user staring at a permanent WARN cannot tell those apart**, and neither could this investigation
until the issue was actually read.
