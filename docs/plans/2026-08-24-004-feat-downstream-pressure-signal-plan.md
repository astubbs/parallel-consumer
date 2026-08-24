---
title: The Downstream Pressure Signal - How a User Tells the Engine It Is Being Pushed Back
type: feat
date: 2026-08-24
topic: downstream-pressure-signal
artifact_contract: ce-unified-plan/v1
artifact_readiness: requirements-only
product_contract_source: ce-plan-bootstrap
execution: code
---

# The Downstream Pressure Signal

## Goal Capsule

- **Objective:** Give a user function a way to say *the thing I am calling is pushing back*, so the
  engine can act on it - and decide what the engine does with that information.
- **Why it is its own artifact:** this is a **public API** decision - naming, discoverability, what a
  user has to learn - and those questions are unlike the control-theory ones in
  [`2026-08-24-003-feat-admission-control-law-design.md`](2026-08-24-003-feat-admission-control-law-design.md).
  Reviewed together, findings about API ergonomics and findings about control law land in one pile and
  contradict each other; that is what produced forty-seven findings on the first attempt. The split is
  for review coherence only - it implies nothing about how the implementation is divided.
- **Feeds:** the `OVERLOAD_DROP` verdict, which `AdmissionOutcomeClassifier` reserves and never
  returns, so the AIMD backoff arm is currently unreachable in production. This signal is what makes
  that socket real.
- **Prior art it must reconcile with:**
  [`docs/ideation/2026-08-17-distributed-throttling-ideation.html`](../ideation/2026-08-17-distributed-throttling-ideation.html) -
  read it before re-opening anything here. It settled three constraints and made one choice this
  document deliberately overrides.

---

## Product Contract

### The decision: a method on the context, not an exception

**"Exceptions are for aborting. Not for messaging."** (owner, 2026-08-24.) The signal is a method on
the `PollContext` the user function already receives:

```java
pc.poll(context -> {
    try {
        paymentsApi.charge(context.getConsumerRecords());
    } catch (TooManyRequests e) {
        context.reportDownstreamPressure("payments", e.retryAfter());   // hard: carries a number
        throw e;                                                        // and still fail the record
    }

    searchApi.index(context.getConsumerRecords());        // an SDK that retried 429s internally
    if (searchApi.wasThrottled()) {
        context.reportDownstreamPressure("search");       // soft: no number, record SUCCEEDED
    }
});
```

**This overrides idea 8 of the throttling ideation**, which specified
`RateLimitExceededException(serviceKey, retryAfter)`. Recorded as an override rather than an
oversight, with the reasoning, so the next reader does not restore the exception taking this for a
mistake:

- **Reporting pressure and failing a record are orthogonal facts**, and an exception welds them
  together.

  | | Record failed | Record succeeded |
  |---|---|---|
  | **Pressure reported** | Rejected, not processed, retry it | **The SDK retried internally and won** |
  | **Nothing reported** | A bug, or a bad record | Normal |

- **The top-right cell is the common case and an exception cannot express it at all** - most HTTP SDKs
  retry 429 internally, so the function returns normally and there *is* no exception. This is the case
  the ideation document does not address, and it is the one that matters most: today the engine learns
  nothing from it.
- An exception carries **one** service, when a function may call several and be throttled by one.
- An exception **unwinds the stack before the user's cleanup**.
- `void poll(Consumer<PollContext<K,V>>)` returns **void**, which is why a richer *return* object is
  not the answer either: there is no return channel on the plainest use of the library, so a
  return-based design needs new overloads of every variant and still cannot serve `poll()`.

The honest cost: a return type appears in a signature and demands attention, while a method on a
parameter does not. Two things narrow that gap - the context is an object users already explore, and
the classifier SPI below gives existing code the behaviour with no discovery required at all.

### Constraints inherited from the ideation, all binding

- **Do not name it `RateLimiter`.** That name is taken by an internal log-debounce, and *throttle*
  already means buffer-driven partition pause in the feature docs.
- **Non-blocking only.** The seam is `Optional<Instant> notBefore(key, cost)` shaped - never a
  blocking `tryAcquire()`. A backend that blocks the control thread is a liveness hazard.
- **Reserve-then-settle.** Batching runs *after* work selection, so a 100-record batch making one
  downstream call must not cost 100 tokens: reserve on dispatch, settle actual cost on completion.
- **One SPI, three number-owners.** Partition-share, downstream-reported and configured limits are
  implementations of a single interface (idea 5), not competing features. A `Supplier<Rate>` style
  live limit was folded into the same interface (rejection #4).

### A pressure signal is retried like a failure but never counted as one

Idea 8 states it: *a rate-limit signal is a deferral instruction, not a failure; it must not
increment failure history.* Both halves bind, and only the second is subtle:

- **It goes in the retry queue**, deferred until `retryAfter` - a 429 usually does mean the record was
  not processed.
- **It must not increment the failure count.** Otherwise being throttled is indistinguishable from
  being poisoned: backoff climbs, the count rises, and the record is eventually dropped or sent to a
  DLQ for the offence of having been rate limited.

Neither half is a flag on existing machinery: retry scheduling currently flows only through failure
bookkeeping (`WorkContainer#updateFailureHistory` sets `retryDueAt` and increments the failure count
in the same write), so satisfying both halves requires a **new deferral write path** in the engine's
core state class, not reuse of the existing one.

And the reported `retryAfter` is the second owner of a record's retry deadline: the existing
`retryDelayProvider` option already computes one on every failure. When both apply to one record,
**the reported deferral wins** - it carries what the downstream actually said, where the provider is
the operator's guess - and the precedence is stated here so two number-owners never race unrecorded.

### Nothing discovered is ever stored

`Retry-After` is a **deferral, not a rate**. It says *do not call me for two seconds*; it never says
*your quota is 100/s*. Three sources, three lifetimes:

| Source | What it actually is | Lifetime |
|---|---|---|
| Operator configuration | A contractual rate the operator holds | Lives in config. No expiry, no discovery |
| `Retry-After` / an explicit 429 | A **timed deferral** | Self-expiring. Applied, then gone |
| Timeouts, 503s, a breaker opening | Evidence feeding an estimate | Continuously re-derived, never persisted |

**"Save it forever" is refused rather than offered as configuration.** A remembered discovered limit
is unfalsifiable - the same disease as the drifting latency baseline the control design exists to
remove. Pin *payments = 100/s* and the tier upgrade is never found, the quota raise is never found,
and nothing can report that the number is now wrong. An operator who genuinely holds a contractual
number states it as configuration, where it is a declared input rather than an unfalsifiable memory.
Nothing survives a restart, for the same reason.

Several at once compose by `min()`, and which constraint binds is what the constraint gauge reports.

### Per-service ceilings need hard limits, because the engine cannot see inside the function

**The engine cannot track what is inside a black box** (owner, 2026-08-24), and that observation
settles the scope question rather than merely complicating it.

The user function is opaque by design. The engine cannot know which services a record will touch
until the function has already run - the ideation says so itself, *"a user function may call dozens of
services depending on record data"*. So a discovered signal always arrives **after** the dispatch that
discovered it, and there is no way to selectively withhold future records bound for that service,
because nothing identifies them.

It follows that **per-service ceilings only apply to hard limits** - ones that are *declared* or
*reported*, i.e. supplied from outside the black box. Adaptive capacity estimation is necessarily
**aggregate**: throughput against concurrency is all the engine can actually observe, so the adaptive
layer has one number for the whole instance and cannot meaningfully be per-service at all.

That is the clean division, and it is a consequence of the architecture rather than a v1 compromise:

- **Adaptive layer** - one global estimate. Per-service is not merely unimplemented, it is not
  observable.
- **Fixed layer** - per-service, keyed by the name the user supplies, and only ever as good as what
  the user declares or the downstream reports.

**v1 captures the service name and keeps enforcement global.** The name costs nothing, makes the log
and the constraint gauge legible, and is forward-compatible. Per-service *enforcement* additionally
needs a declared mapping from work to service, which is the register-time declaration idea and is
recorded here as future work rather than built. Fleet-wide quotas divided by group membership are a
further step again and belong to astubbs#228.

### Requirements

- R1. A user function can report that a named downstream is applying pressure, with an optional
  duration, without failing the record and without throwing.
- R2. Reporting pressure and failing the record are independent; all four combinations are meaningful
  and none is an error.
- R3. A reported pressure defers the record's retry without incrementing its failure history.
- R4. Existing code that throws its own exception type can map it to a pressure verdict without
  editing the function body.
- R5. Nothing discovered is persisted; a contractual rate is configuration, and an unreachable
  configured limit is reported as a binding constraint rather than pursued.
- R6. The reported service name reaches the log and the constraint gauge.

### Scope Boundaries

Not here: the control law itself (its own design); per-service *enforcement*; the register-time
declaration API; fleet-wide budget division (astubbs#228); the coordination substrate and its
backends (idea 1 and idea 5's extension artifacts).

---

## Resolve Before Planning

1. **The method's name**, given `RateLimiter` and *throttle* are both taken. `reportDownstreamPressure`
   is a placeholder, not a decision.
2. **Whether the classifier SPI ships with v1** or the context method alone. It is the whole adoption
   story for existing code, but it is a second public surface.
3. **What a soft signal with no duration actually does** to the controller - it is evidence, not an
   instruction, so it needs a weight, and that weight interacts with the throughput objective.
4. **Whether a reported pressure with no failure should defer the record at all**, given the record
   succeeded. Probably not, but then the signal's only effect is on the controller, which is worth
   stating.
5. **Reserve-then-settle's interaction with the admission target**, since a slot is one batch and the
   token cost is per downstream call, not per record.
6. **Vert.x already mis-measures `pc_user_function_processing_time_seconds`** (confluentinc#766). Any
   signal taken around the user function inherits that, and the engine module wraps the function
   differently.

## Sources

- [`docs/ideation/2026-08-17-distributed-throttling-ideation.html`](../ideation/2026-08-17-distributed-throttling-ideation.html) -
  ideas 4, 5 and 8, and rejections #3 and #4. **Read before re-opening anything here.**
- [`docs/inflight/core-distributed-throttling.md`](../inflight/core-distributed-throttling.md) - owns
  the strategy-menu shape. astubbs#228 (mirror of confluentinc#24), confluentinc#766 as demand evidence.
- [`docs/inflight/core-auto-scaling.md`](../inflight/core-auto-scaling.md) - the umbrella note, and the
  decisions of 2026-08-24.
- RFC 6585 (429 Too Many Requests); RFC 9110 section 10.2.3 (`Retry-After`, previously RFC 7231
  section 7.1.3) - the source of the deferral-not-a-rate reading.
