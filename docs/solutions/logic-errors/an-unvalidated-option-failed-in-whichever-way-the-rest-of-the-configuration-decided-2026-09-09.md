---
title: "batchSize was unvalidated, so batchSize(0) was silent, fatal, or an NPE depending on options that have nothing to do with it"
date: 2026-09-09
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: ParallelConsumerOptions
symptoms:
  - "Parallel Consumer starts cleanly, logs nothing unusual, polls, and processes no records at all - forever"
  - "A bare ArithmeticException: / by zero at construction, from a divisor the caller never configured"
  - "A NullPointerException unboxing an Integer option, naming neither the option nor the bound it broke"
tags:
  - options-validation
  - input-validation
  - fail-fast
  - config-lie
---

`ParallelConsumerOptions.batchSize` was `@Builder.Default private final Integer batchSize = 1` with
no `@NonNull`, no bounds annotation, and no check in `validate()`, which until this work checked only
a non-null consumer, the producer source, the transaction-mode pairing and the load-factor ordering.
Every reference to `batchSize` in main code across every module was a read or a javadoc mention, so
`.batchSize(0)`, a negative, and - the field being boxed - `null` were all reachable through the
public, `@InterfaceStability.Evolving` builder.

The note that recorded this is deleted now that the bound exists; the last commit on master carrying
it is `git show c3813104d:docs/inflight/bug-unvalidated-batchsize.md` (grep anchor
`initDynamicLoadFactor`). It was ranked "the cheapest real fix in the set" by the in-flight vetting
sweep, and it is the validation half of astubbs#311. **The other half of that issue - the
`calculateQuantityToRequest` over-request - is a separate defect and is still open**, in
`docs/inflight/bug-batch-quantity-over-request.md`.

## The finding worth keeping: one misconfiguration, three failures, chosen by unrelated settings

This is why the answer was a bound and not a paragraph of javadoc. `batchSize(0)` did not have *a*
failure mode. It had three, and which one a caller got was decided by options that have nothing to do
with batch size.

- **Silently, on an otherwise default configuration.** `isUsingBatching()` is `getBatchSize() > 1`,
  so it is *false* at zero and the batching arithmetic is never reached. Instead
  `getTargetAmountOfRecordsInFlight()` is `maxConcurrency * 0` = 0, the in-flight delta is never
  positive, and no work is ever requested. The consumer starts, joins the group, polls, commits
  nothing, and processes nothing, forever, with no WARN. A negative behaves identically.
- **As a bare `ArithmeticException` at construction - but only if `messageBufferSize` is set.**
  `PCModule.initDynamicLoadFactor()` divides `messageBufferSize` by that same zeroed in-flight
  target. The division sits behind `if (options().getMessageBufferSize() > 0)`, and
  `messageBufferSize` carries no `@Builder.Default`, so the default path takes the `else` branch and
  never divides. Two callers with the same wrong batch size and different buffer settings get
  opposite outcomes: one runs and does nothing, the other dies at construction with a stack trace
  that names neither `batchSize` nor `messageBufferSize`.
- **As a `NullPointerException` on null**, unboxing inside `isUsingBatching()`.

The generalisation is the durable part: **an option's failure mode is not a property of the option
when nothing validates it - it is a property of the configuration around it.** That is what makes
"document the valid range" the wrong fix. A caller who reads the javadoc is not the caller who has
this problem; the caller who has it is reading a stack trace that names a divisor, or reading nothing
at all.

## The fix, and why null goes in `validate()` rather than on the field

One bound in `ParallelConsumerOptions.batchSizeValidation()`, called from `validate()` beside
`loadFactorValidation()`, rejecting null and anything below 1 with an `IllegalArgumentException` that
names the option, the value and the bound. All three shapes close on it.

`@NonNull` on the field was the alternative for the null shape. It was rejected because this class
had already settled the question for a null commit mode, and says so in `getCommitInterval()`'s
javadoc: reading options has never been able to fail, and a misconfiguration is `validate()`'s to
reject *at the point that can name the option*. Lombok's `@NonNull` fails at `build()` with
"batchSize is marked non-null but is null" - it names the field but not the bound - and it would
split one bound across two exception types and two points in the lifecycle for no gain.

`validate()` runs first thing in `AbstractParallelEoSStreamProcessor`'s
`validateConfiguration()`, which the constructor calls *before* `module.dynamicExtraLoadFactor()` and
before the worker pool is built. So the new bound genuinely pre-empts the `ArithmeticException` site
rather than merely sitting alongside it.

## The defect class: unbounded numeric options are the rule here, not the exception

The class is *a public numeric option with no bound, whose invalid values are absorbed by arithmetic
rather than rejected*. Every option on `ParallelConsumerOptions` was read against it. `batchSize` was
fixed; the rest are recorded here because "none found" would have been false, and because the next
person to bound one should not re-derive the list.

- **`maxConcurrency`** is the closest sibling and is unbounded. Zero or a negative zeroes the same
  `getTargetAmountOfRecordsInFlight()` product, so it reaches the same `ArithmeticException` under a
  configured `messageBufferSize`; and `setupWorkerPool` hands it to a `ThreadPoolExecutor`, which
  throws a message-less `IllegalArgumentException` for a non-positive maximum pool size. Same two
  outcomes, same absence of the option's name in either.
- **`initialLoadFactor`** is bounded only *relative* to `maximumLoadFactor` (the inverted-pair check
  added for astubbs#155). Neither is bounded below, so `initialLoadFactor(0)` passes validation and
  makes the loaded queue target zero. `maximumLoadFactor(0)` happens to be caught, but only as a
  side effect of the default initial factor of 2 being above it - which is not the same as being
  bounded.
- **`messageBufferSize`** has no `@Builder.Default`, so it defaults to 0 meaning "unset", and every
  read is `> 0`. A negative is therefore silently ignored rather than rejected - a smaller version of
  the same config-lie.
- **`maxFailureHistory`** has no main-code reader at all, which is a different defect and already has
  its own note (`bug-max-failure-history-is-inert.md`).
- **The `Duration` options** (`sendTimeout`, `offsetCommitTimeout`, `commitLockAcquisitionTimeout`,
  `produceLockAcquisitionTimeout`, `shutdownTimeout`, `drainTimeout`, `defaultMessageRetryDelay`,
  `thresholdForTimeSpendInQueueWarning`, `commitInterval`) accept zero and negatives. Zero is
  meaningful for at least one of them by documented design -
  `saslAuthenticationRetryTimeout` uses it for "do not retry" - so these need a per-option ruling
  rather than a sweep, and `offsetCommitTimeout` already has an open note saying its default does
  two jobs.
- **The reference options** (`ordering`, `commitMode`, `invalidOffsetMetadataPolicy`, `metricsTags`,
  `managedExecutorService`, `managedThreadFactory`) accept null through the builder. A null
  `commitMode` is already covered - `validate()` throws an NPE for it, deliberately, and there is a
  test pinning that a null commit mode is rejected by validation rather than by the getter.

None of these were fixed here. Bounding a numeric option is a user-visible behaviour change for
anyone currently passing the invalid value, so each is its own decision with its own release-note
line, and folding six of them into one PR would make every one of them unreviewable. This list is
the record of what was checked, not a queue.

## The user-visible consequence, stated plainly

A caller who passed `batchSize(0)` used to get a process that started and did nothing. They now get
an `IllegalArgumentException` at construction. That is the point of the change - but it is a
behaviour change on the public surface, not a silent improvement, and it was marked as one in its
commit subject so the generated release notes carry it.
