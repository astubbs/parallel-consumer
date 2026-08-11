# ReactorProcessor completes a record once per element, not once per publisher

**Open defect in `parallel-consumer-reactor`.** Found 2026-08-08 while implementing the smart-meter
example (U6 of the industry-grounded examples work), which is the first thing in this repo to return a
multi-element publisher from `react(...)`.

## What is wrong

`ReactorProcessor.react(Function<PollContext<K,V>, Publisher<?>>)` subscribes like this
(`parallel-consumer-reactor/src/main/java/io/confluent/parallelconsumer/reactor/ReactorProcessor.java:102`):

```java
.subscribe(ignore -> onComplete(pollContext), throwable -> onError(pollContext, throwable));
```

The first argument to `subscribe` is the **onNext** consumer. So PC reports the record complete **once
per emitted element** rather than once when the publisher terminates.

Return a 48-element `Flux` and the same offset is reported successful 48 times.
`PartitionState.onSuccess` then trips:

```java
boolean removedFromIncompletes = this.incompleteOffsets.remove(offset) != null; // NOSONAR
assert (removedFromIncompletes);
```

(`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/PartitionState.java:260-261`)

Observed symptom before the workaround: commits stall part-way — `expected: 8L but was: 4L` — with the
assertion error surfacing as a suppressed exception. With assertions disabled the failure is worse, not
better: the duplicate removals corrupt offset state silently rather than failing loudly.

## Why nobody has hit it

`Publisher<?>` is the declared parameter type, so a multi-element publisher is a legitimate, documented
input. But **every existing reactor example and test returns a `Mono`** — exactly one element — so the
bug has never had an opportunity to fire. `ReactorApp` returns `Mono.just(...)`; the module's tests do
the same.

That is the whole reason it surfaced now: the smart-meter example needs a genuinely multi-element stream
per record, because `limitRate` is `Flux`-only and a single-element publisher gives backpressure nothing
to throttle.

## The likely fix, not applied here

Complete on the terminal signal rather than on each element — the three-argument overload:

```java
.subscribe(ignore -> {}, throwable -> onError(pollContext, throwable), () -> onComplete(pollContext));
```

Deliberately **not** fixed in the examples branch: it is library behaviour, it needs its own regression
test (a multi-element publisher whose offset commits exactly once), and it should not ride along in a
docs-shaped change. It wants an issue and its own PR.

## What the example does meanwhile

`MeterTelemetryApp` terminates its chain with `.count()`, so the publisher PC sees emits exactly one
element. That is a **workaround for this defect**, not idiomatic Reactor, and the example says so where
it does it. If the fix lands, the `.count()` can go - but check the example still reads well without it,
since the count is also a meaningful value (settlement periods stored).

Related: `finishReading` runs on `doOnTerminate` rather than `doFinally`, because `doFinally` runs
*after* the terminal signal propagates - by which point PC may already have dispatched the next reading
for the same meter while the previous one's in-flight count is still open.
