# The Reactor engine never completes a record whose publisher emits no item - the consumer stalls

<!-- inflight-type: bug -->
<!-- inflight-impact: correctness -->
<!-- inflight-labels: release-note -->

Found 2026-08-21 while wiring the Reactor benchmark arm. **`ReactorProcessor.react` marks a record
successful from its `onNext` consumer and subscribes without an `onComplete` one**, so a user
function returning `Mono.empty()`, `Mono<Void>`, `Mono.fromRunnable(...)` or an empty `Flux` leaves
its record in flight forever. In-flight fills to `maxConcurrency`, core's backpressure halts
selection, and the consumer stops with no exception and no log line.

`parallel-consumer-reactor/.../ReactorProcessor.java`, in `react`:

```java
.subscribe(ignore -> onComplete(pollContext), throwable -> onError(pollContext, throwable));
```

`Flux.subscribe(Consumer, Consumer)` takes onNext and onError. There is no third argument, so
`onComplete(pollContext)` - which calls `wc.onUserFunctionSuccess()` and `addToMailbox` - is reached
only by an emitted item.

## Reproduced, with a control and a counterexample

`MockConsumer`, five records, `maxConcurrency` 4, no broker. Committed offset after 8 seconds:

| Engine | User function returns | User function invoked | Committed offset |
|---|---|---:|---:|
| **Reactor** | **`Mono.empty()`** | **4 of 5** | **-1 - nothing committed** |
| Reactor | `Mono.just(1)` | 5 | 5 |
| Reactor | `Flux.just(1, 2, 3)` | 5 | 5 |
| **Mutiny** | **`Uni` resolving to null** | **5** | **5** |
| Mutiny | `Uni` resolving to an item | 5 | 5 |

**"Invoked 4 of 5" is the stall itself**: four records went out, none came back, the in-flight cap
bound at 4 and the fifth was never selected.

**Mutiny is the counterexample that makes this a Reactor defect rather than an `ExternalEngine` one.**
`MutinyProcessor.onRecord` subscribes with an explicit completion callback
(`subscribe().with(item -> {}, failure -> onError(..), () -> onComplete(..))`) and handles a null item
by mapping it to an empty `Multi`, so a null-resolving `Uni` completes its record correctly. Two
engines, presented to users as interchangeable, disagree on whether "produced no value" means
"succeeded".

**A hypothesis that did NOT hold, stated because it was the obvious second half:** emitting *n* items
should complete the record *n* times - `onUserFunctionSuccess` and `addToMailbox` per item - which
looked like it should drift `numberRecordsOutForProcessing`. `Flux.just(1, 2, 3)` commits all five
records cleanly. Either core absorbs the repeats or the drift is invisible at this scale; **it is not
demonstrated, and this note does not claim it.**

## Why it has not been seen

Every example and every test in `parallel-consumer-reactor` returns a publisher that emits. The
benchmark arm hit it immediately because the natural way to write "call an async callee and wait" -
adapt a `CompletableFuture<Void>` - produces exactly the empty publisher. `bench/Bench.java.template`
now guarantees a non-null completion value for this reason, which is a workaround in the harness, not
a fix in the engine.

## The fix, and the decision it needs

The mechanical fix is to subscribe with the three-argument form and move `onComplete(pollContext)`
onto the completion signal, keeping the onNext consumer for logging only. That also makes the
`n`-item case complete once rather than *n* times.

**It is a behaviour change on a public API and should not be applied silently.** Today a user whose
publisher emits several items gets the success hook per item; afterwards they get it once, at
completion. That is the correct semantic and matches Mutiny, but it wants a changelog line rather than
a quiet commit. No upstream issue covers it - `gh issue list --state all` across both repos returns
nothing on Reactor completion semantics.

## Rebuilding the repro

No broker and no test module needed - `MockConsumer` ships inside `kafka-clients`. Build a
`ReactorProcessor` over one, `maxConcurrency` 4, `PERIODIC_CONSUMER_ASYNCHRONOUS`; seed five records
from a `schedulePollTask` that rebalances the partition in first; call `react(ignored -> Mono.empty())`;
then poll `MockConsumer#committed` for eight seconds. Swap `Mono.empty()` for `Mono.just(1)` for the
control and `MutinyProcessor.onRecord(ignored -> Uni.createFrom().nullItem())` for the counterexample.
Count the user-function invocations as well as the committed offset - the invocation count is what
shows the in-flight cap binding, and it is the difference between "slow" and "stalled".
