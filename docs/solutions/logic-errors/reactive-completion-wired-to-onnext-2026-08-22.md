---
title: "The Reactor engine completed a record on onNext, so a publisher that emitted nothing stalled the consumer"
date: 2026-08-22
category: logic-errors
module: parallel-consumer-reactor
problem_type: logic_error
component: engine adapter / completion signalling
symptoms:
  - "The consumer processes maxConcurrency records and then stops, with no exception, no failed record and nothing in the log"
  - "Nothing is ever committed, and the user function stops being invoked while records remain unprocessed"
  - "Only reproduces when the user function returns a publisher that emits no item - Mono.empty(), Mono<Void>, Mono.fromRunnable(..), a fully-filtering Flux"
tags:
  - reactor
  - mutiny
  - vertx
  - stall
  - in-flight-accounting
  - engine-adapter
---

## The defect

`ReactorProcessor#react` subscribed with the **two**-argument form:

```java
.subscribe(ignore -> onComplete(pollContext), throwable -> onError(pollContext, throwable));
```

`Flux#subscribe(Consumer, Consumer)` takes **onNext and onError**. There is no completion argument,
so `onComplete(..)` - which calls `WorkContainer#onUserFunctionSuccess` and adds the record to the
controller's mailbox - could only be reached by an *emitted item*.

A publisher that terminates without emitting is completely ordinary: `Mono.empty()`, a
`Mono<Void>` from `Mono.fromRunnable(..)`, a `Flux` whose filter removed everything. Every such
record was never completed, so it held its in-flight slot forever. Once `maxConcurrency` slots were
held, the controller stopped selecting work and the consumer stopped - silently, which is what made
this expensive to see.

## The tell: count invocations, not just commits

A missing commit tells you nothing was completed. It does **not** distinguish "stalled" from
"slow", and that distinction is the whole diagnosis. The number that settles it is the count of
**user-function invocations**: when it pins at exactly `maxConcurrency` and stays there, completed
records are not being released and no further work will ever be selected.

`ReactorEmptyPublisherTest` asserts both, with `maxConcurrency` deliberately set *below* the record
count - a leaked in-flight slot is only observable once the cap binds. At the module's usual cap of
1000 every record is still dispatched and the bug looks like nothing worse than a missing commit.

## Emitting many items was ALSO broken, contrary to an earlier reading

An earlier note recorded that a multi-item publisher "committed all five records cleanly", and
raised over-completion as an untested hypothesis. It does not hold. With `Flux.just(1, 2, 3)`, five
records and a cap of four, on a `MockConsumer`:

| | before | after |
|---|---|---|
| `Reactor success` log lines for 5 records | **15** (3 per record) | **5** |
| committed offsets | **none at all** | 5 |
| `numberRecordsOutForProcessing` at rest | **pinned at 1** | 0 |

Each emitted item handed the same `WorkContainer` to the controller again, and each pass ran
`WorkManager#onSuccessResult` - decrementing the in-flight counter, re-running the partition and
shard success paths. The counter is what the engine throttles against, so the state it left behind
was corrupt in both directions. Over-completion is not benign; it just fails somewhere other than
where you are looking.

## The rule

**Bind a record's completion to the publisher's TERMINAL signal, never to a value signal.** A record
is done when its work finishes, whether or not the work produced anything, and it is done exactly
once however many values it produced.

`MutinyProcessor#onRecord` already had this right - `subscribe().with(item, failure, completion)`,
with a null item mapped to an empty `Multi`. That is why Mutiny was the counterexample proving this
was Reactor's bug and not the shared `ExternalEngine`'s. The two engines are offered to users as
interchangeable, so they must not disagree about whether "produced no value" means "succeeded".

## Sweep for the same defect class

The class is *a reactive subscribe whose terminal action is wired to the wrong signal.* Every
`.subscribe(` in the tree was checked:

- **`MutinyProcessor`** - three-argument `subscribe().with(..)`, completion on the completion
  callback. Correct, and the model for the fix.
- **`VertxParallelEoSStreamProcessor#addVertxHooks`** - uses `Future#onSuccess` / `onFailure`, not a
  stream. A Vert.x `Future` completes exactly once and `onSuccess` fires on completion even with a
  null result, so the class does not apply. Structurally immune, not merely correct today.
- **Everything else** - `Consumer#subscribe(topics)` and this library's own
  `ParallelConsumer#subscribe(topics)`. A topic subscription, not a stream subscription; there is no
  completion signal to mis-wire. That is most of the hits, and all of them in tests, examples and
  integration tests.

No other instance found.
