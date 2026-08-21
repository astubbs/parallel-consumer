# Next: let the core user function return a future

<!-- inflight-type: next -->
<!-- inflight-impact: architecture -->
<!-- inflight-labels: needs-measurement -->

Opened 2026-08-21, from the owner's question: *what if core had a return that was a Java future?*

**It is not a new mechanism. It is a generalisation of one that already ships, works, and is locked
inside the Vert.x module.**

## The mechanism already exists

`VertxParallelEoSStreamProcessor` already accepts exactly this shape:

```java
public void vertxFuture(final Function<PollContext<K, V>, Future<?>> result)
```

and completes the record when the future does:

```java
send.onComplete(ar -> {
    wc.onUserFunctionSuccess();
    addToMailbox(context, wc);
});
```

**So "the record finishes later, on somebody else's thread" is a solved problem in this codebase.** It
is bound to `io.vertx.core.Future` and lives in a module that drags in Vert.x.

## What core has instead

```java
void poll(Consumer<PollContext<K, V>> usersVoidConsumptionFunction);
```

**Completion is signalled by returning.** A user holding a `CompletableFuture` from an async HTTP
client or a reactive driver must `.join()` it, **parking the thread they chose that library to avoid** -
see [`next-docs-publish-the-engine-comparison.md`](next-docs-publish-the-engine-comparison.md).

## The proposal

Add a `CompletableFuture`-returning entry point to core:

```java
void pollAsync(Function<PollContext<K, V>, CompletableFuture<?>> userFunction);
```

**Why `CompletableFuture` specifically: it is Java 8.** So this needs no JDK bump, no virtual threads,
no framework, and no change to the project's compatibility baseline -
[`docs/features/java-compatibility.yaml`](../features/java-compatibility.yaml).

**What it would buy**, given the measured thread ceiling
([`perf-platform-threads-are-the-ceiling.md`](perf-platform-threads-are-the-ceiling.md)):

- **The reachable-concurrency term disappears for anyone who uses it.** `min(maxConcurrency, r x
  handler_latency)` exists because the work holds a platform thread. Hand back a future and it does not.
- **Without adopting a framework.** Today the only escape is Vert.x, Reactor or Mutiny - each a
  dependency and a programming model. A user with a plain `AsyncHttpClient` or an R2DBC driver has no
  route at all.
- **It may make the external engines thin.** If core can complete a record on a callback, Vert.x and
  Reactor become adapters over one core capability rather than parallel engines with their own
  `ExternalEngine` subclass. **That would collapse a whole layer**, which is worth more than the
  throughput.

## The classic API becomes an adapter - and this is what makes the change small

**Owner's refinement, and it is the load-bearing part of the design:** the existing API does not have
to change at all. It becomes a thin wrapper over the future-returning one, so there is **one internal
completion path instead of two**.

**One correction to how it was put, because it matters:** the wrapper must not *block* on the future.
The classic user function is already synchronous - it signals completion by returning - so the wrapper
simply returns an **already-completed** future:

```java
void poll(Consumer<PollContext<K, V>> fn) {
    pollAsync(ctx -> {
        fn.accept(ctx);                              // runs on the worker thread, exactly as today
        return CompletableFuture.completedFuture(null);
    });
}
```

**No blocking is added anywhere.** Adding a `join()` would be strictly worse than today - a thread
parked waiting for something already finished. The thread is held for exactly as long as
`fn.accept(ctx)` takes, which is precisely the current behaviour, and `whenComplete` on an
already-complete future fires inline on the same thread - so it lands on `onUserFunctionSuccess()` and
`addToMailbox()` at the same moment the current code does.

**Byte-identical semantics for every existing user**, and the void API stops being a special case in
the engine and becomes a two-line adapter. **That is a deletion, not an addition.**

### And it shrinks the hard parts considerably

**The pressure system only breaks for the genuinely-async caller.** In the adapter path the thread *is*
held for the duration, so `getQueue().size()` still means what it always meant and
`checkPipelinePressure()` still works. **Only a caller who returns an unfinished future needs the new
accounting.**

That is a much better position than the current one, where `ExternalEngine` no-ops the pressure system
for *everybody* on that engine and pays the 35% regression for it. **The blast radius becomes the
async path alone**, and the classic path keeps behaviour that is already measured and understood.

**It also suggests `ExternalEngine` need not be a subclass.** If core can complete a record from a
callback, the difference between core and Vert.x stops being a class hierarchy and becomes which entry
point the user called. Whether that actually holds is the first thing to check.

## What it unlocks - the argument a user would actually read

"It removes a term from a formula" is our reason. **These are theirs:**

- **Use the non-blocking client they already have.** Async HTTP, R2DBC, reactive drivers, gRPC stubs,
  the AWS SDK v2 async clients, Netty. Every one of those must currently be `.join()`ed, throwing away
  the reason they were chosen.
- **Chain without extra threads.** Fetch then write becomes `fetch(ctx).thenCompose(this::write)` -
  today that is two blocking calls on one held thread.
- **Fan out per record.** One record, several parallel calls, `CompletableFuture.allOf(...)`. Currently
  impossible without the user running their own executor *inside* PC's executor.
- **Per-record timeouts for free.** `.orTimeout(5, SECONDS)`. Today that needs a private scheduler and
  an interrupt that cannot be cleanly delivered.
- **Inherit the downstream's backpressure instead of fighting it.** If the HTTP client's connection
  pool is exhausted, its future simply takes longer - **no PC thread is burned waiting for a slot.**
  See below; this is the deepest of the list.
- **High concurrency on Java 8.** Measured on the control: 50,000 in flight on **four** threads, no JDK
  21, no virtual threads, no framework.
- **Use core *from* a reactive framework.** `Mono.toFuture()`, `Uni.subscribeAsCompletionStage()`,
  Vert.x `Future.toCompletionStage()`. A Reactor user could stay on core.
- **Deterministic tests.** Complete a future by hand instead of sleeping.

### The backpressure point, and why it compounds with direct pull

**Today a saturated downstream costs PC a thread.** The worker sits parked inside the client waiting
for a connection, holding a slot against `maxConcurrency`, contributing nothing. **The user's pool
limit silently becomes PC's concurrency limit**, and PC cannot see it happening.

**With futures, a saturated downstream costs nothing.** The future is outstanding; the thread is not.
PC's concurrency and the client's pool stop being coupled.

**And with direct pull it gets simpler still.** Direct pull removes the pre-loaded buffer - workers
take work when they are ready for it
([`parked-2022-central-queue-rework.md`](parked-2022-central-queue-rework.md)). Combine the two and
**backpressure stops being something PC computes and becomes something that just happens**: nothing is
pre-loaded, nothing holds a thread, and the rate is set by how fast completions arrive.
`DynamicLoadFactor`, `checkPipelinePressure`, `isPoolQueueLow`, the queue-depth read and the
conservation counter are all machinery for *estimating* a pressure that this arrangement does not need
to estimate.

**That is the strongest argument for either change**, and it only appears when they are considered
together - which is why neither should be judged alone.

## Do the Vert.x and Reactor modules become redundant?

**The engines do. The type adapters do not - and one of them cannot.**

| Module | Entry point | Fate under a core future API |
|---|---|---|
| Vert.x | `vertxFuture(Function<PollContext, io.vertx.core.Future<?>>)` | **Engine redundant.** Vert.x `Future` converts to `CompletionStage`, so this becomes a one-line adapter |
| Vert.x | `vertxHttpReqInfo(...)`, `vertxWebClient(...)` | **Not redundant** - these build HTTP requests for you. That is a convenience library, not an engine |
| Reactor | `react(Function<PollContext, Publisher<?>>)` | **Cannot be fully replaced.** A `Publisher` is a *stream*; a `CompletableFuture` is one value. A `Flux` emitting many items per record has no faithful `CompletableFuture` form without collecting it first, which changes the semantics |
| Mutiny | `parallel-consumer-mutiny` | Likely as Vert.x - `Uni` is single-valued, so it converts cleanly |

**So the honest answer is: the `ExternalEngine` subclasses, their pressure-system overrides and their
separate threading models all become unnecessary - which is the layer worth collapsing - while the
modules survive as thin adapters plus, in Vert.x's case, a genuinely useful HTTP helper.**

**Reactor is the one to check carefully**, because `Publisher` is strictly more expressive than
`CompletableFuture` and dropping that would be a capability regression rather than a simplification.

## What makes it hard, and none of it is the future itself

1. **It inherits `ExternalEngine`'s known defect.** With no thread held, `getQueue().size()` stops
   meaning anything, which is exactly why `ExternalEngine` no-ops `checkPipelinePressure()` - and **that
   no-op is the cause of the 35% throughput regression** documented in
   [`perf-throughput-regression-since-0-3.md`](perf-throughput-regression-since-0-3.md). Doing this
   naively adopts a defect we already have open. **But see the adapter section above: this bites only the async path.** The classic path still holds a
   thread, so its pressure accounting is unchanged. That makes the coupling to
   [`bug-available-work-counter-needs-a-clamp.md`](bug-available-work-counter-needs-a-clamp.md) a
   sequencing preference rather than a hard dependency.
2. **Transactions: settled - block EoS on the new entry point only.** *Owner's decision,
   2026-08-21.* `ExternalEngine` already rejects transactional commit mode outright:

   ```java
   if (options.isUsingTransactionCommitMode()) {
       throw new IllegalStateException(msg("External engines (such as Vert.x and Reactor) do not support transactions / EoS ({})", ...));
   }
   ```

   **`pollAsync` carries the same restriction. The classic `poll` must not**, and that distinction is
   the whole point of the adapter: its future completes **inline, on the worker thread, before the
   commit path runs**, so the transaction machinery sees exactly what it sees today. **Every existing
   transactional user is unaffected.** A restriction that leaked onto `poll` would be a breaking change
   dressed up as a refactor.

   **And the restriction gets better wording out of this.** Today it reads as a module fact -
   *"external engines do not support transactions"*. Stated in terms of the actual cause it is a rule a
   user can reason about: **you cannot hold a producer transaction open across a completion you do not
   control, for an unbounded time.** Same constraint, but it explains itself, and it stops being
   surprising that Vert.x specifically is excluded.

   **Consequence for the documentation push**, which must be said out loud rather than discovered:
   steering users towards async completion steers them away from exactly-once. See
   [`next-docs-publish-the-engine-comparison.md`](next-docs-publish-the-engine-comparison.md) - the
   comparison needs an EoS column, not just a throughput one.

3. **A future that never completes pins a record forever.** There is no timeout on the Vert.x path
   either, so this is an existing gap rather than a new one - but a core API would be used far more
   widely, and the proxy already had to solve it with `LivenessLease`. **A policy is needed before this
   is a public API**, not after.
4. **Accounting.** The record leaves the thread long before it leaves the system, so every in-flight
   count has to key off completion rather than return. Same question as the conservation rewrite.

## Why it might be worth more than virtual threads

**Virtual threads make a blocked thread cheap. This makes the block unnecessary.** They solve different
halves: virtual threads help the user who genuinely must block - a JDBC driver, a synchronous SDK -
while this helps the user who already has a non-blocking client and currently cannot use it.

**And this one runs on Java 8**, where virtual threads need a JDK 21 runtime, a CI lane, and the
package-rename rebase first.

**Neither replaces the other**, and that is the useful framing: **virtual threads are for work that
must block, an async user function is for work that need not.**

## First step

**Do not start with the API.** Start by asking whether `AbstractParallelEoSStreamProcessor`'s
completion path can already be driven from a callback without the `ExternalEngine` subclass - the Vert.x
implementation suggests it can, since all it does is call `onUserFunctionSuccess()` and `addToMailbox()`
from a handler. **If that is true, the change is small and the difficulty is entirely in the four items
above.** If it is false, find out why before designing anything.
