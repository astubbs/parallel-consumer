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
