---
title: "A progress signal must count work consumed, not work accepted downstream"
date: 2026-08-10
category: architecture-patterns
module: parallel-consumer-streams
problem_type: architecture_pattern
component: background_job
severity: high
applies_when:
  - A caller paces itself on your return value, blocking or sleeping when it reads zero
  - You are replacing the body of a framework method that already had a progress contract
  - An item can leave your input queue by more than one route (dropped, failed, short circuited, served from cache)
  - You are tempted to submit a no-op unit of work so that a counter comes out non-zero
  - A count is serving both flow control and observability from one field
related_components:
  - kafka-streams StreamTask.process
  - kafka-streams StreamThread run loop
  - PcTaskDispatcher
  - PcDispatchCounters
tags:
  - flow-control
  - progress-signal
  - return-value-contract
  - liveness
  - kafka-streams
  - key-ordering
  - happy-path-blindness
---

# A progress signal must count work consumed, not work accepted downstream

## Context

Some return values are statistics. A caller reads them, logs them, maybe puts them on a
dashboard, and nothing about the system's behaviour changes. Other return values are
**flow control**: the caller decides whether to call you again, or to go and block
somewhere, based on what you handed back. Those two look identical at the call site and
they are governed by completely different contracts.

The distinction surfaced on `astubbs/parallel-consumer#271` (tracking issue
`astubbs/parallel-consumer#255`), which replaces the body of Kafka Streams'
`StreamTask.process()` with a pump that hands records to a Parallel Consumer worker pool.
`process()` returns a `boolean`, and the seam implemented it as "did I hand anything to the
worker pool this call" (`parallel-consumer-streams/src/main/patch/pc-streams.patch:512`):

```java
return pcDispatcher.dispatchAvailable(rawRecord -> pcPrepare(wallClockTime, rawRecord)) > 0;
```

That reads as obviously correct, and it was wrong. `dispatchAvailable` returned the number
of records **submitted to the pool**. Records that PC handed out and the seam resolved
without ever reaching a worker - a corrupted record, a record dropped for a bad timestamp,
a record whose preparation threw - counted as zero. A poll batch made entirely of corrupted
records therefore drained PC's queue completely and reported "nothing happened".

That is not a cosmetic under-count, because of who reads the number. Kafka's
`TaskExecutor` loops on the boolean and stops the whole batch for that task on the first
`false` (Kafka 3.9.2, `TaskExecutor.java:97-100`):

```java
while (processed < maxNumRecords && task.process(now)) {
    task.clearTaskTimeout();
    processed++;
}
```

`StreamThread` then sums those counts (`StreamThread.java:1003`) and, on a total of zero,
breaks out of the processing loop back into a blocking poll (`StreamThread.java:1049-1051`):

```java
if (processed == 0) {
    // if there are no records to be processed, exit after punctuate / commit
    break;
}
```

So a false zero does not cost one wasted iteration. It ends the task's batch, ends the
thread's processing loop, and parks the only thread that can dispatch anything in
`poll()` for up to `poll.ms` - 100ms by default. See
[docs/solutions/integration-issues/kafka-streams-couples-polling-and-processing-on-one-thread.md](../integration-issues/kafka-streams-couples-polling-and-processing-on-one-thread.md)
for why that block is so expensive under an asynchronous dispatcher specifically.

The defect was not found by review. It was found by chasing a refuted prediction: two
Kafka `StreamTaskTest` cases were predicted to flip green and stayed red
([docs/solutions/best-practices/chase-refuted-predictions.md](../best-practices/chase-refuted-predictions.md),
example 2). The code read as correct in every pass over it.

## Guidance

**1. Decide, explicitly, whether the value is a contract or a statistic.** Ask what the
caller does with it. If the answer contains the words "block", "sleep", "poll again",
"back off", or "stop looping", it is a contract about progress and it belongs to the
caller, not to you. Write that down at the definition site, because the next person to
touch the method cannot infer it from the type. The current javadoc does exactly that
(`parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcTaskDispatcher.java:244-251`):

```java
 * @return how many records were CONSUMED from the WorkManager this time round - dispatched to the pool,
 *         dropped during preparation, or failed at preparation. Not merely pool submissions: the patched
 *         {@code process()} returns this as its progress signal, and stock's contract is "did the task
 *         make progress", which a consumed-by-drop record satisfies. Counting only pool submissions made
 *         {@code process()} report false after consuming a batch of corrupted records - a lie that
 *         StreamTaskTest catches with assertTrue(task.process(...)) and that stock's TaskExecutor paces
 *         on.
```

**2. Define it as "work that left my input", never as "work I passed downstream".** The
caller is asking one question: *is there any point calling you again immediately, or should
I go and wait?* The answer to that turns on whether your input queue moved. "How many items
I handed to my worker pool" is a description of your internal plumbing. It happens to
answer the caller's question on the route where every item goes to the pool, and it silently
answers it wrongly on every other route.

**3. Enumerate every way an item can leave the input, and confirm each one increments.**
This is the whole test, and it is mechanical. For this dispatcher there are three exits
from `getWorkIfAvailable`, all inside one loop
(`PcTaskDispatcher.java:278-303`):

| Exit | What happens | Counts? |
|---|---|---|
| Preparation throws | `recordFailure(work, e)`, completion queued on this thread (`:283-288`) | yes |
| Preparation returns null (corrupted, or bad timestamp) | `onUserFunctionSuccess()`, completion queued on this thread (`:291-297`) | yes |
| Preparation returns a `Runnable` | submitted to the pool (`:299-302`) | yes |

**4. Increment structurally, at the point of departure, before the branches.** The fix is
not "remember to add a `count++` in the two new branches". It is to put the increment on the
line where the item leaves the queue, above every branch, so a fourth exit added later
cannot forget it (`PcTaskDispatcher.java:278-279`):

```java
for (WorkContainer<byte[], byte[]> work : available) {
    consumed++;
```

**5. Push the definition out to the collaborator interface.** If a plugin point can consume
an item without producing downstream work, its own contract has to say so, or the next
implementor reintroduces the bug from outside your file. Here that is the `WorkPreparer` SPI
(`PcTaskDispatcher.java:110-115`):

```java
 * @return the chain execution to run on a worker, or null if the record was dropped during
 *         preparation (a bad timestamp, say) and there is nothing to run - it still counts as
 *         consumed.
```

**6. Never do real work to make a counter non-zero.** This is the second half of the same
defect, found in the same hour. Before the fix, corrupted records were shipped to workers as
no-op runs - partly because that was how the count came out right. The comment recording the
correction is in the patch (`pc-streams.patch:538-546`):

```java
// PC dispatch (astubbs#255, U9): a corrupted record is consumed, never processed - stock handles it
// synchronously on the processing thread, so shipping it to a worker as a no-op run would make
// corruption handling asynchronous for no benefit: commitNeeded() would lag the consumption by a
// worker round-trip. Dropping it here keeps the corrupted path synchronous, exactly like the
// bad-timestamp drop above, and PC's frontier advances over it the same way.
```

Doing work to satisfy a counter is a strong smell that the counter is measuring the wrong
thing. It also costs: it converted an operation that was synchronous under the original
design into an asynchronous one, and under KEY ordering a synchronously-consumable record's
key-mate then had to wait for a worker round-trip, stalling that key by a full dispatch
cycle. One poison pill held up its whole key for roughly `poll.ms`. Both halves of the fix
have to land together - counting consumption honestly is what makes the inline path
*affordable*, because you no longer need a pool submission to prove you did something
(`PcTaskDispatcher.java:264-268`).

**7. Keep the diagnostic counter, and keep it separate.** "Submitted to the pool" is not a
bad number; it is a bad *progress signal*. It remains the right definition for its own
purpose, and this module keeps it under its own name with its own javadoc, incremented at
exactly one place so that a non-zero reading proves records genuinely travelled the PC path
(`PcTaskDispatcher.java:300` and `:412-415`, `PcDispatchCounters.java:9-29`). One field
cannot serve both contracts. Two fields, two javadocs, two meanings.

**8. Test the bypass routes, because the happy path cannot see this.** On the happy path
every item goes to the pool and the two definitions are numerically identical, so no amount
of happy-path testing distinguishes them. Write the test that consumes a batch entirely by
the bypass route and asserts the caller-visible signal is non-zero.

## Why This Matters

**A false zero is a stall, not an inaccuracy.** The caller is not reporting your number, it
is *obeying* it. Under-reporting progress by one is under-reporting it by everything: zero
is a distinguished value that means "go and block". Here the queue was genuinely draining
and the thread went back to sleep anyway, which presents as a latency cliff or a hang, never
as a wrong count. Nothing in the logs says a counter was off.

**The blast radius is the caller's whole loop, not one iteration.** In this case one `false`
terminated the per-task batch loop (`TaskExecutor.java:97`), which zeroed the thread-level
total, which broke the processing loop back to `poll()` (`StreamThread.java:1049-1051`).
Three layers, each behaving exactly as designed, amplifying a single wrong boolean into a
poll cycle of dead time.

**This bug class is invisible on the happy path, and lives exactly where testing is
thinnest.** The two definitions agree on the normal route and diverge only on error
handling, cache hits, short circuits, already-satisfied requests, and drops - the paths that
get the least coverage and the least review attention. That is not a coincidence, it is the
mechanism: a path that bypasses the normal plumbing is by definition a path where "count the
plumbing" stops being equivalent to "count the work".

**"Make it non-zero" is the tempting fix and it is a design regression.** Shipping a no-op
to the pool would have made the number right without making the definition right, and it
bought a real liveness defect under KEY ordering in exchange. Whenever a fix takes the shape
of *performing an operation so that a count is satisfied*, the count is the thing that needs
changing.

**A stale javadoc reintroduces the bug at the next edit.** The corrected definition is
documented on `dispatchAvailable` and on `WorkPreparer.prepare`, but the caller one layer up
still describes the old contract: `pc-streams.patch:504` reads `@return true if any record
was handed to the worker pool this call`, above a body that now returns consumption
(`:512`). Nothing is broken today. The next person to reason from that javadoc, or to write
a new caller against it, will be reasoning from the defect.

## When to Apply

- Any method whose return value the caller uses to decide whether to loop again, block,
  sleep, back off, or poll. Batch drainers, work pumps, `poll()`/`drain()`/`tick()` shapes,
  reactive `request(n)` accounting.
- Any time you re-implement a framework method that already had a progress contract. The
  framework's own callers were written against the original meaning, and they are not going
  to be adjusted for yours.
- When you add a fast path that resolves an item without going through the normal machinery.
  A cache hit, an idempotency short circuit, an early validation rejection, a
  request that turns out to be already satisfied - each is a new exit from the input queue,
  and each needs to be checked against the count.
- When a counter needs a helper to come out right. If the code performs an operation whose
  only justification is that a number would otherwise be zero, stop and re-derive the number.
- When one field is being read both by an operator dashboard and by control flow. Split it.
- **Not** for values that are purely observational. A metric that no code branches on can be
  defined however is most useful to a human, and "submitted to the pool" is often exactly
  right there.

## Examples

### Before and after, at the definition site

Before, the value was defined by the mechanism:

```java
// consumed only where a WorkContainer was submitted to the executor
inFlight.incrementAndGet();
recordsDispatched.incrementAndGet();
workerPool.execute(() -> runOnWorker(work, chainExecution));
```

After, it is defined by the caller's question, and the javadoc says whose question it is
(`PcTaskDispatcher.java:244-251`, quoted in Guidance 1 above). The increment moved to the
point of departure (`:278-279`), the drop path became a first-class consumed outcome
(`:291-297`), and the preparation-failure path did too (`:283-288`).

### The full caller chain, so the contract is not guesswork

Reading downstream once is what turns "seems fine" into a decision. Kafka 3.9.2, extracted
with:

```
unzip -p ~/.m2/repository/org/apache/kafka/kafka-streams/3.9.2/kafka-streams-3.9.2-sources.jar \
  org/apache/kafka/streams/processor/internals/StreamThread.java
```

| Layer | Code | Consequence of `false` / `0` |
|---|---|---|
| the seam | `pc-streams.patch:512` returns `dispatchAvailable(...) > 0` | this is the value under discussion |
| `TaskExecutor.processTask` | `TaskExecutor.java:97-100` | ends the batch loop for that task immediately |
| `TaskExecutor.process` | `TaskExecutor.java:78, 88` | contributes 0 to the thread's total |
| `StreamThread.runOnceWithoutProcessingThreads` | `StreamThread.java:1003, 1049-1051` | breaks the processing loop, back to a blocking poll |

### The test that caught it, and the test that could not

Kafka's own `shouldUpdateOffsetIfAllRecordsAreCorrupted`
(`StreamTaskTest.java:2743-2772`, in `kafka-streams-3.9.2-test-sources.jar`) feeds a task two
corrupted records and asserts progress at line 2764:

```java
assertTrue(task.process(offset));
```

Every record is consumed, none reaches a processor, and stock still reports progress. That
assertion is the contract stated in executable form.

The module's own unit tests could not have caught it, and it is worth seeing why: every
`WorkPreparer` in `PcTaskDispatcherTest.java` returns a non-null `Runnable` on every record
(lines 299-308, the `ConcurrencyProbe` implementation at 370-391, and the `record -> () ->
{ }` stubs at 451, 471, 499, 515 and 518). None returns null, none throws during preparation.
Every test therefore ran the one route on which "consumed" and "dispatched" are the same
number. The suite is thorough about concurrency, ordering, failure isolation and crash
injection, and it is blind to this by construction. The missing case is a preparer returning
null for every record, asserting `dispatchAvailable(...)` equals the batch size.

### The counter that is correctly defined as "handed downstream"

`PcDispatchCounters.getRecordsDispatchedToPool()` keeps the definition that was wrong for
flow control, because for its purpose it is right: it exists so a green test cannot lie
about whether the seam was exercised at all
(`PcDispatchCounters.java:9-29`, and the single increment site at
`PcTaskDispatcher.java:300`). Two numbers, two contracts, both documented. The lesson is not
"stop counting submissions", it is "do not let one number answer two questions".

### The residual instance, unfixed at time of writing

`pc-streams.patch:504` still documents `pcProcess` as `@return true if any record was handed
to the worker pool this call`, above a body that returns consumption. The behaviour is
correct; the description is the pre-fix one. Documentation is where this defect class
regenerates, because the next implementation is written against the sentence, not the line.

## Related

- [docs/solutions/best-practices/chase-refuted-predictions.md](../best-practices/chase-refuted-predictions.md)
  - how this was found. Two tests predicted green stayed red, and splitting that one
  prediction into its separate claims produced this defect and the no-op-run defect
  independently. Neither was findable by reading the code.
- [docs/solutions/integration-issues/kafka-streams-couples-polling-and-processing-on-one-thread.md](../integration-issues/kafka-streams-couples-polling-and-processing-on-one-thread.md)
  - why a false zero is so expensive here specifically. `StreamThread` polls and processes on
  one thread, so the loop's response to zero progress is to block for up to `poll.ms`, during
  which nothing can be dispatched.
- [a-high-water-mark-cannot-express-out-of-order-completion.md](a-high-water-mark-cannot-express-out-of-order-completion.md)
  - the sibling failure in the same seam: a progress *position* that cannot express what
  concurrency made possible, where this is a progress *count* that cannot express what the
  fast paths made possible.
- `astubbs/parallel-consumer#271` - the Kafka Streams on Parallel Consumer spike, where both
  halves of this defect were found and fixed.
- `astubbs/parallel-consumer#255` - the tracking issue for the Kafka Streams dispatch spike.
