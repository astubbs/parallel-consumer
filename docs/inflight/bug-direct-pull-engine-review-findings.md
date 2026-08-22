# Reading the direct-pull engine for correctness rather than for speed

<!-- inflight-type: bug -->
<!-- inflight-impact: correctness -->

Opened 2026-08-22 on `test/direct-pull-coverage`. `DirectPullWorkerPool` and the three shared-state
changes it forced were merged onto `perf/engine-concurrency` as a measurement, with no
dedicated tests and - as far as the record shows - without anyone reading the code for correctness.
[`perf-direct-pull-measured.md`](perf-direct-pull-measured.md) owns the design and the numbers; this
note owns what a first correctness read found, and it is deliberately separated into what is proven
and what is not.

## The invariant that was feared broken, and is not

The worry, raised while this branch was being written, was that a `KEY` or `PARTITION` shard can hand
out two records at once now that N workers scan it concurrently - because the per-record
compare-and-set in `WorkContainer.onQueueingForExecution()` only stops two workers claiming **the
same** record, and the guarantee is about the **shard**.

**It holds, and two things in `ProcessingShard.getWorkIfAvailable` hold it:**

1. **`if (isOrderRestricted()) break;` sits outside the take/skip branch.** A thread that finds the
   head record unavailable falls into the `else`, and then breaks anyway - it never reaches the next
   offset. In-flight records are removed from the shard on **success**, not on being taken, so an
   occupied shard's head is still the first entry every concurrent scanner meets. The shard excludes
   itself.
2. **The claim decides, and now it is the only thing that decides** - `onQueueingForExecution()`.
   Two threads that both see the head as free both attempt the compare-and-set; the loser takes
   nothing and hits the same break.

   > **Point 2 read `isAvailableToTakeAsWork() && onQueueingForExecution()` when this was written,
   > and that pairing was a live defect**: the check evaluated three terms and the compare-and-set
   > re-validated only the in-flight one, so a puller whose decision predated another puller's
   > completion could claim an already-succeeded record and deliver it twice. Fixed by collapsing
   > both fields into one atomic `WorkContainer.ExecutionState`, so the claim compares against the
   > exact state it evaluated and the pre-filter is gone. Pinned by
   > `WorkClaimStateMachineTest.aClaimDecidedBeforeAnotherPullerCompletedTheRecordIsRefused`, which
   > fails against the old pairing.

**Neither is named anywhere as the ordering lock**, which is the problem: the guarantee lives in a
`break` placement in one class and a return value in another, and an engine change that moved either
would pass every correctness test in the suite. That is not hypothetical - the register records an
earlier attempt to split shard state into selectable and in-flight failing ten tests that nobody
predicted. It is now pinned by
`DirectPullConcurrentSelectionTest.anOccupiedOrderedShardHandsOutNothingElseHoweverManyThreadsAsk`
and by `DirectPullEngineParityTest.sameKeyRecordsAreNeitherConcurrentNorOutOfOrderUnderDirectPull`,
both of which go red when the `break` is moved inside the successful-take branch.

## A defect: a close that throws early leaks the worker threads, and they are not daemons

`AbstractParallelEoSStreamProcessor.innerDoClose` runs

```java
brokerPollSubsystem.drain();
...
directPullPool.ifPresent(DirectPullWorkerPool::stop);
```

in that order, with nothing between them that would still run `stop()` if `drain()` throws.
`doClose`'s `finally` only deregisters meters and sets `CLOSED`; it does not stop the pool.

Under the engine PC ships that ordering is harmless - the worker threads are idle inside a
`ThreadPoolExecutor` and the executor is shut down further down the same method. Under direct pull
they are **running a loop of their own**, so they are never terminated by anything: they park on the
five-second safety net, wake, find `canTakeWork` false, and park again, for the life of the JVM.
`setupWorkerPool` builds them from `Executors.defaultThreadFactory()`, so they are **non-daemon** -
`maxConcurrency` of them, holding the JVM open after a `close()` that reported failure.

**The fix is a `finally`, not a reorder**: `stop()` is idempotent and stopping the workers before the
poller is drained is not obviously safe, so it belongs in a `finally` around the body, next to the
executor shutdown it is paired with.

## Fragilities that are not defects today, listed so the next change to them is deliberate

- **`onWorkMaybeAvailable(0)` returns without bumping the version**, so an announcement of zero is
  not merely a no-op wake, it publishes nothing. The count comes from
  `ShardManager.getUpperBoundOnSelectableWork()`, which is derived from
  `getNumberOfWorkQueuedInShardsAwaitingSelection()` - a figure whose own comment says it "can still
  be negative due to race" and which is floored at zero. A zero read while a record is genuinely
  selectable therefore leaves every parked worker asleep until the five-second safety net, with no
  log line saying so. Not a stall, but a five-second throughput hole that nothing would attribute
  correctly.
- **Only the control loop ever announces.** A worker that claims one record and leaves ten behind
  does not bump the version, so a parked sibling waits for the next control-loop pass. In practice
  the loop announces every pass and this is invisible - but it makes throughput depend on
  control-loop cadence, which is the coupling direct pull exists to remove.
- **`workerLoop` catches `Exception`, not `Throwable`.** Its own comment says a worker that dies
  "takes a slice of the concurrency with it, and the symptom is a throughput number nobody can
  explain" - and then lets every `Error` a user function can raise do exactly that, silently. The
  same asymmetry is in the inner catch around `runner.accept`.
- **`directPullEngine` is validated nowhere.** Setting it on a module whose engine cannot support it
  (`ExternalEngine` - vertx, reactor, mutiny) is silently ignored rather than rejected or warned
  about, and its default is read from ambient JVM state (`Boolean.getBoolean("pc.directPull")`), so
  two identically-built options objects can differ.
- **The virtual-threads claim is very nearly true, but not quite.** `getNumberOfUserFunctionsQueued()`
  -> `ThreadPoolExecutor.getQueue().size()` is out of the pressure system under direct pull, as
  claimed, but `processWorkCompleteMailBox` still calls it inside a `log.isDebugEnabled()` guard.

## Where the tests are

- `parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/internal/DirectPullWorkerPoolTest.java`
  - the parking protocol: no spin, no lost wakeup, no missed shutdown, wake count matches the work.
- `parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/state/DirectPullConcurrentSelectionTest.java`
  - selection driven from many threads: ordered-shard exclusion, the claim CAS, the in-flight counter,
  cross-shard fairness.
- `parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/DirectPullEngineParityTest.java`
  - the engine end to end. **Turns the option on in its own options rather than reading the system
  property**, so the engine is exercised by an ordinary suite run and not only by
  `-Dpc.directPull=true`.

Every one of them was proven red by sabotaging the behaviour it guards; the mutations used are named
in each test's javadoc where the choice was not obvious.
