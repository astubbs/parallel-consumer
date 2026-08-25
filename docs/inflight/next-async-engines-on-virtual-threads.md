# Can the async engines also run on virtual threads?

<!-- inflight-type: next -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

**Antony's question, 2026-08-22, asked on seeing the engine comparison.** Virtual threads are wired
into the core engine's worker pool. The async engines - Vert.x, Reactor, Mutiny, the proxy - go
through `ExternalEngine`, which **overrides `setupWorkerPool` to return a pool of ONE**, because their
user function does not hold a thread. So the two features have never met.

## Why it is not obviously pointless, which is the first thing to check

The naive answer is "an async engine holds no thread, so thread type is irrelevant". That is true of
the **user function** and false of everything around it:

- The **control loop** is still a platform thread, and `core-vt` at 100ms held 5,000 records in flight
  where `core` reached 2,824. Whatever produced that difference is not the user function.
- `ExternalEngine`'s single-thread pool still runs the **dispatch** half of each record - `addVertxHooks`,
  the subscribe call, the mailbox add. At 25,000 msg/s that is 25,000 dispatches through one thread.
- Records **complete** on the engine's own threads (Vert.x event loops, Reactor schedulers), which are
  platform threads sized by that library, not by `maxConcurrency`.

**So the question is really: which of `ExternalEngine`'s threads are the bound, and would any of them
be better as virtual ones?** That is measurable and nobody has measured it.

## What makes it worth asking now

`proxy` measured **25,615 msg/s at 2ms**, within 1% of `core-vt`'s 25,934 - and it is the path every
non-JVM client takes. If an async engine can also take virtual threads, the two best results in the
comparison compose rather than compete. If it cannot, that is a ceiling on the whole proxy story and
should be known before it is built on.

## The experiment

1. Does `useVirtualThreads` even reach an `ExternalEngine`? Read `ExternalEngine#setupWorkerPool` and
   the option's validation path - it may be silently ignored, which would be worth a warning
   regardless of the outcome.
2. If it is ignored, force it: give `ExternalEngine` a virtual-thread dispatch executor and re-run
   the arms at the operating points in
   [`perf-engine-comparison-2026-08-22.md`](perf-engine-comparison-2026-08-22.md). **Same points, or
   the numbers are not comparable.**
3. Control arm: the same engine on JDK 21 with virtual threads OFF, so JDK version is not confounded
   with thread type - the trap `bench/run-bisect.sh` already documents for `core-vt`.

**Expected result, stated in advance so it can be wrong**: little or no change at 100ms, where the
engines already reach 5,000 in flight and the bound is elsewhere; a possible gain at 0-2ms, where
dispatch cost dominates and one platform thread is doing all of it.

See also: [`perf-virtual-threads-measured.md`](perf-virtual-threads-measured.md),
[`next-virtual-threads-under-graalvm-native.md`](next-virtual-threads-under-graalvm-native.md).
