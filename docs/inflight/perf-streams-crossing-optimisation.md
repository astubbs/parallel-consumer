# Parked: everything about making the Streams crossing faster

<!-- inflight-type: register -->
<!-- inflight-state: deferred - until the PoC has shown the concept works at all -->

**Parked 2026-08-24, deliberately and while it was going well.** Three measurements, a reviewed
implementation plan and a demolition of that plan all exist and are worth keeping. None of it should
be built yet, because it optimises a proof of concept that has not yet proved the concept.

The proof of concept runs **one stateless foreign operator, five builder methods, one stream thread,
at-least-once, in-memory state, no joins, no windows, no punctuators, no interactive queries.** Work
that makes that faster is premature against work that makes it *real*.

## What is parked, and what each is worth on return

| Artifact | What it established |
|---|---|
| [`perf-streams-crossing-attribution.md`](perf-streams-crossing-attribution.md) | The crossing is ~150us and the engine's own marginal cost is statistically zero |
| [`perf-crossing-fixed-versus-per-byte.md`](perf-crossing-fixed-versus-per-byte.md) | ~120us fixed + ~6.5us/KB, so bundling's payoff depends on payload: 16x at 1 KB, ~7x at 3 KB, 2x at 16 KB |
| [`perf-crossing-is-cpu-and-serialised.md`](perf-crossing-is-cpu-and-serialised.md) | The crossing is CPU-heavy not blocked; threads plateau at 1.5x; **every crossing is serialised through one `transmitLock`** |
| [`../plans/2026-08-24-002-feat-streams-invocation-bundling-plan.md`](../plans/2026-08-24-002-feat-streams-invocation-bundling-plan.md) | A reviewed bundling design - and its review, which found two P0s in it |

**The single most useful thing in the pile** is that the boundary is serialised at one lock. It
means the ceiling is structural rather than a cost-per-record problem, and it makes *one session per
stream thread* the obvious first thing to measure on return - smaller than bundling, and aimed at
the mechanism actually found.

## Two P0s the bundling plan must fix before it is executable

Do not pick that plan up and start building. Its review left it falsified in two places, both
verified against Kafka 3.9.2 sources:

- **`StateStore.flush()` runs after the commit, not before.** `StreamTask.prepareCommit()` calls
  `stateMgr.flushCache()`, which only reaches `store.flush()` for a `TimeOrderedKeyValueBuffer`; the
  only caller of `ProcessorStateManager.flush()` is reached from `postCommit()`. The plan's leading
  mechanism for "no record is committed while unforwarded" does not exist.
- **`forward()` is illegal from a flush hook.** `ProcessorContextImpl.forward()` throws when there
  is no current node, and `StreamTask` nulls it in the `finally` of both `process()` and
  `punctuate()`.

The suggested repair is a **durable buffer** - hold buffered records in a changelogged store so a
crash restores them, which is how `KTable.suppress()` survives the same hazard - rather than any
ordering trick.

## Why this is parked rather than dropped

The measurements are cheap to lose and expensive to retake, and two of them refuted confident
predictions - including one of the reviewer's. The reasoning is durable even though the build is not
wanted yet.

**Resume when the concept is proven, not before.** The question that reopens this file is "our
topology works and is too slow", and nobody can ask that yet.
