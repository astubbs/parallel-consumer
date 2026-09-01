---
title: "SLF4J defers formatting, not argument evaluation: a disabled log level does not disable the work its arguments do"
date: 2026-09-01
category: performance-issues
module: parallel-consumer-core
problem_type: performance
component: control_loop
severity: high
applies_when:
  - "Passing a method call, not a constant, as a log argument on a hot path"
  - "Reviewing a log statement whose neighbours are guarded by isTraceEnabled/isDebugEnabled and this one is not"
  - "A throughput regression appears only under resource constraint and not on a development box"
  - "Deciding whether a parameterised log line is free when its level is off"
related_components:
  - AbstractParallelEoSStreamProcessor
  - ShardManager
  - WorkManager
tags:
  - slf4j
  - logging
  - hot-path
  - throughput
  - lazy-evaluation
  - control-loop
related:
  - "../best-practices/an-a-b-whose-arms-run-in-time-order-is-confounded-with-time.md - how the fix's effect was measured without the order confound"
  - "../best-practices/ablate-your-own-change-not-only-the-baseline.md - the sibling attribution rule"
---

# SLF4J defers formatting, not argument evaluation

## The mistake, in one line

`log.trace("... {}", expensiveCall())` runs `expensiveCall()` **every time**, at every level,
including levels where nothing is printed. Turning trace off saves the string building and nothing
else.

This is easy to believe backwards, because the parameterised form exists *precisely* to avoid work
when the level is off - and it does avoid work: the concatenation. The arguments are ordinary Java
expressions evaluated at the call site before the logger is ever entered. No logging framework can
change that; it is the language.

## Why it was expensive here rather than merely wasteful

The argument was `wm.getNumberOfWorkQueuedInShardsAwaitingSelection()`, which reaches
`ShardManager.sumOfShardAvailableCounters()` and sums a counter across **every processing shard**.
Two properties turned a wasted call into a throughput defect:

- **The cost scales with the workload.** Under `KEY` ordering the shard map is keyed per record key,
  so the scan grows with in-flight key cardinality.
- **The frequency scales the same way.** Under saturation `timeToBlockFor` collapses toward zero, so
  the control loop spins fastest exactly when the scan is largest.

Both peak together, on the hottest path in the library. On a development box with spare cores this
is invisible; on a constrained runner the control thread is competing for the cores that would
otherwise be processing records, which is why it presented as "only fails on CI".

## The tell that this was a slip and not a habit

Every neighbouring call site was already correct - the other log statement in the same method, and
the one in `WorkManager`, are both wrapped in `isTraceEnabled()`/`isDebugEnabled()`, and the metrics
gauge takes a method reference the meter evaluates lazily. **When the neighbours are right and one
site is wrong, look for a recently added line rather than a misunderstanding.** This one arrived with
a feature branch, and master had nothing at that point in the loop.

## The fix, and why the fluent API rather than a guard

```java
log.atTrace()
        .addArgument(timeToBlockFor)
        .addArgument(() -> wm.getNumberOfWorkQueuedInShardsAwaitingSelection())
        .log("Control loop: blocking on mailbox for {}, queuedInShards={}");
```

`Logger#atTrace()` returns the **NOP builder** when trace is disabled, and the NOP builder never
invokes the supplier. That is what makes it free rather than merely cheap. An `isTraceEnabled()`
guard is equally correct and was the existing local idiom; the fluent form was chosen because it
keeps the arguments beside the message instead of introducing a block. Requires SLF4J 2.x.

## Two mechanisms guard it, because neither can see what the other sees

- **A source gate** (`bin/check-hot-log-args.sh`) - finds an unguarded scanning accessor in a log
  argument. ArchUnit cannot express this: a guard is *control flow*, ArchUnit reads the call graph,
  so it would flag the correctly-guarded neighbours identically, and a rule with permanent
  exceptions is one nobody trusts.
- **A behavioural test** (`HotPathLogArgumentsAreDeferredTest`) - asserts the SLF4J behaviour the fix
  *rests on*, at a pinned level, with the eager form as its control arm. A source check cannot see
  runtime behaviour, so a future SLF4J that evaluated suppliers eagerly would break the fix silently
  while the source still read correctly.

**The gate needed its own self-test, and that is not ceremony.** Its first draft used gawk's
`ENDFILE` on a box whose `awk` is mawk: it parsed, matched nothing, and printed its success line over
a file containing this exact defect. Verified against the fixed tree only, it would have shipped as a
gate that passes everything - the false-green class this repo documents separately under
`../workflow-issues/a-check-that-reports-success-without-having-run.md`.

## What the fix was worth

Measured two ways, on unrelated hardware, changing only this one file:

- **On CI**: a required performance check went from failing at about 43,500 records/second to passing
  at about 77,000, while three neighbouring test classes in the same run stayed within a few percent
  - which is the control that rules out "the machine was faster".
- **Locally**: five runs per arm with the eager form restored from the pre-fix commit. The arms did
  not overlap, and the ratio matched CI's closely on a completely different machine.

**Reproduce, do not quote.** These figures describe two machines on one day; the method is what
transfers. `git diff <pre-fix> <post-fix> -- '*/src/main/java/*'` returning exactly one file is what
made the comparison a control arm rather than a coincidence.

## How to find the next one

`grep` for a log call whose argument list contains `(` - a method call rather than a field - and ask
whether that method is O(1). The dangerous shape is specifically an accessor that *scans*, on a path
that runs per-iteration. `bin/check-hot-log-args.sh` encodes the known scanning accessors; extend its
denylist when a new one appears rather than relying on review to catch it.
