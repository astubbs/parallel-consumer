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

## What guards it now, and what deliberately does not

**`HotPathLogArgumentsAreDeferredTest`** asserts the SLF4J behaviour the fix *rests on*, at a pinned
level, with the eager form as its control arm. Without it, an SLF4J upgrade that evaluated suppliers
eagerly would break the fix silently while the source still read correctly.

**Nothing catches the eager form being written at a NEW call site.** A bespoke source gate for this
one pattern was written and then deleted, on the ruling that a rule should be a row in
`bin/lib/source-patterns.mjs` rather than a script of its own. It was not carried over as a row, so
the gap is real and known rather than overlooked. ArchUnit is not the answer either: a guard is
*control flow*, ArchUnit reads the call graph, so it would flag the correctly-guarded neighbours
identically, and a rule with permanent exceptions is one nobody trusts.

**Building that gate taught something worth keeping even though the gate is gone.** Its first draft
used gawk's `ENDFILE` on a box whose `awk` is mawk: it parsed, matched nothing, and printed its
success line over a file containing this exact defect. Verified against the fixed tree only, it would
have shipped as a gate that passes everything - the false-green class documented under
`../workflow-issues/a-check-that-reports-success-without-having-run.md`. **A gate written to catch a
defect must be run against a tree that still has it**, and that applies to a table row as much as to
a script.

## What the fix was worth, and how it was established

Two independent measurements, on unrelated hardware, with only this one file changed.

**On CI - a one-term comparison between two heads.** The neighbouring classes are the control: a
uniformly slower machine slows everything proportionally, and these did not move together.

| | failing run | passing run |
|---|---|---|
| `VeryLargeMessageVolumeTest` | 53.86 s | 53.28 s |
| `LargeVolumeInMemoryTests` | 39.45 s | 38.28 s |
| `LoadTest` | 41.34 s | 40.32 s |
| `MultiInstanceRebalanceTest` | 0.020 s, all skipped | 170.9 s, 3 tests, all passed |
| **`MultiInstanceHighVolumeTest`** | **FAILED, 43,552 rec/s** | **PASSED, 76,950 rec/s** |

Two confounds were ruled out rather than argued away. **Lane composition**: the capacity profiles
cost 0.020 s with every test skipped in the failing run, and it still failed - and in the passing run
they ran 170.9 s of churn ahead of the subject in the same reused JVM, so the confound was *re-added*
and the number rose anyway. **Runner speed**: the three neighbours land within 1-3%.

**Locally - the same swap, five runs per arm, JVM pinned to two processors.**

| arm | records/second |
|---|---|
| supplier form (fixed) | 109,894 · 147,579 · 131,590 · 122,374 · 101,228 |
| eager form (pre-fix) | 80,398 · 69,772 · 66,815 · 75,441 · 73,733 |

No overlap, and the means differ by roughly the same factor CI showed. The order confound was tested,
not assumed: the first pairs ran fixed-then-eager and the eager arm declined monotonically within
itself, which is also what a thermally drifting machine looks like - so later pairs were interleaved
with the *eager* arm first, and the fixed arm still won from the disadvantaged position. Method:
`../best-practices/an-a-b-whose-arms-run-in-time-order-is-confounded-with-time.md`.

**Reproduce, do not quote.** These figures describe two machines on one day; the method transfers and
the numbers do not. `git diff <pre-fix> <post-fix> -- '*/src/main/java/*'` returning exactly one file
is what made either comparison a control arm rather than a coincidence.

## What this did NOT fix

`largeNumberOfInstances` stalls at a rebalance - a member stops answering, the group dwells in
`PreparingRebalance` - and that reproduces on the fixed tree with the same signature it had before.
It was briefly suspected that a control thread doing this scan every pass was why a live member
answered late; enabling the test put that under test and it lost. Two problems found in the same
week, not one. `../../inflight/test-largenumberofinstances-residual-failures-measured-not-explained.md`
owns the survivor.

## How to find the next one

`grep` for a log call whose argument list contains `(` - a method call rather than a field - and ask
whether that method is O(1). The dangerous shape is specifically an accessor that *scans*, on a path
that runs per-iteration. There is no gate for it today, so this is a review-time check - and if it
ever bites twice, the fix is a row in `bin/lib/source-patterns.mjs`, not another script.
