# The two dark core tests, and the missing produce-failure test - measured behaviour

> **Salvaged 2026-08-12.** These measurements were the only thing in
> `docs/plans/2026-08-08-002-test-inactive-test-remediation-plan.md` that outlived that plan - read
> the original at
> `git show a69cd348:docs/plans/2026-08-08-002-test-inactive-test-remediation-plan.md`. The
> plan's own work landed with astubbs#263/astubbs#264, and `AGENTS.md` says a plan goes stale once
> its work lands - but three of its units were never started, and their evidence existed nowhere
> else in the tree. It is moved here rather than deleted with the plan, for the same reason
> `large-volume-in-memory-tests-oom-diagnostics-2026-04-22.md` was salvaged off a doomed branch:
> the next person to pick this up would otherwise re-derive it from scratch.
>
> This is a **point-in-time record**, per this directory's contract. The tables were produced by
> running the choreographies directly against `master` as of 2026-08-08, and were identical across
> all three commit modes. Re-run them before trusting them if the commit path has moved since.

## What is still outstanding

Three units, all in `ParallelEoSStreamProcessorTest` and its base class:

1. Re-enable `offsetsAreNeverCommittedForMessagesStillInFlightLong`.
2. Re-enable `processInKeyOrder`.
3. Write `userSucceedsButProduceToBrokerFails` - a produce-failure path that exists, is reachable,
   and has no test at all.

Both dark tests fail **100% deterministically**, not intermittently, and the investigation concluded
the **library is correct** - the tests assert the wrong expectations. They need assertion surgery,
not diagnosis. Re-enable the `Long` one first: it and `processInKeyOrder` touch the same file and
share the assertion idiom, and `Long` is the cheaper place to establish it.

**The blocker is gone.** This work was held behind astubbs#260, which rewrote both files all three
units touch and collapsed PC's repeat commits inside `assertCommits`. **astubbs#260 is merged**, so
the stated precondition is satisfied and these are ready to start.

## The rule that governs these tables

**A repeated value is context, not an assertion target.** Where the same offset appears twice in a
row, that is PC re-committing the same base offset, and how many times it does so depends on where
the wall-clock commit ticks fall. Assert the **set** per partition, never the repeat count - this is
exactly what astubbs#260 collapsed inside `assertCommits`.

## `offsetsAreNeverCommittedForMessagesStillInFlightLong`

Partition 0, `UNORDERED`:

| Step | Committed | Note |
|---|---|---|
| all six in flight | (none) | |
| release 1 | 0 | base offset only |
| release 2 | 0, 0 | repeat - do not assert the count |
| release 0 | 0, 0, 3 | commit advances once 0-2 are contiguous |
| release 3 | 0, 0, 3, 4 | |
| release 4 and 5 | 0, 0, 3, 4, 6 | |

Genesis-trimmed, the final assertion is `of(3, 4, 6)`. The test as written expects `of(2, 3, 5)`,
which is the source of its deterministic failure.

## `processInKeyOrder`

Two partitions. Partition 1's base offset is **4**, because record creation uses a global offset
counter:

| Step | p0 | p1 | Note |
|---|---|---|---|
| A: 6, 8, 1 released; 0 blocking | (none) | 4 | p1 bootstrap at its base offset |
| B: 2 released | 0 | 4 | |
| C: 0 released | 0, 3 | 4 | |
| D: 3, 5 released | 0, 3, 4 | 4, 4 | p1 repeat - do not assert the count |
| E: 4 released | 0, 3, 4 | 4, 4, 7 | the invariant worth guarding |
| F: 7 released | 0, 3, 4 | 4, 4, 7, 9 | |

Read through `assertCommitLists`, which trims partition 0's genesis commit unconditionally and
collects each partition into a set, the assertable per-partition expectations are:
`(p0=[], p1=[4])`, `(p0=[], p1=[4])`, `(p0=[3], p1=[4])`, `(p0=[3,4], p1=[4])`,
`(p0=[3,4], p1=[4,7])`, `(p0=[3,4], p1=[4,7,9])`.

**Step E is the invariant worth guarding**: partition 1 will not advance past 4 while `key-2`'s
offset 4 is in flight, even though 5, 6 and 8 have completed - and it then jumps to 7, not 9,
because 8 is done but not contiguous.

## `userSucceedsButProduceToBrokerFails` - how to write it

Place it beside the existing happy-path produce test in `ParallelEoSStreamProcessorTest`, and inject
the send failure through the existing `producerSpy` - the base class already exposes a Mockito spy
over a `MockProducer`, so no new helper is needed. Use the repo's existing classified test exception
rather than a bare `RuntimeException`, and parameterise over `CommitMode` to match its neighbours.

Assert the **consequence**, not just the throw: the record's offset is not committed, and the record
is retried. Write the failing assertion first and confirm it fails for the produce failure rather
than a setup error.

`closePCWhenInvalidPidMappingException` in the same class is the closest existing shape - it covers
the one special-cased produce exception, where this covers the general path. Reuse `assertCommits` /
`assertCommitLists` and the generated `ManagedTruth` assertions, per the `AGENTS.md` rule against
new helpers.

Two things not to do:

- **No producer-callback-thread case.** That asymmetry is owned by open astubbs#261, and it is
  unreachable here anyway: the base class forces every test onto one auto-completing `MockProducer`
  that invokes callbacks on the calling thread, with no I/O thread to raise from.
- **Check astubbs#257 before treating a transactional disagreement as a finding.** The redelivery
  assertion sits on semantics that PR is currently changing for transactional batches, so if the
  transactional parameter disagrees with the other two, that is the likely cause.
