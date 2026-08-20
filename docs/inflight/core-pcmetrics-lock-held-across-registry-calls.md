# `PCMetrics` holds `metersLock` across calls into the user's `MeterRegistry`

<!-- inflight-type: task -->
<!-- inflight-impact: refactor -->

`removeMeter`, `close` and `removeMetersByPrefixAndCommonTags` all call `remove(Meter.Id)` or
`getMeters()` with `metersLock` held. Both are the **user's** code: a Micrometer `MeterRegistry`
whose listener takes a user lock and separately calls back into `PCMetrics` is a textbook AB/BA
inversion.

**Filed as a task, not a bug, deliberately.** No such path exists in this codebase or in Micrometer's
own registries, and the shapes that would reach it are contrived. Raised in review of astubbs#57 and
rated theoretical there. Recording it as a `bug` would rank a hazard nobody can currently trigger
alongside live defects.

## The decision this needs

The fix is to collect the ids under the lock and call the registry outside it. That is a real
restructure of all three methods rather than a tweak, and it **must not undo the never-throws
contract they now carry** - `removeQuietly`, plus the two-level guard in
`removeMetersByPrefixAndCommonTags` (per-meter *and* around the enumeration). astubbs#57 established
why both levels are needed: a loop-level guard alone leaves the confluentinc#859 tracking-set tail
un-pruned, and a per-meter guard alone drops cover for `Search.in(registry)` calling `getMeters()`.

So the sequencing question is whether the lock restructure can be done without weakening either
guard, and that is what makes it more than a `docs/refactoring.md` line.
