# `PCMetrics` holds `metersLock` across calls into the user's `MeterRegistry`

<!-- inflight-type: task -->
<!-- inflight-impact: refactor -->

The private `removeMeter(Meter.Id)`, `close` and `removeMetersByPrefixAndCommonTags` are all
`@Synchronized("metersLock")` and all call `remove(Meter.Id)` or `getMeters()` with that lock held.
Both are the **user's** code: a Micrometer `MeterRegistry` whose listener takes a user lock and
separately calls back into `PCMetrics` is a textbook AB/BA inversion.

The public `removeMeter(Meter)` is the exception, and a useful one - it is **not** synchronised, so
its `meter.getId()` call already happens outside the lock. That is the shape the fix below wants
everywhere.

**Filed as a task, not a bug, deliberately.** No such path exists in this codebase or in Micrometer's
own registries, and the shapes that would reach it are contrived. Raised in review of astubbs#57 and
rated theoretical there. Recording it as a `bug` would rank a hazard nobody can currently trigger
alongside live defects.

## The decision this needs

The fix is to collect the ids under the lock and call the registry outside it. That is a real
restructure of all three methods rather than a tweak, and it **must not undo the never-throws
contract they now carry**, which by now has three parts:

- `removeQuietly`, wrapping every `remove(Meter.Id)` call;
- the two-level guard in `removeMetersByPrefixAndCommonTags` - per-meter *and* around the
  enumeration. astubbs#57 established why both levels are needed: a loop-level guard alone leaves the
  confluentinc#859 tracking-set tail un-pruned, and a per-meter guard alone drops cover for
  `Search.in(registry)` calling `getMeters()`;
- the `meter.getId()` guard in the public `removeMeter(Meter)`, added last and found by review after
  the other two - guarding the two obvious calls into the registry had left the accessor between
  them exposed, on a path `PartitionState` and `WorkManager` take during revocation.

So the sequencing question is whether the lock restructure can be done without weakening either
guard, and that is what makes it more than a `docs/refactoring.md` line.
