# Groom the 1.0 release train issue against the roadmap data

astubbs#135 (mirroring confluentinc#172) is the 1.0 release train. It has not been groomed in years and
is now a second, stale account of something `docs/data/roadmap.yaml` owns.

**The roadmap data is the source of truth.** The issue should stop carrying its own checklist and
point at the data instead, or be closed. Left as it is, the stale copy is the one on the public
tracker, which is the wrong way round.

## What grooming it means

- **Its big-picture items belong in the roadmap data**, not in a checklist. Already carried over:
  thread safety across the public API (astubbs#139), safe user exposure of the Consumer APIs
  (astubbs#158), health checks (astubbs#126), the dead letter queue (astubbs#149) and virtual threads
  (astubbs#147).
- **Two items are backlog, not 1.0.** The shared-nothing architecture refactor (astubbs#142) and
  removing the streaming interfaces are someday items and up for debate - the thread-complexity
  problem the first was raised for has largely been fixed since. They should not sit on a 1.0 train
  implying they gate it.
- **The sweeps are chores, not roadmap entries.** Javadoc cleanup, dead code, and the todo sweep. The
  last is already covered by `docs/TODO_INDEX.md`.
- **The disabled-test sweep has been promoted to a v6 gate**, so it leaves the 1.0 train entirely. It
  lives in `release-0.6.0.0.md`, tracked by astubbs#263. Note the issue claims six files carry
  `@Disabled` while a current count finds four tests; reconcile rather than trusting either number.
- **Its ticks are stale in at least one known place.** The issue's own caveats say confluentinc#192 is
  unticked upstream but shipped in 0.5.2.6, and that some entries are PRs rather than issues. Check
  each entry against its mirror's Fork status rather than the tick.

## Worth keeping from it

The upstream body carries a gating rule that is better than anything invented since: *only items
marked high will block the 1.0 release*. That distinction between blocking and merely targeted is
worth preserving in whatever replaces the checklist.

## Delete when

astubbs#135 no longer carries a checklist that competes with the roadmap data.
