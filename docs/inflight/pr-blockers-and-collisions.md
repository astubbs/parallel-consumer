# Open PRs: what `gh pr list` cannot tell you

<!-- inflight-type: register -->
<!-- inflight-impact: coordination -->


Blockers, collisions, and decisions someone is waiting on. Not a PR list - `gh` has that, and is right.

- **Agreed merge order for the astubbs#322 split and what queues behind it: 323, 324, 325, 57, 322,
  267, 29.** The first three have merged, so what remains is **57, 322, 267, 29**. Recorded here
  because it outlived the note that held it - it lived only in astubbs#323's own note, which that
  PR's merge deleted - and because an ordering is a standing coordination fact rather than one PR's
  business.
- **astubbs#29 and astubbs#31 target `master-confluent`**, the pinned pre-rebrand mirror, so merging either would
  land its fix where no user can reach it. Retarget to `master` - but not mechanically: astubbs#29's deadlock
  fix predates the internals astubbs#80 reshaped, so it needs reconciling rather than replaying.
- **astubbs#38 (JUnit 6) is blocked on something other than the version bump.** JUnit 6 needs Java 17, *and*
  `archunit-junit5` will not run on it with no `archunit-junit6` engine in existence. The ArchUnit
  tests must be rewired first. See `deps-deferred-majors.md`.
- **astubbs#51 (virtual threads) collides with astubbs#57** - both edit `PCMetrics.java`. Sequence, don't parallelise.
- **File ownership right now:** astubbs#57 owns metrics + partition state, astubbs#106 owns the offset encoders, and
  astubbs#29 will want the poll/lifecycle internals astubbs#80 reshaped. Pick parallel work accordingly.
- **astubbs#8 (`features/retry-dlq`, 2022) is an abandoned draft**, kept only because it is the sole
  DLQ code that exists. Close or finish it; it is not in flight.
