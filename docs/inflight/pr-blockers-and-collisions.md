# Open PRs: what `gh pr list` cannot tell you

Blockers, collisions, and decisions someone is waiting on. Not a PR list - `gh` has that, and is right.

- **#29 and #31 target `master-confluent`**, the pinned pre-rebrand mirror, so merging either would
  land its fix where no user can reach it. Retarget to `master` - but not mechanically: #29's deadlock
  fix predates the internals #80 reshaped, so it needs reconciling rather than replaying.
- **#38 (JUnit 6) is blocked on something other than the version bump.** JUnit 6 needs Java 17, *and*
  `archunit-junit5` will not run on it with no `archunit-junit6` engine in existence. The ArchUnit
  tests must be rewired first. See `deps-deferred-majors.md`.
- **#51 (virtual threads) collides with #57** - both edit `PCMetrics.java`. Sequence, don't parallelise.
- **File ownership right now:** #57 owns metrics + partition state, #106 owns the offset encoders, and
  #29 will want the poll/lifecycle internals #80 reshaped. Pick parallel work accordingly.
- **#1 (`codeql`, 2026-04) and #8 (`features/retry-dlq`, 2022) are abandoned drafts**, kept only
  because #8 is the sole DLQ code that exists. Close or finish them; they are not in flight.
