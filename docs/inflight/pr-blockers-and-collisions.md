# Open PRs: what `gh pr list` cannot tell you

<!-- inflight-type: register -->
<!-- inflight-impact: coordination -->


Blockers, collisions, and decisions someone is waiting on. Not a PR list - `gh` has that, and is right.

- **astubbs#29 and astubbs#31 target `master-confluent`**, the pinned pre-rebrand mirror, so merging either would
  land its fix where no user can reach it. Retarget to `master` - but not mechanically: astubbs#29's deadlock
  fix predates the internals astubbs#80 reshaped, so it needs reconciling rather than replaying.
- **astubbs#38 (JUnit 6) is blocked on something other than the version bump.** JUnit 6 needs Java 17, *and*
  `archunit-junit5` will not run on it with no `archunit-junit6` engine in existence. The ArchUnit
  tests must be rewired first. See `deps-deferred-majors.md`.
- **astubbs#51 (virtual threads) no longer collides with astubbs#57.** The collision was
  `PCMetrics.java`, and the fork's port simply drops that hunk: the review finding that motivated
  astubbs#51's `synchronized` -> `ReentrantLock` change there does not hold up (the private
  `removeMeter(Id)` overload is only ever reached from the public one, which already holds the lock,
  and `ReentrantLock` is reentrant), and neither of those monitors is held across a blocking call. So
  there is nothing to sequence. **astubbs#57 owns that file.**
- **The virtual-threads work does collide with astubbs#29**, on `commitCommand`, and the port
  deliberately does *not* touch it. astubbs#51 migrates all four `synchronized (commitCommand)` blocks
  to a plain `ReentrantLock`; astubbs#29 needs that same monitor changed with a specific lock
  *policy* (`tryLock` with a timeout, so the revoke/commit AB-BA cycle cannot close). A plain
  migration landing first either forecloses that fix or makes the file look as though it already has
  it. There is a comment at the site saying so. **astubbs#29 owns that monitor.**
- **File ownership right now:** astubbs#57 owns metrics + partition state, astubbs#106 owns the offset encoders, and
  astubbs#29 will want the poll/lifecycle internals astubbs#80 reshaped. Pick parallel work accordingly.
- **astubbs#1 (`codeql`, 2026-04) and astubbs#8 (`features/retry-dlq`, 2022) are abandoned drafts**, kept only
  because astubbs#8 is the sole DLQ code that exists. Close or finish them; they are not in flight.
