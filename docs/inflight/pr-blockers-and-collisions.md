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
- **astubbs#51 (virtual threads) collides with astubbs#57** - both edit `PCMetrics.java`. Sequence, don't parallelise.
- **File ownership right now:** astubbs#57 owns metrics + partition state, astubbs#106 owns the offset encoders, and
  astubbs#29 will want the poll/lifecycle internals astubbs#80 reshaped. Pick parallel work accordingly.
- **astubbs#1 (`codeql`, 2026-04) is an abandoned draft** - close or finish it; it is not in flight.
- **astubbs#8 (`features/retry-dlq`, 2022) is deliberately kept open**: it is the sole DLQ code that
  exists, and the DLQ brainstorm (branch `docs/310-dlq-brainstorm`, prior-art report in
  `docs/plans/2026-08-18-001-investigate-dlq-prior-art-report.md`) intends to consume it as the
  implementation PR once requirements settle. Decision 2026-08-18, superseding the earlier
  "close or finish" instruction here.
