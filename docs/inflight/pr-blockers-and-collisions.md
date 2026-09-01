# Open PRs: what `gh pr list` cannot tell you

<!-- inflight-type: register -->
<!-- inflight-impact: coordination -->


Blockers, collisions, and decisions someone is waiting on. Not a PR list - `gh` has that, and is right.

- **Agreed merge order for the astubbs#322 split and what queues behind it: 323, 324, 325, 57, 322, <!-- post-merge: checked - the merged/remaining tally was deleted rather than restated; it went stale at every merge in the list -->
  267, 29.** Which of them are still open is `gh pr list -R astubbs/parallel-consumer`'s answer, not
  this file's - the ORDER is the standing coordination fact, and it is recorded here because it
  outlived the note that held it (it lived only in astubbs#323's own note, which that PR's merge
  deleted).
- **astubbs#29 and astubbs#31 target `master-confluent`**, the pinned pre-rebrand mirror, so merging either would
  land its fix where no user can reach it. Retarget to `master` - but not mechanically: astubbs#29's deadlock
  fix predates the internals astubbs#80 reshaped, so it needs reconciling rather than replaying.
- **astubbs#38 (JUnit 6) is blocked on something other than the version bump.** JUnit 6 needs Java 17, *and*
  `archunit-junit5` will not run on it with no `archunit-junit6` engine in existence. The ArchUnit
  tests must be rewired first. See `deps-deferred-majors.md`.
<!-- post-merge: checked-begin -->
- **astubbs#51 (virtual threads) collides with astubbs#57** - both edit `PCMetrics.java`. Sequence,
  don't parallelise; whichever is still open rebases onto the other.
- **File ownership:** metrics and `PartitionStateManager` are astubbs#57's, `PartitionState` is
  astubbs#337's (the confluentinc#893 cherry-pick, split out of astubbs#57 on 2026-08-24), the
  offset encoders are astubbs#106's, and astubbs#29 will want the poll/lifecycle internals
  astubbs#80 reshaped. Pick parallel work accordingly - and check `gh pr list` for which of these
  are still open, since a merged one's files are simply master's again.
<!-- post-merge: checked-end -->
<!-- post-merge: checked-begin -->
- **`LogCapture` is the only supported way to capture a log line in this suite.**
  `bz.stub.parallelconsumer.internal.utils.LogCapture` is an `AutoCloseable` appender plus level
  override, and its javadoc owns the two hazards of raising a JVM-shared logger - reading someone
  else's lines, and flooding everyone with `DEBUG` - along with the different fix each one takes.
  Read it before writing a capture; do not open a second way to do this. Still un-converted:
  `SubmitWorkToPoolShutdownRaceTest`'s two inline `(Logger) LoggerFactory.getLogger(...)` +
  `ListAppender` blocks (`grep -n ListAppender` finds them). The astubbs#201 / astubbs#203 collision
  this bullet used to record is settled - astubbs#203's branch is merged into astubbs#201's and the
  inline copy in `LoadFactorCeilingReportingTest` is converted, so no rival implementation can reach
  master.
<!-- post-merge: checked-end -->
- **astubbs#8 (`features/retry-dlq`, 2022) is an abandoned draft**, kept only because it is the sole
  DLQ code that exists. Close or finish it; it is not in flight.
