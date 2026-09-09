# Open PRs: what `gh pr list` cannot tell you

<!-- inflight-type: register -->
<!-- inflight-impact: coordination -->
<!-- inflight-vetted: 2026-09-07 - re-checked every PR named here with `gh pr view -R astubbs/parallel-consumer`; pruned the four all-merged bullets (the astubbs#322 merge order, the `master-confluent` retarget, the astubbs#51/#57 collision, the file-ownership map), confirmed astubbs#38 and astubbs#8 still open and `SubmitWorkToPoolShutdownRaceTest` still holds two inline `ListAppender` blocks, and corrected the Phase B bullet - `ChaosRevokeUnderWorkTransactionalIT` landed with astubbs#29 and is UNCALIBRATED -->


Blockers, collisions, and decisions someone is waiting on. Not a PR list - `gh` has that, and is right.

- **astubbs#38 (JUnit 6) is blocked on something other than the version bump.** JUnit 6 needs Java 17, *and*
  `archunit-junit5` will not run on it with no `archunit-junit6` engine in existence. The ArchUnit
  tests must be rewired first. See `deps-deferred-majors.md`.
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

## The transactional stack - what it was, and the one debt it left

<!-- post-merge: checked-begin - the chain is recorded as history and the outstanding item is stated
     against master rather than against a PR's live state, so nothing here turns false on a merge -->
Three PRs, one dependency chain, declared with `depends on` in astubbs#262's body and enforced by the
`Check PR Dependencies` required check. **All three are master now** - astubbs#261, then astubbs#257,
then astubbs#262, which was rebase-merged rather than squashed so its separable workstreams stayed
separable in the log. Kept because a reader who knows this work as a three-PR stack needs telling that
none of it is pending, and because the debt below outlived it.

1. **astubbs#261** - a terminally failed send left a partial result set visible at `read_committed`.
2. **astubbs#257** - produce-lock double release. At `batchSize >= 2` the lock was taken per poll
   context but released per record.
3. **astubbs#262** - the battle test itself, and the claim register that is now the standing gate on
   the exactly-once headline.

**The debt: astubbs#257's merged commit message understates the defect, and release notes are
generated from the log.** It describes redelivery - "handed records back for a second delivery" - and
never says the word *stall*. What astubbs#262 established is more severe: because only a *success*
marks a partition dirty, every batch failing meant **no commit was ever attempted**, and the source
offset froze at 3 of 201. `grep -i stall` against `b36ad9428` returns nothing. Correcting it now means
either an amended note in the release section when 0.6.0.0 is cut, or a follow-up commit that says so
- but the changelog generator will publish the weaker claim until somebody does one of them.
<!-- post-merge: checked-end -->

### Decisions waiting on a human

- **Register hardening** (`next-transactional-register-hardening.md`) - ranked by how much false
  assurance each item buys. The top one is not subtle: `-Dexcluded.groups=transactions` is a
  documented, supported invocation that runs **zero** claim proofs while the register reports every
  claim covered.
- **Phase B, the transactional chaos scenario, has landed and is UNARMED.**
  `ChaosRevokeUnderWorkTransactionalIT` came in with astubbs#29 and its javadoc carries the
  `Calibration status: UNCALIBRATED` block - it has been shown to RUN, never to go red on a tree
  that should fail. The decision waiting on a human is whether anyone arms it before v6; until
  then a green run is not evidence the transactional revoke path is healthy. Do not re-derive
  the reasoning here - the class javadoc owns it.
