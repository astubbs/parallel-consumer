# Open PRs: what `gh pr list` cannot tell you

<!-- inflight-type: register -->
<!-- inflight-impact: coordination -->


Blockers, collisions, and decisions someone is waiting on. Not a PR list - `gh` has that, and is right.

- **Agreed merge order for the astubbs#322 split and what queues behind it: 323, 324, 325, 57, 322, <!-- post-merge: checked - the merged/remaining tally was deleted rather than restated; it went stale at every merge in the list -->
  267, 29.** Which of them are still open is `gh pr list -R astubbs/parallel-consumer`'s answer, not
  this file's - the ORDER is the standing coordination fact, and it is recorded here because it
  outlived the note that held it (it lived only in astubbs#323's own note, which that PR's merge
  deleted).
- **The `master-confluent` retarget is done - what is left is the reconciliation.** astubbs#29 and
  astubbs#31 both used to target the pinned pre-rebrand mirror, where a merge lands a fix no user can
  reach. astubbs#31 has since merged, and astubbs#29 now targets `master` and is out of draft. The
  half that was never mechanical still stands: astubbs#29's deadlock fix predates the internals
  astubbs#80 reshaped, so it needs reconciling rather than replaying.
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
- **astubbs#8 (`features/retry-dlq`, 2022) is an abandoned draft**, kept only because it is the sole
  DLQ code that exists. Close or finish it; it is not in flight.

## The transactional stack - what it was, and the one debt it left

<!-- post-merge: checked-begin - the chain is recorded as history and the outstanding item is stated
     against master rather than against a PR's live state, so nothing here turns false on a merge -->
Three PRs, one dependency chain, declared with `depends on` in astubbs#262's body and enforced by the
`Check PR Dependencies` required check. Both parents have merged - astubbs#261 on 2026-08-14, then
astubbs#257 - so the chain is discharged and only astubbs#262 is still open. Kept because a reader who
knows this work as a three-PR stack needs telling which parts are already master.

1. **astubbs#261**, merged - a terminally failed send left a partial result set visible at
   `read_committed`.
2. **astubbs#257**, merged - produce-lock double release. At `batchSize >= 2` the lock was taken per
   poll context but released per record.
3. **astubbs#262**, open - the battle test itself. **Rebase-merge, not squash**: it holds separable
   workstreams, and squashing buries two real defect discoveries under one 5,000-line test commit.

**The debt: astubbs#257's merged commit message understates the defect, and release notes are
generated from the log.** It describes redelivery - "handed records back for a second delivery" - and
never says the word *stall*. What astubbs#262 established is more severe: because only a *success*
marks a partition dirty, every batch failing meant **no commit was ever attempted**, and the source
offset froze at 3 of 201. `grep -i stall` against `b36ad9428` returns nothing. Correcting it now means
either an amended note in the release section when 0.6.0.0 is cut, or a follow-up commit that says so
- but the changelog generator will publish the weaker claim until somebody does one of them.
<!-- post-merge: checked-end -->

### Decisions waiting on a human

- **Two pre-existing main-code holes need their own PR**, written up with fix and test shapes in
  `bug-eos-swallowed-produce-failures.md`. The `InvalidPidMappingException` one is the serious one: a
  whole batch is marked *succeeded* and its offsets committed for records whose output was never
  produced. Same shape as the defect astubbs#261 fixed, and the single exception to the rationale that
  justified it.
- **The commit-interval identity check** (`bug-commit-interval-identity-check.md`) - an explicit
  `Duration.ofSeconds(5)` is silently replaced with 100ms. One-line fix, wants its own change so the
  behaviour change is visible.
- **Register hardening** (`next-transactional-register-hardening.md`) - ranked by how much false
  assurance each item buys. The top one is not subtle: `-Dexcluded.groups=transactions` is a
  documented, supported invocation that runs **zero** claim proofs while the register reports every
  claim covered.
- **Phase B, the transactional chaos scenario**, is deliberately deferred and should follow
  **astubbs#29**. Calibrating its SLOs against a master that still carries the confluentinc#803
  commit-lock deadlock would fold that defect into the baseline and then report the live bug as an
  in-SLO event. This is the one place the transactional work and the astubbs#29 backlog actually meet.
