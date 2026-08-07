# Open PRs: what `gh pr list` cannot tell you

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
- **astubbs#1 (`codeql`, 2026-04) and astubbs#8 (`features/retry-dlq`, 2022) are abandoned drafts**, kept only
  because astubbs#8 is the sole DLQ code that exists. Close or finish them; they are not in flight.

## The transactional stack - land in this order

Three PRs, one dependency chain, enforced by the `Check PR Dependencies` required check. astubbs#262
will sit red on that check until both parents merge; that is the gate working, not a failure.

1. **astubbs#257** - produce-lock double release. At `batchSize >= 2` the lock was taken per poll
   context but released per record, so every batch failed and - because only a *success* marks a
   partition dirty - **no commit was ever attempted** and the source offset froze. Its own commit
   message describes the symptom as duplicates; astubbs#262 established it is a **stall**, which is
   more severe. Worth correcting the description before merge so the changelog generator has the right
   severity.
2. **astubbs#261** - a terminally failed send left a **partial result set visible** at
   `read_committed`. All checks green, `mergeStateStatus: CLEAN`. Cut from `master`, so it does not
   contain astubbs#257 and either can land first.
3. **astubbs#262** - the battle test itself. Merges both parents in. Recommends **rebase-merge, not
   squash**: the branch holds separable workstreams, and squashing buries two real defect discoveries
   under one 5,000-line test commit.

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
