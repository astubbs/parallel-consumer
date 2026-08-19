# astubbs#322 - the confluentinc#909 reproduction: what is still open

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

Bottom of the three-PR stack (`fix/909-load-reproduction`, base `master`). **11 files.** The
confluentinc#909 fix itself is already on master - `git diff origin/master HEAD` is empty for
`ProcessingShard.java` - so this PR is the *reproduction and the harness*, not the fix. Delete this
note when it merges.

## The one that needs a decision: the reproduction cannot observe the collision

**`RegistrationRaceStaleResidentIT` rules out the two known heal paths but never observes the
collision itself.** It fails a healed run as invalid rather than passing it, which is right - but the
guard is indirect. The uncovered path is `ProcessingShard`'s take-scan stale eviction, suppressed
today only by `WorkManager`'s `delta < 1` guard. **If that arithmetic ever shifts, the IT goes
permanently green with the defect branch unexercised** - a reproduction that silently stops
reproducing, which is worse than none.

**Proposed fix:** count stale-resident collisions at insert and assert the count is exactly 25, so
the test asserts the mechanism rather than the absence of two known escapes. Needs a broker to
verify, so it is **not applied**.

This is the honest answer to "can it pass vacuously?": not today, and not for the heals we know
about. The hole is the heals we do not.

## Also open

- **`produceKeyedRange` belongs in the shared helper.** `docs/testing.md` requires it; the fix is an
  offset-keyed `KafkaClientUtils` overload. Left because the shared helper's key sequence restarts
  per call, so it is not a pure move.
- **The PR body still describes work that landed elsewhere** - it narrates the confluentinc#909,
  astubbs#177 and astubbs#209 fixes as delivered here. None are in the 11-file diff. The "Title & body
  reflect final content" box is therefore wrong.
- **Two claims nobody has verified**: that the IT has had four green runs, and - separately from the
  measurement already recorded in `docs/testing.md` - whether `-Dpc.log.level` propagates into
  *failsafe* forks as well as surefire.

## Already fixed, recorded so it is not re-litigated

- **vertx lost its integration-test narration.** It is the only sibling with a failsafe suite
  (`VertxConcurrencyIT`, nine `log.info` calls) and the `integrationTests` info-floor had been added
  to core only.
- **The quarantine rule was severed from its own documentation by the split.** `Quarantined.java`
  said "no quarantine without EVIDENCE" and cited a registry that still said "without diagnosis",
  because the annotation sorted into this PR and the doc into astubbs#323. The registry hunk was
  pulled in so the rule and its enforcement land atomically.

## Residual risks, informational

`LatchTestUtils.awaitLatch` re-arming can consume the whole `@Timeout(300)` budget worst-case; the
deferred `availableWorkContainerCnt` undercount still has no `docs/refactoring.md` entry; the
instrument's own narration is mute below `warn`.
