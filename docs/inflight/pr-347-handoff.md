# astubbs#347 handoff - the lane INVERTS as the fix PRs merge, and that is the designed behaviour

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

<!-- post-merge: checked-begin -->
Written 2026-08-25 for whoever finishes this PR. **Delete this file in this PR at merge prep.**
State at time of writing; verify against the live PR before acting.
<!-- post-merge: checked-end -->

## The one thing to internalise before touching anything

Every harness currently `assertThrows(LincheckAssertionError)` - **green means "Lincheck found the
bug"**, because the branch calibrated against a master where the bugs still exist. The operator's
stated merge order is tooling first, so as each fix PR lands, the corresponding harness goes red
**by design**. The remedy is flipping that harness to assert-no-failure - it then becomes a
regression detector over the whole operation set - never reverting the fix, never loosening bounds.

| Fix PR | Harness that inverts on its merge |
|---|---|
| astubbs#346 (checkpoint-3 double lookup) | `WorkManagerLincheckTest` |
| astubbs#345 (ShardManager NPE) | `ShardManagerLincheckTest` |
| astubbs#337 (confluentinc#894 two-read) | `PartitionStateLincheckTest` |
| astubbs#57 (PCMetrics ArrayList - the unprompted find) | the PCMetrics arm; also delete `bug-pcmetrics-registered-meters-is-a-plain-arraylist.md` (this branch) |

The lane is non-gating and must stay so - `bin/lincheck-test.sh`, ~28s. It is excluded from the
gating suites at **five** distinct points; the commit `test(ci): a new test lane must be excluded in
five places` documents where, and any new wrapper script must repeat the exclusion.

## Constraints established, not assumed - do not re-litigate

- `LincheckToolchainProbeTest` is a deliberately-broken red control. It exists because the first run
  reported a clean pass while instrumenting nothing (`wiremock-jre8` pins ASM 9.4; Lincheck needs
  9.6+). **If a dependency change ever downgrades ASM again, this probe is what goes red.** Do not
  delete it as "a test that asserts a bug".
- Stress strategy only. The model checker is blocked by the Lombok
  `@EqualsAndHashCode(callSuper = true)` interaction (`LincheckSuperHashCodeProbeTest` is the
  two-arm tripwire - its control arm starting to throw means upstream fixed it) and by replay
  non-determinism from micrometer and `parallelStream()` in two `PartitionState` accessors
  (removal of those is item 6 of `ci-build-hardening-register.md`, astubbs#344's branch).
- Jabel is a non-issue - established by experiment, recorded in the plan doc.
- The `#1`s in `docs/plans/2026-08-25-001-test-lincheck-poc-plan.md` are quoted Lincheck trace
  labels inside `issue-refs: exempt` markers - not unqualified issue refs; do not "fix" them.

## Cross-branch pointers

The evaluation this executes is `test-lincheck-jcstress-evaluation.md` on astubbs#344's branch -
tick its Lincheck arm when this merges. The full calibration verdicts and cost tables are in the
plan doc on this branch; the encoder tear's HALF-FOUND verdict is honest and should not be upgraded
without the range-top leg actually reproducing under Lincheck.
