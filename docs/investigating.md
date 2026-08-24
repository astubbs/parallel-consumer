# Settling an investigation

How to prove a cause rather than accept a fix that appears to work. AGENTS.md carries the prior-art
checks you run *before* forming a hypothesis, because an agent mid-debug will not think to open a
document about investigating. This is the method for what comes after.

Promoted here from
[`docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md`](plans/2026-08-03-001-investigate-transactional-commit-flake.md)
§11, because a dated plan goes stale once its work lands and this method must not go with it.

## A fix that works is not evidence of the cause

- **Confirm a cause with a control arm, not with a fix that appears to work.** Change the one term
  you believe is responsible, hold everything else identical, and show the outcome flips.
  Same-magnitude, different-position beats bigger-hammer. The worked example: an identical 400ms
  delay injected on either side of a lock release - *after* it (opening the window) failed 8/8;
  *before* it (same added latency, inside the lock) passed 8/8, against a ~1-in-6 baseline. The
  control arm is what ruled out "it is just slower under load", which every previous look at that
  flake had concluded.
- **State the prediction before running it, and report the refuted ones.** A prediction that fails
  is the cheapest result you will get. If a fix works but its prediction was wrong, you have a
  symptom.
- **Report the rate and the conditions, never a bare verdict.** "0 failures" is meaningless without
  N and the load. `bin/soak-test.sh <Class#method> <runs>` at a low `SOAK_FREE_CORES` is the house
  reproducer; its own closing line says it - no failures is not proof the flake is gone. Distinguish
  "cannot reproduce" from "did not happen".
- **A guard added with a fix must be verified by negative control.** Break the thing it guards and
  confirm it fails deterministically. An assertion nobody has seen fail is decoration.

## Verify your instrumentation actually reached the run

Logging or config changes the build does not pick up produce a silent false negative, and the result
reads as a real "no effect". Confirm the build step succeeded and that the new setting is visible in
the output before believing any instrumented result. Two traps have each voided an experiment here:

- `./mvnw -pl <module>` **without `-am`** fails the `ReactorModuleConvergence` enforcer, so the test
  never recompiles and both arms run the stale class.
- `surefire:test` alone **does not reprocess test resources**, so an edited `logback-test.xml` never
  reaches `target/test-classes` and your new logging silently does not exist.

Use `./mvnw -pl parallel-consumer-core -am verify` (what `bin/soak-test.sh` runs) and confirm
`BUILD SUCCESS` on the compile step. Better, assert the setting in the run's own output - PC logs
its full options at INFO on init, so the arm proves itself.

## Worked example of the prior-art rule

2026-08-07: the `TransactionTimeoutsTest.commitTimeout` handoff searched for the test's own name,
found nothing, and classified the failure by analogy. Grepping the *mechanism*
(`producerTransactionLock` / `commitLockAcquisitionTimeout`) finds
`docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md`, the only prior investigation
into that exact lock - which already documented the lock's ordering invariant, the
controlled-experiment method above, and the build trap that had silently voided an earlier
experiment. All of it applied; none of it was used.
