# `StreamThreadTest` invalid-timestamps case is flaky, and it breaks a gate we rely on

**Found 2026-08-11, characterised with a control arm, pre-existing and recorded nowhere until now.**

`StreamThreadTest.shouldLogAndRecordSkippedRecordsForInvalidTimestamps[3]` fails intermittently in the
upstream-Kafka suite that `parallel-consumer-streams` runs against its patched classes.

**Measured, not assumed:** 2 failures in 5 runs at HEAD **without** the investigating agent's changes,
against 3 in 7 with them. Pre-existing. The control arm itself needed a second attempt - the first reran
already-built binaries and would have "proved" the flake was pre-existing on no evidence at all. What
made it real was asserting the changed symbol was absent from the compiled class before trusting the run.

**Mechanism:** the test embeds `Thread.currentThread().getName()` in an expected log line, while
`processingThreadsEnabled=true` logs from Kafka's own processing thread. The names do not match, so the
assertion depends on which thread got there.

## Why this matters more than one flaky test

**Every agent working on this module has been told that "Kafka's own suites, 419 run, zero failures with
the seam off" is a non-negotiable gate.** That instruction is not satisfiable: it will go red roughly 40%
of the time on any branch in this chain, through no fault of the change under test. An unsatisfiable gate
is worse than no gate - it teaches people to re-run until green, which is exactly the habit that lets a
real regression through.

So the gate needs restating rather than repeating. Options, in rough order of preference:

- **Quarantine it** through the repo's existing `@Quarantined` mechanism with this diagnosis attached, so
  the count becomes deterministic and the exclusion is visible. Note the release gate forbids shipping
  while the quarantine registry is non-empty, which is a real cost to weigh.
- **Fix it upstream-style** by not asserting on a thread name, and carry that in the test-fixtures patch.
- **State the gate as "419 run, zero failures other than this named case"**, which is honest but relies on
  every future agent knowing the exception.

Until then, when this case fails, **do not chase it and do not "fix" the code under test.** Confirm it is
this test and this parameterisation, then re-run. Anything else that fails is real.

## Delete when

The count is deterministic - the test is fixed, quarantined with a diagnosis, or the gate is restated in
the places agents actually read (the module README's verification section and the plan's Verification
Contract).
