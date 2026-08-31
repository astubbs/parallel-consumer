# `StreamThreadTest` invalid-timestamps case is flaky, and it breaks the gate this module rests on

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`StreamThreadTest.shouldLogAndRecordSkippedRecordsForInvalidTimestamps[3]` fails intermittently in the
upstream-Kafka suite that `parallel-consumer-streams` runs against its patched classes. Diagnosed
2026-08-11 with a control arm; carried onto the reconstructed stack when it was sighted again on the
refusal-envelope branch.

**Mechanism.** The test embeds `Thread.currentThread().getName()` in an expected log line, while with
`processingThreadsEnabled=true` the line is logged from Kafka's own processing thread. The names do not
match, so the assertion depends on which thread got there. It is Kafka's test, on Kafka's code, and no
patch in this module touches either side of that race.

**Established pre-existing, with a control arm.** Two failures in five runs at the then-HEAD *without*
the investigating agent's changes, against three in seven with them. The control arm itself needed a
second attempt: the first re-ran already-built binaries and would have "proved" the flake pre-existing
on no evidence at all. What made it real was asserting the changed symbol was absent from the compiled
class before trusting the run.

<!-- post-merge: checked-begin -->
<!-- Reads the same after the merge: the PR is cited as the landed change that recorded this sighting,
     which is a permanent link, and no live branch is named. -->
**Sighted again while the refusal envelope was being verified** (astubbs/parallel-consumer#389),
seam off: once in seven consecutive runs of the module's whole `test` phase on one machine - green on
the immediate re-run, and green in every other run, including both arms of that work's before/after
control. Lower than the original rate, on one machine, which is a data point rather than a new
measurement.

**And then on CI, on the very next run** - the `Unit Tests` lane of the same PR, failing on
parameterisation `[3]` with the whole upstream execution otherwise clean. That is worth more than
another tally mark: every sighting until now had been on a developer machine, so "a loaded laptop"
was still available as an explanation. It is not any more. This flake reaches the shared lane, where
it costs every branch in the chain a red build and a re-run, and where the temptation to re-run until
green is strongest.
<!-- post-merge: checked-end -->

## Why this matters more than one flaky test

**Every agent working on this module is told that "Kafka's own suites, zero failures with the seam
off" is a non-negotiable gate.** That instruction is not satisfiable as stated: it will go red some of
the time on any branch in this chain, through no fault of the change under test. An unsatisfiable gate
is worse than no gate - it teaches people to re-run until green, which is exactly the habit that lets a
real regression through.

So the gate needs restating rather than repeating. Options, in rough order of preference:

- **Quarantine it** through the repo's `@Quarantined` mechanism with this diagnosis attached, so the
  count becomes deterministic and the exclusion is visible. It is not obvious this is even reachable -
  the case is in Apache Kafka's own compiled test class, which this module runs unmodified and does not
  recompile, so the annotation has nowhere to go without an exclude in the surefire execution. That is
  the first thing to establish. Note also that the release gate forbids shipping while the quarantine
  registry is non-empty, which is a real cost to weigh.
- **Fix it upstream-style** by not asserting on a thread name, and carry that in the test-fixtures
  patch - which is where a change to a Kafka test class can actually live.
- **State the gate as "zero failures other than this named case"**, which is honest but relies on every
  future agent knowing the exception.

Until then, when this case fails: **do not chase it, and do not "fix" the code under test.** Confirm it
is this test and this parameterisation, then re-run. Anything else that fails is real.

## Delete when

The outcome is deterministic - the test is fixed, excluded with this diagnosis attached, or the gate is
restated in the places agents actually read (the module README's verification section, and the surefire
execution's own comment).
