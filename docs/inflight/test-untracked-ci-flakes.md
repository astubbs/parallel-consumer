# Three flakes CI was hiding, none of them tracked anywhere

Found 2026-08-07 by scanning surefire `Flakes:` markers across the 45 most recent CI runs (Integration
and Unit lanes). 8 of 45 runs carried markers. None of these tests appear in any ledger.

The retry that hid them is gone - that half is done and written up in
[`docs/solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md`](../solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md),
which also has the scan method. What is open is the three tests themselves.

| Test | Rate | Why it is worth attention |
|---|---|---|
| `OffsetEncodingBackPressureTest.backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing` | 4/45 | The most frequent. Backpressure area - compare `vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md`, a *different* class in the same area, so rule it in or out rather than assuming |
| `ParallelEoSStreamProcessorTest.queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown` | 3/45 | **A regression** - see below |
| `PCMetricsTest.metricsRegisterBinding` | 1/45 | One sighting is not a rate |

**Start with the regression.** astubbs#101 fixed this exact test as "the shutdown-commit flake that
was aborting PIT", and it is back. It has the best starting position of the three: a known prior fix
to diff against, and a documented consequence -
`docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md` §1 records that instability in
this test made the PIT mutation lane report *suite stability* rather than mutation coverage. While it
flakes, a green PIT lane means less than it appears to.

Failures surface as `AbstractParallelEoSStreamProcessorTestBase.assertCommits`, so the assertion
helper is where the message comes from, not necessarily where the cause is.

**Classify before touching any of them** - the same rule that governs the load-tightness family next
door, and for the same reason: two of that family turned out to be real product bugs, and the third
was neither tight nor a stall but a test that could not force its own trigger.

## Still open beyond the three

Nothing reads the `Flakes:` markers automatically. With the retry removed, the *gating* lanes now
surface flakes as failures, but the `highcpu`, quarantine and chaos lanes can still retry or tolerate
them, so a periodic marker scan keeps its value. Until something automates it, the rates above are one
scan on one day and will go stale.
