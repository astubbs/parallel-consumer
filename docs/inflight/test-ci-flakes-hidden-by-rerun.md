# CI retries hide flakes, so the ledger only knows the ones we went hunting for

`bin/ci-integration-test.sh`, `bin/ci-unit-test.sh` and `bin/ci-build.sh` all pass
`-Dsurefire.rerunFailingTestsCount=2`. A test that fails and then passes on retry does not fail the
build - surefire records it as a `Flakes:` line in the job log and the run goes green.

Nothing reads those lines. The consequence is a blind spot with a specific shape:

- **The flake ledger tracks what someone deliberately hunted** (the 2026-07-30 fork16 acceptance run,
  and one-off sightings people happened to notice).
- **CI silently accumulates everything else.** A test can flake repeatedly, on master, for weeks, and
  leave no trace anyone looks at.
- **Searching CI history for *failures* cannot find these**, by construction. That search looks in the
  one place a retried flake never appears. Twice on 2026-08-07 a failure-based search returned a
  confident "none found" that was an artefact of the method, not a fact about the repo.

## What a marker scan found (2026-08-07, 45 most recent CI runs, Integration + Unit lanes)

8 of 45 runs carried flake markers. **None of these tests are in any ledger.**

| Test | Runs affected | Notes |
|---|---|---|
| `OffsetEncodingBackPressureTest.backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing` | 4/45 | The most frequent. Backpressure area - compare with the solved `vacuous-await` write-up, which was a different class in the same area |
| `ParallelEoSStreamProcessorTest.queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown` | 3/45 | **A regression.** Fixed in astubbs#101 ("uncollected tests, cross-test static state, and a timing flake"). Fails via `AbstractParallelEoSStreamProcessorTestBase.assertCommits` |
| `PCMetricsTest.metricsRegisterBinding` | 1/45 | Single sighting; one sighting is not a rate |

The regression is the one to look at first, and it has the best starting position of the three: a
known prior fix to diff against, and a documented reason to care - per
`docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md` §1, instability in this exact
test previously disabled the entire PIT mutation lane, so a green PIT lane means suite stability
rather than mutation coverage while it is flaking.

## How to run the scan

There is no tooling for this; it was done by hand. For each recent run, fetch the Integration and Unit
job logs and grep for surefire's `Flakes:` section, then read the test names beneath it. Scan runs of
**any** conclusion - green runs are exactly where retried flakes live.

## Two decisions this raises, neither taken

1. **Should the retry stay?** It buys green builds at the cost of visibility.
   `flaky-topic-creation-timeout-2026-07-28.md` already argues the case against reaching for it:
   *"Reaching for `rerunFailingTestsCount` would have hidden it instead."* It is currently applied
   suite-wide by default, which is the opposite of that lesson.
2. **Should the marker scan be automated?** A job that reads `Flakes:` from each run and opens or
   updates a ledger entry would close the blind spot permanently. Until something does, this file is a
   point-in-time snapshot and will go stale - the rates above are from one scan on one day.
