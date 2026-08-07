---
title: "CI retried flakes into green builds, so the flake ledger only knew the ones someone went hunting for"
date: 2026-08-07
category: workflow-issues
module: build-system
problem_type: workflow_issue
component: development_workflow
severity: medium
status: "SOLVED - `-Dsurefire.rerunFailingTestsCount=2` removed from all three CI scripts. The flakes it was hiding are now tracked in `docs/inflight/test-untracked-ci-flakes.md`."
applies_when:
  - Searching CI history for a flaky test and finding nothing
  - Deciding whether to add or keep a test-retry setting
  - Maintaining a hand-written flake ledger alongside an automated CI suite
  - Reading a green build as evidence the suite is stable
tags:
  - flaky-tests
  - ci
  - surefire
  - false-negative
  - observability
---

# CI retried flakes into green builds

## Context

All three CI entry points - `bin/ci-build.sh`, `bin/ci-unit-test.sh`, `bin/ci-integration-test.sh` -
passed `-Dsurefire.rerunFailingTestsCount=2`. A test that failed and passed on retry did not fail the
build. Surefire recorded it as a `Flakes:` line in the job log, and nothing ever read those lines.

The flake ledger, meanwhile, was maintained by hand from deliberate hunts. So the repo tracked the
flakes someone had gone looking for, while CI quietly accumulated the ones nobody had.

This surfaced while investigating whether `TransactionTimeoutsTest.produceTimeout` was still flaking.
It wasn't - but the search for it kept returning "nothing found", and the reason turned out to be the
method, not the repo.

## Guidance

**A retry does not fix a flake, it deletes the evidence of one.** Removed from all three scripts. A
flake now fails the build.

**When a test is retried, searching CI history for *failures* cannot find it.** That search looks in
the one place a retried flake never appears. Search for the marker instead - surefire's `Flakes:`
section - and scan runs of **any** conclusion, because green runs are exactly where retried flakes
live.

**Expect removing the retry to cost you red builds**, at roughly the rate the tests were already
flaking. That is the trade being made deliberately: visibility over green. When a red build blocks
something urgent, the lever is `@Quarantined` with a diagnosis (`docs/QUARANTINED_TESTS.md` requires
one), which keeps the test running in the non-gating lane. Restoring the retry destroys the signal
instead of relocating it.

## Why This Matters

The failure mode is silent, and it compounds:

- A green suite stops meaning a stable suite, while still being read as one.
- A previously-fixed flake can regress and nobody learns. One of the three found this way,
  `ParallelEoSStreamProcessorTest.queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown`, had
  been fixed in astubbs#101 as "the shutdown-commit flake that was aborting PIT", and had come back.
- Instability in that same test previously meant the PIT mutation lane reported *suite stability*
  rather than mutation coverage - so the hidden flake also hid the fact that a whole quality gate was
  not doing its job.

`docs/solutions/test-issues/flaky-topic-creation-timeout-2026-07-28.md` had already reached the same
conclusion from the other direction - *"Reaching for `rerunFailingTestsCount` would have hidden it
instead"* - while the flag was applied suite-wide by default. A rule written in one doc does not
enforce itself elsewhere.

## When to Apply

- Before concluding a flake is gone because CI history shows no failures.
- Before adding a retry setting to any suite.
- When a hand-maintained ledger coexists with automated runs: ask what the automation records that
  nobody reads.

## Examples

The scan that found them - 45 recent runs, Integration and Unit lanes, any conclusion:

```
for each run:  fetch the job log;  grep -i "Flakes:";  read the test names beneath it
```

8 of 45 runs carried flake markers, none of the named tests in any ledger:

```
4/45  OffsetEncodingBackPressureTest.backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing
3/45  ParallelEoSStreamProcessorTest.queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown
1/45  PCMetricsTest.metricsRegisterBinding
0/45  TransactionTimeoutsTest.produceTimeout      <- the test actually being investigated
```

The same class of error bit twice in one day, in a different guise: an earlier CI history search
filtered on `conclusion == "failure"` and returned a confident "none", because the failing run had
been *re-run* and therefore reported success. Retries at the test level and retries at the run level
hide failures identically.

## Related

- `docs/inflight/test-untracked-ci-flakes.md` - the three flakes this surfaced, still open
- `docs/solutions/test-issues/flaky-topic-creation-timeout-2026-07-28.md` - reached the same conclusion
  about retries from the other direction
- `docs/inflight/test-load-tightness-flakes.md` - the hand-maintained ledger this compared against
- `AGENTS.md` (Testing) - carries the no-retry rule and the quarantine alternative
