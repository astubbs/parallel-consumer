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
| `PCMetricsTest.metricsRegisterBinding` | 2 seen | Second sighting, mechanism known, quarantined on astubbs#286 - see below |

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

### `PCMetricsTest.metricsRegisterBinding` - second sighting, and it is a test defect

Seen again 2026-08-11 on astubbs#286, a PR containing **no Java and no `pom.xml`** - workflow and
markdown only.

**Record the control that was tried and was void, because it is the trap next door.** The first
attempt at one was "`master` at `a797f756`, the exact base commit, passed the same suite 35 minutes
earlier". It did not. A push to `master` **skips the whole test matrix** - run 31459241709 shows
`matrix.name: skipped`, and only `full build (master)` runs. The unit lane exists on `pull_request`
only. That control was not weak, it was structurally incapable of failing, which is exactly the
"instrument that could have said yes" failure documented next door in
[`negative-results-need-an-instrument-that-could-have-said-yes.md`](../solutions/workflow-issues/negative-results-need-an-instrument-that-could-have-said-yes.md).
Anyone reaching for a green master run as a baseline for these tests is holding nothing.

**The control that does work** is other PR runs of the same lane. On 2026-08-11 the unit lane was
green on eight consecutive `pull_request` runs across three branches - `docs/citation-anchors`,
`ci/on-demand-code-review`, `docs/v6-release-ideas`, and **this branch's own previous head** - with
only `821a91af` failing.

```
[ERROR] PCMetricsTest.metricsRegisterBinding:115
  expected: 203.0
   but was: 207.0
```

The mechanism is visible in the source rather than inferred. The test snapshots a **test-side**
counter to build its expectations:

```java
int highestProcessedOffsetP0 = counterP0.get() - 1;      // reads 204 -> expects 203
...
assertThat(registeredGaugeValueFor(PARTITION_HIGHEST_COMPLETED_OFFSET, 0))
        .isEqualTo(highestProcessedOffsetP0);            // gauge has moved on to 207
```

Two independently-advancing values are sampled at different instants, with nothing holding the system
still between them. Processing had completed four more records for partition 0 between the counter
read and the gauge read. Nothing is wrong with the metric - it was **more** current than the
expectation built to test it.

Same family as the fix in `16ac63b1` ("await the metric, not a counter that leads it"), running the
other way round: there the counter led the metric, here a stale counter snapshot trails it. The rule
generalises - **do not compare two moving values; await a quiescent state, then read both.**

Rate is now 2 sightings rather than the 1/45 that could be dismissed.

### The rerun failed somewhere else - which is weaker evidence than it first looks

Re-running the identical job on the identical commit did not reproduce it. It failed at
`OffsetEncodingBackPressureTest.backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing`
instead - `ConditionTimeout`, `expected: 139 but was: 136 within 30 seconds` - which is **row 1 of
the table above**, the 4/45 entry.

An earlier revision of this entry called that "the strongest evidence", on the reasoning that a code
regression fails the same way twice and this did not. **That reasoning does not hold and is withdrawn.**
Under concurrent or stress execution one defect can perturb timing enough to surface different tests
and different failure modes, so two dissimilar failures do not exclude a regression - they show only
that the first did not reproduce. Review caught this; it is exactly the invalid-diagnostic-rule trap
that AGENTS.md warns about, and left standing it would have licensed quarantining a real product bug.

What the rerun **does** establish: the failure is not deterministic, and the unit lane is currently
producing red from more than one already-tracked test. The load-bearing evidence for the
`PCMetricsTest` diagnosis is the source-level read above - the counter snapshot and the gauge are
read at different instants - not the rerun.

### `OffsetEncodingBackPressureTest.backPressure...` is NOT diagnosed - do not quarantine it

It was quarantined on astubbs#286 and **removed again in the same PR**, because the diagnosis was
wrong. Recorded here so the mistake is not repeated.

The failure was attributed to the retry section - "sleeps out the static retry delay instead of
awaiting the retry event" - and owned by astubbs#265, which replaces that
`sleepQuietly(DEFAULT_STATIC_RETRY_DELAY)` with an `await`. Review checked the line number instead of
the narrative and found it does not fit:

- The failure is at line 211 of the commit CI ran, which is the
  `waitAtMost(defaultTimeout).untilAsserted(...)` block asserting the committed offset metadata -
  specifically `Truth8.assertThat(incompletes.getHighestSeenOffset()).hasValue(expectedHighestSeen)`.
  The `value of: optional.get()` in the failure text is that `Optional`.
- That block runs **before** the retry section astubbs#265 rewrites. A change downstream of a failing
  assertion cannot fix it.

So the true cause is a timeout waiting for the high-water mark to reach `expectedHighestSeen` (136 of
an expected 139), and nothing currently explains why. Under rule 1 - no quarantine without diagnosis -
it stays in the gating lane and stays red until someone works out why three records never arrive.

The general lesson is the one that produced the error: the fix PR was matched to the failure by
**subject-matter resemblance** (both concern this test, both concern waiting) rather than by checking
that the changed lines execute before the failing assertion. Match a `fixedBy` to a stack line, not
to a theme.
