# Flakes CI was hiding, none of them tracked when found

Found 2026-08-07 by scanning surefire `Flakes:` markers across the 45 most recent CI runs (Integration
and Unit lanes). 8 of 45 runs carried markers. None of these tests appear in any ledger.

The retry that hid them is gone - that half is done and written up in
[`docs/solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md`](../solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md),
which also has the scan method. What is open is the three tests themselves.

| Test | Rate | Why it is worth attention |
|---|---|---|
| `OffsetEncodingBackPressureTest.backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing` | 4/45 | The most frequent. UNDIAGNOSED but quarantined by explicit rule-1 exception - see below. Backpressure area - compare `vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md`, a *different* class in the same area, so rule it in or out rather than assuming |
| `ParallelEoSStreamProcessorTest.queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown` | 3/45 | **A regression** - see below |
| `PCMetricsTest.metricsRegisterBinding` | 2 seen | Second sighting, mechanism known, quarantined (owner astubbs#265) - see below |
| `ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` | 1 seen (2026-08-12) | Not from the original scan - found while babysitting astubbs#287. **Fixed by astubbs#262**; quarantine lifted and registry entry deleted when that branch merged master - see below |

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

### `ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` - a helper defect, not a test defect

Seen 2026-08-12 on astubbs#287, a PR whose diff contained **no Java at all** - which is what settles
rule 2 (master-state, not PR-state) without needing a rate: nothing in the change could have caused
it.

```
ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect:367
  value of: getElapsed()  expected to be at least PT20S  but was PT19.998S
```

**Two milliseconds short on a twenty-second bound**, which is the shape of a measurement error rather
than a behavioural one - the code under test either blocks for the full delay or it does not, and it
does not miss by 0.01%.

**The defect is in the shared helper, not in this test.** `BlockedThreadAsserter#assertUnblocksAfter`
arms the unblocking task with `scheduledExecutorService.schedule(...)` and only *then* starts the
clock it later compares against `unblocksAfter`. The scheduler begins counting its delay from inside
that `schedule()` call, so the measured window starts **after** the delay does, and is short by
however long arming plus lambda setup takes. Under load that gap widens past a millisecond and
`isAtLeast` fails a correct implementation. Any test using this helper can show the same signature,
which is why it is filed against the helper.

**Fixed in astubbs#262**, which stamps `armedAtNanos` immediately before `schedule()` and asserts
against that instead. Its own comment is honest about the residual: the window measured is now
slightly *longer* than the true one, so the error is sub-millisecond and in the safe direction - a
genuinely early return is still caught unless it is early by less than the arming cost.

That branch has now merged master, so under rule 3 it carries the re-enable: the `@Quarantined`
annotation and the `docs/quarantined-tests.md` entry are both deleted, returning the test to the
gating lane.

**Note astubbs#265 touches the same line differently**, deleting the assertion along with the sleeps
it removes. astubbs#262 landed the fix first, so that conflict is now astubbs#265's to resolve - and
it is still a real decision, measure it correctly or stop measuring it, not a mechanical merge.

**Why it was not in this ledger already.** The 2026-08-07 scan read surefire `Flakes:` markers, which
only appear when the retry re-ran a test and it then passed. This one failed the run outright, so it
left no marker and no scan would have found it. Flakes now get quarantined as they are met, rather
than waiting for a sweep.

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

### `OffsetEncodingBackPressureTest.backPressure...` is NOT diagnosed - quarantined anyway, by explicit exception

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
  (Citation repair: "the commit CI ran" is never named, so that 211 cannot be resolved by a reader,
  and on master today it lands on a *different* `waitAtMost` block - the one asserting
  `isBlocked()` - which is close enough to the description to be believed. The durable anchor is the
  assertion already quoted: grep `hasValue(expectedHighestSeen)` in
  `OffsetEncodingBackPressureTest`, exactly one hit. The number is left in place because it is what
  the failure report said, not a pointer this note chose.)
- That block runs **before** the retry section astubbs#265 rewrites. A change downstream of a failing
  assertion cannot fix it.

So the true cause is a timeout waiting for the high-water mark to reach `expectedHighestSeen`
(actuals vary run to run - 136 and 132 have both been seen against an expected 139), and nothing
currently explains why. Rule 1 - no quarantine without diagnosis - would keep it in the gating lane,
but it fails often enough (4/45, the most frequent tracked flake) that leaving it red blocked every
PR. **The repository owner decided to quarantine it anyway as an explicit rule-1 exception**: the
registry entry carries no Owner (unowned, flagged advisory by the audit), `flapping = true`, and the
diagnosis below remains the open task. The exception is a pressure-release, not a resolution - this
entry stays open until the test is understood and fixed.

**The open lead - an UNVERIFIED hypothesis, test it before acting on it.** The test computes
`expectedHighestSeen = numberOfRecordsToPrimeWith + extraRecordsToBlockWithThresholdBlocks - 1`, and
the extra records exist precisely to push the offset encoding past the size threshold that makes the
partition block and stop taking records. If back-pressure engages before the last extra record is
polled, the expectation is **unreachable rather than late** - matching the varying shortfall and the
fact that a 30-second wait never rescues it. Falsification: if the actual value tracks the encoding
block point, the hypothesis holds; if the high-water mark eventually reaches 139 given long enough,
it is dead and this is a slowness problem. Compare
`vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md` (same area, different test,
`root_cause: test_design_bug`) - rule it in or out, don't assume.

The general lesson is the one that produced the error: the fix PR was matched to the failure by
**subject-matter resemblance** (both concern this test, both concern waiting) rather than by checking
that the changed lines execute before the failing assertion. Match a `fixedBy` to a stack line, not
to a theme.
