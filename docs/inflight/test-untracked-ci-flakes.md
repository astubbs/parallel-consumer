# Flakes CI was hiding, none of them tracked when found

<!-- inflight-class: misdirection -->

Found 2026-08-07 by scanning surefire `Flakes:` markers across the 45 most recent CI runs (Integration
and Unit lanes). 8 of 45 runs carried markers. None of these tests appear in any ledger.

The retry that hid them is gone - that half is done and written up in
[`docs/solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md`](../solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md),
which also has the scan method. What is open is the tests themselves - one of the scan's three, plus
one met later. The other two are fixed and out of this ledger (astubbs#260 and astubbs#265); where
their diagnoses generalised, the rule is in [`docs/solutions/`](../solutions/).

| Test | Rate | Why it is worth attention |
|---|---|---|
| `OffsetEncodingBackPressureTest.backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing` | 4/45 | The most frequent. UNDIAGNOSED but quarantined by explicit rule-1 exception - see below. Backpressure area - compare `vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md`, a *different* class in the same area, so rule it in or out rather than assuming |
| `ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` | 1 seen (2026-08-12) | Not from the original scan - found while babysitting astubbs#287. Mechanism known and owned (astubbs#262), quarantined - see below |
| `simpleBatchTest` in **both** `ReactorBatchTest` and `MutinyBatchTest` | 2 seen (2026-08-18, 2026-08-19) | Not from the original scan - both found while babysitting a **docs-only** branch (astubbs#308 head `d930ca98d`; astubbs#320 head `70a247184`). Same Awaitility `ConditionTimeout`, same alias 'expected number of batches' (30s), same shared `BatchTestMethods` lambda - see below, the second sighting is what makes it worth diagnosing. UNDIAGNOSED - classify (contention vs product) before touching |

**Classify before touching any of them** - the same rule that governs the load-tightness family next
door, and for the same reason: two of that family turned out to be real product bugs, and the third
was neither tight nor a stall but a test that could not force its own trigger.

### `simpleBatchTest` - the second sighting says it is the shared helper, not either wrapper

2026-08-18 it was `ReactorBatchTest`. 2026-08-19 it was `MutinyBatchTest`, on
astubbs/parallel-consumer#320 - and the failure is the same one, not a similar one: the alias, the
30-second timeout and the lambda all come from `BatchTestMethods` in **core**, which both wrapper
modules drive. One sighting looked like a Reactor flake. Two, in different modules, means the
Reactor and Mutiny wrappers are not the variable.

The 2026-08-19 assertion is the useful part, because it is off by exactly one in the direction that
matters: **`Expected size: 3 but was: 4`** - grep `BatchTestMethods` for `expected number of
batches`. The test received the right records in one batch too many, so this is a batch-BOUNDARY
question, not a lost-work question. Two readings, and they need separating rather than assuming:

- **Contention.** The runner is slow or loaded, so work arrives spread out and the batcher closes a
  batch early. Test-side, and the honest fix is making the test drive the boundary it asserts
  instead of racing it.
- **Product.** The batcher can split a batch under a timing the library is supposed to tolerate,
  in which case an over-eager boundary is a real defect and the test is right to complain.

**Both sightings are on branches whose diffs contain no Java at all**, which is what rules out "a PR
broke it" and makes it master state - the same reasoning applied to `ProducerManagerTest` below.
That is also why neither was quarantined on the branch that met it: quarantine is master-state and
needs a diagnosis, and neither sighting has one.

The 2026-08-18 sighting passed on re-run. A re-run is diagnosis here, not a way to go green - it
distinguishes flaky from deterministic - and AGENTS.md's ban is on the automatic
`surefire.rerunFailingTestsCount` that hid this whole ledger, not on re-running a job to learn
something.

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

**The mechanism above no longer exists, and this entry's open task has changed accordingly.** Two
PRs proposed different fixes - astubbs#262 stamping `armedAtNanos` just before `schedule()` so the
measurement is correct, and astubbs#265 deleting the wall-clock assertion outright. This ledger
predicted the collision and called it a real decision: measure it correctly, or stop measuring it.
**astubbs#265 landed second and chose to stop measuring it.**

`BlockedThreadAsserter#assertUnblocksAfter` now asserts an ordering fact - both events take a tick
from a shared monotonic sequence and the return must come after the unblock - so there is no elapsed
clock left to be short, and `isAtLeast(unblocksAfter)`/`getElapsed()` are gone from the helper. Its
javadoc states the new contract: *"That is a causality assertion, so it is asserted as an ordering
fact rather than as a duration."*

**So the diagnosis this test is quarantined under describes code that is not there.** Measured
2026-08-17 on `master` merged in: 4 runs, 4 passes, 2.66-4.37s (astubbs#265 reported the same test
going from 23.06s to 3.32s). Run it with
`bin/quarantined-test.sh` or `-Dincluded.groups=quarantined` - a plain `-Dtest=` run reports
`Tests run: 0` because the gating suites exclude it, which is not a pass.

**What is open is the re-enable, not the fix.** Under rule 3 of
[`docs/quarantined-tests.md`](../quarantined-tests.md) the annotation and the registry entry come out
together, in the owning change, after merging master. astubbs#262 is still open and still named as
the owner, but its fix is now redundant - so whoever picks this up should decide whether astubbs#262
still carries the re-enable or whether it belongs in a change of its own.

**Why it was not in this ledger already.** The 2026-08-07 scan read surefire `Flakes:` markers, which
only appear when the retry re-ran a test and it then passed. This one failed the run outright, so it
left no marker and no scan would have found it. Flakes now get quarantined as they are met, rather
than waiting for a sweep.

### Controls for these flakes - the void one, and the one that works

Method for the two tests still open, not a diagnosis of any one of them. It is written from a
2026-08-11 sighting on astubbs#286, a PR containing **no Java and no `pom.xml`** - workflow and
markdown only - which is what made the control question sharp enough to answer.

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
`ci/on-demand-code-review`, `docs/v6-release-ideas`, and `ci/claude-yml-script-grant`'s own previous
head - with only `821a91af` failing.

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
producing red from more than one already-tracked test. What it is *not* is evidence about any one
test's mechanism - that has always come from a source-level read, never from a rerun's landing spot.

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
