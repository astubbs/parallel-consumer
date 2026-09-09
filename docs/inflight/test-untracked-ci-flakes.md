# Flakes CI was hiding, none of them tracked when found

<!-- inflight-type: register -->
<!-- inflight-impact: misdirection -->
<!-- inflight-vetted: 2026-09-09 - every row re-read against the tree and worked, not just checked. Four retired to the header's fixed-and-out list with their knowledge migrated to docs/solutions/test-flakiness/: processInKeyOrder's input-sanity failure (fixed on master since a6941020f; every recorded sighting predates it), inFlightMessagesCommittedIfProcessedDuringShutdown (same class, caught red by this pass's own control arm, fixed here), AmbientProbeExtensionTest's headroom trio (red 4/4 on master, green 6/6 with a ResourceLock, sabotage-checked), and the JStream and ProducerManagerTest rows whose owning PRs had merged without retiring them. Four rows left open with the reason stated in each: the PIT lane, rapidToggleShouldNotCreateDuplicateInstances, ReactorPCTest.concurrencyTest and CommitResponseTimeoutSymptomTest. Two new local sightings added as their own row. Nothing quarantined - docs/quarantined-tests.md is empty and nothing here has the rate or the diagnosis that rule 1 wants. Carried forward from the 2026-09-07 vet, still true: the PIT row matches maven.yml (step timeout-minutes 20, job 25, still continue-on-error). -->

Found 2026-08-07 by scanning surefire `Flakes:` markers across the 45 most recent CI runs (Integration
and Unit lanes). 8 of 45 runs carried markers. None of these tests appear in any ledger.

The retry that hid them is gone - that half is done and written up in
[`docs/solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md`](../solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md),
which also has the scan method. What is open is the tests themselves - the ones met after it. All
three the scan found are fixed and out of this ledger: astubbs#260,
astubbs#265, and `OffsetEncodingBackPressureTest.backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing`
(4/45, the most frequent), which asserted an offset that back pressure exists to stop advancing -
written up in
[`back-pressure-freezes-the-frontier-the-test-asserted-2026-08-24.md`](../solutions/test-flakiness/back-pressure-freezes-the-frontier-the-test-asserted-2026-08-24.md).
Also fixed and out: `simpleBatchTest` in `CoreBatchTest`, `ReactorBatchTest`, `MutinyBatchTest` and
`VertxBatchTest` (six sightings, the most in this register), which asserted a batch count of
`ceil(records / batchSize)` over an input whose key distribution it randomised - under KEY ordering a
key drawn three times out of five forces a fourth batch, deterministically, and the library is right to
produce it -
[`a-randomised-key-draw-decided-a-batch-count-the-test-computed-from-the-record-count-2026-09-08.md`](../solutions/test-flakiness/a-randomised-key-draw-decided-a-batch-count-the-test-computed-from-the-record-count-2026-09-08.md).
Also fixed and out: `MdcContextPropagationTest.anEmptyCallerContextIsHandledAndNothingLeaks`, which
asserted a null MDC on a runner thread two other classes had left holding `{}` (four sightings, the
last one master's own build for the astubbs#415 merge; fixed twice over, by two sessions that did not see each other) -
[`mdc-null-precondition-armed-by-an-earlier-class-in-the-same-fork-2026-09-02.md`](../solutions/test-flakiness/mdc-null-precondition-armed-by-an-earlier-class-in-the-same-fork-2026-09-02.md).
Also fixed and out: `RegistrationRaceStaleResidentIT.freshArrivalCollidingWithStaleShardResidentMustStillGetProcessed`,
whose failures were its own **setup guard** timing out, not the confluentinc#909 assertion it exists
to make - the test's own saturation had closed the record-intake gate, so the pause-point records were
never fetched. The annotation and this row went together, per rule 3 -
[`the-setup-guard-was-waiting-on-records-back-pressure-had-stopped-fetching-2026-09-07.md`](../solutions/test-flakiness/the-setup-guard-was-waiting-on-records-back-pressure-had-stopped-fetching-2026-09-07.md).
Also fixed and out, 2026-09-09, in one pass - four rows, plus one test that never had a row of its
own and was caught by the campaign that retired them:

- `ParallelEoSStreamProcessorTest.processInKeyOrder` failing its own `[sanity check input data]` -
  **already fixed when this register still called it undiagnosed**, which is the part worth
  remembering. The wait in front of that assertion counted CONTROL loop cycles while `polled` is
  filled by the POLL thread; astubbs#29 replaced it with a wait on the data, and that reached
  `master` as `a6941020f` on 2026-09-02. Every sighting this register and its branch-side ledger
  recorded predates that date - including the control arm that reddened "unmodified master", which
  was at a commit five days older than the fix. Diagnosis, the provenance check and the class sweep:
  [`the-sanity-check-counted-a-poll-the-control-loop-never-waited-for-2026-09-09.md`](../solutions/test-flakiness/the-sanity-check-counted-a-poll-the-control-loop-never-waited-for-2026-09-09.md).
  A branch-side sighting ledger, `test-processinkeyorder-sanity-check-races-the-first-poll.md`,
  carries the same story on the refs that predate the fix and never reached `master`
  (`node bin/inflight.mjs docs show` it); that solution doc supersedes it, so read it first if you
  arrive from there.
- `ParallelEoSStreamProcessorTest.inFlightMessagesCommittedIfProcessedDuringShutdown` - never its
  own row, only a name inside the `processInKeyOrder` narrative, and **caught red by this pass's own
  control arm**. `awaitForSomeLoopCycles(2)` stood in for "the record is in flight", so `close()`
  could find nothing to complete and the commit assertion read `[]`. The same defect class as the
  row above, and named as an outstanding instance by the very commit that fixed that one. Now waits
  on the user function actually being entered, which is a stronger precondition: a `[]` after it is
  a product signal. Covered by the same solution doc - **read its sabotage section before touching
  this test**, because three of the four mutations that look like they cut the commit do not reach
  it, and a green sabotage there is a false "this test is dark", not a result.
- `AmbientProbeExtensionTest`'s three headroom methods - `LogCapture` attaches to a **process-global**
  logger, and three methods asserting the exact set of lines they saw ran concurrently and captured
  each other. Red 4 of 4 class-alone runs on `master`, green 6 of 6 with a shared `@ResourceLock`;
  never red on CI, because the `ci` profile turns JUnit method parallelism off. The sweep of every
  other `LogCapture` user found no second instance -
  [`two-tests-captured-one-process-global-logger-at-the-same-time-2026-09-09.md`](../solutions/test-flakiness/two-tests-captured-one-process-global-logger-at-the-same-time-2026-09-09.md).
- `JStreamParallelEoSStreamProcessorTest.testConsumeAndProduce` and `.testFlatMapProduce` - the row
  said its mechanism was astubbs#116's and that whoever merged that PR owned retiring it. astubbs#116
  merged on 2026-09-03 and `JStreamLiveResultStreamTest` is in the tree; the retirement is this pass's.
- `ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` - the helper defect
  behind it (`BlockedThreadAsserter#assertUnblocksAfter` starting its clock after the scheduler
  started the delay) was settled by astubbs#265 deleting the wall-clock assertion for a causal one,
  and astubbs#262 lifted the quarantine and the registry entry. Both merged; the row was describing
  finished work.

Where their diagnoses generalised, the rule is in [`docs/solutions/`](../solutions/).

| Test | Rate | Why it is worth attention |
|---|---|---|
| `Mutation Tests (PIT, PR-scoped)` lane | 1 seen (2026-09-02, astubbs#207, [run 33610711974](https://github.com/astubbs/parallel-consumer/actions/runs/33610711974)) | Not a test - the LANE hit its `timeout-minutes: 30` cap and was cancelled, on a **markdown-only** delta from a head where it had scored in 19m18s with the same class set. The cap had about a third headroom over a normal run, so it flapped on a slow runner. Addressed 2026-09-07: the bound is now `timeout-minutes: 20` on the PIT **step**, so a hit ends that step and the job still reports, where a hit on the old job cap cancelled the whole row. It arrived with the fold into `scan: repo` and outlived it - astubbs#463 un-folded PIT into its own `mutation` job again and the step bound moved with the step ([`ci-fewer-jobs-ruleset-edits.md`](ci-fewer-jobs-ruleset-edits.md)). Still `continue-on-error: true`, so it never gates a merge <!-- post-merge: checked --> |
| `ManagedPCInstanceLifecycleTest.rapidToggleShouldNotCreateDuplicateInstances` | 3 seen (2026-09-02, astubbs#207, [job 100175277225](https://github.com/astubbs/parallel-consumer/actions/runs/33607572165/job/100175277225); 2026-09-07, astubbs#428, [job 101592337448](https://github.com/astubbs/parallel-consumer/actions/runs/34072492940/job/101592337448); 2026-09-07, astubbs#452, [job 101607850077](https://github.com/astubbs/parallel-consumer/actions/runs/34078008311/job/101607850077)) - the first two the first run of a branch that had just taken a change to how this lane runs; the third a re-run after a merge from master, with no `.github/` change in the merged range | Not from the original scan - **arrived on master with astubbs#29 and failed on the first PR to merge it**. `consumeCount` 0, repetition 1 of 5, `forkCount=4`, `probe clean`. Every wait in the test is a fixed sleep, and its assertion names a cause it cannot discriminate - see below <!-- post-merge: checked --> |
| `ReactorPCTest.concurrencyTest` | Local only, 2026-09-07: 2 of 3 full unit-suite runs and 1 of 3 reactor-module runs on the astubbs/parallel-consumer#469 tree; 0 of 6 module runs and 0 of 1 full-suite run on a worktree detached at that branch's own base commit, built and run the same way | **The test hugs its ceiling in EVERY run on BOTH trees, passing ones included** - `grep -c 'More records submitted'` on a *green* control run returns 59-76, because the fail-fast log fires whenever in-flight exceeds `MAX_CONCURRENCY` while the assertion tolerates `MAX_CONCURRENCY * MAX_CONCURRENCY_OVERFLOW_ALLOWANCE`. So a failure is the peak crossing a tolerance the run is already sitting against, not a new behaviour appearing. Passes 4/4 in isolation, so it is load-sensitive. The asymmetry against the control is real and unexplained, and **the direction-of-effect argument that used to stand against it does not hold - it was withdrawn on astubbs/parallel-consumer#469 after a Codex review, and is recorded here so nobody re-derives it.** It claimed the only work-admission field the PR touched, `PartitionState.allowedMoreRecords` made `volatile`, could only publish back-pressure's `false` sooner and so admit **fewer** records. That is one-way reasoning about a two-way field: `tryToEncodeOffsets()` also calls `setAllowedMoreRecords(true)` - on the `incompleteOffsets.isEmpty()` path, and in `updateBlockFromEncodingResult` when the payload comes back under the pressure threshold - so the fence publishes the **un**blocking transition sooner as well, which admits **more** records sooner. The two directions are not obviously equal in size, and nothing here has measured which dominates. **So this change is not ruled out as a contributor, and classification needs a measured control rather than an argument**: contention, tolerance, or a real submission-path defect this ceiling has been masking | <!-- post-merge: checked -->
| `CommitResponseTimeoutSymptomTest.aRebalanceStormUnderAHighFailureRateNeitherStallsNorKillsTheConsumer` | Local only, 2026-09-07: 1 of 3 full unit-suite runs on the astubbs/parallel-consumer#469 tree; 3/3 pass in isolation; 0 of 1 full-suite run on the same-base control worktree. `bin/inflight.mjs codecov test aRebalanceStormUnderAHighFailureRateNeitherStallsNorKillsTheConsumer` had no recorded failure before this, so CI had never seen it - ask the command rather than trusting a total written here | Failed its `commitsRejected >= MIN_REJECTIONS` await (3 vs 4) - **a count of commit ATTEMPTS, which only accrues while the backlog is draining**; once drained nothing is dirty, no further commit is attempted, and the 30s await can only time out. So the assertion is a race between wall-clock commit ticks and drain speed, and a loaded machine drains before enough ticks land. Not a stall assertion; do not read the failure as PC stopping | <!-- post-merge: checked -->

| `ParallelEoSStreamProcessorTest.queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown` and `.closeAfterSingleMessageShouldBeEventBasedFast` | 1 local sighting each, 2026-09-09, during the matched-pair campaign that retired the four rows above - a deliberately oversubscribed 12-core box running two full core suites at once. Neither reproduced again in that campaign; **that is one sighting each, not a rate** | Recorded rather than diagnosed, because the seeds die with the logs. Both are shutdown-path tests in the class this pass was already working in, and the class's own defect history is cycle-counts standing in for events - so read them against that first. `queuedMessages...` is the harder one to place, because astubbs#101 *already* replaced its cycle count with `awaitUntilTrue(gotK0::get)` plus `awaitForCommit(1)`; if its mechanism is the same class, it is a second instance in a test that was supposed to be immune, and that is worth knowing. Nothing here establishes it is master-state: both arms of the pair carry `master`'s code for these two tests, so a sighting on either arm says nothing about a branch, but one sighting says nothing about a rate either |

**Classify before touching any of them** - the same rule that governs the load-tightness family next
door, and for the same reason: two of that family turned out to be real product bugs, and the third
was neither tight nor a stall but a test that could not force its own trigger.

### `ManagedPCInstanceLifecycleTest` - a sleep-timed test that names one cause for a symptom with several

<!-- post-merge: checked-begin - names astubbs#29 and astubbs#207 in the past tense as, respectively, the
     change that introduced the test and the branch the sighting came from; both stay true once landed -->
Seen 2026-09-02 on astubbs#207's CI, one failure in 187 integration tests, at repetition 1 of 5.

**Provenance first, because it decides who owns it.** `git log --diff-filter=A` on the file shows this class
was **added by astubbs#29**, the confluentinc#857 revoke-path fix, and astubbs#207 merged that commit hours
earlier. astubbs#207 does not touch the test, and it is cleared on mechanism rather than on timing: the test
runs `PERIODIC_CONSUMER_ASYNCHRONOUS` + `UNORDERED` against a freshly created topic and never puts foreign
metadata in a commit, so there is no offset metadata for an offset-*decoding* change to reach.

Master's own CI was green at `a6941020f` (astubbs#29's merge) and at the head after it. One green run per
commit cannot rule out a low-rate flake, so that is corroboration, not proof - the mechanism above is what
clears the branch.

**Every wait in it is a fixed sleep standing in for an event**, which is the defect class this repo has
already met twice (`processInKeyOrder`'s `awaitForOneLoopCycle`, and what astubbs#265 removed elsewhere).
Read the method: 2s to join the group, 10 toggle cycles at 100ms, 3s to settle, then produce 10 records and
sleep 5s before asserting `consumeCount > 0`. Under `forkCount=4` on a shared runner with a Testcontainers
broker, 5 seconds is not a guaranteed window for a rejoin, an assignment, a poll and ten records.

**The assertion attributes a cause it cannot discriminate**, and that is the part worth fixing rather than
the timing. Its message is *"if 0, the PC died from CME during rapid toggles"* - but a count of zero is also
what starvation looks like, and the test has no way to tell the two apart. So a failure here does not
establish the defect it was written for, and the honest fix is to assert on the thing that distinguishes
them (a CME actually observed) and to wait on the consume rather than on a clock.

The ambient probe said `probe clean` and, unusually, said why that is worth little here: **`detector reach:
UNKNOWN - this test declares no @Timeout`**, so nothing in the autopsy says the long-bound detectors had time
to fire. Take the clean verdict as unproven rather than as evidence.

**Control arm: not always red.** The next head was a one-file markdown delta - this note itself - and its
integration lane ran the same code and passed. That separates *always red* from *not always red*, and nothing
more; it is not a rate and it does not identify which of the sleeps lost. The prior run had been *cancelled*
by that push rather than completing, which is worth saying because the cancelled run's absence from a failure
list reads exactly like a pass.

Not quarantined: quarantine is master-state and needs evidence, and one sighting is not a rate.
<!-- post-merge: checked-end -->

<!-- post-merge: checked-begin - names astubbs#428 in the past tense as the branch a sighting came
     from, which stays true once that work has landed -->
**Second sighting, 2026-09-07 on astubbs#428, and what it adds is the condition rather than the
count.** Same assertion, same repetition 1 of 5, same `probe clean` with the same unproven
`detector reach: UNKNOWN`. astubbs#428 is cleared on mechanism the way astubbs#207 was: its whole
main-code change is one argument on the commit-failure ERROR line and a DEBUG line beside it, both
inside the `exception != null` guard of the async callback, and the symptom is zero records consumed
after toggling an instance - reachable without that branch present.

**What connects the two sightings is not the branch but the lane.** Both fired on the first run of a
branch that had just merged a change to how this lane is executed:
`docs/inflight/pr-integration-gate-wall-time.md` already records "a first-ever timeout failure in
`ManagedPCInstanceLifecycleTest`" when `forkCount` went 4 to 6, and this run was the branch's first
under the sharded integration and chaos lanes. That makes the test a load-shape canary rather than a
random flake, which is a sharper claim than the first sighting could support and points at the same
fix: the sleeps are the thing that loses when the runner is busier, so waiting on the consume rather
than on a clock removes the whole class.

`docs/solutions/best-practices/attribute-a-red-only-after-a-control-arm-on-the-gates-own-configuration.md`
owns the method this is an instance of, and its warning applies here too: the same head re-run passed,
which separates *always red* from *not always red* and establishes nothing about the rate.
<!-- post-merge: checked-end -->

<!-- post-merge: checked-begin - names astubbs#452 in the past tense as the branch a sighting came
     from, which stays true once that work has landed -->
**Third sighting, 2026-09-07 on astubbs#452, and it breaks the "first run under this lane" pattern
the first two shared.** Same repetition 1 of 5, same `consumeCount` 0 message. This run was `2c4c53a`,
a merge of `origin/master` into the branch; the head immediately before it, `1088840a`, had passed the
same lane cleanly, and `git diff --name-only 1088840a 2c4c53a -- .github/` shows nothing changed in
the workflow or lane configuration in the merged range - so this sighting is **not** the "branch just
took a lane change" condition the first two established, only a plain re-run. That widens the claim
rather than narrowing it: the test goes red on an ordinary run too, not only on a first run under a
freshly changed lane, which is more consistent with the fixed-sleep-under-load defect named above than
with a load-shape canary specific to lane changes.

Cleared on mechanism the same way as the first two: astubbs#452's whole diff is
`OffsetMapCodecManager.java` and its lincheck/unit tests plus docs, none of which this test's
`PERIODIC_CONSUMER_ASYNCHRONOUS` + `UNORDERED` path against a fresh topic can reach. Not re-diagnosed
here - the fixed-sleep defect and assertion-attribution problem above already cover it. Not
quarantined for the same reason as before: quarantine needs a rate, not a third data point.
<!-- post-merge: checked-end -->

### Controls for these flakes - the void one, and the one that works

Method for the rows still open, not a diagnosis of any one of them. It is written from a
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
instead - `ConditionTimeout`, `expected: 139 but was: 136 within 30 seconds` - the 4/45 entry, since
diagnosed and removed from this ledger (see the header).

An earlier revision of this entry called that "the strongest evidence", on the reasoning that a code
regression fails the same way twice and this did not. **That reasoning does not hold and is withdrawn.**
Under concurrent or stress execution one defect can perturb timing enough to surface different tests
and different failure modes, so two dissimilar failures do not exclude a regression - they show only
that the first did not reproduce. Review caught this; it is exactly the invalid-diagnostic-rule trap
that AGENTS.md warns about, and left standing it would have licensed quarantining a real product bug.

What the rerun **does** establish: the failure is not deterministic, and the unit lane is currently
producing red from more than one already-tracked test. What it is *not* is evidence about any one
test's mechanism - that has always come from a source-level read, never from a rerun's landing spot.
