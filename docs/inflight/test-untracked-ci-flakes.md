# Flakes CI was hiding, none of them tracked when found

<!-- inflight-type: register -->
<!-- inflight-impact: misdirection -->
<!-- inflight-vetted: 2026-09-07 - register re-read; every row still open, one stale line fixed. The RegistrationRaceStaleResidentIT section still said "deliberately NOT quarantined here", but the owner has since quarantined it - the test carries @Quarantined(flapping = true, tracking = "docs/inflight/test-untracked-ci-flakes.md") and docs/quarantined-tests.md lists it citing this file - so that paragraph now records the quarantine and keeps the classification open; nothing was lifted. The rest checks out: the PIT row matches maven.yml (step timeout-minutes: 20, job 25, still continue-on-error), and bin/inflight.mjs codecov test on freshArrivalColliding..., rapidToggleShouldNotCreateDuplicateInstances, simpleBatchTest and processInKeyOrder shows none of them fixed - recent runs pass, and the tool warns its page is bounded -->

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
Where their diagnoses generalised, the rule is in [`docs/solutions/`](../solutions/).

| Test | Rate | Why it is worth attention |
|---|---|---|
<!-- post-merge: checked - the row states the fix and the lift as things that happened -->
| `ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` | 1 seen (2026-08-12) | Not from the original scan - found while babysitting astubbs#287. **Fixed by astubbs#265**, which deleted the wall-clock assertion rather than repairing it. astubbs#262, its owner, lifted the quarantine and deleted the registry entry - see below |
| `JStreamParallelEoSStreamProcessorTest.testConsumeAndProduce` and `.testFlatMapProduce` | 1 seen (2026-09-01) | Not from the original scan - found in a **local** core unit run on a parallel re-cut of astubbs#207, not on astubbs#207 itself. Both failed together on produced-record count (`Expected size: 1/2 but was: 0`), i.e. the returned stream carried nothing. **Mechanism now known and owned by astubbs#116** - see below | <!-- post-merge: checked -->
| `Mutation Tests (PIT, PR-scoped)` lane | 1 seen (2026-09-02, astubbs#207, [run 33610711974](https://github.com/astubbs/parallel-consumer/actions/runs/33610711974)) | Not a test - the LANE hit its `timeout-minutes: 30` cap and was cancelled, on a **markdown-only** delta from a head where it had scored in 19m18s with the same class set. The cap had about a third headroom over a normal run, so it flapped on a slow runner. Addressed 2026-09-07: the bound is now `timeout-minutes: 20` on the PIT **step**, so a hit ends that step and the job still reports, where a hit on the old job cap cancelled the whole row. It arrived with the fold into `scan: repo` and outlived it - astubbs#463 un-folded PIT into its own `mutation` job again and the step bound moved with the step ([`ci-fewer-jobs-ruleset-edits.md`](ci-fewer-jobs-ruleset-edits.md)). Still `continue-on-error: true`, so it never gates a merge <!-- post-merge: checked --> |
| `ManagedPCInstanceLifecycleTest.rapidToggleShouldNotCreateDuplicateInstances` | 3 seen (2026-09-02, astubbs#207, [job 100175277225](https://github.com/astubbs/parallel-consumer/actions/runs/33607572165/job/100175277225); 2026-09-07, astubbs#428, [job 101592337448](https://github.com/astubbs/parallel-consumer/actions/runs/34072492940/job/101592337448); 2026-09-07, astubbs#452, [job 101607850077](https://github.com/astubbs/parallel-consumer/actions/runs/34078008311/job/101607850077)) - the first two the first run of a branch that had just taken a change to how this lane runs; the third a re-run after a merge from master, with no `.github/` change in the merged range | Not from the original scan - **arrived on master with astubbs#29 and failed on the first PR to merge it**. `consumeCount` 0, repetition 1 of 5, `forkCount=4`, `probe clean`. Every wait in the test is a fixed sleep, and its assertion names a cause it cannot discriminate - see below <!-- post-merge: checked --> |
| `AmbientProbeExtensionTest.headroomIsReportedOnAPassingTestToo` and `.headroomOutcomeComesFromTheWatcherPhaseNotTheEndOfTheTestMethod`; a third method, `.headroomIsSilentWithoutADeadlineAndWithoutAMeasurement`, by the same mechanism - it captured another method's `PC-DEADLINE-HEADROOM ... test=mockedTest()` line | 2/2 isolated runs, 1 seen in a full core run (2026-09-02, local, astubbs#116); the third method 1 seen in 2 full core runs, and the original two 1 more full-run sighting (2026-09-07, local, the revoke-drain fix branch, which does not touch the probe) | **DIAGNOSED, and not a product defect** - two methods of one class each capture the *same process-global* logger with `LogCapture.of(AmbientProbeExtension.class)` while the suite runs them concurrently, so each sees the other's headroom line and the `hasSize(1)` assertion gets 2. Reproduces on demand - see below | <!-- post-merge: checked -->
| `LoadFactorCeilingReportingTest.fixedMessageBufferSizeDoesNotWarnOnEveryPass` | 5 seen (2026-09-02, and four times 2026-09-03, all local full runs) | Its WARN capture on the processor's shared logger caught a user-function failure line from an instance an earlier class left running; passes alone and beside `UserFunctionFailureLoggingTest`. Same shape as the `AmbientProbeExtensionTest` row - see below |
| `Chaos Pain Suite` and `Lincheck` lanes - `Could not find or load main class org.apache.maven.wrapper.MavenWrapperMain` | 2 seen (2026-09-03: astubbs#410 chaos on [run 33700215189](https://github.com/astubbs/parallel-consumer/actions/runs/33700215189), astubbs#426 Lincheck on [run 33705127539](https://github.com/astubbs/parallel-consumer/actions/runs/33705127539)) | Not a test - the LANE fails before Maven starts. `.mvn/wrapper/maven-wrapper.jar` is gitignored and `mvnw` downloads it from Maven Central on first use, with `--quiet`, so a failed or partial download leaves no error and no class. Every other job in the same run passed, so it is the download on that runner, not the tree; the chaos lane passed on the next head. Durable fix, for a CI PR on master: commit the jar, or switch the wrapper to `only-script` so there is no jar to fetch. <!-- post-merge: checked - dated sightings on named runs -->
| `TransactionTimeoutsTest.commitTimeout(int, int, List)[2]` (the long-multiplier arm) | 1 seen (2026-09-03, astubbs#410, [run 33718892721](https://github.com/astubbs/parallel-consumer/actions/runs/33718892721)) | Committed offsets `[8]` where `[8, 12]` was expected: the shutdown commit that carries the slowed record's offset did not land inside the assertion's window. On a docs-only head, with the identical engine code passing on the branch stacked above it in the same minute; `codecov flaky` already lists the variant. Shape: the test's own javadoc says the long arm races the shutdown timeout by design. <!-- post-merge: checked - dated sighting on a named run --> |
| `ReactorPCTest.concurrencyTest` | Local only, 2026-09-07: 2 of 3 full unit-suite runs and 1 of 3 reactor-module runs on the astubbs/parallel-consumer#469 tree; 0 of 6 module runs and 0 of 1 full-suite run on a worktree detached at that branch's own base commit, built and run the same way | **The test hugs its ceiling in EVERY run on BOTH trees, passing ones included** - `grep -c 'More records submitted'` on a *green* control run returns 59-76, because the fail-fast log fires whenever in-flight exceeds `MAX_CONCURRENCY` while the assertion tolerates `MAX_CONCURRENCY * MAX_CONCURRENCY_OVERFLOW_ALLOWANCE`. So a failure is the peak crossing a tolerance the run is already sitting against, not a new behaviour appearing. Passes 4/4 in isolation, so it is load-sensitive. The asymmetry against the control is real and unexplained, and **the direction-of-effect argument that used to stand against it does not hold - it was withdrawn on astubbs/parallel-consumer#469 after a Codex review, and is recorded here so nobody re-derives it.** It claimed the only work-admission field the PR touched, `PartitionState.allowedMoreRecords` made `volatile`, could only publish back-pressure's `false` sooner and so admit **fewer** records. That is one-way reasoning about a two-way field: `tryToEncodeOffsets()` also calls `setAllowedMoreRecords(true)` - on the `incompleteOffsets.isEmpty()` path, and in `updateBlockFromEncodingResult` when the payload comes back under the pressure threshold - so the fence publishes the **un**blocking transition sooner as well, which admits **more** records sooner. The two directions are not obviously equal in size, and nothing here has measured which dominates. **So this change is not ruled out as a contributor, and classification needs a measured control rather than an argument**: contention, tolerance, or a real submission-path defect this ceiling has been masking | <!-- post-merge: checked -->
| `CommitResponseTimeoutSymptomTest.aRebalanceStormUnderAHighFailureRateNeitherStallsNorKillsTheConsumer` | Local only, 2026-09-07: 1 of 3 full unit-suite runs on the astubbs/parallel-consumer#469 tree; 3/3 pass in isolation; 0 of 1 full-suite run on the same-base control worktree. `bin/inflight.mjs codecov test aRebalanceStormUnderAHighFailureRateNeitherStallsNorKillsTheConsumer` had no recorded failure before this, so CI had never seen it - ask the command rather than trusting a total written here | Failed its `commitsRejected >= MIN_REJECTIONS` await (3 vs 4) - **a count of commit ATTEMPTS, which only accrues while the backlog is draining**; once drained nothing is dirty, no further commit is attempted, and the 30s await can only time out. So the assertion is a race between wall-clock commit ticks and drain speed, and a loaded machine drains before enough ticks land. Not a stall assertion; do not read the failure as PC stopping | <!-- post-merge: checked -->
| `ParallelEoSStreamProcessorTest.processInKeyOrder` | 8 seen locally (2026-09-01) across three branches, 1 in 3 isolated runs; the input-data failure separately **1 of 8 on unmodified `master`** | **Two DIFFERENT failures under one test name, and the documented fix is already in the tree.** See below - this one is not a fresh flake, it is a solved one still firing. The second failure now has a control arm on master and a source-level lead, so classify from those rather than re-measuring |

**Classify before touching any of them** - the same rule that governs the load-tightness family next
door, and for the same reason: two of that family turned out to be real product bugs, and the third
was neither tight nor a stall but a test that could not force its own trigger.

### `processInKeyOrder` - a solved flake that still fires, and a second failure hiding under the same name

<!-- post-merge: checked-begin - names the branch the sighting came from, in the past tense, which
     stays true once that work has landed -->
Seen 2026-09-01 while running the core unit suite on the branch that became
astubbs/parallel-consumer#381, which carried no main Java - so nothing in that work can be the cause.
Recorded rather than diagnosed, because a sighting has to be written down before the branch that saw
it merges: the evidence expires with the logs.
<!-- post-merge: checked-end -->

**Two distinct failures, and conflating them would waste the next person's time:**

- **Parameter `[1]`, `ConditionTimeoutException` after ~41s**, on the assertion labelled
  *"Which offsets are committed and in the expected order"*. This is, symptom for symptom, the flake
  written up in
  [`../solutions/test-flakiness/assert-the-commit-frontier-not-the-tick-path.md`](../solutions/test-flakiness/assert-the-commit-frontier-not-the-tick-path.md).
  Reproduced 1 run in 3 in isolation, so it is cheap to work on.
- **Parameter `[3]`, an `AssertionError` on the test's own input-data sanity check** - "actual size
  is 0 while expected size is 9", the latch list empty. Seen ONCE, in a full 533-test suite run, and
  NOT reproduced in two subsequent full-class runs or three method runs. Different parameter,
  different phase, different message. Nothing yet says the two share a cause.

<!-- post-merge: checked-begin - a dated sighting, written in the past tense against a PR number
     rather than a branch name, so it stays resolvable after the branch is deleted -->
**A second, independent sighting of `[3]` - three tests, not one.** 2026-08-13, full core unit suite
on astubbs/parallel-consumer#262, at the head that had just merged master:
`processInKeyOrder`, `executorThreadsInterruptedOnShutdownTimeout` and
`inFlightMessagesCommittedIfProcessedDuringShutdown` failed together, all on `(CommitMode)[3]`, all
in about ten seconds - the shared elapsed time being the only positive signal, and it points at a
common timeout rather than at three defects. A different subset failed on each repeat and every one
of them passed in isolation. That branch's two main-code changes are ruled out by grep, not by
argument: `Produce lock already held` and `Could not return the produce lock` are the strings they
would have emitted, and the run log carries neither.

The assertion messages from that run were not kept, so **it is not established that this is the same
`[3]` failure as the one above** - what it establishes is that the `[3]` parameter fails on branches
whose changes cannot explain it, which is the same conclusion from a second direction. The control
that would settle it is still unrun and still cheap: the same suite on plain `origin/master`.
<!-- post-merge: checked-end -->

**The part worth acting on: that solution doc records its fix as `e8c9bb12` on astubbs#264 and
"UNMERGED as of 2026-08-13". astubbs#264 merged that same day, and the frontier assertion it
introduced IS in the tree** - `KafkaTestUtils` carries the frontier helper. Yet the failing
assertion still reports the OLD label, which `KafkaTestUtils` still offers as a default description
from two call sites. So the fix landed and this path did not adopt it.

That makes this a **stale-resolution** case rather than a new flake, and it is the more useful
reading: a solution doc that says "fixed" is why nobody re-opened this. Whoever picks it up should
start by checking which call sites still take the ordered-list assertion, and update that doc's
`status`, which is wrong in a way that suppresses attention.

<!-- post-merge: checked-begin - the sighting and the control arm are dated facts about a master
     commit, stated in the past tense, so both stay true once the branch that measured them lands -->
#### The parameter `[3]` failure: a control arm, and a lead in the test rather than the product

**It is not confined to one branch, and that was measured rather than argued.** Met again on
2026-09-01 while re-cutting astubbs/parallel-consumer#203 (2 of 5 full `-pl :parallel-consumer-core`
runs), so a control arm was run before anything was touched: a detached worktree at the `master`
commit that re-cut was based on, no changes, same box - **1 of 8 runs failed on the identical line
with the identical message**. Same assertion, same failure mode, none of that branch's changes
applied. Re-running the class alone was green 5 of 5, so it needs the concurrent suite's load to
fire - which is why the single full-suite sighting above did not reproduce in class or method runs.

```
ParallelEoSStreamProcessorTest.processInKeyOrder:1147 [sanity check input data]
  Actual and expected should have same size but actual size is: 0 while expected size is: 9
```

**The lead is in the test, not the product**, and it is a shape `docs/solutions/` already names -
awaiting a proxy that leads the value under assertion. Grep `processInKeyOrder` for
`awaitForOneLoopCycle`: the assertion immediately after it counts the records the `consumerSpy.poll`
doAnswer has accumulated, but one control-loop iteration is not a promise that the poll delivering
all nine has happened - the first iteration can turn before the broker poller has returned anything,
and `polled` is then empty rather than short. That reading predicts exactly the observed all-or-
nothing size (0, never 3 or 7), and it predicts the load dependence.

**Classify before touching it**, per this ledger's rule. The cheap experiment: replace the
`awaitForOneLoopCycle()` with an await on `polled` reaching nine and predict it goes deterministic;
if it still fails, the poll genuinely is not happening and that is a product question.

<!-- post-merge: checked-begin - names astubbs/parallel-consumer#207 in the past tense as the branch
     the arms were measured on, which stays true once that PR has landed -->
**Three further load arms, from a fourth branch, agreeing with the control above.** Measured on
astubbs/parallel-consumer#207 while it ran the full core suite:

| Arm | Result |
|---|---|
| Full suite, fresh worktree, machine otherwise idle | green 559/0, at two consecutive commits |
| Full suite, machine also running CI and a second build | 2 failures, twice running |
| `ParallelEoSStreamProcessorTest` alone, machine loaded | green 68/68 |

The failing parameters differed between those runs (`[2]`, then `[1]` and `[3]`), which is what rules out
a deterministic break from any one branch - and the isolated green agrees with the class-alone result
above rather than contradicting it, because isolation removes the load the failure needs.
<!-- post-merge: checked-end -->

<!-- post-merge: checked-begin -->

### `JStreamParallelEoSStreamProcessorTest` - both produce tests, empty stream, seen once

Seen once, locally, in the middle of a full `parallel-consumer-core` unit run. `testConsumeAndProduce`
and `testFlatMapProduce` failed in the same execution, both because the returned stream held nothing
at the point the assertion ran.

**Recorded here rather than lost, but it was not seen on astubbs#207.** It surfaced in a second
session that was independently re-cutting astubbs#207 onto master (branch
`recut/207-offset-policy-bypass`, since stood down); the offsets change that run carried was that
re-cut's, not the one astubbs#207 now ships. The sighting is carried across because the ledger's job
is to stop a flake going unrecorded, and a branch that no longer exists cannot hold it.

What is established, and it is only elimination: the same full suite was then run once on unmodified
master and twice with that change, all green, and this class passes in isolation on both sides. So no
offsets change is implicated and the failure did not reproduce - which also means nothing here is
diagnosed.

**The mechanism is astubbs#116's, and this sighting is evidence for it.** That PR - *"a result stream
that ends before the results arrive"*, fixing astubbs#122 / confluentinc#912 - found that the bridge
from the result queue to the returned `Stream` returned `false` from `Spliterator#tryAdvance` the
first time the queue polled empty. `tryAdvance` has no way to say "nothing right now": `false` means
*no more, ever*. So a momentary gap ended the stream permanently.

Its own description says eight tests across core and vertx "collected the stream on the calling
thread and asserted a size" and "passed **only because the stream quit early** - they encoded the
defect". `testConsumeAndProduce` and `testFlatMapProduce` are two of them, and they assert exactly
the sizes seen empty here. So this is not a test-infrastructure flake: it is the product defect
astubbs#116 fixes, observed racing the other way for once, and it explains why both failed together
and why the class passes in isolation.

**Do not diagnose or quarantine this separately - it goes away with astubbs#116**, whose
`JStreamLiveResultStreamTest` covers the behaviour directly. Recorded anyway rather than dropped,
because a sighting that confirms a fix is already written is worth more than one nobody wrote down.
Whoever merges astubbs#116 owns retiring this entry, per the four outcomes in this directory's
`AGENTS.md` - the sighting's value is that it corroborates that fix, so it migrates into the fix
rather than being deleted.
<!-- post-merge: checked-end -->

### `AmbientProbeExtensionTest` - two tests, one global logger, run concurrently

<!-- post-merge: checked-begin -->
Found while babysitting astubbs#116, whose merge had touched `AmbientProbeExtension` itself - so the
first question was whether that merge broke it. It did not, and the control arm is what settles it
rather than argument. That PR is cited as where the sighting came from, which stays true once it
lands; the flake is not its to own.
<!-- post-merge: checked-end -->

**Reproduce it, which is the unusual part - this one does not need luck:**

    ./mvnw -o -pl parallel-consumer-core -am test -Dtest=AmbientProbeExtensionTest

Run as a class on its own it failed **2 of 2** attempts, two methods each time. Inside a full
`parallel-consumer-core` run it is intermittent: one failure in one run, clean in another on the same
head. That direction is backwards for ordinary contention - a busier suite fails it *less* - and it is
the tell for the mechanism, because a full suite interleaves other classes between these two methods
while running the class alone puts them side by side.

**The mechanism is in the test, and it is visible in one line.** Both
`headroomIsReportedOnAPassingTestToo` and `headroomOutcomeComesFromTheWatcherPhaseNotTheEndOfTheTestMethod`
open `LogCapture.of(AmbientProbeExtension.class)`. That attaches to the logger for that class, which is
process-global, not test-scoped - so while both are inside their `try`, each captures BOTH lines:

    value of    : iterable.size()
    expected    : 1
    but was     : 2
    iterable was: [PC-DEADLINE-HEADROOM ... outcome=PASSED,
                   PC-DEADLINE-HEADROOM ... outcome=FAILED]

The `outcome=FAILED` line belongs to the sibling test. Each method's own assertion is correct; what is
missing is that only one of them may hold the capture at a time.

<!-- post-merge: checked-begin -->
**Control arm.** With astubbs#116's own change to `JStreamLiveResultStreamTest` reverted entirely, both
runs above still failed 2 of 2 - so the flake is inherited, and that PR only perturbed scheduling.
`AmbientProbeExtension.java` (main) was untouched by it in any way that reaches this.
<!-- post-merge: checked-end -->

<!-- post-merge: checked-begin -->
**Not fixed by the PR that found it, deliberately.** `bin/check-pr-analysis-surfaces.sh` classified this
class as inherited for astubbs#116, and this directory's rule is that inherited findings go to a
register rather than being bulk-fixed by a PR that happens to meet them.
<!-- post-merge: checked-end --> The fix is test-isolation, not a product change - forcing
these two methods onto one thread, or giving `LogCapture` a per-test scope - and whoever takes it should
check the other `LogCapture` users for the same shape rather than patching these two.

Unowned.

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

<!-- post-merge: checked-begin - the collision and both its halves are recorded as history, and the
     rule-3 lift as done rather than owed, since both PRs are cited for what they did -->
**Fixed by astubbs#265**, and the way it was fixed settled the open question this entry used to pose.
Two answers were live - measure it correctly, or stop measuring it. astubbs#262 took the first,
anchoring the elapsed window to a nanos stamp taken just before `schedule()`; astubbs#265 took the
second, deleting the wall-clock assertion outright and replacing it with a causal one (still parked
when the unblocker ran, return ordered after it), plus `BlockedThreadAsserterTest` to hold it.

astubbs#265 reached master first, and astubbs#262 resolved the collision by taking it wholesale and
dropping its own anchoring. That was the better outcome and not a reluctant one: anchoring shrank the
error and kept the run slow, while removing the assertion ends the whole class of scheduler-jitter
failure and stops the helper sleeping out its own timeout. The residual the anchoring approach had to
disclose does not exist, because nothing is measured.

The rule-3 re-enable was astubbs#262's to perform, being the entry's owner, and it performed it: the
`@Quarantined` annotation and the `docs/quarantined-tests.md` entry went in the same change that
merged master, returning the test to the gating lane.
<!-- post-merge: checked-end -->

**Why it was not in this ledger already.** The 2026-08-07 scan read surefire `Flakes:` markers, which
only appear when the retry re-ran a test and it then passed. This one failed the run outright, so it
left no marker and no scan would have found it. Flakes now get quarantined as they are met, rather
than waiting for a sweep.

### `ParallelEoSStreamProcessorTest.processInKeyOrder` - fails its own INPUT sanity check, undiagnosed

<!-- post-merge: checked -->
Seen once, 2026-08-18, on astubbs/parallel-consumer#29's branch, in a full unit-suite run - two
parameterised cases at once (`[2]` and `[3]`, ~1.87s each), while `[1]` passed:

```
java.lang.AssertionError:
[sanity check input data]
Actual and expected should have same size but actual size is: 0
```

**The failing assertion is on the test's own input, before the behaviour under test.** The priming
step produced zero records, so nothing about key-ordered processing was actually exercised - which
makes this a test-infrastructure fault rather than evidence about the product, unless the priming
path itself is racing something real.

**What rules out the branch it appeared on.** That run's only uncommitted change was a
`log.isTraceEnabled()` guard in `ThreadConfinedConsumer` plus a markdown file. Neither can affect test
input. The suite passed 2/2 immediately after on the same tree, and the test passes 3/3 in isolation
(`-Dtest='ParallelEoSStreamProcessorTest#processInKeyOrder'`).

**Not yet established:** whether it reproduces on `master`, which is what decides PR-state versus
master-state and therefore who owns it. Nobody has run that comparison. Do that before quarantining -
quarantine needs a diagnosis, and "the input primer occasionally yields nothing under load" is a
hypothesis, not one.

**Why two cases and not one** is the most promising thread: `[2]` and `[3]` failing together while
`[1]` passed suggests shared setup state rather than independent bad luck, which points at the
harness's record priming rather than at timing.

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
