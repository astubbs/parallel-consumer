# Flakes CI was hiding, none of them tracked when found

<!-- inflight-type: register -->
<!-- inflight-impact: misdirection -->

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
Also fixed and out: `MdcContextPropagationTest.anEmptyCallerContextIsHandledAndNothingLeaks`, which
asserted a null MDC on a runner thread two other classes had left holding `{}` (four sightings, the
last one master's own build for the astubbs#415 merge; fixed twice over, by two sessions that did not see each other) -
[`mdc-null-precondition-armed-by-an-earlier-class-in-the-same-fork-2026-09-02.md`](../solutions/test-flakiness/mdc-null-precondition-armed-by-an-earlier-class-in-the-same-fork-2026-09-02.md).
Where their diagnoses generalised, the rule is in [`docs/solutions/`](../solutions/).

| Test | Rate | Why it is worth attention |
|---|---|---|
<!-- post-merge: checked - the row states the fix and the lift as things that happened -->
| `ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` | 1 seen (2026-08-12) | Not from the original scan - found while babysitting astubbs#287. **Fixed by astubbs#265**, which deleted the wall-clock assertion rather than repairing it. astubbs#262, its owner, lifted the quarantine and deleted the registry entry - see below |
| `AmbientProbeExtensionTest.headroomIsReportedOnAPassingTestToo`, `headroomOutcomeComesFromTheWatcherPhaseNotTheEndOfTheTestMethod`, `headroomIsSilentWithoutADeadlineAndWithoutAMeasurement` | 4 of 5 local runs (2026-09-02); 3 of 3 local full runs (2026-09-03) | Found while verifying the producer-recovery work (astubbs#225) locally; that change does not touch the class. UNDIAGNOSED - see below. **The 2026-09-03 sightings are a control arm**: two runs on the poisoned-transaction branch and one on its unmodified base failed here, with a different subset of the three cases each time, so the class is load-sensitive rather than reacting to any change |
| `JStreamParallelEoSStreamProcessorTest.testConsumeAndProduce` and `.testFlatMapProduce` | 2 seen (2026-09-01; 2026-09-03, `testConsumeAndProduce` alone, local full run on the poisoned-transaction branch, 0 of 5 on isolated re-run) | Not from the original scan - found in a **local** core unit run on a parallel re-cut of astubbs#207, not on astubbs#207 itself. Both failed together on produced-record count (`Expected size: 1/2 but was: 0`), i.e. the returned stream carried nothing. **Mechanism now known and owned by astubbs#116** - see below | <!-- post-merge: checked -->
| `LoadFactorCeilingReportingTest.fixedMessageBufferSizeDoesNotWarnOnEveryPass` | 6 seen (2026-09-02, and five times 2026-09-03, all local full runs) |
| `ParallelEoSStreamProcessorTest.inFlightMessagesCommittedIfProcessedDuringShutdown(CommitMode)[3]` | 1 seen (2026-09-03, local full run on the poisoned-transaction branch, 0 of 3 on isolated re-run) | Committed `[]` where `[1]` was expected - the shutdown commit did not land inside the assertion window. The `[3]` arm is the transactional commit mode, which is why it was checked against the branch's change first and cleared: the test never produces, and its base supplies a `Producer` instance (`AbstractParallelEoSStreamProcessorTestBase` calls `.producer(producerSpy)`), which makes `canRecover()` false, so the branch's new send-callback path cannot execute here at all. Same shutdown-race shape as the `TransactionTimeoutsTest.commitTimeout` row. UNDIAGNOSED | Its WARN capture on the processor's shared logger caught a user-function failure line from an instance an earlier class left running; passes alone and beside `UserFunctionFailureLoggingTest`. Same shape as the `AmbientProbeExtensionTest` row - see below |
| `simpleBatchTest` in **all three** of `ReactorBatchTest`, `MutinyBatchTest` and `VertxBatchTest` | 5 seen (2026-08-18, 2026-08-19, 2026-08-25, 2026-09-01, 2026-09-02) | Not from the original scan - each found while babysitting a branch. Same Awaitility `ConditionTimeout`, same alias 'expected number of batches' (30s), same shared `BatchTestMethods` lambda. UNDIAGNOSED, but the third, fourth and fifth sightings independently carry the **same three-way key collision** in the failing batch contents, which points at the test's own randomised input - see below, and classify (contention vs product vs expectation) before touching |
| `Chaos Pain Suite` and `Lincheck` lanes - `Could not find or load main class org.apache.maven.wrapper.MavenWrapperMain` | 2 seen (2026-09-03: astubbs#410 chaos on [run 33700215189](https://github.com/astubbs/parallel-consumer/actions/runs/33700215189), astubbs#426 Lincheck on [run 33705127539](https://github.com/astubbs/parallel-consumer/actions/runs/33705127539)) | Not a test - the LANE fails before Maven starts. `.mvn/wrapper/maven-wrapper.jar` is gitignored and `mvnw` downloads it from Maven Central on first use, with `--quiet`, so a failed or partial download leaves no error and no class. Every other job in the same run passed, so it is the download on that runner, not the tree; the chaos lane passed on the next head. Durable fix, for a CI PR on master: commit the jar, or switch the wrapper to `only-script` so there is no jar to fetch. <!-- post-merge: checked - dated sightings on named runs -->
| `TransactionTimeoutsTest.commitTimeout(int, int, List)[2]` (the long-multiplier arm) | 1 seen (2026-09-03, astubbs#410, [run 33718892721](https://github.com/astubbs/parallel-consumer/actions/runs/33718892721)) | Committed offsets `[8]` where `[8, 12]` was expected: the shutdown commit that carries the slowed record's offset did not land inside the assertion's window. On a docs-only head, with the identical engine code passing on the branch stacked above it in the same minute; `codecov flaky` already lists the variant. Shape: the test's own javadoc says the long arm races the shutdown timeout by design. <!-- post-merge: checked - dated sighting on a named run --> |
| `Mutation Tests (PIT, PR-scoped)` lane | 1 seen (2026-09-02, astubbs#207, [run 33610711974](https://github.com/astubbs/parallel-consumer/actions/runs/33610711974)) | Not a test - the LANE hit its `timeout-minutes: 30` cap and was cancelled, on a **markdown-only** delta from a head where it had scored in 19m18s with the same class set. The cap has about a third headroom over a normal run, so it will flap on a slow runner. `continue-on-error: true`, so it never gates a merge - but a cancelled row reads like a failure <!-- post-merge: checked --> |
| `ManagedPCInstanceLifecycleTest.rapidToggleShouldNotCreateDuplicateInstances` | 2 seen (2026-09-02, astubbs#207, [job 100175277225](https://github.com/astubbs/parallel-consumer/actions/runs/33607572165/job/100175277225); 2026-09-03, astubbs#410, [job 100508674056](https://github.com/astubbs/parallel-consumer/actions/runs/33710182789/job/100508674056), `consumeCount` 0 again, passing on three sibling heads within minutes) | Not from the original scan - **arrived on master with astubbs#29 and failed on the first PR to merge it**. `consumeCount` 0, repetition 1 of 5, `forkCount=4`, `probe clean`. Every wait in the test is a fixed sleep, and its assertion names a cause it cannot discriminate - see below <!-- post-merge: checked --> |
| `RegistrationRaceStaleResidentIT.freshArrivalCollidingWithStaleShardResidentMustStillGetProcessed` | 7 seen (2026-09-01; 2026-09-03 six times: on astubbs#420, [run 33706716163](https://github.com/astubbs/parallel-consumer/actions/runs/33706716163), twice in a row on astubbs#429 whose same head then passed on a deliberate re-run, and three times on astubbs#410, the last on [run 33716898092](https://github.com/astubbs/parallel-consumer/actions/runs/33716898092) with a green run between - the recorded history since astubbs#429 landed reads red on about one head in four across branches that carry it and green on the ones that do not, which is too few runs to attribute but is the next thing to check, [run 33710182789](https://github.com/astubbs/parallel-consumer/actions/runs/33710182789) and [run 33711953542](https://github.com/astubbs/parallel-consumer/actions/runs/33711953542) - same setup-guard failure each time, probe clean; the recorded history shows it red on an unrelated branch the same hour and green on ten other heads in the same window, including astubbs#420, a superset of astubbs#410. astubbs#410 does not touch the consumer-sync registration path this test drives) | Not from the original scan - found while babysitting astubbs#257. Failed its **saturation/pause-point setup guard**, not the confluentinc#909 signature assertion, so it proves nothing about the defect it reproduces - see below <!-- post-merge: checked --> |
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

### `RegistrationRaceStaleResidentIT` - the setup guard timed out, which is not the 909 assertion

<!-- post-merge: checked-begin - names astubbs#257 in the past tense as the branch the sighting came
     from, which stays true once that work has landed -->
Seen 2026-09-01 on astubbs#257's CI ([job 99873226946](https://github.com/astubbs/parallel-consumer/actions/runs/33513016782/job/99873226946)),
one failure in 161 integration tests. Recorded rather than diagnosed, per this ledger's own rule: the
evidence expires with the logs.

**What failed is the precondition, not the reproduction.** The assertion was
`control thread must reach the mid-loop pause point (offset 25)` - `awaitPausePoint(30, SECONDS)`
returned false. That is stage 2 of the test's setup, so the confluentinc#909 stale-resident assertion
this IT exists for was never evaluated. Do not read this as evidence about the 909 defect in either
direction, and do not conflate it with
[`test-909-reproduction-cannot-observe-the-collision.md`](test-909-reproduction-cannot-observe-the-collision.md),
which is the opposite worry - that the same test goes silently *green* with the defect branch
unexercised.

**Ruled out as astubbs#257's doing, on mechanism rather than counts:**

- The test is `CommitMode.PERIODIC_CONSUMER_SYNC` and drives `pc.poll(...)`, never
  `pollAndProduceMany`, so `beginProducing` is never called and no produce lock is ever set on its
  contexts. Both paths astubbs#257 changed - `cleanUpContext`'s release and the deleted per-record
  release in `addToMailbox` - are `Optional`-guarded and are therefore no-ops here, before and after.
- The IT is untouched by that PR.
- `master` passed this lane at `54301ebd`, the exact base the failing run merged against, and on every
  recent run before it. One green run per commit cannot rule out a low-rate flake, so that is
  corroboration, not proof - the mechanism above is what clears the branch.

**The ambient probe called it test-side**: `probe clean - no rebalance dwell, no lag stagnation, no
frozen partitions observed`. Worth weighing against the probe's own thresholds before trusting it, but
it points away from broker contention and toward the test's own 30s timing budget - which is
`forkCount=4` on a shared runner, waiting on a hand-orchestrated race between a paused registration
loop and a forced eager rebalance.

**Seen three times more on 2026-09-03.** Identical assertion each time - `control thread must reach the mid-loop pause point
(offset 25)` - on two branches with nothing in common:
`feat/225-pc-built-producer` at `e35db1e` (02:24) and astubbs#429 at `fc99d29` (02:41,
[job 100501224076](https://github.com/astubbs/parallel-consumer/actions/runs/33707938531/job/100501224076),
1 failure in 201 integration tests). The probe was clean again, and the failing runs took 30.8s and
30.9s against 14-19s on the passing ones either side - the shape of a 30s budget being waited out in
full, not of work going wrong.

**Then astubbs#429 failed it a second time in a row**, at `6fd8e70` (03:03, 30.7s, the same assertion),
which is the count that stops the sentence above from being safe: two consecutive failures on one
branch against roughly one in a dozen elsewhere is not a coincidence anyone should merge through. So
the same head was re-run once as a **measurement, with the outcome recorded here whichever way it
went** - not the retry-to-green that AGENTS.md forbids. It passed, in 17.0s, the same length as the
19.1s pass this branch had at `909a885` before it merged master. Two of three on this branch, every
failure at the guard's 30s ceiling and every pass at the length other branches see; the only delta
between the green head and the two red ones is the pom exclusion and the doc astubbs#430 landed.

**The control arm is unusually good here and cost nothing**, because the same lane ran on four other
branches inside the same 25 minutes: `fix/422-commit-interval-unset-detection` (02:42), astubbs#428
at `def16c6` (02:41), `optimize/chaos-ci-perf` (02:44) and `feat/225-producer-config` (02:49) all
PASSED it. So the failure is not a property of any one branch's diff, and it is not the runner being
uniformly slow that half-hour either. `node bin/inflight.mjs codecov test
freshArrivalCollidingWithStaleShardResidentMustStillGetProcessed` reproduces that table.

**Ruled out as astubbs#429's doing, on mechanism:** that PR changes the produce path's
`InvalidPidMappingException` arm, `ProducerManager#close` and `innerDoClose`'s producer step. This IT
is `CommitMode.PERIODIC_CONSUMER_SYNC` driving `pc.poll(...)`, so it never produces and never reaches
any of them, and it fails in stage 2 of its own setup - before the instance is closed at all. Same
reasoning that cleared astubbs#257 above, for the same reason: the test never enters the changed code.
<!-- post-merge: checked-end -->

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

### `simpleBatchTest` - three modules, one shared helper, and a lead nobody has tested

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

**No sighting is on a branch whose diff contains main Java**, which is what rules out "a PR broke
it" and makes it master state - the same reasoning applied to `ProducerManagerTest` below. That is
also why none was quarantined on the branch that met it: quarantine is master-state and needs a
diagnosis, and no sighting has one.

The 2026-08-18 sighting passed on re-run, and so did the 2026-08-25 one - three consecutive clean
re-runs of the same test, so **1 failure in 4 local runs, not deterministic**. A re-run is diagnosis
here, not a way to go green - it distinguishes flaky from deterministic - and AGENTS.md's ban is on
the automatic `surefire.rerunFailingTestsCount` that hid this whole ledger, not on re-running a job
to learn something.

#### The 2026-08-25 sighting: a third module, and the first look at what was in the batches

`VertxBatchTest`, KEY ordering, on a local macOS full unit run of the branch that added
`ShardMapIsNeverReplacedArchTest` - one new core test class plus docs, no main Java. Same
`Expected size: 3 but was: 4`. **Three modules now, so the wrapper is definitively not the
variable.**

What is new is the payload the assertion printed. The five records carried keys `29, 36, 36, 36,
71` - a **three-way key collision** - and arrived as `{o0, o4}`, `{o1}`, `{o2}`, `{o3}`, so every
key-36 record came alone. Meanwhile `simpleBatchTest` computes its expectation from the record count
and nothing else: grep `BatchTestMethods` for `expectedNumOfBatches`, which is
`ceil(numRecsExpected / batchSizeSetting)` for every ordering except PARTITION. **The keys are
random** - `KafkaTestUtils.getRandomKey` draws from `defaultKeys`, a hundred integers - so the shard
distribution the batcher works against varies run to run while the expected batch count does not.

That is a third reading to separate, not a diagnosis, and it displaces neither of the two above:

- **Expectation-versus-input.** The test randomises the key distribution and then asserts a batch
  count that only holds for some distributions. A rare draw would explain a rare failure without any
  contention or product defect at all.

The experiment that settles it is cheap and has a control arm, and **nobody has run it**: pin the
keys through the Lombok setter on `KafkaTestUtils`'s `defaultKeys`, force a three-way collision
under KEY ordering, and predict a deterministic failure; then five distinct keys, and predict it
always passes. If both hold, this is
the test's own input and neither the runner nor the batcher. If the collision case passes, the draw
is a red herring and contention-versus-product stands as before.

<!-- post-merge: checked-begin -->
#### The 2026-09-01 sighting: the collision reproduces, in a second module, unprompted

`ReactorBatchTest`, KEY ordering (`simpleBatchTest(ProcessingOrder)[3]` - `@EnumSource` orders the
enum `UNORDERED, PARTITION, KEY`, so index 3 is KEY, the same parameter as 2026-08-25). Seen on the
Unit Tests lane of astubbs/parallel-consumer#393, the thread-confinement extraction. Same
`Expected size: 3 but was: 4`.

**This is the first sighting whose branch carried main Java, so the master-state argument above is
restated here rather than reused.** It still holds, for two reasons specific to
astubbs/parallel-consumer#393. Relative to the head the failure was first seen on, the commit under
test changed only comments and one core test fixture, and that fixture is loaded by two core
ownership tests `ReactorBatchTest` never touches. That PR's one behavioural change to a poll path
moves an existing `updateCache()` call from after `pollingBroker.set(true)` to before it - a
reordering, not an addition, so the poll does no more work than master's does. Neither could reach a
batch boundary in another module.

**What makes the sighting worth recording is the payload, because it is the 2026-08-25 one again.**
The five records carried keys `34, 62, 34, 34, 77` by offset - a **three-way collision on key 34** -
and arrived as `{o0, o1}`, `{o4}`, `{o2}`, `{o3}`. 2026-08-25 saw keys `29, 36, 36, 36, 71`, a
three-way collision on key 36, arriving as `{o0, o4}`, `{o1}`, `{o2}`, `{o3}`. Two independent
draws, different modules, different collided key, **same shape**: one key drawn three times, two
drawn once, and four batches where the expectation is `ceil(5 / 2) = 3`.

That is what the expectation-versus-input reading predicts, and it is no longer resting on a single
observation. Under KEY ordering the three colliding records share a shard and must be processed in
order, so they cannot batch with each other however fast the runner is; only the two singletons are
free to pair with anything. A three-way collision therefore forces at least four batches on
arithmetic, while `expectedNumOfBatches` - grep `BatchTestMethods` for it - is computed from the
record count alone and stays at three.

**It still is not a diagnosis, and the experiment named above is still the thing that settles it**
(pin `defaultKeys` through the Lombok setter on `KafkaTestUtils`, force the collision, predict a
deterministic failure; then five distinct keys, predict it always passes). What has changed is the
prior: the collision is now the leading reading rather than one of three equals, and a control arm
that failed to reproduce under a forced collision would be a genuinely surprising result. Recorded
while the CI log carrying the payload still existed - those logs expire, and the payload is the
whole value of the sighting.
<!-- post-merge: checked-end -->

<!-- post-merge: checked-begin - a dated sighting against a PR number and a sha, both durable -->
#### The 2026-09-02 sighting: the same shape a third time, on a branch with no Java at all

`ReactorBatchTest`, KEY ordering (`simpleBatchTest(ProcessingOrder)[3]` again), on the Unit Tests
lane of astubbs/parallel-consumer#414 at `36cd68593`. Same `Expected size: 3 but was: 4`, the alias
timing out at 30.53s. That branch's diff was workflow YAML and docs - no Java of any kind - so the
master-state argument needs no restating: nothing in it can reach a batch boundary in any module.

**The payload is the 2026-08-25 and 2026-09-01 shape for the third time.** Keys by offset
`49, 74, 74, 74, 59` - a **three-way collision on key 74** - arriving as `{o0, o1}`, `{o4}`, `{o2}`,
`{o3}`: the two singletons paired, the three colliding records one to a batch, four batches against
an expectation of three. Three independent draws, two modules between them, three different
collided keys, one shape.

What it changes is not the diagnosis - that is still the unrun pin-the-keys experiment above, and a
third collision-shaped payload moves the prior very little past where the second left it. What it
changes is the cost: this is the second day running that the flake failed a PR's required Unit Tests
lane outright, each time on a branch that could not have caused it, so it now charges a CI round to
work that has nothing to do with it. That is the condition under which a cheap unrun experiment
stops being deferrable.
<!-- post-merge: checked-end -->

### `AmbientProbeExtensionTest` headroom cases - a captured line from a neighbouring test method

Seen 2026-09-02 while verifying the producer-recovery work (astubbs#225) locally, whose diff does not
touch `AmbientProbeExtensionTest` or the extension it tests. Rule 2 (master-state, not PR-state) was
settled by a control arm rather than a rate: the same class was run on the branch tip *before* that
day's two new commits (a detached worktree at the merge commit) and failed there too.

```
headroomIsReportedOnAPassingTestToo:124  value of: iterable.size()  expected: 1  but was: 2
iterable was: [PC-DEADLINE-HEADROOM test=mockedTest() ... outcome=FAILED,
               PC-DEADLINE-HEADROOM test=mockedTest() ... outcome=PASSED]
```

Five local runs: the full core suite passed once (the baseline) and failed once (two
cases); three classes together failed with three; the class alone failed with two; the control arm failed with three. **The shape is a capture that
holds another test method's line** - the FAILED line belongs to a different mocked test than the one
asserting - so the first suspect is an appender that outlives its method, or two methods sharing the
`PC-DEADLINE-HEADROOM` logger without the `@ResourceLock` the environment-dump cases carry. Not
diagnosed; nothing was changed. The count varying between runs (2 or 3) says the order of the
methods decides which capture sees the stray line.

### `LoadFactorCeilingReportingTest` - a leaked instance from an earlier class logs into its capture

Seen 2026-09-02 in one local full run of the core suite on the producer-recovery branch (astubbs#225),
about forty seconds after `UserFunctionFailureLoggingTest` finished. The class is `@Isolated`, so no
other class ran beside it; the line it captured is the user-function failure summary
(`... registering WC as failed, returning to mailbox. Context: input-0.56...-0: 1 record, offset 0`),
which some earlier class's still-running control thread emitted on the shared processor logger after
its own class had ended. Passes alone, passes paired with `UserFunctionFailureLoggingTest`, and the
recorded history on its own branch is all-pass, so the leak is an instance not closed by a test in the
same fork, not this test. `@Isolated` cannot protect a capture from a thread that outlives its class;
the fix is in whichever test leaks, once identified - the surefire report timestamps name the
candidates that ran in the preceding minute.

<!-- post-merge: checked-begin - a dated sighting, past tense, on branches named as they were -->
Seen three times more on 2026-09-03, in local full runs of the core suite on two branches of the same
stack (twice on the recovery branch re-cut onto astubbs#426, once on astubbs#420 re-based above it), each
time passing alone immediately afterwards. Same shape: a captured line from an earlier class; nothing in
either diff touches the load factor. Four sightings in two days, all local, none yet in CI.
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
