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
Where their diagnoses generalised, the rule is in [`docs/solutions/`](../solutions/).

| Test | Rate | Why it is worth attention |
|---|---|---|
<!-- post-merge: checked - the row states the fix and the lift as things that happened -->
| `ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` | 1 seen (2026-08-12) | Not from the original scan - found while babysitting astubbs#287. **Fixed by astubbs#265**, which deleted the wall-clock assertion rather than repairing it. astubbs#262, its owner, lifted the quarantine and deleted the registry entry - see below |
| `AmbientProbeExtensionTest.headroomIsReportedOnAPassingTestToo`, `headroomOutcomeComesFromTheWatcherPhaseNotTheEndOfTheTestMethod`, `headroomIsSilentWithoutADeadlineAndWithoutAMeasurement` | 4 of 5 local runs (2026-09-02) | Found while verifying the producer-recovery work (astubbs#225) locally; that change does not touch the class. UNDIAGNOSED - see below |
| `simpleBatchTest` in **all three** of `ReactorBatchTest`, `MutinyBatchTest` and `VertxBatchTest` | 4 seen (2026-08-18, 2026-08-19, 2026-08-25, 2026-09-01) | Not from the original scan - each found while babysitting a branch. Same Awaitility `ConditionTimeout`, same alias 'expected number of batches' (30s), same shared `BatchTestMethods` lambda. UNDIAGNOSED, but the third and fourth sightings independently carry the **same three-way key collision** in the failing batch contents, which points at the test's own randomised input - see below, and classify (contention vs product vs expectation) before touching |
| `RegistrationRaceStaleResidentIT.freshArrivalCollidingWithStaleShardResidentMustStillGetProcessed` | 1 seen (2026-09-01) | Not from the original scan - found while babysitting astubbs#257. Failed its **saturation/pause-point setup guard**, not the confluentinc#909 signature assertion, so it proves nothing about the defect it reproduces - see below <!-- post-merge: checked --> |
| `ParallelEoSStreamProcessorTest.processInKeyOrder` | 2 seen locally (2026-09-01), 1 in 3 isolated runs | **Two DIFFERENT failures under one test name, and the documented fix is already in the tree.** See below - this one is not a fresh flake, it is a solved one still firing |
| `MdcContextPropagationTest.anEmptyCallerContextIsHandledAndNothingLeaks` | 1 seen locally (2026-09-01) | Not from the original scan. Its own PRECONDITION fails - `expected: null but was: {}` on the JUnit thread before PC is involved - so it says nothing yet about MDC propagation. Diagnosis, the logback-clear hypothesis and its falsification path live in [`test-mdc-empty-context-precondition-is-order-dependent.md`](test-mdc-empty-context-precondition-is-order-dependent.md); not quarantined, on one sighting |

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

Five local runs: the full core suite passed once (646 tests, the baseline) and failed once (two
cases); three classes together failed with three; the class alone failed with two; the control arm failed with three. **The shape is a capture that
holds another test method's line** - the FAILED line belongs to a different mocked test than the one
asserting - so the first suspect is an appender that outlives its method, or two methods sharing the
`PC-DEADLINE-HEADROOM` logger without the `@ResourceLock` the environment-dump cases carry. Not
diagnosed; nothing was changed. The count varying between runs (2 or 3) says the order of the
methods decides which capture sees the stray line.

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
