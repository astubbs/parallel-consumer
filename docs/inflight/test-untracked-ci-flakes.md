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
| `ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` | 1 seen (2026-08-12) | Not from the original scan - found while babysitting astubbs#287. Mechanism known and owned (astubbs#262), quarantined - see below |
| `simpleBatchTest` in **all three** of `ReactorBatchTest`, `MutinyBatchTest` and `VertxBatchTest` | 3 seen (2026-08-18, 2026-08-19, 2026-08-25) | Not from the original scan - each found while babysitting a branch carrying **no main Java**. Same Awaitility `ConditionTimeout`, same alias 'expected number of batches' (30s), same shared `BatchTestMethods` lambda. UNDIAGNOSED, but the third sighting carries the failing batch contents and they point at the test's own randomised input - see below, and classify (contention vs product vs expectation) before touching |
| `ParallelEoSStreamProcessorTest.processInKeyOrder` | 1 of 8 local full-suite runs on unmodified `master` (2026-09-01) | Not from the original scan - found while re-cutting astubbs#203, which is why the control arm exists. Source-level lead below, so classify from it rather than re-measuring <!-- post-merge: checked --> |

**Classify before touching any of them** - the same rule that governs the load-tightness family next
door, and for the same reason: two of that family turned out to be real product bugs, and the third
was neither tight nor a stall but a test that could not force its own trigger.

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

### `processInKeyOrder` - one control-loop cycle standing in for "all nine records polled"

```
ParallelEoSStreamProcessorTest.processInKeyOrder:1147 [sanity check input data]
  Actual and expected should have same size but actual size is: 0 while expected size is: 9
```

<!-- post-merge: checked-begin -->
**Master state, measured rather than argued.** Found while re-cutting astubbs#203 (2 of 5 full
`-pl :parallel-consumer-core` runs), so the control arm was run before touching anything: a detached
worktree at the `master` commit that re-cut was based on, no changes, same box - **1 of 8 runs failed
on the identical line with the identical message**. Same assertion, same failure mode, with none of
astubbs#203's changes applied. Re-running the test class alone was green 5 of 5, so it needs the
concurrent suite's load to fire.
<!-- post-merge: checked-end -->

**The lead is in the test, not the product**, and it is the shape `docs/solutions/` already names -
awaiting a proxy that leads the value under assertion. Grep `processInKeyOrder` for
`awaitForOneLoopCycle`: the assertion immediately after it counts the records the `consumerSpy.poll`
doAnswer has accumulated, but one control-loop iteration is not a promise that the poll delivering
all nine has happened - the first iteration can turn before the broker poller has returned anything,
and `polled` is then empty rather than short. That reading predicts exactly the observed all-or-
nothing size (0, never 3 or 7), and it predicts the load dependence.

**Classify before touching it**, per the rule above. The cheap experiment: replace the
`awaitForOneLoopCycle()` with an await on `polled` reaching nine and predict it goes deterministic;
if it still fails, the poll genuinely is not happening and that is a product question.

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
