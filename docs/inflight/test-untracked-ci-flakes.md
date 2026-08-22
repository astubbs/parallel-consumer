# Flakes CI was hiding, none of them tracked when found

<!-- inflight-type: register -->
<!-- inflight-impact: misdirection -->

Found 2026-08-07 by scanning surefire `Flakes:` markers across the 45 most recent CI runs (Integration
and Unit lanes). 8 of 45 runs carried markers. None of these tests appear in any ledger.

The retry that hid them is gone - that half is done and written up in
[`docs/solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md`](../solutions/workflow-issues/ci-retries-hid-flakes-from-the-ledger-2026-08-07.md),
which also has the scan method. What is open is the tests themselves - two of the scan's three, plus
one met later. The scan's third,
`ParallelEoSStreamProcessorTest.queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown`
(3/45), was fixed and removed from this ledger: astubbs#260 established the extra commit was correct
product behaviour and the assertion was wrong, so no product change was needed. **It has been seen
again since** - see below.

### Two more point checks in the same class, 2026-08-23

A full core unit run on `throttling-ideation` (adding the admission controller's metrics) failed four
times, all in `ParallelEoSStreamProcessorTest`, all point checks:

- `processInKeyOrder(CommitMode)[1]` and `[2]`, at `[sanity check input data]` - the symptom the
  2026-08-22 entry below already owns, now seen on both parameters of the same run.
- `consumeFlowDoesntRequireProducer(CommitMode)[2]` - *"Expecting AtomicBoolean(false) to have value
  true"*. New to this ledger.
- `offsetsAreNeverCommittedForMessagesStillInFlightLong(CommitMode)[1]` - *"[1 record completed during
  shutdown] ... to contain exactly [1]"*. New to this ledger; the `Short` sibling is discussed in
  [`perf-direct-pull-measured.md`](perf-direct-pull-measured.md).

**Conditions and rate:** one full-suite run, on a box with several agent sessions building at once.
The whole class then passed **58/58 in isolation** on the same build, immediately afterwards.

**Why the change in flight is not the suspect:** it registers meters and adds one rate-limited log
line, both gated on the adaptive-concurrency mode, and the mode defaults to `DISABLED` with nothing in
the suite or the poms setting `pc.adaptiveConcurrency` - so in these tests the controller registers no
meter and never ticks. Same population, same load-tightness shape as below; no quarantine on this
evidence.

### Seen again after being called fixed: the shutdown one, 2026-08-22

`ParallelEoSStreamProcessorTest.queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown`
failed once in a full core unit run on `perf/direct-pull-measured`, asserting
*"primed record and first key=0 record completed only, followup key 0 records skipped"*. **Recorded
because it is a recurrence of something declared fixed, which is the one case where a single sighting
is worth writing down.**

What is known: it **passed 3/3 in isolation immediately afterwards**, on the same build, with the box
at a one-minute load of 6-8. The failing run was on a box whose five- and fifteen-minute load averages
were 97 and 317 - several other agent sessions, plus a benchmark that had just finished driving five
thousand threads.

What it is **not**: the branch it was seen on changes `WorkContainer.inFlight` to a CAS,
`WorkManager.numberRecordsOutForProcessing` to an atomic, and `ShardManager.iterationResumePoint` to
volatile. Under the shipped engine the control loop is the only thread that selects work, so the CAS
cannot lose and the counters carry identical values - the default path is semantically unchanged, and
the direct-pull engine those changes exist for was **off** in this run.

**So it is the load-tightness shape again, not a new defect - but astubbs#260's claim that the
assertion was simply wrong now has a counter-example, and the next sighting is a third.**

### A test that passes on JDK 17 and fails on JDK 21 under load, 2026-08-22

**`OrderingModeDispatchParityTest.keyAndUnorderedCostTheSameToDispatch`** - recorded because the
virtual-threads work adds a lane that runs the unit suite on a **JDK 21 test JVM**, and this is the
one test whose verdict changes with that JVM. It asserts the UNORDERED and KEY shard shapes cost the
same to walk, with a ratio bound.

**Controlled, because the obvious attribution is the wrong one.** Virtual threads were the suspect and
are ruled out:

| Run | Load (1m) | Result |
|---|---:|---|
| JDK 17, full suite | ~12 | pass |
| JDK 21, full suite, **mode off** | ~18 | **fail, ratio 4.95** |
| JDK 21, full suite, mode on | ~22 | **fail, ratio 4.21** |
| JDK 21, full suite, mode on | ~14 | pass |
| JDK 21, full suite, mode on | ~67 | **fail, ratio 7.68** |
| JDK 21, **isolated**, mode on | ~30 | pass, 3/3 |

**The control arm fails too, with a worse ratio than the treatment** - so this is JDK 21 plus
concurrent suite load, not the execution mode. It also passes in isolation on JDK 21 every time, so it
is not a JDK 21 regression in the dispatch scan either. What changes is the surrounding parallelism:
the ratio compares two timed walks, and the shorter one (KEY, 41-55ms) is short enough that scheduler
noise moves the ratio more than the code does.

**Not quarantined**, because quarantine is master-state and this is only reachable on a lane that does
not exist on master yet. **It is why the virtual-thread CI entry is advisory.** If the lane is ever
made a required check, this has to be settled first - and the settlement is probably to compare work
done rather than wall-clock, since a timing ratio between a 44ms and a 338ms measurement is not a
property of the dispatch scan on a shared machine.

| Test | Rate | Why it is worth attention |
|---|---|---|
| `OffsetEncodingBackPressureTest.backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing` | 4/45 | The most frequent. UNDIAGNOSED; quarantined on its sighting ledger (rule 1) - see below. Backpressure area - compare `vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md`, a *different* class in the same area, so rule it in or out rather than assuming |
| `ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` | 1 seen (2026-08-12) | Not from the original scan - found while babysitting astubbs#287. Mechanism known and owned (astubbs#262), quarantined - see below |
| `simpleBatchTest` in **both** `ReactorBatchTest` and `MutinyBatchTest` | 2 seen (2026-08-18, 2026-08-19) | Not from the original scan - both found while babysitting a **docs-only** branch (astubbs#308 head `d930ca98d`; astubbs#320 head `70a247184`). Same Awaitility `ConditionTimeout`, same alias 'expected number of batches' (30s), same shared `BatchTestMethods` lambda - see below, the second sighting is what makes it worth diagnosing. UNDIAGNOSED - classify (contention vs product) before touching |

### An EXTRA delivery under the direct-pull engine, 2026-08-22 - DIAGNOSED, and neither sighting was a flake

> **Both sightings below are explained, reproduced, and FIXED.** The claim in
> `ProcessingShard#getWorkIfAvailable` was check-then-act: `isAvailableToTakeAsWork()` read three
> terms and `onQueueingForExecution()`'s compare-and-set re-validated only the in-flight one, which
> `onSuccessResult` resets - via `endFlight()` - *before* it removes the offset. A puller whose
> availability check predated the record's completion therefore won the CAS on an already-completed
> record, and the claim cleared the success verdict that would have refused it. **A product bug, not
> a harness bug, and only the direct-pull engine could reach it.** Reproduced at 4 occurrences in
> 14,400,000 record completions.
>
> Fixed by collapsing the in-flight boolean and the verdict into one atomic
> `WorkContainer.ExecutionState`, so the claim is a single compare-and-set from the state it
> evaluated. `git log --grep='ExecutionState'` finds it. The deterministic proof of the interleaving
> is in the suite as
> `WorkClaimStateMachineTest.aClaimDecidedBeforeAnotherPullerCompletedTheRecordIsRefused`; the
> concurrent reproduction is a soak, not a gate, and is not committed. **The text below is kept as
> the sighting record**, and the one part of it that turned out to be wrong is marked at the end.

**`ParallelEoSStreamProcessorPauseResumeTest.pausingAndResumingProcessingShouldWork(PERIODIC_CONSUMER_ASYNC)`**
failed once in a full core unit run with `-Dpc.directPull=true`, on the branch that adds the
direct-pull engine:

```
Condition with alias '1000 records should be processed' didn't complete within 30 seconds
because ... expected: 1000 but was : 1001
```

**Read the number before reading the word "flake".** The other entries in this ledger are assertions
that fired early, or two moving values compared at different instants. This one is the user function
having been called **one time more than there are records** - the signature of a record delivered
twice, which is the single thing the direct-pull engine most needs not to do. It is filed here rather
than as a bug because it could not be reproduced, and it is filed *at all* because a sighting with
this signature must not be lost.

**Reproduction rate: 0 out of 11.** Five runs of the method alone and six of the whole class, all
with `-Dpc.directPull=true`, at a one-minute load of 8-13 on twelve cores. The failing run was a full
suite with surefire's parallel forks. So it is load- or interleaving-dependent, and the arithmetic
above is all that is known about it.

**What was ruled out, and what was not.** Selection cannot hand the same record to two workers: that
is guarded by `WorkContainer.onQueueingForExecution()`'s compare-and-set and, under an ordered mode,
by `ProcessingShard.getWorkIfAvailable` breaking after the head record whether or not it claimed it.
Both are now covered by `DirectPullConcurrentSelectionTest`, and both were proven to fail when
sabotaged. **That rules out the selection layer only.** It says nothing about the redelivery paths -
retry, abandonment, the stale sweep - and the failing test's own arithmetic cannot say which of its
two record sets the extra delivery belonged to, because it resets its counter between them.

**2026-08-22: the same defect caught by a PRODUCTION ASSERT, which is far better evidence than the
sighting above.** `DirectPullConcurrentSelectionTest.theInFlightCounterNetsBackToZeroWithPullsAndReturnsOverlapping`
failed in a full core unit run - not a timeout, not an assertion of the test's own, but a raw
`java.lang.AssertionError` thrown out of product code:

```
Caused by: java.lang.AssertionError
    at bz.stub.parallelconsumer.state.PartitionState.onSuccess(PartitionState.java:261)
    at bz.stub.parallelconsumer.state.PartitionStateManager.onSuccess(PartitionStateManager.java:315)
    at bz.stub.parallelconsumer.state.WorkManager.onSuccessResult(WorkManager.java:177)
    at bz.stub.parallelconsumer.state.WorkManager.handleFutureResult(WorkManager.java:384)
```

`PartitionState#onSuccess` is `assert (removedFromIncompletes);` - it fires when a record is completed
whose offset is **no longer in `incompleteOffsets`**. That is a record succeeding twice, which is the
same arithmetic as the `1001 for 1000` sighting above and the thing direct pull most needs not to do.
Java `assert` is enabled under surefire, which is the only reason this was visible at all; **in
production, with assertions off, this completes silently and the offset is committed twice.**

**Why this sighting is worth more than the last one.** The 2026-08-22 pause/resume sighting could say
only that a count was wrong. This one names the class, the method and the invariant, and it is on the
test built specifically to overlap pulls with returns - so the interleaving is deliberate rather than
incidental.

**Reproduction: 0 out of 8 in isolation**, running the whole class, at a one-minute load of 12.8 on
twelve cores - so this is not the weaker claim that it only passes on an idle machine. Both sightings
are therefore suite-only, which points at interleaving rather than a deterministic path.

**The condition that produced both, and the next experiment.** The suite runs test methods
concurrently at `junit.jupiter.execution.parallel.config.dynamic.factor=20` - up to twenty times the
core count of methods in flight, each with its own PC instances and worker pools. Running this class
alone reproduces none of that no matter what the machine load is, because load is not the variable;
concurrent *interleaving inside the JVM* is. So the next experiment is to raise the pressure in that
dimension specifically - many concurrent copies of this one test in one JVM - rather than to run it
more times on a busier box, which is what 0/8 above has already ruled out.

**What has NOT been ruled out, and must be before this is called a test bug.** The test's returner
thread is single, and each `WorkContainer` reaches it once per `toReturn` insertion, so a double
completion means either (a) `getWorkIfAvailable` handed the same container to two pullers - which the
claim CAS is supposed to prevent and which `takenCount` would also have caught - or (b) an offset was
completed once through the returner and once through some other path. **(b) is the unexamined one**,
and it is the same gap the earlier entry names: the redelivery paths (retry, abandonment, the stale
sweep) were never covered.

> **This paragraph is the part that was wrong, and it is left standing because the mistake is worth
> keeping.** The answer is (a), and (a) was dismissed here on the grounds that "the claim CAS is
> supposed to prevent" it. The CAS prevents two *simultaneous* claims; it does not prevent a claim
> whose availability decision was taken before the record completed, because it re-validates only the
> one term completion resets. Ruling a branch out by what a guard is *supposed* to do, rather than by
> which terms it actually re-reads, sent the investigation to (b) - where there was nothing.
> `takenCount` would not have caught it either: the assert fires on the returner thread and aborts the
> run before that assertion is ever reached.

**The guard that now exists**, so a recurrence is a diagnosis rather than another sighting:
`DirectPullEngineParityTest.pausingStopsDeliveryAndResumingDeliversTheRestExactlyOnce` runs the same
pause/resume shape against the direct-pull engine and asserts the delivered count is **exactly** the
number produced, reporting which offsets were duplicated. It runs in the default suite, not only
under the system property.

### `OrderingModeDispatchParityTest` fails inside a full run and passes alone - and it is not a direct-pull failure

**`OrderingModeDispatchParityTest.keyAndUnorderedCostTheSameToDispatch`**, seen 2026-08-22 in **four
of four** full core unit runs on the same box, against its `MAX_RATIO` of 4.0:

```
UNORDERED 204ms / KEY  44ms  ratio 4.559   # default engine
UNORDERED 188ms / KEY  34ms  ratio 5.428   # -Dpc.directPull=true
UNORDERED 134ms / KEY  19ms  ratio 7.099   # default engine
UNORDERED 209ms / KEY  29ms  ratio 7.220   # -Dpc.directPull=true
```

**Two controls, and between them they name the condition.**

1. **The engine flag is not it.** The test never starts an engine - it drives
   `WorkManager.getWorkIfAvailable` directly - and it failed on both arms of two engine pairs. Anyone
   meeting this on a direct-pull run should not spend a minute on the engine.
2. **Box load is not it either, which is the useful part.** Run **alone, 4 out of 4 passes** at a
   one-minute load average of ~20 on twelve cores - higher than the load under which it failed. What
   distinguishes the failing runs is not how busy the machine is but that **surefire is running other
   forks of the same suite alongside it**. It is fork parallelism, not ambient load.

**The mechanism this points at, stated as a hypothesis.** The two arms are timed **sequentially**
inside one JVM and compared as a ratio, so the ratio only cancels machine speed if both arms see the
same machine. A sibling fork starting or finishing between them lands on one arm and not the other,
and the ratio inherits the whole difference. Consistent with the numbers: the KEY arm is 19-44ms
where the javadoc's clean run has the ratio at ~2.3, so it is the *short* arm being disturbed that
moves the ratio most.

**Do not widen `MAX_RATIO` on this evidence.** The margin between the clean 2.3 and the ~6.4 an
injected regression produced is the whole test, and 7.2 has already crossed it - a wider bound would
be a bound that cannot detect the regression it is named for. If it is to be fixed, fix the
measurement: interleave the arms, or take the ratio per-repeat and use the best ratio rather than the
ratio of the bests.

### The shutdown-commit family surfaced again, 2026-08-21 - and this time a sibling test

**`ParallelEoSStreamProcessorTest.inFlightMessagesCommittedIfProcessedDuringShutdown`** - failed once
in a full core unit run at load average ~58 on twelve cores, asserting
`[1 record completed during shutdown]`. **Passed 3/3 in isolation** immediately afterwards.

**This name is new to the ledger.** The one already here is its sibling,
`queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown`, which astubbs#260 closed by
establishing that the extra commit was correct product behaviour and the *assertion* was wrong.
**astubbs#260 fixed one assertion in that family; this is a second test making a similar assertion, and
nobody checked whether it had the same problem.**

**Two sightings, independently, on the same day**, which is what makes it worth an entry rather than a
shrug:

1. This one, in a run verifying the conservation-counter change - which touches `drain()`, so it had to
   be ruled in or out rather than dismissed.
2. `queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown` **itself**, failing once in a
   **default-engine** full run during the direct-pull measurement, and passing 3/3 in isolation. **A
   recurrence of the one this ledger records as fixed.**

**What that pair suggests, stated as a hypothesis rather than a finding:** astubbs#260 corrected one
assertion but not the class of assertion. Prior art points the same way -
[`unit-tests-parallelise-by-forking-not-threading-2026-07-29.md`](../solutions/test-flakiness/unit-tests-parallelise-by-forking-not-threading-2026-07-29.md)
records this family failing "intermittently only under thread parallelism", and both sightings here
were under heavy load.

**Why it is recorded rather than chased:** neither sighting is reproducible on demand, both were on
uncommitted branches, and **a seed recorded now outlives the logs**. The useful next step is not a
repro attempt but a read: **take astubbs#260's reasoning about why its assertion was wrong, and check
whether `inFlightMessagesCommittedIfProcessedDuringShutdown` makes the same mistake.** That is a
five-minute question and it may close both.

**Do not let this block the conservation merge without checking it first.** The concern that prompted
the entry was that the conservation change touches `drain()` - but the second sighting was on the
*default engine* with none of that work present, which is evidence the family is flaky independently.

### Two new names, 2026-08-22 - and a control arm that says they are not the change that found them

Both surfaced in full **core** unit runs while verifying the claim-state change (the one that closed
the double-delivery entry above), which is precisely the change you would suspect: it rewrites
selection.

- **`ParallelEoSStreamProcessorTest.processInKeyOrder`**, at `[sanity check input data]` - a point
  check that all nine records have been polled, taken one loop cycle after start. New to this ledger.
- **`CommitResponseTimeoutSymptomTest.aRebalanceStormUnderAHighFailureRateNeitherStallsNorKillsTheConsumer`**,
  timing out awaiting at least 4 commit rejections. New to this ledger, and the file already carries
  a solutions write-up about a *different* assertion in it.

Also seen in the same window, and both already named above:
`ParallelEoSStreamProcessorTest.executorThreadsInterruptedOnShutdownTimeout` and
`inFlightMessagesCommittedIfProcessedDuringShutdown`.

**The control arm, which is why this is a sighting record and not a bug.** The same core suite was
run six times on the branch **without** the change, at the same load, and produced a failure of the
same family (`executorThreadsInterruptedOnShutdownTimeout` +
`inFlightMessagesCommittedIfProcessedDuringShutdown`) in 1 of 6; six runs **with** the change passed
6 of 6. In isolation, `processInKeyOrder` passed 6/6 and the commit-storm test 8/8, both at
one-minute load 7-12 on twelve cores. So the population that fails is the same on both sides of the
change, and the machine's load is the moving part.

**What this costs to leave alone:** every one of these is a *point check* taken after an await on
something else - the shape [`vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md`](../solutions/test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md)
owns. `processInKeyOrder`'s own comments already record two sites in it converted from point checks
to awaits after measuring 1-in-10 failures; line 783 is a third that was not. That is the fix to
reach for if it recurs - not a quarantine, and not a deadline.

### Four more seen under concurrent agent load, 2026-08-15 - unclassified

Recorded because this ledger exists so a flake is not met twice as a surprise, not because any of
them is diagnosed. All four surfaced while several agents built the reactor at once (`-am` drags
core's full suite into every client build), **every one passed on retry**, and none is a client
defect:

- `ParallelEoSStreamProcessorTest.executorThreadsInterruptedOnShutdownTimeout`
- `CheckQuarantineOwnersScriptTest` - two different methods, on different runs
- `ProxyProcessorLivenessTest.aSlowWorkerKeepsItsRecordWhileHeartbeatsContinueAndLosesItWhenTheyStop`
- `JStreamParallelEoSStreamProcessorTest.testConsumeAndProduce` - added 2026-08-17, seen with ~60
  worktrees live on the box; passed in isolation and in the same session's full post-change run

**Do not quarantine any of them on this evidence.** Contention on a box running many JVMs is exactly
the condition rule 2 exists to rule out, and the first uncontended full run of this branch passed all
of them - see [`branch-clean-verification-2026-08-15.md`](branch-clean-verification-2026-08-15.md),
which also records the one test that did survive that filter. What they earn is a name here, so the
next sighting is a second sighting rather than a first.

The liveness one is worth a closer look than the others if it recurs: it is new code on this branch,
it asserts on a lease deadline, and a test that measures elapsed time under load is the shape this
repo has been bitten by twice already.

**One candidate explanation for this group has since been found and fixed - check for it before
diagnosing the next sighting.** A fixture race with exactly the "only under load" story was diagnosed
on the Kotlin client's CI row and fixed on this branch: the mock consumer's partitions were assigned
before their beginning offsets were recorded, and a poll landing in that window killed PC's
broker-poll thread, so the test failed on whatever deadline it was awaiting. Mechanism, control arm
and the fix are in
[`assign-the-mock-consumer-after-seeding-its-offsets-2026-08-15.md`](../solutions/test-flakiness/assign-the-mock-consumer-after-seeding-its-offsets-2026-08-15.md).
It is a *candidate*, not a retraction of any entry above: two of the three tests named here reach
their fixture through the helper that had the window
(`AbstractParallelEoSStreamProcessorTestBase` and `EngineFixture` both call
`subscribeWithRebalanceAndAssignment`), but the sightings' logs were not checked for it. **The check
is one grep of the failing job's log for `didn't have beginning offset specified`** - present means
it was this and is now fixed, absent means it was not and the entry stands.

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

#### 2026-08-22, `CoreBatchTest` under `-Dpc.directPull=true` - and this one has a cause, which settles the "two readings" above for the direct-pull engine at least

Same alias, same 30-second timeout, same helper, and **the same `Expected size: 3 but was: 4`**. This
sighting names what produced it: the batches were `[0,2] [4] [3] [1]`, so **all five records were
delivered exactly once, no batch exceeded the batch size of two, and four workers did the selecting
where the test assumes one.** `BatchTestMethods.simpleBatchTest` computes
`ceil(numRecsExpected / batchSize)`, which is only the number of batches when a single selector fills
each one - true of the shipped engine's control loop, never true of direct pull.

**So on the direct-pull engine this is the third instance of a known class**, not a new mystery:
`perf-direct-pull-measured.md` already records two tests asserting dispatch granularity that direct
pull does not provide. It is not a lost-work failure and not a batcher defect.

**It says nothing about the 2026-08-18 and 2026-08-19 sightings**, which were on the default engine
where one thread does select every batch - the "contention versus product" split above still stands
for those, and this does not close it.

**Rates, because the change that surfaced it had to be ruled in or out.** `ShardOccupancy` (unheld-offset
index for `UNORDERED` dispatch) makes concurrent selection genuinely concurrent, and it moved this from
never-seen to every-run. Full core unit suite, twelve cores, one-minute load 5-9: direct pull **before**
the change, 5 runs, one failure every time and always the deterministic
`SubmitWorkToPoolShutdownRaceTest` one; direct pull **after**, 3 runs, that failure plus one more every
time (`simpleBatchTest` twice, `inFlightMessagesCommittedIfProcessedDuringShutdown` once). **0 of 5
against 3 of 3 is attributable, and it is attributed**, which is the only reason this sighting is worth
more than a shrug.

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

**Owned: astubbs#262** stamps `armedAtNanos` immediately before `schedule()` and asserts against that
instead. Its own comment is honest about the residual: the window measured is now slightly *longer*
than the true one, so the error is sub-millisecond and in the safe direction - a genuinely early
return is still caught unless it is early by less than the arming cost.

**Note astubbs#265 touches the same line differently**, deleting the assertion along with the sleeps
it removes. Whichever of the two lands second will conflict here, and the conflict is a real
decision - measure it correctly, or stop measuring it - not a mechanical merge.

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

#### Sightings 3 and 4 (2026-08-14, astubbs#116) - released too early, and failing the other way

`astubbs#265` released this test from quarantine on a **causal** fix, explicitly not a run count, and
said so: *"if it flakes on master, re-quarantining is the inverse of that commit."* It has, so it is
re-quarantined here. The release was reasoned, not careless - but the fix closed one direction of a
two-directional defect.

The fix addressed the metric being **more current** than the expectation: the `Thread.sleep(1000)`
became `await().untilAsserted(...)` on the trailing meters, which is right if the metric merely lags
and then catches up. Both new sightings are the metric **behind and never catching up** - the await
burns its full 120s:

```
PCMetricsTest.metricsRegisterBinding:108   (PARTITION_LAST_COMMITTED_OFFSET, partition 1)
  attempt 1: expected: 1213.0  but was: 1209.0     (4 short)
  attempt 2: expected: 1207.0  but was: 1195.0    (12 short)
```

**Both on the same head, back to back** - the second is a rerun of the first, so this is no longer a
flapper you can rerun past. And the shortfall is not a fixed off-by-N: 4 then 12. No amount of
waiting closes a gap that varies, which is what distinguishes this from the lag the await was
written for.

Two things worth ruling in or out before anyone "fixes" it again:

1. **It is the shape the sibling doc already named.** `assert-the-commit-frontier-not-the-tick-path.md`
   (written 2026-08-13, one day earlier, for `processInKeyOrder`) lists the symptom *"the await burns
   its full 30s: once the tick has landed, the condition is permanently false"*, and its
   `applies_when` covers any assertion over a PC commit history in a core unit test. This test asserts
   `PARTITION_LAST_COMMITTED_OFFSET == counterP1 + p1StartingOffset` - an exact commit position, not a
   frontier.
2. **It rhymes with the undiagnosed entry next door.**
   `OffsetEncodingBackPressureTest.backPressureShouldPreventTooManyMessagesBeingQueuedForProcessing`
   is recorded as *"the committed high-water mark never reaches `expectedHighestSeen` (139), with a
   different actual each run (136 and 132 seen)"*. Same sentence shape as ours. Two tests whose
   committed offset stalls short of expectation by a varying amount may be one phenomenon, and that
   entry is the most frequent tracked flake in the repo. Worth testing as one hypothesis rather than
   two coincidences.

**Do not assume this is a test defect.** The open question is whether the un-committed tail is a wrong
assumption in the test or real commit behaviour under a paused/blocked partition; the file's own
standing rule is to classify before touching, and two of the load-tightness family turned out to be
product bugs.

The integration failure alongside it on the same run was unrelated infrastructure -
`ContainerLaunchException: Container startup failed for image confluentinc/cp-kafka:7.9.0` - and
passed on rerun. Worth separating: one rerun cleared the container flake and did **not** clear this.

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
currently explains why.

**This entry is why rule 1 changed.** Under the old wording - *no quarantine without diagnosis* - an
undiagnosed test stayed in the gating lane, so this one (4/45, the most frequent tracked flake)
blocked every unrelated PR, and the repository owner had to quarantine it as an explicit
rule-1 exception. It now qualifies on the rule itself: 4 failures in 45 runs, with the signature and
runs recorded above, is exactly the *sighting ledger* rule 1 asks for. No exception is needed, and
the entry is unchanged in every other respect - no Owner (unowned, flagged advisory by the audit),
`flapping = true`, and the diagnosis below still the open task. Quarantine defers; it does not
resolve. This entry stays open until the test is understood and fixed.

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
