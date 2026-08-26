<!-- post-merge: checked -->
# The Lincheck lane: what it does not yet cover, and what it left open

<!-- inflight-type: task -->
<!-- inflight-labels: concurrency -->
<!-- inflight-impact: test-debt -->

<!-- post-merge: checked-begin -->
The lane arrived with astubbs#347. What is below is what that PR deliberately did **not** close, plus
the coverage the lane is worth extending to now that it has been shown to work. Delete this note when
these items are resolved - not when any PR merges, which is why it is named for the lane rather than
for a PR number.
<!-- post-merge: checked-end -->

## Where to point it next

### The selection rule, which matters more than the list

**Lincheck's leverage is highest where two individually-atomic things have to be atomic together.**
That is the whole heuristic, and it is what makes expansion a search rather than a guess.

A `ConcurrentSkipListMap` is safe. An `AtomicLong` is safe. A counter that must *agree* with the map
is not safe, and **no choice of thread-safe collection fixes it** - which is exactly why this class of
defect survives code review: every field in the diff is a concurrent type, so the diff looks correct.
Lincheck attacks precisely this, because it is a statement about a *pair* of operations that no single
data structure can enforce.

So grep for the signature rather than reading classes hoping to spot something:

- a concurrent collection **plus a derived counter or size field** (the standout below);
- **two maps keyed identically** that must agree (item 4);
- a lock that guards **one half** of an invariant, or one entry point out of several (item 3);
- a "safe" collection whose *iteration or check-then-act* spans two calls.

**The counterpart rule - where not to point it.** Lincheck reasons about interleavings of operations,
not about the memory model, so a plain non-volatile field's *visibility* is outside it however many
harnesses are added. `offsetHighestSucceeded` and `offsetHighestSeen` are that case. jcstress owns
that half; the `jcstress-poc` probe module is where it is measured.

### What it costs, and the prerequisite nobody can skip

Not free coverage, and it should not be sold as such:

- **Every harness is hand-written, and neither tool discovers *what* to test.** The lane's
  demonstrated value is that it finds seams nobody named *within a class it was pointed at*. Pointing
  it is still a human judgement, which is what the heuristic above is for.
- **The prerequisite is the bound-pricing problem, not any of the targets below.** A stress arm's hit
  rate is machine-dependent by 3.4x here (p = 0.011, section below), so **each new harness needs its
  bound priced on the machine that will gate it, not on the machine that wrote it.** Four more
  harnesses priced the way these were would be four more latent flakes. Settling that is the honest
  answer to "what do we build first" - it is cheap (see the section below) and it unblocks everything
  in this list.

### The ranked targets

Ranked by what each buys, not by effort. Prefer widening what an existing harness explores over
adding a class with a narrow guess in it.

<!-- post-merge: checked-begin -->
1. **The work claim in `ProcessingShard#getWorkIfAvailable` - a check-then-act on two fields that
   had to move together.** Selection asked three terms via `isAvailableToTakeAsWork()` and acted via
   `onQueueingForExecution()`, re-validating none of them. astubbs#335 replaced the plain
   `boolean inFlight` and `Optional<Boolean> maybeUserFunctionSucceeded` with a single atomic
   holding one `ExecutionState` and the attempt it belongs to, so the check IS the act - a six-state
   CAS machine, which is the shape bounded model checking is best at.

   **Why this one first:** the verification that landed with astubbs#335 says the losing interleaving
   had to be "played out by hand with no threads - the concurrent reproduction needed millions of
   completions per occurrence" (4 in 14,400,000), and the same is true of the ABA that review found
   on top of it. That is the exact gap between *one bad schedule exists* and *every schedule is
   safe*, and only a model checker closes it. It is also **time-sensitive**: the gap is unreachable
   on the shipped engine only because the control loop is the sole selector, and astubbs#361 gives
   every worker its own. Calibrate against the merge base of astubbs#335, where the defect still
   lives.
<!-- post-merge: checked-end -->

   Same trigger, also worth a harness, and deliberately unfixed:
   `bug-370-a-record-is-selectable-before-its-offset-is-registered.md` - registration order in
   `PartitionState#maybeRegisterNewPollBatchAsWork` makes a record selectable before its offset is
   registered, latent for the same single-selector reason.

   **`ProcessingShard.availableWorkContainerCnt` is NOT a Lincheck target, and this list said it was.**
   The `AtomicLong`-must-track-`ConcurrentSkipListMap` signature fits, and the clamp in
   `dcrAvailableWorkContainerCntByDelta` is commented `// in case of possible race condition`, so the
   drift reads as a race. astubbs#336 measured it and it is not one: both drift paths are conditional
   mismatches **reproducible on a single thread**, the clamp's comment is wrong, and that PR deletes
   the clamp. Lincheck finds interleavings; this needs none, and astubbs#336 already has the
   single-threaded tests. Recorded rather than quietly dropped, because the wrong inference here was
   made twice - reading a comment claiming a race instead of measuring - which is the same error this
   lane's own calibration caught in itself.
2. **`PCMetrics.registeredMeters` - a plain `ArrayList` written from two threads.** Not a concurrent
   collection at all (`private List<Meter.Id> registeredMeters = new ArrayList<>()`, added to from
   four registration paths). **The lane found this unprompted, during calibration, from a scenario
   aimed at something else** - which is the strongest single piece of evidence that the technique
   generalises beyond the seams it was pointed at, and the best answer available to "is this worth
   expanding". astubbs#57 fixes it, so a harness here becomes a regression detector the moment that
   lands. See `bug-pcmetrics-registered-meters-is-a-plain-arraylist.md`.
3. **`ProducerManager`'s produce/commit lock pair - a known defect in a *named* protocol.** The pair
   is project vocabulary (`CONCEPTS.md`), which means the invariant is already written down in
   prose - the ideal case for a sequential specification. `producerTransactionLock` is a
   `ReentrantReadWriteLock`, but `syncBeginTransaction` is guarded by a *separate* `synchronized`
   method, so two different mechanisms protect one protocol. `bug-producing-lock-double-release.md`
   records the defect. A known bug in a named protocol is the best target available after item 1.
4. **`PartitionStateManager` - two maps, one invariant.** `partitionStates` and
   `partitionsAssignmentEpochs` are both `ConcurrentHashMap`s keyed by the same `TopicPartition` and
   must agree; each is individually safe and the pair is not. The identically-keyed-maps signature,
   verbatim.
5. **The unsynchronised counter maps.** `PartitionStateManager.slowWorkCounters`,
   `WorkManager.succeededRecordsCounters` and `failedRecordsCounters` are plain `HashMap`s mutated
   from the rebalance callbacks and the completion path, and `RemovedPartitionState.READ_ONLY_EMPTY_SET`
   is a mutable `TreeSet` shared by every PC instance in the JVM
   (`bug-shared-collections-across-the-poll-boundary.md`). Cheap, and it becomes the regression
   detector the moment the sweep on `fix/concurrent-collection-sweep` lands.
6. **Close the encoder's range-top leg.** The one verdict in the calibration that is not clean:
   `OffsetMapCodecManager.encodeOffsetsCompressed` came back HALF-FOUND, because the *snapshot* leg
   was exhibited and the two-reads-return-different-values leg was not, and is not expressible in
   the harness as committed. `PartitionStateLincheckTest`'s javadoc already states what widening its
   generator would need. Turning one half-verdict into a whole one is worth more than a new class.
7. **`RetryQueue` is not modelled at all**, and carries ordering and scheduling state across the same
   two threads the existing harnesses already prove race.
8. **Model-checking arms over the product classes, which cost nothing once they are possible.**
   `ShardManagerLincheckTest` and `WorkManagerLincheckTest` gain one for free - same operations, one
   different `Options` - the day `LincheckSuperHashCodeProbeTest` starts failing. That tripwire is
   already in the lane precisely so nobody has to remember to check.
9. **`AbstractParallelEoSStreamProcessor` - highest value, highest cost, and last for that reason
   only.** It holds the most concurrency-primitive fields of any class in core (7 by a `private`-field
   count, more once locals and inherited state are included) and owns shutdown and the state
   transitions. Lincheck wants a small `@State` class and this is the opposite of small, so it is
   ranked last on tractability, **not** on value - if the pricing prerequisite above is solved and
   appetite exists, this is where the undiscovered defects most plausibly are.

**Not this, and the plan doc says why**: `@Validate` invariants would catch the retry-queue leak in
§1.1, and they are still the wrong next step, because an invariant naming the retry queue is a hint
and the value demonstrated here is what the tool finds *unaided*.


<!-- post-merge: checked-begin -->
## The inversion contract assumed one bug per harness, and the harnesses have disproved it

astubbs#347 committed the lane with every harness asserting that Lincheck FINDS a race, and named
the fixes that would invert them: astubbs#337 and astubbs#344 over `PartitionStateLincheckTest`,
astubbs#345 over `ShardManagerLincheckTest`, astubbs#346 over `WorkManagerLincheckTest`. The
contract itself is still right about what to do - flip the affected harness to assert-no-failure,
never revert a fix, never loosen a bound. What it did not anticipate is that a harness pointed at
one seam also explores the others, so **a harness can stop finding its own bug without becoming
quiet**, and the two that have been run against a fixed tree failed to invert for two different
reasons.

**`ShardManagerLincheckTest`: the flip is red, and what it exposed is not the fixed bug.** With
astubbs#345's torn `containsKey`/`get` pair gone, Lincheck still reports
`= Invalid execution results =`, with no `NullPointerException` anywhere in it. Controlled both ways
in one sitting - reverting only the main-code fix restores the NPE counterexample and a green
harness, restoring the fix produces the new report - so the second violation was always present,
masked because Lincheck stops at the first thing it finds. The counterexample is `revokeSweep(0)` in
the sequential prefix, then `addWork(0)` against `addWork(0)` in parallel, landing on the
`entries.get(key)` / `entries.put(key, wc)` + `availableWorkContainerCnt.incrementAndGet()`
check-then-act in `ProcessingShard#addWorkContainer` - this lane's own "concurrent collection plus a
derived counter" signature, verbatim.

Two reasons not to call that a product defect yet, and they pull in opposite directions. Production
registers work from the **control** thread alone, so two concurrent `addWork` calls are not an
interleaving the library can take today - the harness declares an operation set wider than the real
thread model. The broker poller does not register work itself: `BrokerPollSystem` calls
`pc.registerWork`, which only enqueues onto `workMailBox`, and the control thread reaches
`wm.registerWork` after `workMailBox.drainTo` inside `processWorkCompleteMailBox`. (An earlier
revision of this paragraph named the broker-poll thread here. The conclusion is unchanged - one
thread registers, so the interleaving is unreachable - but the thread was wrong, and which one it is
decides what a future harness must model.) Against that, the same stops being
obviously true once every worker selects its own work, which is the change item 1 above calls
time-sensitive. Settle the thread-model question before writing a harness for it; if the operation
set is the artefact, the fix is constraining this harness with a non-parallel group over `addWork`,
not a change to `ProcessingShard`.

**That narrows rather than overturns item 1's ruling that `availableWorkContainerCnt` is not a
Lincheck target.** The ruling rests on astubbs#336 measuring the *drift* as single-threaded
conditional mismatches, and it stands - nothing here re-opens the clamp question. What is new is a
machine-produced interleaving over the same field, which is a different claim from "the drift is a
race", and it arrived unaided from a harness pointed somewhere else.

So that harness asserts the strongest thing the evidence supports - that no report over these
operations mentions `NullPointerException` again - a real regression detector for astubbs#345's fix
rather than the vacuous green an unexamined inversion would have produced.

**`WorkManagerLincheckTest`: it passes, and cannot tell you why.** The checkpoint-3 signature it was
calibrated on - `PartitionState.onSuccess`'s `assert removedFromIncompletes`, thrown out of
`completeWork` - is gone, which is independent confirmation that astubbs#346's fix works. Lincheck
still reports a violation, and it is astubbs#345's `NullPointerException` from
`ShardManager.removeWorkFromShardFor`, reached through the same `revokeAndReassign` operation. The
harness's own assertion cannot discriminate: `assertThat(report).contains("completeWork")` matches
the interleaving table, which names both operations whichever one threw. Tightening it onto the
checkpoint-3 signature would not be a durable pin either, because Lincheck stops at the first
violation and which of the two it reaches is not ordered.

**One prescription written here has already been falsified, which is the argument for the rule
below.** The reading from `WorkManagerLincheckTest` alone was that astubbs#345's fix would let both
harnesses invert together. Measured on a tree carrying that fix, `ShardManagerLincheckTest` does not
invert at all - the paragraphs above are that measurement. A harness's next assertion is not
derivable from its own diff, or from another harness's behaviour.

**So: re-run the lane, never reason about it.** Each of these fixes changes what the OTHER harnesses
find. `LINCHECK_TEST=<class> bin/lincheck-test.sh` is the check and it is cheap. The two harnesses
above carry assertions matched to what was measured on a tree holding astubbs#345's and
astubbs#346's fixes together; `PartitionStateLincheckTest` has never been run against a tree
carrying astubbs#337's or astubbs#344's fix, so its inversion is unexamined and its javadoc still
promises one. Whoever lands any of these four re-runs the whole lane and re-checks every harness's
assertion, not only the one their own PR is named for.
<!-- post-merge: checked-end -->

## A stress arm's hit rate is machine-dependent, so one machine cannot calibrate it

**The transferable rule from this is now written down and owned elsewhere**:
`docs/solutions/best-practices/a-stress-probes-calibration-is-a-claim-about-one-machine.md` states
the method and the machine-dependence finding for probabilistic probes generally. What is below is
only the part still OPEN on this lane - the decision nobody has taken yet.

`WorkManagerLincheckTest` was raised from `iterations(200)` to `iterations(1_000)` on measurement,
and the measurement turned up something worth more than the bound: **the two machines it was run on
differ by 3.4x in how quickly they find the tear** - 2.33% per iteration against 0.69% - and a
likelihood-ratio test rejects their being equal (LR 6.42 on 1 df, p = 0.011). That is a real
difference, not sampling noise, so **no single-machine calibration of a stress arm transfers**, and
every bound in this lane is currently justified by runs on one machine only.

What this leaves genuinely unsettled: on the slower machine's own estimate 1,000 iterations misses
about 1 run in 1,000, which is fine - but only **8 runs exist from that machine**, so the pessimistic
end of its interval is about 1 in 14. Reaching 0.1% at that end would need roughly **2,700**
iterations, at a measured 0.142s per iteration on the exhaust path, i.e. a ~6.4 minute designed-red
against ~2.4 minutes at 1,000.

The bound was **not** raised to 2,700, deliberately. That number defends against the tail of an
8-sample estimate from a machine that was not available to re-measure, and inflating a bound to cover
an interval nobody has narrowed is the same unfounded precision this note exists to catch. What
settles it is cheap and specific:

- Run `LINCHECK_TEST=WorkManagerLincheckTest bin/lincheck-test.sh` about 24 times on the slower
  machine with the harness temporarily starved to `iterations(25)`, and read off the miss fraction.
  That is ~10 minutes and it collapses the interval; the arithmetic is in the correction to
  [`docs/plans/2026-08-25-001-test-lincheck-poc-plan.md`](../plans/2026-08-25-001-test-lincheck-poc-plan.md)
  section 3.1.
- Starving the harness is **measurement scaffolding and must never be committed** - it is a 40x
  coverage reduction on a harness whose bounds are its coverage.
- This becomes load-bearing the moment the item below is fixed and something actually runs the lane
  on hardware nobody calibrated against.

The other two harnesses inherit the same caveat: `ShardManagerLincheckTest` and
`PartitionStateLincheckTest` hit 8 of 8 at a tenth of their committed bounds, but on the fast machine
only.

<!-- post-merge: checked-begin -->

### Measured when astubbs#345 landed: the bound that held is a coin flip

The lane re-run astubbs#345 owed - the obligation the "re-run the lane, never reason about it" rule
above puts on whoever lands one of these four - produced **2 misses in 4 runs** of
`WorkManagerLincheckTest.stressRediscoversTheCheckpointThreeTear`, on a third machine, at the
committed `iterations(1_000)`. The runs are bimodal rather than merely slow: the two hits landed the
violation in 9.1s and 13.4s, the two misses exhausted the full thousand iterations at 121.7s and
123.9s.

**That is not the machine-dependence above, and it is not sampling noise around 1-in-14.** The
mechanism is astubbs#345's fix. Both prior hit rates - 2.33% and 0.69% per iteration - were measured
on trees where the checkpoint-3 tear *and* astubbs#345's `NullPointerException` were reachable
through the same `revokeAndReassign` operation, and Lincheck stops at the first violation it
reaches. So those numbers were the rate of finding **either**, never the rate of finding
checkpoint-3. Removing the NPE removed the cheaper of the two, and what is left is the harness's
true hit rate on its named target.

So `iterations(1_000)` was never calibrated against what this harness now has to find. **Do not
raise it from this measurement either** - four runs on one more machine is the same unfounded
precision the section above refuses, and the starve-and-count procedure it prescribes is still what
settles it.

**The prediction this creates, which must be re-run rather than reasoned about:** with astubbs#346's
fix landed as well, both violations this harness can reach are gone, and it should fail every run -
at which point it needs inverting, not re-bounding. Nothing here establishes that; no tree carrying
both fixes has been run since astubbs#345's fix was measured this way.

<!-- post-merge: checked-end -->

## Nothing runs the lane, so the tripwire it promises cannot fire

`bin/lincheck-test.sh` is excluded from every gating suite by design, and no workflow invokes it. The
ASM instrumentation tripwire - the control that exists because a broken transformer once reported a
clean pass against code that cannot survive two threads - therefore never runs. Three reviewers
converged on this independently.

## The red control has drifted from a standard that landed after it

`LincheckToolchainProbeTest` was calibrated before `18a61321b`, which now requires every red control
to carry a green near-miss arm. It has none. It also omits the `.actorsBefore(0)` / `.actorsAfter(0)`
that all four other harnesses set - Lincheck defaults to 5/5 (verified via `javap`), so the init
prefix can destroy the probe's own fixture.

Neither would make the probe pass, and both change a control the PR calls settled, so they are the
author's call rather than a review fix.

## Smaller, still open

- `containsAtLeastElementsIn` vs `containsExactlyElementsIn` in the exclusion contract test - a
  policy decision about whether a wrapper may over-exclude, raised by two reviewers.
- The fifth exclusion point (the pitest glob) is pinned by nothing.
- The MPL-2.0 test-scope invariant is unenforced, and the ASM pin has no retirement trigger.
- The ASM silent-instrumentation incident deserves a `docs/solutions/` entry: a detector reporting
  success while its transformer failed per-class would have made every calibration verdict read
  "not found".

## Disproven, recorded so it is not re-raised

The claim that core's `<argLine>@{argLine} ${lincheck.jvm.args}</argLine>` feeds a literal
`@{argLine}` to pitest's minion JVMs and silently breaks the mutation lane is **false**. A scoped
`mutationCoverage` run scored 35 mutants across 496 tests with zero minion errors; pitest's own
`SurefireConfigConverter` logs `Replacing properties in argLine` and resolves it. No
`-DparseSurefireArgLine=false` is warranted.

## Cross-branch obligation this note now owns

`test-lincheck-jcstress-evaluation.md` scopes a two-tool evaluation - a Lincheck arm and a jcstress
arm - and it lives on astubbs#344's branch, not on master, so it could not be updated from here. Its
**Lincheck arm is executed**: the calibration ran against a pre-fix tree and refound four real races
unaided, with the verdicts and cost tables in
[`docs/plans/2026-08-25-001-test-lincheck-poc-plan.md`](../plans/2026-08-25-001-test-lincheck-poc-plan.md).
Whoever lands astubbs#344 records that against the evaluation note and leaves the jcstress arm open;
the `jcstress-poc` probe module carries it.

This paragraph exists because the handoff note that used to carry the obligation was deleted at merge
prep, as `docs/inflight/AGENTS.md` requires - a "delete this when it merges" marker must never reach
master. Everything else that note held is already stated where it is looked up: the inversion
contract and the red control in [`docs/testing.md`](../testing.md); the five **gating-exclusion
points** in the plan doc's "Adding a lane touches five places" section, enforced by
`QuarantinedAnnotationContractTest` rather than by prose; the five **invocation flags**, which are a
different list, in `bin/lincheck-test.sh`'s own header; and the Jabel and model-checker findings in
the plan doc.
