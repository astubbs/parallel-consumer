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

   The sibling that used to sit here - registration order in
   `PartitionState#maybeRegisterNewPollBatchAsWork` making a record selectable before its offset was
   registered, latent for the same single-selector reason - is no longer a harness target: astubbs#370
   swapped the order so the offset is registered before the container is published, and
   `PartitionStateRegistrationOrder370Test` plays the losing interleaving by hand. **On the
   registration path** no schedule can now reach a container whose offset is absent, so there is
   nothing left for a model checker to explore there. The claim is deliberately scoped to that path:
   an absent offset under a live container is still reachable *after* completion, because
   `WorkManager#onSuccessResult` removes the offset before the container leaves its shard, so a
   scanner in that window sees exactly that pair. What refuses it there is the container's own
   `SUCCEEDED` claim state (astubbs#335), not the offset - which is candidate 1 above, not this one.

   <!-- post-merge: checked-begin -->
   **`ProcessingShard.workAwaitingSelectionCount` (once `availableWorkContainerCnt`) is NOT a Lincheck
   target, and this list said it was.** The `AtomicLong`-must-track-`ConcurrentSkipListMap` signature
   fits, and the clamp that used to sit on it carried the comment `// in case of possible race
   condition`, so the drift read as a race. It was measured and it was not one: the drift paths were
   conditional mismatches **reproducible on a single thread**, the comment was wrong, and the clamp is
   gone. Lincheck finds interleavings; that needed none, and the tests that hold the fix in place are
   single-threaded. Recorded rather than quietly dropped, because the wrong inference here was made
   twice - reading a comment claiming a race instead of measuring - which is the same error this
   lane's own calibration caught in itself.

   The counter is now owned by a compare-and-set on each container's selection claim rather than
   inferred from its observable state, so the remaining question is not "does it drift" but the
   two-atomics transient the claim protocol accepts deliberately - the claim and the count move
   separately, and `ProcessingShard.workAwaitingSelectionCount`'s own javadoc names the one reader
   that can see it. That IS interleaving-shaped, and is the version of this candidate worth a harness.
   <!-- post-merge: checked-end -->
2. **`PCMetrics.registeredMeters` - a plain `ArrayList` written from two threads.** Not a concurrent
   collection at all (`private List<Meter.Id> registeredMeters = new ArrayList<>()`, added to from
   four registration paths). **The lane found this unprompted, during calibration, from a scenario
   aimed at something else** - which is the strongest single piece of evidence that the technique
   generalises beyond the seams it was pointed at, and the best answer available to "is this worth
   expanding".
   <!-- post-merge: checked-begin -->
   astubbs#57 fixed it - `registeredMeters` is a `LinkedHashSet` and every mutation of it is behind
   a `metersLock` monitor - so a harness here is a regression detector against that fix rather than
   a hunt for an open defect. The reproduction it produced is carried in `PCMetrics859Test`'s class
   javadoc; the sibling defect the same harness found, the plain `HashMap` counter maps in
   `WorkManager` and `PartitionStateManager`, is still open in
   [`bug-metrics-counter-maps-are-plain-hashmaps.md`](bug-metrics-counter-maps-are-plain-hashmaps.md).
   <!-- post-merge: checked-end -->
3. **`ProducerManager`'s produce/commit lock pair - a known defect in a *named* protocol.** The pair
   is project vocabulary (`CONCEPTS.md`), which means the invariant is already written down in
   prose - the ideal case for a sequential specification. `producerTransactionLock` is a
   `ReentrantReadWriteLock`, but `syncBeginTransaction` is guarded by a *separate* `synchronized`
   method, so two different mechanisms protect one protocol. **The known defect here has since been
   <!-- post-merge: checked -->
   fixed** - the double release, closed by astubbs#257 - so this is no longer a target with a live bug
   waiting to be caught, which was most of its appeal. The two-mechanisms-for-one-protocol shape
   stands on its own and is still worth specifying; rank it accordingly.
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
reasons. **`ShardManagerLincheckTest` has since inverted after all** - see the correction directly
below - so the contract's record is one harness inverted late by a commit it never named, which is a
different failure from the one this section was written about.

**`ShardManagerLincheckTest`: the flip WAS red, and astubbs#336 has since made it green (2026-09-01).**
The arm is now flipped and asserts no violation - what follows is the history that produced it, kept
because the second violation and the artefact question below both outlived the flip.

astubbs#336 removed the counterexample by admitting to the population before the put and reading the
outcome from the map rather than from the earlier read. Bisected, not assumed: the unchanged harness
fires at astubbs#345, at confluentinc#905's hot-shard metric and at astubbs#373's claim
compare-and-set, and misses at astubbs#336 - the sole commit touching core's main sources in that
interval. Replicated with a **fresh worktree per commit**, which matters: the first pass reused one
working copy, and a second worktree built at a different commit first reported 0 hits out of 10 at a
commit that in fact fires 5 out of 5. A shared `target/` hands you a clean, wrong bisect and nothing
in the output says so. **astubbs#336's own commit message claims the lane was green and "still
finding the violation it is calibrated to find"**; run on that tree it is RED on this arm alone, and
the message's own "adapted cherry-pick of `fa4d1cf251`" is the likely mechanism. Recorded, not
explained away. **Three hand-written controls said otherwise and all three were wrong**, because each
reverted one half of astubbs#336 onto today's tree and the defect was in neither half alone; the
method that settles this class of question is in
[`../solutions/best-practices/reverting-half-a-fix-is-not-a-control-2026-09-01.md`](../solutions/best-practices/reverting-half-a-fix-is-not-a-control-2026-09-01.md).
The flip is green at the bound the counterexample was found at - 0 in 250,000 invocations, and 0 in
2,500,000 at ten times it, on a 32-core box with Temurin 17.0.20+8 and Lincheck 3.7.

The history, as it stood before that: with
astubbs#345's torn `containsKey`/`get` pair gone, Lincheck still reports
`= Invalid execution results =`, with no `NullPointerException` anywhere in it. Controlled both ways
in one sitting - reverting only the main-code fix restores the NPE counterexample and a green
harness, restoring the fix produces the new report - so the second violation was always present,
masked because Lincheck stops at the first thing it finds. The counterexample is `revokeSweep(0)` in
the sequential prefix, then `addWork(0)` against `addWork(0)` in parallel, landing on the
`workMap.get(key)` / `workMap.put(key, incomingWorkContainer)` + `availableWorkContainerCount.incrementAndGet()` (was `entries`/`availableWorkContainerCnt`)
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

**That narrows rather than overturns item 1's ruling that `availableWorkContainerCount` (then `availableWorkContainerCnt`) is not a
Lincheck target.** The ruling rests on astubbs#336 measuring the *drift* as single-threaded
conditional mismatches, and it stands - nothing here re-opens the clamp question. What is new is a
machine-produced interleaving over the same field, which is a different claim from "the drift is a
race", and it arrived unaided from a harness pointed somewhere else.

So that harness asserts the strongest thing the evidence supports - that no report over these
operations mentions `NullPointerException` again - a real regression detector for astubbs#345's fix
rather than the vacuous green an unexamined inversion would have produced.

**`WorkManagerLincheckTest`: inverted, and the inversion is the one that went as the contract said it
would.** The paragraph that used to sit here described a tree carrying astubbs#346's fix but not
astubbs#345's, where the harness passed because Lincheck reached astubbs#345's
`NullPointerException` out of `ShardManager.removeWorkFromShardFor` through the same
`revokeAndReassign` operation, and `assertThat(report).contains("completeWork")` could not tell that
from the checkpoint-3 tear. With both fixes in the same tree neither violation is reachable, the
harness fails every run, and the only move left is the one astubbs#347 named. It is now
`stressMustNotRediscoverTheCheckpointThreeTear`, asserting Lincheck's own linearizability check over
the two operations, in the shape `RetryQueueLincheckTest` already uses.

**What that inversion is worth, measured rather than assumed - and it is worth less than a green
suggests.** The re-run below is the evidence for the flip, but on its own it is *weak* evidence of
absence, because the control that re-introduces the defect misses most runs too. The proof the tear
is gone is `WorkManagerStaleCheckDoubleLookupTest`, which forces the interleaving deterministically;
this arm is a search. Whoever reads a pass here as a proof has mis-read it, which is why the
harness's own javadoc now says so.

**One prescription written here has already been falsified, which is the argument for the rule
below.** The reading from `WorkManagerLincheckTest` alone was that astubbs#345's fix would let both
harnesses invert together. Measured on a tree carrying that fix, `ShardManagerLincheckTest` does not
invert at all - the paragraphs above are that measurement. A harness's next assertion is not
derivable from its own diff, or from another harness's behaviour.

**So: re-run the lane, never reason about it.** Each of these fixes changes what the OTHER harnesses
find. `LINCHECK_TEST=<class> bin/lincheck-test.sh` is the check; it is no longer *cheap*, because
the inverted `WorkManagerLincheckTest` can never stop early and now pays its whole bound on every
run - the lane went from well under a minute to about two and a half. `PartitionStateLincheckTest`
has now been run against a tree carrying both astubbs#337's and astubbs#344's fixes (the section
below), and it does **not** invert - so every trigger the contract named has now fired, and no
harness is waiting on a fix. What remains is the open thread-model question two harnesses now share.
Re-run the whole lane and re-read every harness's assertion whenever anything in `state` moves, not
only when a named fix lands.
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
`WorkManagerLincheckTest.stressRediscoversTheCheckpointThreeTear` (renamed to
`stressMustNotRediscoverTheCheckpointThreeTear` by the inversion in the section below), on a third
machine, at the committed `iterations(1_000)`. The runs are bimodal rather than merely slow: the two hits landed the
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

<!-- post-merge: checked-begin -->

### Measured when astubbs#346 landed: the prediction held, and the control says why that is thin evidence

The re-run astubbs#346 owed, on a tree carrying both fixes. The prediction above is **confirmed**:
every valid run of `WorkManagerLincheckTest` exhausted the whole committed bound without a
violation, none of them anywhere near the 9-13s a hit used to take, so the designed-red arm could
not stay. It is inverted, and the class javadoc carries the numbers.

**The part that is not confirmed is the inference everyone will draw from it.** The same sitting ran
a control - the compiling mutant that re-resolves the partition state at `handleFutureResult`'s
action sites, i.e. the checkpoint-3 tear put back and nothing else - and that control **misses most
of its runs too**, hitting once in six at the same bound. So the misses on the fixed tree are not,
by themselves, a measurement that the tear is gone: a control that finds a *present* defect one run
in six cannot make a handful of clean runs mean much. What makes the flip safe is that
`WorkManagerStaleCheckDoubleLookupTest` forces the interleaving deterministically and pins the fix;
the Lincheck arm is a search running beside it.

**Two things this settles that reasoning had got wrong.**

- The one hit the control did produce is *exactly* the checkpoint-3 signature -
  `AssertionError` at `PartitionState.onSuccess`, through `PartitionStateManager.onSuccess` and
  `WorkManager.onSuccessResult` out of `handleFutureResult`, with `completeWork` racing
  `revokeAndReassign` - and **no `NullPointerException` anywhere**, because astubbs#345 has landed.
  So the harness still reaches the seam it was built for; it has not gone quiet, it is simply a poor
  detector at this bound.
- The section above put this arm's hit rate at a coin flip on 4 runs. Six more runs of a control
  carrying the same defect put it lower. Neither sample is big enough to name a number, which is the
  same conclusion as before: the starve-and-count procedure is still what settles it, and nothing
  here licenses re-bounding in either direction.

**The other harnesses, re-checked in the same sitting as the rule above requires.**

- `ShardManagerLincheckTest` is **unchanged by astubbs#335 landing**. Same counterexample as the
  section above records - `revokeSweep(0)` sequentially, then `addWork(0)` against `addWork(0)` -
  so nothing in item 1's ranking moves on that evidence. **(2026-09-01: astubbs#336, two commits
  later, did remove it, and the arm is now flipped to assert-no-violation.)**
- `PartitionStateLincheckTest` **has now been run against a tree carrying BOTH astubbs#337's and
  astubbs#344's fixes - the two the inversion contract named - and it does NOT invert.** It reports
  a violation on every run and its `assertThat(report).contains("commit()")` still passes, but on
  reports about neither tear: the interleaving table names `commit()` whatever threw. The reports
  are not stable between runs, and two shapes recur.

  The first is an `ArrayIndexOutOfBoundsException` out of `ArrayList.add` - **item 2's
  plain-`ArrayList` defect, now with a frame naming it rather than the frameless sighting an earlier
  revision of this bullet recorded**, so the lane has re-found it a second time from a harness
  pointed elsewhere. The second is `PartitionState.onSuccess`'s `assert` reached from two `succeed`
  operations in parallel - which production cannot perform, since only the control thread completes
  work.

  **That second shape is the same artefact-or-defect question this note already parks over
  `ShardManagerLincheckTest`'s `addWork`, arriving in a second harness**: an operation set declared
  wider than the real thread model, found by a harness aimed at something else. If it is the
  artefact, the fix is a non-parallel group over `succeed`, not a change to `PartitionState`. That
  decision is open, it is what this arm's next assertion depends on, and **it should be taken for
  both harnesses at once** rather than one at a time - which is the argument for settling the thread
  model before writing any more harnesses.

  **Take it AFTER astubbs#57 lands, and the ordering is not arbitrary.** Two shapes recur in this
  arm's reports, and astubbs#57 removes one of them: the `ArrayIndexOutOfBoundsException` out of
  `ArrayList.add` is the plain-`ArrayList` defect that PR fixes. Deciding while both shapes are live
  means reasoning against a moving target - once astubbs#57 is in, only the parallel-`succeed` shape
  remains, which is the question actually being asked. Nothing is gated on this: astubbs#346 does not
  re-point the assertion, so it needs no dependency on astubbs#57; the ordering binds whoever takes
  the decision, not the PRs.

  Its javadoc has been corrected to say all of this, because until now it told readers the arm would
  invert when astubbs#344 landed. **The contract's per-PR trigger list is now fully spent, and it
  went 1 for 4 on the fixes it NAMED** - which is worth stating precisely, because the one that held
  did so for a reason the contract did not name, and because `ShardManagerLincheckTest` did
  eventually invert, on astubbs#336, which the contract never mentioned. astubbs#345 and astubbs#337/#344 left their harnesses finding
  something else entirely. `WorkManagerLincheckTest` did invert when astubbs#346 landed, but only
  because astubbs#345 had removed the OTHER violation reachable through the same operation first -
  had the two landed in the other order, that prediction would have failed too. What the
  contract got right is the instruction; what it got wrong is the assumption of one bug per
  harness.
- `RetryQueueLincheckTest`, `LincheckToolchainProbeTest` and `LincheckSuperHashCodeProbeTest` are
  green and unchanged - the model checker is still blocked on the Lombok `callSuper` defect, so
  item 8's free arms are still not free.
- **The lane is green, and one of those greens is vacuous.** `PartitionStateLincheckTest` passes an
  assertion that no longer pins what it names. Nothing goes red to say so, which is exactly the
  failure mode this note exists for, and it is the reason the re-run rule is written as *re-run and
  re-read every harness's assertion*, not merely *re-run*.

**What the inversion costs, which is now the lane's largest single number.** An inverted arm cannot
stop at the first violation, so it pays its whole bound on every run. The whole lane used to finish
well under a minute; it is now about two and a half, essentially all of it this one arm. That is a
real trade against item 8 and against ever gating the lane, and it is the price of keeping the bound
where it was measured rather than re-pricing it on thin evidence.

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

## Cross-branch obligation this note used to own - discharged

<!-- post-merge: checked-begin -->
The two-tool evaluation that scoped this lane and the jcstress probe was never updatable from here,
because it lived only on astubbs#344. That PR settled it: both arms executed and were adopted - the
Lincheck calibration ran against a pre-fix tree and refound four real races unaided, with the verdicts
and cost tables in
[`docs/plans/2026-08-25-001-test-lincheck-poc-plan.md`](../plans/2026-08-25-001-test-lincheck-poc-plan.md)
- so the evaluation note was deleted rather than kept as a record of finished work. This note and
[`test-jcstress-probe-module-open-items.md`](test-jcstress-probe-module-open-items.md) are its
successors, and each names its own deletion condition.
<!-- post-merge: checked-end -->

This paragraph exists because the handoff note that used to carry the obligation was deleted at merge
prep, as `docs/inflight/AGENTS.md` requires - a "delete this when it merges" marker must never reach
master. Everything else that note held is already stated where it is looked up: the inversion
contract and the red control in [`docs/testing.md`](../testing.md); the five **gating-exclusion
points** in the plan doc's "Adding a lane touches five places" section, enforced by
`QuarantinedAnnotationContractTest` rather than by prose; the five **invocation flags**, which are a
different list, in `bin/lincheck-test.sh`'s own header; and the Jabel and model-checker findings in
the plan doc.
