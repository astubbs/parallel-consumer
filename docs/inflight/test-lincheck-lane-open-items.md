<!-- post-merge: checked -->
# The Lincheck lane: what it does not yet cover, and what it left open

<!-- inflight-type: task -->
<!-- inflight-impact: test-debt -->

<!-- post-merge: checked-begin -->
The lane arrived with astubbs#347. What is below is what that PR deliberately did **not** close, plus
the coverage the lane is worth extending to now that it has been shown to work. Delete this note when
these items are resolved - not when any PR merges, which is why it is named for the lane rather than
for a PR number.
<!-- post-merge: checked-end -->

## Where to point it next

Ranked by what each buys, not by effort. The lane's value is that it finds seams nobody named, so
prefer widening what an existing harness explores over adding a class with a narrow guess in it.

1. **Close the encoder's range-top leg.** The one verdict in the calibration that is not clean:
   `OffsetMapCodecManager.encodeOffsetsCompressed` came back HALF-FOUND, because the *snapshot* leg
   was exhibited and the two-reads-return-different-values leg was not, and is not expressible in
   the harness as committed. `PartitionStateLincheckTest`'s javadoc already states what widening its
   generator would need. Turning one half-verdict into a whole one is worth more than a new class.
2. **The unsynchronised counter maps, which the lane already tripped over unprompted.**
   `PartitionStateManager.slowWorkCounters`, `WorkManager.succeededRecordsCounters` and
   `failedRecordsCounters` are plain `HashMap`s mutated from the rebalance callbacks and the
   completion path, and `RemovedPartitionState.READ_ONLY_EMPTY_SET` is a mutable `TreeSet` shared by
   every PC instance in the JVM (`bug-shared-collections-across-the-poll-boundary.md`). The PoC hit
   this class of defect from a scenario aimed at something else. A harness here is cheap, and it
   becomes the regression detector the moment the sweep on `fix/concurrent-collection-sweep` lands.
3. **`ProcessingShard` and `RetryQueue` are not modelled at all.** Of the ten classes in
   `bz.stub.parallelconsumer.state`, the lane covers three. These two are the ones carrying ordering
   and scheduling state across the same two threads the existing harnesses already prove race.
4. **Model-checking arms over the product classes, which cost nothing once they are possible.**
   `ShardManagerLincheckTest` and `WorkManagerLincheckTest` gain one for free - same operations, one
   different `Options` - the day `LincheckSuperHashCodeProbeTest` starts failing. That tripwire is
   already in the lane precisely so nobody has to remember to check.

**Not this, and the plan doc says why**: `@Validate` invariants would catch the retry-queue leak in
§1.1, and they are still the wrong next step, because an invariant naming the retry queue is a hint
and the value demonstrated here is what the tool finds *unaided*.

**Ground Lincheck structurally cannot cover.** It reasons about interleavings of operations, not
about the memory model, so the plain non-volatile `long` reads of `offsetHighestSucceeded` and
`offsetHighestSeen` are outside it however many harnesses are added. That half of the evaluation is
jcstress's, and it is still open on `test/jcstress-poc-plain-long-visibility`.

## A stress arm's hit rate is machine-dependent, so one machine cannot calibrate it

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
`test/jcstress-poc-plain-long-visibility` is the branch carrying it.

This paragraph exists because the handoff note that used to carry the obligation was deleted at merge
prep, as `docs/inflight/AGENTS.md` requires - a "delete this when it merges" marker must never reach
master. Everything else that note held is already stated where it is looked up: the inversion
contract and the red control in [`docs/testing.md`](../testing.md); the five **gating-exclusion
points** in the plan doc's "Adding a lane touches five places" section, enforced by
`QuarantinedAnnotationContractTest` rather than by prose; the five **invocation flags**, which are a
different list, in `bin/lincheck-test.sh`'s own header; and the Jabel and model-checker findings in
the plan doc.
