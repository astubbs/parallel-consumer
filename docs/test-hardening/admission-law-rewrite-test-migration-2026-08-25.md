# Admission law rewrite - test migration audit

**Date:** 2026-08-25
**Scope:** every test deleted, retired or expectation-migrated by the U5 band-machine rewrite of
`AdmissionControlLaw` (plan `docs/plans/2026-08-24-003-feat-admission-control-law-design.md`, KTD8:
*test deletion is deliberate - each deletion is named with its replacement falsifier*).
**Companion:** the falsifier harness itself landed in U8 (`FalsifierScenarios`,
`AdmissionFalsifierHarnessTest`) and is the successor for most of what is deleted here.

This is a point-in-time record, not a generated index. Findings are keyed by class and method, never
by line number. The reproducing command for the surviving suites:

```bash
./mvnw -pl parallel-consumer-core -am test -Dtest='Admission*Test,AdaptiveConcurrency*Test' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

---

## Why tests were deleted at all

The U5 rewrite replaced the six-arm Gradient2 port (latency-gradient law: long-EWMA baseline,
anti-drift decay, probe-down re-measure, starvation probe-up, in-flight-median hold) with the band
machine (one elasticity estimator read as three bands, warmup allowance, absolute brakes, binding
gate). The deleted tests pinned the *deleted machinery's* behaviour - keeping them green would have
required keeping the ratchet they pinned. Per KTD8, each is named below with its successor or the
reason no successor is owed. A test that stops running silently is the failure mode this file
exists to prevent; nothing here went dark - everything was either replaced or consciously retired.

## Classes deleted whole

| Class | Cases | Successor / reason |
|---|---|---|
| `ServiceTimeExpAvgTest` | `warmupPhaseIsArithmeticMean`, `exponentialPhaseUsesSpanFactor`, `shortSpanWeightsNewSamplesHeavily`, `updateAppliesOperatorWithoutCountingAsSample`, `updateSticksOncePastWarmup` | Deleted with its class: `ServiceTimeExpAvg` was the law's learned latency baseline, and R8 forbids the law any learned latency reference. No successor owed - the class it tested no longer exists. |
| `ContaminatedBaselineGateTest` | `portedLawAloneCannotDescendFromAContaminatedBaseline`, `probeDownRemeasuresAndDescendsToNearCapacity` | Its premise (the gradient cannot descend from a contaminated baseline, so a probe-down arm must) is deleted machinery: the band machine has no baseline to contaminate. Successor: the graceful-saturation **plateau falsifier** (`FalsifierScenarios.gracefulSaturationPlateau`, asserted in `AdmissionLawFalsifierTest.gracefulSaturationPlateauDoesNotRatchet`), which falsifies the same underlying defect - a law that reads sustained saturation as health. |
| `AdmissionControlSimulationTest` | `throughputSettlesNearModeledCapacityWithoutCollapse`, `simulationIsDeterministicUnderTheFixedSeed` | Retired (KTD8): it was the placeholder closed-loop simulation, superseded by the falsifier suite - `DeterministicPlant` + `ScenarioRunner` are its successor plant (deterministic *without* a seed), `AdmissionLawFalsifierTest.sweepConvergesFromBelowAndHoldsTheKnee` its successor settling assertion, and `AdmissionFalsifierHarnessTest.plantIsDeterministic` its successor determinism pin. |
| `OldLawPlateauControlTest` | `oldLawRatchetsUpThePlateau`, `oldLawFailsTheGracefulSaturationPlateauScenario`, `oldLawSweepResultIsRecordedNotAsserted` | THE CONTROL RUN, deleted by design - its own javadoc said "U5 DELETES this class". It asserted the old law's *defect* (the plateau walk 20 -> 60 over 250 windows); the flip is `AdmissionLawFalsifierTest`, where the same scenario is asserted to PASS (new law: 20 -> 24 warmup step, retracted to exactly 20; pinned in `plateauTrajectoryWarmupStepIsRetractedToExactlyTheKnee`). |
| `OldLawAdmissionPolicy` (helper, not a test) | - | Replaced by `LawAdmissionPolicy`, the same adapter shape over the new law. |

## `AdmissionControlLawTest` - rewritten deliberately, case by case

The file was rewritten whole; the old cases and their fates:

| Deleted case | Fate |
|---|---|
| `latencyStepContractsWithinBoundedWindows_perWindowCutBoundedByGradientFloorTimesSmoothing` | Pinned the latency gradient's step response - deleted machinery (R8: no latency in the law). Successors: contraction now comes from the FALL band - `fallingSeriesContractsMultiplicatively` (the cut and its 0.9 bound) and `fallCutsOncePerSettle_notOncePerWindow` (the per-window cut bound, restated for band dynamics). |
| `recoveryRegrowsViaHeadroom_andDecayUnsticksStaleHighBaseline` | Pinned the additive-headroom regrowth and the PR-88 anti-drift decay - both deleted. Successors: growth is now `WarmupBand.bindingAloneGrowsByTheAcceleratorStep_untilTheAllowanceIsSpent` plus `ElasticityBands.risingSeriesTakesOneAcceleratorStep`; there is no baseline left to unstick, and the plateau falsifier guards the ratchet the decay existed to soften. |
| `appLimitedWindowsHoldTheLimitBitIdentical` | The in-flight-median hold arm is deleted; the *property* (unbound windows preserve bit-identically) survives strengthened as `unboundWindowsPreserveBitIdentical_namedForTheirStarvationCause` - the binding verdict now comes from engine signals (R2), and the hold is named per starvation cause (R13). The app-limited **lull** falsifier (`AdmissionLawFalsifierTest.appLimitedLullPreservesTheTarget`) asserts the same property closed-loop. |
| `backoffNeverCutsBelowOneSlotFloor` | Kept, same name modulo article (`backoffNeverCutsBelowTheOneSlotFloor`) - the BACKOFF brake survives the rewrite intact, floor pin and reason identity included. |
| `gradientContractionClampsAtFloorWithReasonAtFloor` | Gradient deleted. Successor: `contractionClippedByTheFloorReportsAtFloor` - the FALL band's cut clipping at the floor, same AT_FLOOR reason semantics. |
| `growthClampsAtCeilingWithReasonAtCap` | Successor: `growthClippedByTheCeilingReportsAtCap` - the warmup grant clipping at the ceiling, same AT_CAP reason semantics. |
| `zeroLatencyWindowIsGuardedAgainstNaN` | Guarded the gradient's division by a zero latency - the law no longer divides by latency. The analogous numeric hazard (log of non-positive throughput/slots) is owned and tested by the estimator: `AdmissionElasticityEstimatorTest` refusal cases, untouched by this rewrite. |
| `extremeLatenciesNeitherOverflowNorEscapeTheClamps` | Same reasoning: latency never enters the law's arithmetic now. Clamp integrity is pinned by the two clip cases above; estimator input hygiene by its own suite. |
| `constantLatencyKeepsTheLimitInABoundedBand` | Pinned the probe-down oscillation band at the cap - deleted machinery. Successor: the plateau falsifier's settled-tail pin (`plateauTrajectoryWarmupStepIsRetractedToExactlyTheKnee` asserts the tail is CONSTANT at the knee - a stronger claim than a bounded band). |
| `overloadDropsFireTheAimdArmOncePerWindow_regardlessOfDropCount` | Kept, same name - the brake survives; the assertion set gained "braked windows are never offered to the estimator". |
| `ignoreOutcomesBelowTheFailureThresholdLeaveTheLimitUntouched` | Successor: `ignoresBelowTheThresholdDoNotFreeze` - same threshold property, restated against the band machine (the un-frozen window proceeds to warmup rather than to the gradient). |
| `risingFailureFractionWithFallingLatencyMustNotGrowTheLimit` | Successor: `failureFractionAboveThresholdFreezesGrowthBitIdentical` with its clean-window control arm. The "falling latency" half of the trap is moot - the law cannot see latency - but the freeze itself is pinned, plus its new non-offer obligation. |
| `failureLimitedWindowsMayStillContractViaTheGradient` | The contract-only-via-gradient semantics died with the gradient: the band machine takes no decision on a failure-poisoned window (it is not offered), so `min(current, decision)` degenerates to a bit-identical hold - the documented U5 reading, pinned by the freeze case above. Contraction under failure now arrives via the BACKOFF brake (drops classify separately) - `overloadDropsFireOneAimdCutPerWindow_regardlessOfDropCount`. |
| `windowsBelowTheSampleMinimumAreHeld_thenTheLimitRecoversOnceSamplesSuffice` | Successor: `thinWindowsHoldWithAllStateUntouched_evenWhenTheyCarryDrops` - the same adjudication gate, now reason `INSUFFICIENT_SIGNAL` (the old `APP_LIMITED` overload of "thin" and "idle" is split), with the stronger obligation that a thin window shadows even the brakes. |
| `starvedWindowProbesUpOneBoundedStep_notAPersistentFreeze` | The starvation probe-up arm is deleted (KTD8). Its *job* - keeping a contraction from manufacturing its own starvation evidence and locking - is now split: unbound windows PRESERVE (never decay, so no lock-in), and the ungated floor escape is **U6's** unit; the floor-pin falsifier (`FalsifierScenarios.floorPin`) is built and stays unasserted until U6 lands it. |
| `bimodalInFlightSpreadDoesNotClassifyAsStarved` | The in-flight-distribution starvation *signature* is deleted with the arm - binding is now measured from engine signals (slot saturation, R2/KTD1), not inferred from distribution shape. No successor owed: the classification it guarded no longer exists, and the real classification's cases are pinned in `AdmissionSampleWindowTest` / `unboundWindowsPreserveBitIdentical_namedForTheirStarvationCause`. |
| `starvationRequiresFlatLatency` | Same - the latency-flatness input to the deleted signature. No successor owed. |
| `starvedProbeNeverStepsPastTheCeiling` | Successor: `growthClippedByTheCeilingReportsAtCap` (no growth path may pass the ceiling; the probe-up path is gone). |

New cases with no predecessor (the band machine's own obligations): precedence shadowing
(`thinWindowsHoldWithAllStateUntouched_evenWhenTheyCarryDrops`, `offsetBackPressureHoldsAndNeverGrows`),
the warmup allowance and its episode reset (`WarmupBand.*`), the settle cadence
(`afterAStepTheLawSettlesBeforeSteppingAgain`, `fallCutsOncePerSettle_notOncePerWindow`), retraction
(`warmupGrowthThatBoughtNothingIsRetractedToTheEpisodeBaseline`), the burst-then-idle round trip,
the R7 construction invariant (`aLawAtTheFloorIsConstructibleAndCanAccelerate`), and the whole of
`AdmissionLawFalsifierTest`.

## Suites migrated (expectations moved, tests kept)

Growth now requires LIMIT-BOUND boundary signals (the binding gate), and the growth mechanics are
warmup-allowance + settle-cadence rather than smoothing*headroom per window. Every moved pin is a
new exact pin, never a loosening:

- **`AdmissionControllerTest`**: `feedWindowAndTick` grew a bound-signal variant (the no-arg tick
  closes UNSAMPLED, never-bound windows). `APP_LIMITED` pins became `INSUFFICIENT_SIGNAL` (empty
  window) / `WARMUP_EXHAUSTED` (bound, allowance spent); `ADAPTING`-after-cooldown became `WARMUP`
  (a reconstructed law's fresh episode); the ceiling test pins `AT_CAP` then `WARMUP_EXHAUSTED`; the
  old gradient-to-floor test became `overloadBackoffClampsAtTheOneSlotFloor` (BACKOFF is the
  controller-reachable floor path; AT_FLOOR's pin lives at law level); OBSERVE's growth is pinned to
  exactly `DEFAULT_MAX_CONCURRENCY + 4` (the warmup allowance). Law-internal fixture probes moved
  from `getServiceTimeBaselineNanos` (deleted) to `estimatorHistorySize()`.
- **`AdmissionMetricsTest`**: the growth-driving tests feed bound windows; the AT_CAP fixtures use
  two windows (inside the warmup allowance) instead of four.
- **`AdmissionLifecycleTest`**: boundary-decides pin `ADAPTING` -> `NO_WORK` (real signals, no active
  tasks); pause-poison pin `APP_LIMITED` -> `INSUFFICIENT_SIGNAL` (sabotage signature is now
  `NO_WORK`); the poller-wakeup growth test marks the task accounting saturated so the boundary
  reads bound.
- **`AdaptiveConcurrencyModeTest`**: the rip-out triad's expectations FLIPPED deliberately - the old
  law's climb-to-the-ceiling on this drive *was* the ratchet. New exact pins: blind growth is the
  seed + 4-slot warmup allowance and not one slot more; the ceiling test seeds one grant below the
  cap so the clamp genuinely binds; the bounded-contraction test drives the BACKOFF brake explicitly
  (the band machine's deeper retraction lands only on a recorded baseline, pinned at law level).
  The BACKOFF-shaped tests (`observeComputesADifferentTargetFromTheOneItPublishes`,
  `theReportedInternalViewMatchesAnIndependentlyComputedExpectation`) survived unchanged - the brake
  and its arithmetic are identical in both laws.
- **`AdmissionPoolActuatorTest`** (not named in the unit packet - migrated under the full-suite
  rule): the growth fixtures mark the task accounting saturated at the boundary target
  (`markPoolSaturatedAt`), and the hold-then-grow shape is now unbound-window-then-bound-window.

## Sabotage evidence (break the code before you trust the test)

Two manual mutations were applied to the new law and reverted, per `docs/testing-at-write-time.md`:

1. **Retraction disabled** (the HOLD-band retract branch short-circuited to `false`): 7 tests failed
   across `AdmissionControlLawTest` (the retract, episode-reset and burst-then-idle pins) and
   `AdmissionLawFalsifierTest` (both sweeps, the batch-4 oracle pin, the plateau trajectory pin).
2. **HOLD band routed to the RISE step** (the resurrected ratchet): 6 of `AdmissionLawFalsifierTest`'s
   8 tests failed, the graceful-saturation plateau and the arrival-burst dual among them.

Both mutations were fully reverted; the suites were re-run green afterwards.

## What is deliberately not asserted yet

- **Sweep arms starting above the knee** (50, ceiling): descent on a flat plateau requires either a
  signal R8 forbids the law (queueing latency) or U6's escape probe. TODO(refactor) recorded in
  `AdmissionLawFalsifierTest`.
- **`pauseCycling`, `rebalanceShrink`, `floorPin`**: built, unasserted; each carries its
  TODO(refactor) in `FalsifierScenarios`, owned by U6.
