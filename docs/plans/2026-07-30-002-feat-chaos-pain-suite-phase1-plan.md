---
title: "feat: Chaos Pain Suite Phase 1 - seeded churn storm with zombie-member probe (skateboard)"
type: feat
status: active
date: 2026-07-30
origin: docs/plans/2026-07-30-001-feat-chaos-pain-suite-design-plan.md
---

# feat: Chaos Pain Suite Phase 1 - seeded churn storm with zombie-member probe

**Target branch:** `experiment/chaos-pain-suite-phase1` (worktree `.claude/worktrees/chaos-phase1`), based
on the uber composition `experiment/stall-uber-fix`@`c72c923c` - which carries #29's harness
(`ManagedPCInstance`), #31, and PR #80's full fix stack with all four guards green. **Unlandable base by
design** (see origin doc): Phase 1 is written **additive-only** (zero edits to #29-owned files) so it
transplants onto the eventual rebased-#29 slices as a near-pure file copy.

## Problem Frame

The origin design's one-liner: turn "the suite sometimes reddens under load" into "the suite hunts stalls
on purpose and hands you the autopsy." Phase 1 is the skateboard: ONE scenario (W1 churn storm), a
seeded/replayable conductor, first-class SLO assertions including the zombie-member probe this whole
investigation lacked, at today's scale - calibrated against the real zombie-drain bug at its pre-fix commit before its
greens are trusted.

## Requirements Trace (from origin doc, Phase 1 section)

- R1. **Seeded ChaosConductor**: replaces ad-hoc `Math.random()` chaos with a seeded, logged, replayable
  action schedule (`-Dchaos.seed=`); actions limited to stopDrain / stopNoDrain / memberJoin / memberLeave.
- R2. **ProgressProbe** SLO assertions: (a) group-level progress watermarks (generalised "no progress for
  N seconds" detection); (b) **zombie-member assertion keyed on protocol-unresponsiveness** - no rebalance
  may stay blocked beyond T (drainers exempt within `drainTimeout`); (c) every `stop(DRAIN)` completes
  within `drainTimeout + margin`; (d) correctness ledger - no record ever lost; duplicates bounded to
  uncommitted tails of disturbed drains.
- R3. **W1 churn-storm scenario** at today's scale (~12 instances / 80 partitions / 500k msgs, 3-5 min).
- R4. `@Tag("chaos")`, excluded from all default/PR suites; one **`workflow_dispatch`** job on the highcpu
  runner (inputs: seed, reps).
- R5. **Calibration (test-the-test)**: the suite MUST go red on the pre-fix zombie-drain defect (the real bug, not injected)
  (uber-NOFIX arm) and green on this branch. A chaos suite that never caught a known bug is decoration.

## Scope Boundaries

- Phase 1 ONLY: no Toxiproxy, freeze-thaw, kill -9, ResourcePressurizer, DiagnosticBundle runtime-DEBUG
  flip, multi-group, soak mode, or scale sweep (Phases 2-3 in the origin doc).
- **Additive-only**: no edits to `ManagedPCInstance`, `MultiInstanceRebalanceTest`, or any #29/#80-owned
  file. Permitted shared-file touches: root `pom.xml` (one-line tag exclusion) + new workflow file.
- Not PR-gating, ever; failures are investigation food with artifacts.
- No production (`src/main`) changes.

## Context & Research (session-verified)

- **`ManagedPCInstance` public API** (on this base): `new ManagedPCInstance(Config, kcu, onConsumed)`,
  `start(ExecutorService)`, `stop()`, `stopAsync()`, `toggle(ExecutorService)`, `close()`,
  `Config.builder()` (maxPoll, pollDelayMs, maxConcurrency, useCooperativeAssignor...). Fleet-ready.
- **`MultiInstanceRebalanceTest.runTest` is private** → the suite writes its own thin orchestration using
  `ManagedPCInstance` + `KafkaClientUtils` directly (accepted DRY tension, forced by additive-only; noted
  for consolidation when the suite lands post-#29-rebase).
- **Tag exclusion mechanism**: pom `<excluded.groups>performance</excluded.groups>` default feeds
  failsafe; extend to `performance,chaos` (comma list is the documented pattern, pom L86).
- **Zombie probe building blocks already in-tree**: AdminClient `describeConsumerGroups` +
  `ConsumerGroupState` pattern (used by `LatestResetTailNudgeIT`); rebalance-blocked = group in
  `PREPARING_REBALANCE`/`COMPLETING_REBALANCE` beyond T.
- **Correctness-ledger pattern** proven in `DrainingMemberRebalanceIT` (produced-keys vs per-instance
  consumed-key sets; no-loss union + bounded-duplicates).
- **Calibration bench exists**: `experiment/stall-uber-nofix` (drain defect + #29 + #31). The suite commit
  cherry-picks onto it cleanly (additive files).
- Seeded determinism caveat (origin doc): same seed = same action *sequence*; wall-clock jitter still
  varies interleaving. Log the seed + full timestamped action timeline every run.

## Key Technical Decisions

- **New package `io.confluent.parallelconsumer.integrationTests.chaostests`** - keeps ArchUnit's
  integration-test placement rule satisfied (package contains `integrationTests`) while grouping the suite.
- **Conductor is a plain seeded scheduler, not a framework**: `java.util.Random(seed)` + weighted action
  enum + a `List<ChaosAction>` timeline recorded with timestamps and printed at start/end (the future
  DiagnosticBundle's first artifact). Runs on one thread; actions applied via `ManagedPCInstance`
  start/stopAsync/stop - mirroring the existing monkey's non-blocking discipline.
- **ProgressProbe asserts SLOs and invariants, never exact timings** (origin doc's flakiness-by-design
  mitigation): progress watermark (total consumed strictly increases within window), rebalance-blocked
  bound, drain bound, ledger. Thresholds are constants with rationale comments, generous by default.
- **Zombie assertion via group-state dwell**: sample `describeConsumerGroups` on a poll interval; fail if
  the group dwells in a rebalancing state longer than T_REBALANCE (default 60s at Phase-1 scale - far
  below the 5-min `max.poll.interval.ms` a zombie causes, far above healthy rebalances measured in
  seconds). This is exactly the discriminator that separated the fixed/defective drain arms.
- **Calibration is the acceptance test of the whole phase** (R5): the same suite commit must produce
  RED-on-nofix / GREEN-here. If the nofix arm doesn't go red, the probe thresholds are wrong - fix the
  probe, not the bound, until the known real bug (pre-fix composition, not injected) is caught.

## Open Questions

### Resolved During Planning
- Where does orchestration live given `runTest` is private → suite-local thin orchestration (additive-only wins).
- Tag mechanics → pom default `excluded.groups` extended to `performance,chaos`.
- Plan/doc placement → plan rides this branch with the code; design doc stays on its landable branch.

### Deferred to Implementation
- Exact conductor action weights + tick interval for W1 (start from the existing monkey's 0-500ms cadence,
  tune against calibration).
- Whether `stopAsync` vs `stop` mix matters for the RED calibration (the defect bites hardest when a join
  lands mid-drain; conductor may need a "join-after-stopDrain" pairing bias to make RED reliable).
- Ledger duplicate bound at fleet scale (per-disturbance capacity-shaped, following the
  `DrainingMemberRebalanceIT` lesson - flat per-drain allowance, not fraction-of-throughput).
- Workflow inputs plumbing (seed/reps as `-Dchaos.seed`/loop) - finalize once local runs are stable.

## Implementation Units

- [ ] **Unit 1: ChaosConductor + action timeline**

**Files:**
- Create: `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/chaostests/ChaosConductor.java`

**Approach:** seeded `Random`; action enum {STOP_DRAIN, STOP_NO_DRAIN, RESTART, MEMBER_JOIN, MEMBER_LEAVE}
with per-scenario weights; `tick()` loop on its own thread selecting a target instance + action via
`ManagedPCInstance` API; every action appended to a timestamped timeline; seed + timeline logged. Clean
shutdown hook. No assertions here - the conductor only perturbs.

**Test scenarios:**
- Unit-ish (same IT run): same seed → identical action sequence (compare two conductor instances' planned
  sequences without executing).
- Timeline completeness: every executed action appears with timestamp + target.

**Verification:** conductor drives a 3-instance fleet for 60s without harness errors; log shows seed +
timeline.

- [ ] **Unit 2: ProgressProbe (SLOs + zombie-member + ledger)**

**Files:**
- Create: `.../integrationTests/chaostests/ProgressProbe.java`

**Approach:** composed of independent checks run on a sampling thread: (a) progress watermark - fleet-wide
consumed-count must advance within `NO_PROGRESS_WINDOW` (generalising #857's 11s check, default 30s);
(b) rebalance-dwell bound via AdminClient group state (T_REBALANCE=60s); (c) drain-completion bound -
conductor reports STOP_DRAIN start, probe asserts instance terminal within `drainTimeout+margin`;
(d) end-of-run ledger - union(consumed) ⊇ produced (no loss), duplicates ≤ perDrainAllowance ×
disturbedDrains. Failures throw with the chaos timeline attached to the message.

**Test scenarios:**
- Happy: quiet fleet (no chaos) passes all probes.
- Error path: probe failure message contains seed + timeline tail (verified in calibration RED).
- Ledger: no-loss holds across a run with several drain/restart cycles; duplicates within allowance.

**Verification:** probes green on an undisturbed run; each probe individually triggerable in calibration.

- [ ] **Unit 3: W1 churn-storm scenario (`ChaosChurnStormIT`)**

**Files:**
- Create: `.../integrationTests/chaostests/ChaosChurnStormIT.java`

**Approach:** `@Tag("chaos")`; today's scale (12 instances, 80 partitions, 500k msgs; pre-produce 30% +
background producer, mirroring the existing chaos test's shape via suite-local orchestration);
ChaosConductor(W1 weights: stopDrain-heavy + join/leave, incl. the join-after-stopDrain pairing bias) +
ProgressProbe wrapping the run; 5-min cap. Seed from `-Dchaos.seed` (default: random, always logged).

**Test scenarios:** (the IT *is* the scenario)
- Happy: full run completes, all probes green, ledger balanced - on THIS branch.
- The RED calibration lives in Unit 5.

**Verification:** 2 consecutive green runs locally at Phase-1 scale, ≤6 min each.

- [ ] **Unit 4: tag exclusion + highcpu `workflow_dispatch` workflow**

**Files:**
- Modify: `pom.xml` (default `excluded.groups` → `performance,chaos`)
- Create: `.github/workflows/chaos-pain.yml` (`workflow_dispatch`; inputs: seed, reps; runs-on
  `[self-hosted, highcpu]`; same-repo guard comment block mirroring `pr-highcpu-fast-feedback.yml`;
  uploads reports + the run log as artifacts; never required)

**Test scenarios:**
- Regression: default builds (unit + integration suites) run ZERO chaos tests (verify by absence in
  reports).
- `-Dexcluded.groups=performance` alone still excludes chaos? No - document that override lists must now
  include both, mirroring pom comment (update the pom comment accordingly).
- actionlint clean.

**Verification:** `mvn verify` on core runs no `chaostests`; explicit `-Dincluded.groups=chaos` runs them.

- [ ] **Unit 5: calibration - RED on the known-bug (pre-fix) composition, GREEN here (R5, phase acceptance)**

**Files:** none new (cherry-pick exercise + docs updates in Unit 6).

**Approach:** cherry-pick the suite commits onto `experiment/stall-uber-nofix` (additive files → clean);
run `ChaosChurnStormIT` there 3×: expect **RED via the rebalance-dwell (zombie) probe or drain-bound
probe** in ≥2/3 runs (the drain defect makes a mid-drain join freeze the group). Then 3× on this branch:
expect **GREEN 3/3**. If nofix doesn't red reliably, bias the conductor (join-after-stopDrain weight) and
re-run - tune until the known real bug (pre-fix composition, not injected) is caught, never by loosening probes on the green side.

**Verification:** documented 3+3 run table; RED failures name the zombie probe and attach the timeline.

- [ ] **Unit 6: docs + inflight**

**Files:**
- Modify: origin design doc (on `docs/chaos-pain-suite-design` branch): Phase 1 status → IMPLEMENTED
  (branch pointer + calibration results table).
- Modify: `docs/inflight.md` (this branch): chaos suite Phase 1 entry - where it lives, how to run
  (`-Dincluded.groups=chaos`, seed protocol), calibration evidence, transplant plan (post-#29-rebase).

**Test expectation:** none - docs.

**Verification:** docs point at real branch/commits; run instructions work as written.

## System-Wide Impact

- **Default suites untouched**: chaos tests excluded everywhere by default; pom change is the only shared
  edit and only *adds* an exclusion.
- **Transplant posture preserved**: all Java is new files in a new package; the eventual move onto
  rebased-#29 is file-copy + compile-check (the plan's whole additive-only point).
- **Unchanged invariants**: no `src/main` changes; existing guards (drain, nudge) unaffected - re-run as
  part of Unit 3 verification.

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Suite flaky-by-design | SLO/invariant assertions only; generous defaults; seed+timeline on every failure; never PR-gating |
| RED calibration won't fire (defect needs precise join-mid-drain timing) | Conductor pairing bias is a first-class knob; calibration loop tunes the *conductor*, not the probes |
| Suite-local orchestration drifts from `MultiInstanceRebalanceTest`'s | Accepted for Phase 1 (additive-only); consolidation flagged for post-#29-rebase landing |
| pom tag change surprises override users | Update the pom's own comment showing the multi-group override syntax |
| Box contention skews probe bounds | Bounds sized ≥5× healthy baseline, ≤1/5 defect signature (60s dwell vs seconds-healthy vs 5-min-zombie) |

## Verification (end-to-end)

1. Units 1-3: `ChaosChurnStormIT` green 2× locally; conductor determinism check passes; quiet-fleet probes green.
2. Unit 4: default suites run zero chaos tests; actionlint clean.
3. Unit 5: **RED ≥2/3 on nofix arm naming the zombie/drain probe; GREEN 3/3 here** - the phase's definition of done.
4. Existing guards still green on this branch; full unit suite green.
