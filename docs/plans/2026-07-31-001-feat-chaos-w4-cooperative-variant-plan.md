# feat: Cooperative-sticky W4 variant - Class 2 stall hunt, lever 2 (chaos Phase 2)

> ce-plan formatted. On approval, copied to
> `docs/plans/2026-07-31-001-feat-chaos-w4-cooperative-variant-plan.md` as the durable artifact.

## Context

The W4 revoke-under-work scenario (PR astubbs#85) is calibrated artifact-free but its target - a true,
unbounded **Class 2 protocol-invisible stall** (the "confluentinc#857 locks forever" family) - did not reproduce
on master under the EAGER assignor: 9 seeds, 0 hits. The rostered next lever is the
**cooperative-sticky variant**, which changes the physics twice over:

1. **It removes the eager-restart artifact class entirely.** Under eager, every membership change
   revokes ALL partitions, restarting every in-flight heavy - which pinned commit low-watermarks and
   produced the false-positive class we root-caused. Under cooperative-sticky, unaffected partitions
   keep processing through rebalances; the legit lag-stagnation window shrinks to a single dwell +
   slack, giving the Class 2 probe far more headroom.
2. **It raises exposure to the actual quarry.** Exploration confirmed the confluentinc#857 commit-during-revoke
   deadlock (`synchronized(commitCommand)` between `onPartitionsRevoked` and
   `commitOffsetsThatAreReady`) fires per-revoke - and cooperative mode produces *more frequent,
   smaller* revokes. More revokes under in-flight work = more draws at the probabilistic stall.

Exploration verdict (session, 2026-07-31): PC's state layer is **delta-correct and
incremental-safe** (per-partition epochs in `PartitionStateManager.onPartitionsRemoved`; subset-scoped
truncation `resetOffsetMapAndRemoveWork`; delta arithmetic in
`AbstractParallelEoSStreamProcessor.onPartitionsRevoked/Assigned`; commit-then-truncate order safe for
subset revokes - it commits ALL dirty partitions, which is legal), **but cooperative mode has never
been exercised anywhere**: the `ManagedPCInstance.Config.useCooperativeAssignor` flag is pre-built
scaffolding with zero users, and no docs claim cooperative support. This variant is therefore both a
Class 2 trigger attempt AND the codebase's first-ever cooperative-mode end-to-end exercise.

**Branch:** new `feats/chaos-w4-cooperative` off `feats/chaos-w4-revoke-under-work` (stacked on
PR astubbs#85 → astubbs#83; PR body carries `depends on #85`). New worktree `.claude/worktrees/chaos-w4-coop`.

## Requirements Trace

- R1. A cooperative-sticky revoke-under-work scenario reusing the W4 machinery (DRY - no copy-paste
  of the two-phase driver).
- R2. Behavior of the existing eager W4 IT unchanged by the refactor (same-seed peaks comparable).
- R3. Calibration by the measured-instrument method: same-seed A/B (defect arm = branch's master
  base; fixed arm = `chaos-phase1` bench), peaks recorded, outcomes classified honestly - including
  the possibility that cooperative mode breaks PC in novel ways on BOTH arms (that is a finding to
  document/roster, never to mask).
- R4. Results recorded durably (scenario javadoc calibration record + inflight roster update).

## Scope Boundaries

- NOT fixing any cooperative-mode product bug found - document + roster (the suite is a detector;
  fixes belong to the confluentinc#857/#29 stream). Main-source changes are out of scope entirely.
- NOT re-tuning the eager W4 constants or any ProgressProbe bound in this pass (unless a measured
  false positive forces it, with the arithmetic documented as before).
- NOT adding cooperative variants of W1 (rosterable later if this one earns it).
- NOT gating anything: `chaos` tag exclusion applies as-is; no CI changes needed.

## Context & Research (session-verified)

- `ManagedPCInstance.java` (grep `useCooperativeAssignor`) - it wires `CooperativeStickyAssignor`
  into consumer props; zero current users. (Point-in-time, and this plan is what changed it: the
  variant proposed below landed in `192d32bc`, so a grep at HEAD now returns its callers in
  `AbstractRevokeUnderWorkScenario`, `ChaosRevokeUnderWorkIT` and `ChaosRevokeUnderWorkCooperativeIT`.
  For the tree this claim describes, search before that commit - `git grep useCooperativeAssignor
  192d32bc^` - which finds only the flag's declaration and wiring, and no callers.)
- `ChaosRevokeUnderWorkIT` (PR astubbs#85) - the two-phase driver to extract: storm (60s, no-drain weights
  `ChaosConductor.defaultW4Weights()`, sync commits, heavy 1-in-2000 @ 20s non-interruptible, ticks
  300-1000ms, fleet 10-14, 250k backlog, `max.poll.interval.ms=30s` via `extraConsumerProps`) then
  quiet observation (probe `disableRebalanceDwellViolation()` + `withNoProgressWindow(60s)`,
  fail-fast await, `QUIET_CAP` 5min).
- `ChaosScenarioBase` - shared scaffolding (producer, heavy dwell, coverage, settle).
- Calibration bench: defect arm = this branch's own base (master + suite); fixed arm =
  `.claude/worktrees/chaos-phase1` (all-fixes) with scenario files copied in (same method as before).
- Probe semantics under cooperative: group still transitions through PREPARING/COMPLETING_REBALANCE
  (dwell probe is W1-gated anyway and disabled in W4); ledger duplicate allowance stays as-is
  (cooperative should *reduce* duplicates - tightening is a later, separate calibration).

## Key Technical Decisions

- **Shared driver, two thin ITs.** Extract the W4 run into an abstract
  `AbstractRevokeUnderWorkScenario extends ChaosScenarioBase` exposing the knobs the variants differ
  on (assignor flag; scenario name for topic/log labels; constants stay protected fields with the
  current values as defaults - post-review clarification: the constants are shared fixed calibration
  values, NOT overridable knobs; static fields can't be overridden by subclass shadowing, so a
  variant that ever needs a different value must first promote that constant to an overridable
  accessor). `ChaosRevokeUnderWorkIT` (eager) becomes a thin subclass - byte-equal
  behavior; new `ChaosRevokeUnderWorkCooperativeIT` sets `useCooperativeAssignor=true`. Separate
  classes keep per-variant javadoc calibration records, independent seeds/timeouts, and selective
  `-Dit.test` runs (a parameterized single class would couple tuning constants and muddle the
  calibration narrative).
- **First cooperative runs use identical constants to eager W4.** One variable changes (the
  assignor). Only after measuring do we exploit the removed artifact class (e.g. longer heavy dwell
  or storm for more collision pressure) - each change with its legit-window arithmetic documented,
  per the calibrate-the-workload lessons.
- **Outcome classification matrix, pre-declared** (honesty guard, mirrors the W4 discipline):
  - defect RED via `CLASS2_STALL/LAG_STAGNATION` + fixed GREEN → the probe's RED calibration is
    complete; record seeds + peaks; headline result.
  - both GREEN → cooperative doesn't trigger it either; record peaks (expect much lower lag
    stagnation - artifact class gone); hunt continues (roster: more seeds / W1-coop / low
    max.poll.interval experiments).
  - both RED (same signature) → suspect a cooperative-mode workload artifact or a PC
    cooperative-mode bug present in ALL compositions; diagnose with the no-fail-fast progression
    method before touching any bound (freeze-resolves = artifact; freeze-persists = real bug →
    document as a finding, likely the first-ever cooperative-mode PC defect on record).
  - fixed RED only / crashes / rebalance storms → novel cooperative finding; capture timeline +
    seeds, roster, and stop tuning until classified.
- **Fixed-arm sync:** copy the scenario + base files to `chaos-phase1` for GREEN runs (bench stays
  uncommitted overlay, as throughout Phase 1/2).

## Open Questions

### Deferred to Implementation
- Whether cooperative mode needs a longer storm to accumulate enough incremental revokes (measure
  revoke counts in the conductor timeline first run; tune only on evidence).
- Ledger duplicate expectations under sticky (observe first; no assertion changes this pass).
- Whether `session.timeout`/`max.poll.interval` interplay differs enough under cooperative to move
  the 30s eviction horizon (observe dwell/eviction behavior in run 1).

## Implementation Units

- [x] **Unit 1: extract the shared driver (no behavior change)** - DONE; same-seed eager re-run in family (dwell 28.1s, stag 101s)

**Files:**
- Create: `parallel-consumer-core/src/test-integration/java/io/confluent/parallelconsumer/integrationTests/chaostests/AbstractRevokeUnderWorkScenario.java`
- Modify: `.../chaostests/ChaosRevokeUnderWorkIT.java` (becomes thin eager subclass; javadoc + calibration record stay here)

**Approach:** mechanical extraction of the two-phase driver; knobs = `useCooperativeAssignor()`
(default false) + `scenarioLabel()`. Constants become protected fields on the abstract class with
current values. No logic edits.

**Verification:** compile clean; `ChaosConductorPlanIT` green; one same-seed eager defect-arm run
(seed 7612284256787897904) - verdict and peaks in family with the pre-refactor record (GREEN, dwell
peak ~30s band, stagnation under 150s).

- [x] **Unit 2: the cooperative variant IT** - DONE; smoke run green, incremental revokes confirmed (9 events vs eager 57)

**Files:**
- Create: `.../chaostests/ChaosRevokeUnderWorkCooperativeIT.java`

**Approach:** subclass setting `useCooperativeAssignor=true`; javadoc states the two physics changes
(artifact class removed; confluentinc#857 exposure raised), the never-exercised-before status, and the
calibration-pending banner. Same constants as eager (decision above).

**Test scenarios (the IT is the scenario):** two-phase storm+quiet run; probe SLOs + ledger as in W4.

**Verification:** compile; single local defect-arm smoke run completes without harness-level errors
(fleet starts, conductor timeline shows incremental revokes - look for non-full-fleet assignment
changes; scenario reaches the quiet phase).

- [x] **Unit 3: calibration** - DONE; outcome-matrix row: BOTH GREEN (defect 4 seeds 0 hits; fixed 3/3; dwell non-discriminating under cooperative)

**Approach:** (a) defect arm (this branch base): 3 seeds; (b) fixed arm (`chaos-phase1` + copied
files): same seeds; (c) classify per the pre-declared matrix; on ambiguity use the no-fail-fast
diagnostic variant (bench-only edits) with per-partition progression logging, as done for eager W4.
Record revoke-count + peak table per run.

**Verification:** every run's seed, verdict, and peaks captured; outcome matrix row selected with
evidence; no bound changed without documented arithmetic.

- [x] **Unit 4: record + roster** - DONE; javadoc calibration record + inflight hunt-status (tripwire stance)

**Files:**
- Modify: `ChaosRevokeUnderWorkCooperativeIT` javadoc (calibration record), `ChaosRevokeUnderWorkIT`
  javadoc (pointer to sibling), `docs/inflight.md` (Class 2 hunt status update; any new findings
  rostered), `docs/plans/2026-07-31-001-...-plan.md` (this plan, durable copy, status annotations).

**Verification:** docs match the measured record; commits on `feats/chaos-w4-cooperative`; push +
propose the stacked PR (`depends on #85`) - ask before opening, per convention.

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Cooperative mode breaks PC in unknown ways (first-ever exercise) | That's data, not failure: pre-declared outcome matrix; capture timeline/seed; roster as finding; no masking |
| Refactor perturbs eager W4 behavior | Same-seed re-run compared against the recorded peak band |
| Cooperative changes rebalance cadence so much the storm under/over-shoots | Conductor timeline revoke counts measured in run 1; tune only on evidence with documented arithmetic |
| Probe false positives from unforeseen cooperative timing | Same diagnostic method as eager (no-fail-fast + progression logging) before touching any bound |
| Bench (chaos-phase1) drift vs branch files | Copy scenario files fresh before each fixed-arm run (established practice) |

## Verification (end-to-end)

1. Unit 1: compile + plan IT + same-seed eager re-run in family with recorded peaks.
2. Unit 2: cooperative smoke run reaches quiet phase; timeline shows incremental (subset) revokes.
3. Unit 3: A/B seed runs executed; outcome matrix row selected with evidence; peaks tabulated.
4. Unit 4: javadoc + inflight updated; branch pushed; stacked PR proposed (not opened without ask).
