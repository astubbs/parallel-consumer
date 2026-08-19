# astubbs#29 - the confluentinc#857 deadlock fix, and what measuring it taught

Context `gh` cannot give you about PR astubbs/parallel-consumer#29
(`bugs/857-paused-consumption-multi-consumers-bug`). Delete this file when the PR merges, promoting
anything below that is still wanted into `next-candidates.md`.

## What the PR is, in one line

Fixes the AB-BA deadlock between the poll thread's `onPartitionsRevoked` and the control thread's
`commitOffsetsThatAreReady`, proven on its own instrument at 60/60 failures before and 0/60 after.

## What the PR is NOT, which took a day to establish

The eager `ChaosRevokeUnderWorkIT` `CLASS2_STALL` sightings were this family's leading candidates for
this PR, on the strength of `PERIODIC_CONSUMER_SYNC` being the one commit mode where the AB-BA cycle
can close. **They are not this defect, and probably not any defect.** The evidence is in
`test-857-revoke-under-work-sightings.md`: two recorded seeds reproduced 6/6 on the arm carrying the
fix, and the four-cell assignor x stop-mode matrix explains every sighting as eager reassignment
restarting in-flight heavy work until a commit watermark is legitimately pinned past a 150s bound.

## Still open on this PR, 2026-08-19

Ordered by what blocks a merge. **This file should have existed from the branch's first commit and
did not** - the April investigation log (`docs/BUG_857_INVESTIGATION.md`) was retired in `69a670de4`
into the solutions write-up and the per-mode inflight split, which kept the SETTLED knowledge and
left the live threads without a home. Append here as you go rather than reconstructing later.

**Landed 2026-08-19 (was "in flight" above this line's earlier revision)**

- **Instance-granularity progress detector** - `INSTANCE_STALL/NO_WORK_COMPLETED` in
  `ProgressProbe`, wired for every chaos scenario by `ChaosScenarioBase#startRun`. Per INSTANCE,
  not per shard: which shards hold queued work is `ShardManager`'s private `processingShards`, so
  shard granularity needs a main-code accessor this deliberately does not add - and per-instance is
  the confluentinc#857 wedge signature anyway, because completions are counted where
  `WorkManager#onSuccessResult` runs, PC's CONTROL thread, which is the thread the AB-BA cycle
  freezes. Additive: no existing probe, bound or assertion changed. Non-vacuity proven both ways:
  broker-free `InstanceStallProbeIT` (7 tests, gates every integration build) fires on
  held-work-no-completions and stays silent on advance/idle/stopped/restart, and survived four
  guard-deletion mutations; live, the eager arm on seed `4734674029169027864` in diagnostic mode
  tripped `CLASS2_STALL` 55 times while draining fully and the new check fired ZERO times, peak
  36.4s against the 150s bound. Silent on all five live runs (eager, coop x2, KEY cell x2).
- **Ordering coverage under churn** - `ChaosRevokeUnderWorkKeyOrderIT` ("w4key"), a NEW eager cell
  over the shared driver (matrix cells untouched; `processingOrder()`/`heavySleep()`/tick hooks
  promoted with byte-identical defaults), reusing `KeyOrderLedger`. First run rediscovered W5's tick
  trap at the matrix cells' 300-1000ms rate - single-delivery windows plus a fake 154s stagnation -
  so the cell runs W5's 1000-2500ms ticks, with the cost recorded in its `STORM_TICK_MIN` javadoc.
  Passing run on seed `4734674029169027864`: `comparedDeliveries=244022` of 250,608 (~2.9 epoch
  windows per key), `orderRegressions=0 overlaps=0`, zero loss, 608 duplicates, zero violations.
- **Sighting, not retuned**: on today's contended box the COOPERATIVE unordered arm also trips the
  150s bound (twice: 1 violation each, ~154s, drains fully under
  `-Dchaos.diagnoseStallRecovery=true`) - where the matrix run recorded 0. More weight behind the
  recorded decision to stop gating on the bound; nothing was changed to make anything pass.

**Decisions that are the owner's, not an agent's**

- **DECIDED 2026-08-19 by the owner: stop GATING on `LAG_STAGNATION_BOUND`.** The question asked was
  "shouldn't we just remove it - it isn't testing anything", and it holds up: a genuinely wedged run
  is already caught twice without it, by the quiet-phase `await()` failing at `QUIET_CAP` and by the
  scenario's `@Timeout(600)` behind that. The bound's only unique contributions are detecting
  earlier and naming the partition, bought at the cost of firing on every slow-but-correct run and -
  through `failFast` - destroying the evidence at the moment of detection.
  <br>
  **Keep the measurement, drop the gate.** The peak stagnation figure is worth having in an autopsy;
  it must not fail a build. `ProgressProbe` already has the mechanism - observer mode records
  violations without gating - so this is a mode change rather than a deletion, and the peak stays in
  the autopsy block either way.
  <br>
  What takes over as the gate is the shard-progress check: holding work while completing none is a
  real wedge, and unlike a duration it cannot fire on slowness. **Sequence matters** - land the
  shard check, prove it fires and stays silent in the right places, and only then stop gating on the
  bound. Doing it the other way leaves a window with no Class 2 gate at all. The three options
  previously listed here (raise the bound, shorten `HEAVY_SLEEP`, retire the eager arm) are
  superseded; the rejected move is unchanged - nudging a threshold until a run goes green, which the
  July recalibration already did once. Background:
  `test-class2-probe-asserts-timing-not-correctness.md`.
- **Whether the ordering ledger should gate.** `ChaosKeyOrderIT` is `@Tag("chaos")`, so ordering
  under real churn runs only on demand; what gates every build is `KeyOrderLedgerIT`, which checks
  the ledger's LOGIC against synthetic histories. So a genuine ordering regression under churn would
  not be caught by a normal build. Defensible - chaos runs are long, and a red chaos run is
  investigation material rather than a merge blocker - but it should be a decision rather than an
  accident.

**Evidence gaps, stated rather than hidden**

- **The four-cell matrix is one seed and one run per cell.** The direction is unambiguous (0 vs 53
  violations, 405 vs 2,421 duplicates) but the numbers are not repeat-measured. **Addressed rather
  than left implicit**: the README table now states the date, the seed, the scenario classes and
  one-run-per-cell in its caption, and carries the command to regenerate them - so a reader can tell
  what the numbers are worth and reproduce them. Repeating across seeds would still strengthen it.
  The owner's note is that the demo app supersedes this table entirely once it can run the
  configurations against a user's own topic, which is recorded in the caption as a forward pointer.
- **The drain arm was run twice** because the first run's log was truncated by a full `/tmp`; only
  the second is valid. Any re-measurement should filter maven output at source rather than writing
  raw logs to a shared tmpfs.

**Merge mechanics not yet done**

- **Review is DEFERRED on purpose, decided 2026-08-19 - do not request it yet.** `reviewDecision` is
  empty and a red `claude-review` is the expected state meanwhile, not a fault. The merge order is
  astubbs#204, then astubbs#31, then astubbs#57, then this PR, then astubbs#267, so three PRs land
  under this branch before it merges; reviewing now spends a review cycle on a tree that is about to
  change and buys a second one later. **Sequence: let those merge -> merge master here -> re-verify
  -> then request review** (`@claude review this` on the PR; it does not run on push). A human LGTM
  is required regardless of CI.
- **What the re-verification must cover when that happens**, because a clean textual merge proves
  nothing here: astubbs#31 is "replace a stale container at a reused offset after rebalance", which
  is this PR's own territory - epoch fencing, revocation, stale work - and astubbs#57 touches
  `PartitionStateManager`, where a counter has already drifted from ground truth once. Run
  `Rebalance857CommitSyncDeadlockProbeIT` (the 60/60 to 0/60 proof, 20 tests / ~5.6 min),
  `ShardManagerStaleContainerTest`, `OutForProcessingCounterDriftProbeTest` and
  `InstanceStallProbeIT`. Compiling is not evidence that the fencing argument survived.
- **A roadmap edit falls due at merge, and no gate will ask for it.** `docs/data/roadmap.yaml`'s
  `known-defects-cleared` entry says the deadlock's "mitigation drafted on astubbs#29", which stops
  being true when this merges. `roadmap-stage-gate.js` (arrived on master in `a78299794`) only fires
  for entries carrying a `pull_request:` field, and this entry has none, so it is out of reach by
  design. `stage` stays `in-progress` - this PR does not clear every known critical - so it is the
  `stage_detail` wording only, and it must not be edited before the merge, when it would assert
  something not yet true.
- **A merge strategy has not been recommended**, and the squash message has not been offered - both
  are owed before merge (`docs/merge-checklist.md`).
- **Duplicate-code and file-similarity reports** need reading once review runs; clones introduced by
  this PR are in scope, pre-existing ones are not.

**Noticed here, not this PR's to fix**

- `AGENTS.md` is ~498 lines against the ~400-line backstop it sets for itself. Pre-existing, and by
  its own rule that means something situational has crept in and wants relocating to a topic doc.

## Compounding ideas this work produced, 2026-08-19

Kept here rather than in `next-candidates.md` because they belong to this PR until it lands. Each
came from an INSTRUMENT being wrong rather than the product, which is why they generalise.

Each of these came out of an instrument being wrong rather than the product being wrong, which is
why they generalise past this investigation.

- **Truth probes for internal state, made routine** (`next-truth-probes-for-internal-state.md` owns
  this) - the chaos suite judged PC from outside, via committed offsets read by an admin client,
  while `WorkManager` and `ShardManager` expose the real answer publicly
  (`getNumberOfWorkQueuedInShardsAwaitingSelection`, `isRecordsAwaitingProcessing`,
  `isNoRecordsOutForProcessing`, `getNumberOfIncompleteOffsets`, and `pc.getWm()` is public). A test
  that infers internal state from an external signal will eventually infer it wrongly. Where a
  component knows the answer, ask it.
- **Measure both ends of anything you count.** A completion counter alone cannot distinguish
  "nothing is finishing" from "nothing is happening"; a fleet inside a 20s user function reads as a
  flat line while fully busy. Counting entry as well as exit made in-flight work visible and turned
  an apparent stall into an obvious back-pressure pause. Any counter used to judge liveness needs
  its partner.
- **Assert the property, report the timing.** A correctness suite gating on a duration turns every
  slow-but-correct run into a failure and every threshold into an argument
  (`test-class2-probe-asserts-timing-not-correctness.md`). Gate on completion, loss, duplicates and
  ordering; publish recovery time and peaks as measurements.
- **Granularity is part of a liveness check's correctness.** The existing `NO_PROGRESS` probe has
  the right SHAPE - while work remains, completions must advance - but is fleet-wide, so one wedged
  shard hides behind seventy-nine healthy ones. A check at the wrong granularity is not a weak check,
  it is a check for a different property.
- **A scale knob turns a stress test into an experiment.** `-Dperf.scale` on the capacity profiles
  exists because a measurement welded to one size can only answer the question that size happens to
  ask. The same applies to any workload constant that was chosen for one machine.
- **The harness cannot model a crash** (`parked-chaos-crash-fidelity-variant.md`) - every stop is an
  orderly close, so the most-reported confluentinc#857 shape is the one no scenario produces.
- **Run-mode experiments belong in the demo app** (`branch-polyglot-demo-ideation.md`) - the
  assignor x stop-mode matrix is a user-facing result, and the harness that produced it is a
  ready-made engine for the bring-your-own-topic direction.

## Related

- `docs/inflight/bug-857-family.md` - which defects sit behind the one upstream symptom
- `docs/inflight/test-857-revoke-under-work-sightings.md` - the replays, the four arms, the matrix
- `docs/inflight/test-class2-probe-asserts-timing-not-correctness.md` - the probe critique
- `docs/inflight/parked-chaos-crash-fidelity-variant.md` - why no scenario can model a crash
