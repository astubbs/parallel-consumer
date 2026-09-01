# astubbs#29 - the confluentinc#857 deadlock fix, and what measuring it taught

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
<!-- post-merge: exempt-file - this note IS the record of astubbs#29; it exists to describe that PR's in-flight state and is deleted when the PR lands, so every mention of it here is the subject, not a stale tense -->

## START HERE - handoff, 2026-08-27

**The master merge has happened.** It was asked for directly, ahead of the dependencies landing,
which overrides the 2026-08-19 "one merge at the end" decision recorded in `81e08e100`. That
decision's reasoning was that merging early resolves the same conflicts repeatedly and resolves them
differently from how the still-open PRs will - so **the cost is now real and lands on whoever merges
next**: astubbs#57, astubbs#322 and astubbs#267 will each meet these files again, and their
resolutions may differ from the ones here. The merge commit `d2cbf33bf` states every resolution and
why, so a later merge can follow rather than re-derive them.

**Both findings the aborted trial merge recorded came true, exactly as written.**
`ConsumerManagerCommitRetryBudgetTest` needed the `ThreadConfinedConsumer` wrap at three sites (no
conflict marker showed it - only `test-compile` did), and `bug-857-family.md` conflicted
structurally. The second was NOT resolved by taking master's file: neither side is a superset, so it
was rebuilt from both. Master's seven dated sections and this branch's six analytical ones are all
present; taking master's file wholesale would have deleted the commit-mode discriminator table.

**SUPERSEDED 2026-09-01 - and it was wrong when written.** This paragraph said astubbs#57,
astubbs#322 and astubbs#267 "remain open (checked 2026-08-27)". All three are merged, and two of
them - astubbs#57 and astubbs#322 - landed on 2026-08-26, the day *before* that check claims to have
run. The check either did not happen or read something other than the PRs' states. Left visible
rather than deleted, because a stale status line that nobody notices is exactly what produced the
sentence above it.

**DISCHARGED 2026-09-01: `PCMetrics.java` now carries master's version, which is what the
condition asked for.** The instruction in `835b593cf` was conditional - *merge master AFTER
astubbs#57 lands, then take master's side*. astubbs#57 merged on 2026-08-26 and today's master merge
took it, so the condition is met and the action is done. What follows is the reasoning as it stood
while the condition was still open; it is kept because it explains why the order mattered. Master's
`removeMeter` today has no guard at all, so taking it now would delete this branch's teardown fix and
reintroduce the throwing-registry defect. The file did not conflict, so this branch's version
survived untouched, which is the right state. **When astubbs#57 lands, merge master again and take
master's side then** - the reasoning is in `835b593cf` and is unchanged.

**Still true, and it refers to the 2026-08-27 merge - a LATER master merge happened on
2026-09-01 and has had no behavioural verification either.** The tree compiles (`./mvnw
test-compile`, all modules including test-integration) and `bin/check-all.sh` passes 15/15, but the
deadlock probe, `ShardManagerStaleContainerTest`, `OutForProcessingCounterDriftProbeTest` and
`InstanceStallProbeIT` have not been run against the merged tree. A compile is not evidence about
behaviour, and this branch's whole record is about not mistaking one for the other.

Worktrees: `.claude/worktrees/pr29` on the mac; `sweep-29` on the linux box holds the pre-merge state.

**Two pieces have been cut out into their own PRs, and this branch now depends on both.**
astubbs/parallel-consumer#375 carries the three probes and the brokerless harness they share -
lifted unmodified, green on master, and it is where `OutForProcessingCounterDriftProbeTest` now
lives, so the deleted revoke-time counter adjustment cannot be re-proposed without meeting the
evidence that killed it. astubbs/parallel-consumer#376 carries the back-pressure pause derived from
Kafka rather than mirrored in a field, with its write-up.

When each lands, **take its version rather than keeping this branch's copy** - the same rule already
recorded for `PCMetrics.java` and astubbs#57, and for the same reason: the extracted copy is the one
that got reviewed on its own merits. astubbs/parallel-consumer#376's cooperative-pause test passes on
master as well as here, so do not read it as a control arm for anything; it guards against
reintroducing the reset-on-assignment this branch once had.

The decomposition those came from is `docs/plans/2026-08-18-002-fix-857-revoke-path-cluster-decomposition-plan.md`,
which is a dated record and is deliberately NOT being rewritten as pieces leave - it says what was
true on 2026-08-18, and two of its four clusters have moved since (cluster 3 deleted outright,
cluster 4 resolved by deleting the mirror rather than gating it). Read it with this paragraph beside
it.

## MERGE IS PAUSED - operator decision, 2026-09-01

> **UPDATE, same day: the question this hold was placed on has been ANSWERED, and the hold is still
> the operator's to lift.** `Performance Tests` passed at `92c5d5b70` with `MultiInstanceHighVolumeTest`
> at 76,950 rec/s against 43,552 in the failing run, the only main-code difference being the
> control-loop fix, the neighbours within 1-3%, and the capacity profiles re-enabled ahead of it
> rather than skipped. The table and the caveats are in
> `docs/solutions/performance-issues/slf4j-defers-formatting-not-argument-evaluation-2026-09-01.md`. It is one run per side against
> a 1.54x instrument spread - strong, not conclusive. Nothing here lifts the hold; the rest of this
> section is kept as written so the reasoning that produced it stays readable.

**Do not merge this PR, and do not treat a green CI as clearing the hold.** It is paused until the
`Performance Tests` failure is understood, because that failure is currently unexplained rather than
merely inconvenient.

What is ruled out, by measurement rather than argument: **lane composition** (the capacity profiles
that shared a JVM with the throughput test are now `@Disabled` and cost 0.020s - it still failed) and
**runner speed** (the neighbouring tests in the same run are within 5% of the passing baseline while
the throughput test is 39% down; a slower machine slows everything proportionally).

That left this branch's tree as the only remaining difference between a passing run and a failing one
- and **a mechanism on this tree has since been found**, which is what changed on 2026-09-01.

`handoff/enable-large-number-of-instances` was merged in for it (merge commit's body carries the
detail). The control loop passed a shard-wide sum as a plain `log.trace` argument: SLF4J defers
formatting, not argument evaluation, so it ran every pass at every level, and it scales with in-flight
key cardinality while the loop spins fastest under saturation. Both costs peak together, and the
failing test is the only `KEY`-ordered member of the lane - which fits the selectivity that ruled
runner speed out. `docs/solutions/performance-issues/slf4j-defers-formatting-not-argument-evaluation-2026-09-01.md` owns it.

**Fitting is still not measuring.** The instrument's spread across identical code is 1.54x, and none
of it reproduces on a development machine, where this tree gave 73,722 rec/s alone and 72,498 in the
full lane. What the merge does change is that the hypothesis is now testable *here*: the lane runs
this tree, and this tree now has the fix, so the next `Performance Tests` run is the first
like-for-like read on it. One run inside a 1.54x spread is evidence, not a verdict, in either
direction.

**An earlier claim on this branch that there is no product regression has been WITHDRAWN.** It rested
on a local like-for-like pair, and was retracted once the neighbour timings showed the CI machines
were comparable. Treat it as unproven in both directions.

The measurement that would settle it - `MultiInstanceHighVolumeTest` alone, on CI, on this tree and on
master, more than once per side - has still never been run, and the fix above does not remove the need
for it. `docs/inflight/test-perf-lane-asserts-a-deadline-on-a-varying-machine.md` carries the full
evidence, the two ruled-out explanations and the wrong paths already taken.

**All three capacity profiles are now ENABLED in the gating lane, operator decision 2026-09-01.**
They were `@Disabled` here for three days because the lane gates and a capacity profile's output is a
rate rather than a verdict. That is reversed deliberately: a profile that silently degrades teaches
nobody anything, and a shifting baseline is what we want to be told about. It also makes the next
`Performance Tests` run the first real read on the control-loop fix. The cost is accepted - an
unlucky run blocks a merge, and `largeNumberOfInstances` was measured at one failure in ten on Linux
*before* that fix existed, so whether the fix moves that rate is itself now under test.

## NEXT TASKS, in order

**CLOSED 2026-08-31 - whether the fix covers the COOPERATIVE revoke path.** It was an inference
(the fix sits on the revoke path, so it should be assignor-independent) until measured. A seed
replay of the family's twentieth capture could not settle it - a chaos seed fixes the conductor's
schedule, not the poll-versus-control interleaving - so it was asked on the deterministic probe
instead, which forces the window open. Four cells, {fix, pre-fix control} x {eager, cooperative},
twenty repetitions each: both fix cells pass throughout, both control cells fail throughout. The
pre-fix cycle is therefore not eager-specific, which fits the twentieth capture being a cooperative
revoke. Cells, caveats and the two instrumentation faults caught along the way are in
`docs/solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md` and
the probe's own Calibration status javadoc.

**DONE 2026-09-01 - master is merged and all three predicted traps fired exactly as written.** The
merge commit records each resolution; in short:

- **The six scripts duplicated with ZERO conflicts reported**, as predicted. The originals are
  deleted. Diffing first was not ceremony: the pairs differ by 33-93 lines and the `exp-` copies are
  the improved lineage, and two of the six had no twin at all because
  astubbs/parallel-consumer#381 had RETIRED them - re-adding those would have resurrected
  instruments master deliberately buried.
- **`getAssignmentSize` broke the build**, which is the good case. Fixed as prescribed rather than by
  reverting: the public getter is gone and the failure-path dump drops the field, with the reasoning
  left at the site.
- **`getConsumerClass` did not come back.** Master's side won, which is what the note demanded.

Of the thirteen conflicts, ten resolved to master after verifying it was the superset or successor
each time, one (`AmbientProbeExtension`) was a genuine both-sides merge, and `ChaosChurnStormIT` took
master's corrected calibration text over this branch's retracted claim.

1. **Settle whether the stall detector MISSES real failures - REOPENED 2026-08-31.** It was closed
   on 2026-08-28 after eight replays on the pre-astubbs#344 tree, where the silence had been seen:
   five failures, every one caught by some detector, and one caught by a DIFFERENT detector while
   `NO_PROGRESS` sat at zero. That explained the original reading - it counted only `NO_PROGRESS`
   hits, so a failure caught elsewhere looked like a failure caught by nothing - and the verdict was
   that the detector over-fires on slow runs and that is the whole of its fault.
   **The overnight torture run then produced the case that closure said did not exist:** cycle 16
   failed on a bare awaitility timeout with no detector firing at all. So the detector over-fires on
   slow runs AND can stay silent on a real failure, which is the combination that matters, because
   the suite goes green on its silence. `bin/exp-audit-stall-detector-silence.sh` classifies a failing
   run by what actually caught it; re-run it against the torture corpus rather than the astubbs#344
   arms, since the corpus is where the counter-example lives.

2. **CLOSED 2026-09-01: the drain result is confirmed, and its instruments are retired.** This asked
   for a second firing because the demotion rested on one. `ChaosChurnStormIT`'s calibration javadoc
   now records the backlog draining on ALL SIX firings collected, which settles it. The two runners
   built to ask the question were deleted on astubbs/parallel-consumer#381 and their method written
   up in `docs/solutions/test-flakiness/` - so do not go looking for them.

3. **Migrate the records out of this note, then fix this PR's title and body.** The note is deleted
   when the PR lands and it holds the week's findings; the title claims the symptom rather than the
   mechanism, and the body is stale in four ways.

4. **The transactional revoke wait - NOT YET.** It carries astubbs#44, which holds upstream's
   `verified bug` label - one of a couple of dozen that do; an earlier revision of this line called
   it the only one, which is false. astubbs#257 and astubbs#262 are open over the same area.
   astubbs#262 sets out to prove or falsify every documented transactional guarantee, which will
   likely reframe the question. Chasing it first means resolving against a moving target.

5. **One unreproduced field report, not two.** astubbs#177 was CLOSED on 2026-09-01, leaving
   astubbs#175. astubbs#352, the commit-failure seam, addresses its shape and is already open - but
   that issue's own Fork status already names a likely cause (an unhandled
   `RebalanceInProgressException`) and rates it very likely fixed on the fork, so confirming that
   against the reporter's version outranks waiting on astubbs#352.

**Still owed at merge:** the squash message. The strategy is settled - squash, because the separable
workstreams have already left as their own PRs and what remains is one idea with a long research
narrative. The roadmap half of this item is DONE: `known-defects-cleared`'s `stage_detail` no longer
says "mitigation drafted" and now records a fix measured against a one-term control on both
assignors.

**DONE 2026-09-01: the compounding pass is discharged.** Both learnings landed - the metrics-teardown
one in `docs/solutions/runtime-errors/a-throwing-meter-registry-kills-the-poll-thread-and-strands-close.md`,
and the rest across `docs/solutions/best-practices/` and `docs/solutions/workflow-issues/`. The note
that tracked the pass was deleted once it had nothing left, as it said it should be.

**Do not re-attribute the eager `CLASS2_STALL` sightings to this PR.** Four measured arms say
otherwise; details below and in `test-857-revoke-under-work-sightings.md`.


Context `gh` cannot give you about PR astubbs/parallel-consumer#29
(`bugs/857-paused-consumption-multi-consumers-bug`). Delete this file when the PR merges, promoting
anything below that is still wanted into `next-candidates.md`.

## What the PR is, in one line

Fixes the AB-BA deadlock between the poll thread's `onPartitionsRevoked` and the control thread's
`commitOffsetsThatAreReady`, proven on its own instrument at 60/60 failures before and 0/60 after.

## Still open on this PR, 2026-08-19

Ordered by what blocks a merge. **This file should have existed from the branch's first commit and
did not** - the April investigation log (`docs/BUG_857_INVESTIGATION.md`) was retired in `69a670de4`
into the solutions write-up and the per-mode inflight split, which kept the SETTLED knowledge and
left the live threads without a home. Append here as you go rather than reconstructing later.
<!-- file-refs: N/A - the sentence records that this file was retired; naming it is the point -->

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
  <br>
  **Still unimplemented on this branch as of 2026-08-20, deliberately.** Flipping the gate right
  after the sightings above, with no demonstrating chaos run (fires on a wedge, silent on a
  slow-but-correct drain), would be indistinguishable in the history from tuning-to-green - the
  rejected move. It wants its own small change where that demonstration is the content; the
  principle itself is now durable in `docs/investigating.md` ("Designing a liveness check").
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

- **Review deferral: its precondition is now MET, 2026-09-01.** It was deferred on 2026-08-19 until
  astubbs#204, astubbs#31, astubbs#57 and astubbs#267 had landed, because reviewing a tree about to
  change spends a cycle and buys a second one later. All four have merged, master has been merged in,
  and the deadlock fix was re-verified afterwards on a four-cell control across both assignors. So
  the sequence is discharged and review is now the right next step - `@claude review this` on the PR,
  which does not run on push. A human LGTM is still required regardless of CI.
  **Two NEW parents now sit under this branch** and were not part of the 2026-08-19 order:
  astubbs#381 and astubbs#393. The dependency gate blocks the merge until they land, but neither
  changes this tree, so neither is a reason to defer the review again.
- **What the re-verification must cover when that happens**, because a clean textual merge proves
  nothing here: astubbs#31 is "replace a stale container at a reused offset after rebalance", which
  is this PR's own territory - epoch fencing, revocation, stale work - and astubbs#57 touches
  `PartitionStateManager`, where a counter has already drifted from ground truth once. Run
  `Rebalance857CommitSyncDeadlockProbeIT` (the 60/60 to 0/60 proof, 20 tests / ~5.6 min),
  `ShardManagerStaleContainerTest`, `OutForProcessingCounterDriftProbeTest` and
  `InstanceStallProbeIT`. Compiling is not evidence that the fencing argument survived.
- **DONE 2026-09-01: the roadmap edit.** `known-defects-cleared`'s `stage_detail` no longer says
  "mitigation drafted"; it records a fix measured against a one-term control on both assignors, and
  states that the family is NOT closed by it because the transactional revoke wait (astubbs#44) is a
  separate defect in a commit mode this fix cannot reach. `stage` stays `in-progress`, as this entry
  required. The 2026-08-19 constraint - do not edit early, because it would assert something not yet
  true - is respected: the wording says the fix is measured and unmerged, which is what is true now.
  Still out of reach of `roadmap-stage-gate.js`, which only fires for entries carrying a
  `pull_request:` field.
- **DONE 2026-09-01: merge strategy recommended and the squash message written.** Squash, not
  re-cut: the separable workstreams have already left as astubbs#375, astubbs#376, astubbs#381 and
  astubbs#393, so what remains is one idea with a long research narrative - and a re-cut means a
  force-push onto a PR carrying inline review comments, which re-anchors or orphans them. The
  message lives in the session scratchpad, deliberately NOT in the PR body, per
  `docs/merge-checklist.md`.
- **Duplicate-code and file-similarity reports** need reading once review runs; clones introduced by
  this PR are in scope, pre-existing ones are not.

**Noticed here, not this PR's to fix**

- `AGENTS.md` is ~498 lines against the ~400-line backstop it sets for itself. Pre-existing, and by
  its own rule that means something situational has crept in and wants relocating to a topic doc.

## Compounding ideas this work produced, 2026-08-19

Four more from the split and hand-off, 2026-08-20 - these came from the PROCESS rather than the
defect, which is why they were not in the first list.

- **Extracting code to a minimal branch is a bug-finding technique, not just a review-simplification
  one.** `PCMetrics.close()` iterates the registry directly instead of going through the guarded
  `removeMeter`, and that hole was invisible where it was written, because its only caller wraps the
  call in a try/catch. It surfaced the moment the change was lifted onto a branch without that
  wrapper. **A contract that holds only because every caller guards it is not a contract** - and the
  cheapest test of whether one is real is to move it somewhere its callers are not. Worth doing
  deliberately for any guarantee that matters, not just when a PR needs splitting.
- **Verify the assumption a split rests on before offering it.** The metrics extraction was offered
  on the belief that its test would pass on master without this branch's `doClose` guards. It did
  not. Two minutes of checking found a real hole; asserting it would have shipped a fix that only
  worked in the place it came from.
- **A test that fails for the "wrong" reason is often the one earning its keep.** The exploding-registry
  test failed twice before it passed, and the first failure - the registry killing the instance before
  the close path was reached - IS how the revoke-path exposure was found. A narrower test written to
  pass first time would have shipped the `finally` guard and missed the larger defect. Ask why a test
  fails before making it not fail.
- **Encode a merge order as `depends on` lines, not as prose.** The PR-dependency gate blocks a child
  until every parent merges, so an ordering that matters becomes mechanically impossible to get wrong
  rather than merely written down. This is `docs/agent-harness.md`'s principle applied to PROCESS
  instead of code, and it replaced a note that was already being ignored - by its own author, who
  merged master early anyway.

**And one about this file.** It did not exist until the work was nearly done, so learnings landed in
topic docs or nowhere and had to be reconstructed at the end - several survived only because the
owner asked the right question at the right moment. **A PR earns its working note at its first
commit, not its last.** The cost is one file; the thing it buys is that a finding gets written where
it happens rather than recalled later.

**Landed 2026-08-20**: the rule now lives in `docs/inflight/AGENTS.md` ("A PR earns its working
note at its first commit") and `.github/PULL_REQUEST_TEMPLATE.md` carries a checklist box for it,
which the existing `PR Checklist` gate enforces on every human PR - so a missing note is caught at
PR-open rather than never.


Kept here rather than in `next-candidates.md` because they belong to this PR until it lands. Each
came from an INSTRUMENT being wrong rather than the product, which is why they generalise.
**Promoted 2026-08-20**: the both-ends, assert-the-property and granularity lessons now have their
durable home in `docs/investigating.md` ("Designing a liveness check") - do not promote them again
at merge; the remaining items below keep their existing owners or await promotion.

- **Truth probes for internal state, made routine** (`test-truth-probes-for-internal-state.md` owns
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
- **The harness cannot model a crash** (`test-chaos-crash-fidelity-variant.md`) - every stop is an
  orderly close, so the most-reported confluentinc#857 shape is the one no scenario produces.
- **Run-mode experiments belong in the demo app** (`branch-polyglot-demo-ideation.md`) - the
  assignor x stop-mode matrix is a user-facing result, and the harness that produced it is a
  ready-made engine for the bring-your-own-topic direction.

## Already fixed

**Everything below this heading is settled, and is kept rather than deleted because the reasoning is
what a later reader needs.** `bin/check-pr-ready.sh` counts items ABOVE this heading as outstanding,
so moving a section here is how a resolved thing stops being reported as open work. Adding a section
above it again is a claim that it is live.

Settled as of 2026-09-01: all six dependencies this note was waiting on - astubbs#57, astubbs#267,
astubbs#322, astubbs#323, astubbs#324 and astubbs#325 - have merged, which resolves the merge-order
decision and the PCMetrics instruction outright. The astubbs#119 question is answered rather than in
progress: it does not close with this PR, and the section states why.

## Does astubbs#119 close with this PR? ANSWERED: no - settled 2026-09-01

**Answer: no, and the reason is that astubbs#119 is a symptom bucket rather than a defect.** It was
provisional when written and is not any more: the sighting inventory and the merged-PR mapping were
gathered, and the transactional revoke wait (astubbs#44) was established as a separate defect in a
commit mode this PR's fix cannot reach. Closing astubbs#119 here would take the rest of the family
with it.

### The reported symptoms are not one mechanism

Read from upstream confluentinc#857's own comments rather than the mirror's summary, which does not
carry them. At least four distinguishable behaviours are reported:

1. **Paused subscription with growing lag** - the April 2025 log. sangreal reads it as stale
   containers pausing the subscription.
2. **Not paused, but polling zero records** - the July 2025 log, from the same reporter. sangreal
   says so explicitly and in the same breath says he cannot find a cause: *"actually is not paused.
   but somehow polling 0 records, which is weird"*. Nothing in this family explains it.
3. **Out-of-range fetch position** - netroute-js, 2025-07-21: two records permanently stuck, a
   redeploy no longer clearing it, and `Fetch position FetchPosition{offset=8, ...} is out for`.
   That is offset territory, adjacent to confluentinc#894 / astubbs#121, not to a lock.
4. **Lag that does not clear although the records were processed** - netroute-js, 2025-12-08. That
   is the committed offset failing to advance, which is not consumption stopping at all.

A fifth is reported on THIS PR and has never been chased: dmironowicz, 2026-07-24, has *"processing
pauses for a small number of partitions... and resumes after few hours"*, and when asked directly
whether a rebalance triggers the recovery answered *"I don't see any rebalancing activity around
that time"*.

**The deadlock this PR fixes cannot produce that.** The AB-BA cycle needs `onPartitionsRevoked` to be
executing, so it needs a rebalance, and it is reachable only in `PERIODIC_CONSUMER_SYNC`.

### The best candidate for the no-rebalance pause is the load gate, and it is already fixed

Not the pause mirror, and not the paused-partition cache - both were checked:

- The cache (`ConsumerManager.pausedPartitionSizeCache`) is refreshed in `updateCache()` on **every**
  poll, and a paused consumer still polls (the paused long poll is the loop's sleep). So it does not
  freeze while the consumer is paused, which is the shape a stale-cache stall would need.
- The mirror is gone as of astubbs#376, and on master it was never reset on assignment anyway, so
  the cooperative-retention failure needed a rebalance - which this symptom does not have.

The gate that decides whether a paused poller is woken is `maybeWakeupPoller`:
`!wm.isSufficientlyLoaded() && brokerPollSubsystem.isSubscriptionsPausedForBackPressure()`. **If the
load term is stuck high, the second term stays true forever and nothing ever wakes the poller** - no
rebalance required, no error logged, and the poller keeps long-polling on paused subscriptions so it
looks alive throughout.

That is precisely what astubbs#336 describes and fixes. Its own words: the drift *"did not
misreport, it mis-gated record intake, and enough phantoms keep intake paused until a restart clears
the counter"*, with both drifts reproducing **single-threaded**. astubbs#373 then took the shard's
available-count spend to a compare-and-set.

**Both landed on master after this branch's analysis was written, and neither is part of this PR.**
So the reporter symptom this PR cannot explain may already be closed by work that is not in it.

### What this means for closing astubbs#119

Closing it on this PR would take the other four mechanisms with it. The issue's own `## Fork status`
also needs correcting before anything is closed: it cites *"Live evidence it is still present:
`RebalanceEoSDeadlockTest` failed once under a 20-run stress hunt"*, and this branch established that
that test runs `PERIODIC_TRANSACTIONAL_PRODUCER` - a mode the cycle cannot close in - and counts a
latch by overriding a method the fixed revoke path no longer calls. It is inverted: 5/5 on the defect
arm, 5/5 failing on the fixed arm. The issue's headline evidence for "still open" is not evidence.

### The reproducer nobody has measured

amrynsky, 2026-01-11, on upstream: `MultiInstanceRebalanceTest.largeNumberOfInstances` is disabled by
default and *"every other run of this test is failing"* with `No progress beyond 285591 records after
11 rounds`. It is the closest thing to a repeatable in-repo instance of the reported symptom and it
was switched off. **Both halves of that changed on 2026-09-01**: it is enabled in the gating lane,
and the rate has been measured - one failure in ten on Linux, failing as a rebalance stall rather
than the overload an earlier sweep produced. What has NOT been established is whether the residual is
Kafka's or PC's; the ambient probe found a coordinator blocked on a member that stopped answering,
and the members are PC instances.
`test-largenumberofinstances-residual-failures-measured-not-explained.md` owns that thread.

### The experiment that would settle the no-rebalance pause

Stated before running it: take the reporter's shape - back-pressure engaged, no rebalance - and drive
the load gate to a phantom count on `master` **before** astubbs#336, then after. The prediction is
that the pre-astubbs#336 arm strands with the poller paused and `isSufficientlyLoaded()` true against
an empty pipeline, and the post arm does not. If the pre arm does NOT strand, the load gate is not
this symptom's mechanism either and the reading above is wrong - say so here.

### The sighting ledger, read end to end: the family is far smaller than its length suggests

Twenty numbered sightings plus a dozen unnumbered captures. Sorted by what survives scrutiny:

**Withdrawn or explained - not evidence of anything (roughly half the ledger).** The whole
`CLASS2_STALL` line went on 2026-08-25, when the pre-declared discriminator finally ran and both
nominated seeds **crossed the bound and then drained completely**: *"Every `CLASS2_STALL` entry above
is a timing measurement, not a family sighting."* With it went the "~154s constant as corroboration"
reading (*"That is arithmetic, not signal"*). Separately retired: the seventh (a test defect - an
assertion comparing a commit offset to a completion counter), the fourth (astubbs#292's harness
double-start), and the `ChaosKeyOrderIT` `ZOMBIE_MEMBER` (a calibration gap - the scenario does not
inherit `disableRebalanceDwellViolation()`, and it overshoots by 2.7-4.4%).

**Positively confirmed, and this is new.** Six independent thread-dump captures between 2026-08-26
and 2026-08-27 show the poll thread **BLOCKED on the `commitCommand` monitor** held by the control
thread, with frames resolving to `commitOffsetsThatAreReady` / `onPartitionsRevoked` - astubbs#29's
AB-BA pair, observed rather than inferred. Across both the eager and cooperative arms, on five
different PR branches including ones that compile no Java, and after per-PR VMs removed the last
co-residency explanation. **The deadlock this PR fixes is real and is being hit.**

**Still genuinely open, and none of it is astubbs#29's:**

- The `ChaosChurnStormIT` `NO_PROGRESS` / `ZOMBIE_MEMBER` line in `PERIODIC_CONSUMER_ASYNCHRONOUS`,
  where the AB-BA cycle cannot close and the transactional wait cannot run - *"either a fourth defect
  or something outside the product"*. astubbs#373's shard-counter fix was tested against it directly
  and **it fired anyway on that branch's own head**, so that fix is necessary-not-sufficient.
- The unbounded transactional revoke wait, carrying astubbs#44 - which holds upstream's
  `verified bug` label - one of a couple of dozen that carry it. Its design decision is explicitly
  unsettled.
- astubbs#175 and astubbs#177, two field reports of `Timeout waiting for commit response`, open for
  months with no reproduction attempt and no owner.
- The no-rebalance pause described above.

### The verification this PR owes, and has not done

**Six seeds now exist that captured the exact deadlock, and not one has been replayed with the fix
applied.** The ledger names this as the outstanding step in its own words - *"What is wanted next is
no longer a discriminator but a verification"* - and the seeds are on file:
`7728704565782280867`, `2867310537409227917`, `3649400609451361367`, `1355976854716465757`,
`3135248854766953145`, `3198328355855848347`, `818084281700661522`.

That is the cheapest, highest-value experiment available to this PR, and it is worth more than any
further chaos-suite runs: it converts "the A/B soak says the symptom stops" into "the mechanism we
photographed is gone".

**A prediction is already on record for after this lands**, and it must not be quietly dropped: the
Class 2 findings should **continue at roughly the same rate**, because they are the bound meeting the
load and no deadlock fix touches that. *"If they instead drop off, this reading is wrong"* - and the
2026-08-25 section then needs revisiting.

### Verdict on closing astubbs#119

**Land this PR, verify it against a captured seed, and do NOT close astubbs#119 on it.**

The issue is a symptom bucket. This PR closes one mechanism in it - now directly observed, which is
much stronger ground than the ledger was on a week ago. Four others remain, at least one of which
(the async churn-storm line) is unexplained by every known member of the family, and one of which is
a reporter on this very PR whose symptom has no rebalance in it at all.

What should happen on the issue instead: record that the deadlock is confirmed and fixed, list the
remaining mechanisms by name, and correct the `## Fork status` sentence that still cites
`RebalanceEoSDeadlockTest` as live evidence - that test is inverted and cannot support the claim.

### The seed replay, 2026-08-27: first verification of the fix against a captured deadlock

Seed `2867310537409227917` - the 2026-08-26 cooperative-arm capture, one of the six where a thread
dump caught the poll thread `BLOCKED` on the `commitCommand` monitor - replayed on this branch, which
carries the `tryLock()` fix, with `-Dchaos.seed=2867310537409227917
-Dit.test=ChaosRevokeUnderWorkCooperativeIT`.

**Result: green.** `probe violations=[]`, and **not one `BLOCKED` line in the whole run** - the
signature that defined the capture did not recur.

**This is one run, and it is not proof.** The ledger already records that this capture is
intermittent: the commit immediately after one of the six passed with no code change at all, which is
why it says *"the trigger is the schedule, not the tree"*. A single green on an intermittent failure
is equally consistent with the schedule not landing on the window. What would settle it is repetition
on this seed plus a control arm at the same seed on a tree WITHOUT the fix.

**A pre-registered prediction held, and that is worth more than the green.** The recorded prediction
was that Class 2 findings would **continue at roughly the same rate** after the fix, because they are
the timing bound meeting the load and no deadlock fix touches that - *"If they instead drop off, this
reading is wrong."* They continued: `CLASS2_STALL/LAG_STAGNATION` observations fired, all non-gating,
all in the familiar band just past the 150s bound, while the run passed. The 2026-08-25 demotion
survived its own test rather than being quietly vindicated by a clean run.

**Next, in order of value:** repeat this seed; then the control arm without the fix; then the
remaining five seeds. Only the control arm turns "did not recur" into "the mechanism is gone".

### RETRACTED, same day: the replay above proves nothing, and the control arm is why

The green run recorded above was reported as "the signature did not recur". **That reading is
withdrawn.** A control arm was then run on an identical tree with exactly one term changed - the
revoke side made to BLOCK on `commitLock` instead of declining, restoring the pre-fix AB-BA shape.

**Prediction, recorded before the run: the control would stall. It did not.** It passed green, with
no blocked lines, on the same seed.

Two arms, opposite on the one term that is supposed to decide the outcome, both green. So the
experiment has no discriminating power as configured, and the fixed arm's green was never evidence.

**The tell was available in the fixed arm's own log and I did not check it before reporting.** Neither
`Skipping offset commit during partition revocation` (the declined path - the arm that IS the fix) nor
`Acquired commitLock on revoke` (the uncontended path) appears even once. `tryCommitOffsetsOnRevoke()`
did not execute at all, so the revoke-with-pending-commit window never opened on this box.

This is the exact failure
`docs/plans/2026-08-18-002-fix-857-revoke-path-cluster-decomposition-plan.md` warned about in
advance: *"A clean fixed arm with a zero skip-count would be indistinguishable from a probe that
never opened the window, which is exactly how this fix looked unproven for four months."*

**Consequence for any future soak: a rep in which the window did not open is not a data point.** It
must be counted separately, never folded into a pass rate - otherwise twenty reps manufacture the
same false green as one did here, with twenty times the confidence. The signature to gate on is the
execution of the revoke commit path, not the test's exit code.

One thing does survive: the Class 2 observations continued in the fixed arm while the run passed,
which is what the pre-registered prediction said would happen. That prediction concerns the timing
bound and not the deadlock, so it is unaffected by this retraction.

### VERIFIED, 2026-08-27: the fix holds against a red control, on the deterministic instrument

The chaos-seed replays earlier in this section could not open the window and were retracted. This is
the experiment that discriminates. Instrument: `Rebalance857CommitSyncDeadlockProbeIT`, which forces
the overlap by construction rather than waiting for a schedule to land on it - a 4s dwell in
`onPartitionsRevoked` against a 1s commit interval, 500 records at 25ms to keep the manager dirty.
`@RepeatedTest(20)`. Two trees differing by exactly one term: the revoke side declines with
`tryLock()` (FIXED) or blocks on `lock()` (CONTROL).

| Arm | Failures | `Skipping offset commit during partition revocation` | `Timeout waiting for commit response` |
|---|---|---|---|
| CONTROL - revoke blocks | **all repetitions** | 0 | present, several per failure |
| FIXED - revoke declines | **none** | **fired, ~2 per repetition** | 0 |

**The skip-log count is what makes the green arm mean anything.** It is the INFO line on the
contended `tryLock` branch - the arm that IS the fix - so a non-zero count proves the window opened
and the fix declined it, rather than the probe never reaching the contended state. Zero on the
control is also correct: a blocking revoke never declines, it deadlocks.

This reproduces the 2026-08-18 A/B soak's result independently, on different hardware, and it is the
verification this branch has owed since April.

**Run it unforked.** `surefire.forkCount` is 1 by default but `1C` under `-Pci`, and forking one
broker per fork removes the window - which is how the suite went green while the deadlock sat
untouched. The pom warns separately that `-DforkCount` is silently ignored; only
`-Dsurefire.forkCount` works. So a run of this probe under `-Pci` produces a meaningless green.

**Read the run log, not the failsafe `.txt`.** That file is a few lines of summary and carries no log
output, so grepping it for the skip-log returns zero whatever happened. That mistake was made twice
here in one day and both times the zero looked like a closed window.

**What is still not established.** This proves the mechanism is closed under a forced overlap. It does
not establish a rate in the wild, and none of the six captured chaos seeds has been shown to refire
or not refire against the fix - those runs are dispatched but unread.

### The ledger's "three landed, one open" framing is badly out of date

astubbs#119's `## Fork status` still says three defects landed (astubbs#100, astubbs#80,
astubbs#108) and one remains. Reading the merged commit bodies on master, **at least six more
857-family mechanisms have been fixed since that was written**, and none appears in the family's own
accounting:

- **astubbs#346** - `handleFutureResult`'s staleness checkpoint and its acting reads were two
  separate `partitionStates.get(tp)` lookups, so a rebalance landing between them orphaned a
  retry-queue entry and `workIsWaitingToBeProcessed()` then **read true forever**. Its own body names
  this a confluentinc#857-family permanent stall. Both race arms red on the unfixed tree, green
  fixed, mutation-tested.
- **astubbs#345** - a `containsKey`-then-`get` pair in `ShardManager.removeWorkFromShardFor` raced a
  concurrent shard removal and **NPEd on the broker-poll thread inside the rebalance listener**.
  Poller death under KEY ordering is paused consumption by another name. 4/4 deterministic
  reproduction, fix-applied/fix-reverted flip.
- **astubbs#373** - the shard's available-count spend was inferred rather than owned; four
  instances of one class, including a **sign-reversed overcount** its body calls the
  "confluentinc#857 stall signature", reachable by revoking a failed record inside its retry delay.
- **astubbs#336** - the load gate, described above.
- **astubbs#344** - the offset encoder read `offsetHighestSucceeded` twice, so a completion landing
  between the reads could **silently mark still-incomplete offsets as complete**.
- **astubbs#349** - `PartitionState.dirty` written on the control thread and read unfenced on the
  poll thread; jcstress-measured, 0 anomalies in 4.29e9 samples after the fix.

**So the honest picture is not "one deadlock left". It is that the bucket has been drained steadily
from several directions, and the ledger never re-totalled.** Any close-out on astubbs#119 has to
account for these, and the issue's fork-status section has to be rewritten rather than appended to.

### A testable hypothesis for the async churn-storm line, which is the last unexplained one

The `ChaosChurnStormIT` sightings that nothing explains are all in
`PERIODIC_CONSUMER_ASYNCHRONOUS` - the mode where the AB-BA cycle cannot close and the transactional
wait cannot run, which is exactly why they were set aside as *"either a fourth defect or something
outside the product"*.

**astubbs#344's defect is live in that same mode and only that mode.** Its body says so: the
double-read of `offsetHighestSucceeded` is reachable in `PERIODIC_CONSUMER_ASYNCHRONOUS`, and its
consequence is offsets marked complete while still incomplete - which presents as records that are
never reprocessed and a fleet that stops making progress against its expected count. That is the
`NO_PROGRESS` shape.

**Prediction, recorded before running it:** replay the async `NO_PROGRESS` seeds
(`3086917415748208232`, `8603691233664838594`, `7543483068749855826`) on a tree that predates
astubbs#344 and on one that includes it. If the mode match is the mechanism, the pre arm reproduces
and the post arm does not. **If both reproduce, astubbs#344 is not it** and the fourth-defect
hypothesis survives intact - say so here rather than quietly dropping this paragraph.

Note the counter-example already on file, which is why this is a prediction and not a claim:
astubbs#373 was tested against this same arm and **it fired anyway on that branch's own head**, so
one plausible mode-matching fix has already failed to close this line.

### The cross-references, resolved

Both timelines paged in full. Almost all `referenced` events are this fork's own commit-citation
convention firing, not independent references. Three items actually matter:

**Upstream's own fix for this issue merged, and did not fix it.** confluentinc#882 (sangreal, merged
upstream 2025-08-07) reworked stale-container removal during partition reassignment. **This fork
carries that logic** - `ProcessingShard.removeStaleWorkContainersFromShard` and
`PartitionStateManager`'s `removeStaleContainers` are its shape. netroute-js reported on 2025-11-24,
after upgrading past it, that *"the problem is still there... probability seems to be reduced"* -
recurring after two months of uninterrupted traffic and triggered by a **broker leader election**,
not a consumer-group rebalance. So the stale-container theory has been tried, shipped, and outlived.

**confluentinc#875 / astubbs#183 is filed under this symptom on somebody's assumption, and looks
like a different defect.** The reporter describes a **silently skipped offset** - delivered
`[1,2,3,5,6,7...]` with 4 never arriving - lag then growing until consumption stops, and a restart
making the missing message reappear. No rebalance is mentioned anywhere in the report. It was linked
to confluentinc#857 by a third party (*"That's potentially the same issue"*), never by its own
reporter. **A skipped-then-recoverable offset is the shape of the offset-completion defects**
(astubbs#344 marking incomplete offsets complete; astubbs#108 recording a post-departure commit as
successful), not of a lock. It should be assessed on its own before being counted here.

**The no-rebalance case is genuinely rare, which makes dmironowicz's report more interesting, not
less.** Across every duplicate report on both timelines, the pause ties to a rebalance, a redeploy
or (once) a leader election. Only two exceptions exist: sangreal's July `pc_log` reading of *"not
paused, but somehow polling 0 records"*, and dmironowicz on this PR. Two independent observations of
a stall with no rebalance narrative is thin, but it is not nothing, and neither has ever been chased.

## What the PR is NOT, which took a day to establish

The eager `ChaosRevokeUnderWorkIT` `CLASS2_STALL` sightings were this family's leading candidates for
this PR, on the strength of `PERIODIC_CONSUMER_SYNC` being the one commit mode where the AB-BA cycle
can close. **They are not this defect, and probably not any defect.** The evidence is in
`test-857-revoke-under-work-sightings.md`: two recorded seeds reproduced 6/6 on the arm carrying the
fix, and the four-cell assignor x stop-mode matrix explains every sighting as eager reassignment
restarting in-flight heavy work until a commit watermark is legitimately pinned past a 150s bound.

## Interaction with `bug-shutdown-teardown-race.md`, checked 2026-08-19

That note lives on astubbs#57's branch and asks a question addressed to whoever is hardening
shutdown-under-load - which is this PR. Checked, and the answer is partial:

- **The consumer half is hardened here.** The note's race is teardown running while the broker-poll
  thread is still alive. Previously the control thread would close a consumer the poll thread was
  still using, a genuine data race. Now `tryClaimOwnership()` refuses to steal from a live owner, the
  close fails, and `doClose` catches it and warns with the user-visible cost - no LeaveGroup, so the
  group's next rebalance is delayed by up to `session.timeout.ms`. The `closeAndWait()` catch above
  it says the same thing in advance ("the consumer close below may legitimately refuse").
- **The metrics half is fixed at the metrics end by astubbs#57**, defensively, in `PCMetrics.track()`.
- **The question the note actually asks is still open**: whether `doClose` must guarantee the poll and
  worker threads are joined before the `finally` teardown, or whether that teardown should be guarded
  on join success. Neither PR answers it, and the note is right that it is a sequencing change rather
  than a local patch. Any teardown in that `finally` is exposed; metrics is simply where it was
  noticed.

**One small thing worth fixing here while the file is open**: `ConsumerManager.close()` calls
`tryClaimOwnership()` and discards the boolean, then closes anyway and relies on `checkThread`
throwing. Reading the result would let it log "the poll thread still owns the consumer" directly
rather than routing a foreseeable, expected condition through an exception. Same outcome, cheaper,
and it stops a normal shutdown-race outcome looking like a bug in a stack trace.

## DO NOT merge master until every dependency has landed - decided 2026-08-19

**One merge at the end, not one per dependency.** The order is astubbs#323, astubbs#324,
astubbs#325, astubbs#57, astubbs#322, astubbs#267, then this branch. Merging master as each lands
means resolving the same conflicts repeatedly and, worse, resolving them DIFFERENTLY from how the
still-unmerged PRs resolve them - spending effort to arrive at an answer another branch has already
got right.

A trial merge on 2026-08-19 was aborted after demonstrating exactly that, twice, and what it found is
kept here so the real merge does not re-derive it:

- **`ConsumerManagerCommitRetryBudgetTest` will not compile.** It arrived with astubbs#204 and
  constructs `new ConsumerManager<>(mockConsumer, ...)` at three sites, where this branch changed
  that first parameter to `ThreadConfinedConsumer<K, V>`. Wrapping it -
  `new ConsumerManager<>(new ThreadConfinedConsumer<>(mockConsumer), ...)` - compiles and preserves
  the test's semantics, because an UNCLAIMED consumer admits any thread. **Confirm astubbs#267 has
  not already changed this** before applying it; that PR touches the same area.
- **`docs/inflight/bug-857-family.md` conflicts structurally, not textually.** This branch split the
  ledger by commit mode while master kept appending sightings, so the two diverge across ~340 lines.
  Take MASTER's side: it carries the tenth and eleventh sightings, and this branch's ledger ends at
  the ninth. The four-arm conclusion is NOT lost by doing that - it lives in
  `test-857-revoke-under-work-sightings.md`, which this branch owns. astubbs#323 also touches this
  file, which is the other reason not to resolve it early.

The cost is honest: the final merge is larger, and a large merge resolved carelessly is worse than
several small ones. The mitigation is that it is resolved ONCE, deliberately, with the re-verification
set named above rather than a compile treated as evidence.

## AT MERGE TIME: take master's PCMetrics, do not keep this branch's

**Instruction from astubbs/parallel-consumer#57's author, 2026-08-19.** This branch's metrics
teardown fix was extracted to a branch so it could ride with astubbs#57, which owns `PCMetrics.java`.
That branch has since been **deleted** - worktree, local and remote - after being merged into
astubbs#57, so there is nothing to point at and no comparison to make by hand.

**The rule is therefore simple: merge master after astubbs#57 lands, and take MASTER'S side in
`PCMetrics.java`.** Not this branch's.

**Why, concretely - master's version is better in a way that is easy to lose in a conflict.** Two
improvements happened after the code left here:

- `PCMetrics.close()` iterates the registry directly rather than going through `removeMeter`, so it
  needed its own guard. That was invisible on this branch, because `doClose`'s `finally` wraps the
  call - the escape only appeared when the code was lifted onto a branch without that wrapper.
- `removeMetersByPrefixAndCommonTags` guards **per meter**, where this branch wraps the whole
  `forEach` in one try. The loop-level shape aborts on the first throw, so the remaining meters are
  never removed - and with astubbs#57's leak fix present, the tracking set is left un-pruned too.
  This branch is inconsistent about it: `close()` already guards per meter, and this method does not.

astubbs#57 pinned both with a test that fails against either weaker shape, so a wrong resolution is
caught rather than merely regretted - but only if master's side is the one kept.

## Related

- `docs/inflight/bug-857-family.md` - which defects sit behind the one upstream symptom
- `docs/inflight/test-857-revoke-under-work-sightings.md` - the replays, the four arms, the matrix
- the probe critique - deleted once settled, since the Class 2 bound is a non-gating
  observation as of 2026-08-25. Read it at
  `git show 77beb4f31:docs/inflight/test-class2-probe-asserts-timing-not-correctness.md`;
  the settled knowledge is in
  [`a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md`](../solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md)
- `docs/inflight/test-chaos-crash-fidelity-variant.md` - why no scenario can model a crash
