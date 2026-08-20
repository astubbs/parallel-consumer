# astubbs#29 - the confluentinc#857 deadlock fix, and what measuring it taught

## START HERE - handoff, 2026-08-20

**PR astubbs/parallel-consumer#29 is BLOCKED and that is correct.** Six `depends on` lines in its
body; astubbs#323/#324/#325 merged, **astubbs#57, astubbs#322, astubbs#267 have not**. Nothing to do
on this branch until they land.

Worktree `/home/astubbs/git/parallel-consumer/.claude/worktrees/sweep-29`, branch
`bugs/857-paused-consumption-multi-consumers-bug-rename`, pushed to
`bugs/857-paused-consumption-multi-consumers-bug`. Tree clean, 0/0 with origin, ~29 behind master
deliberately.

**Do not:** merge master early (one merge at the end - a trial merge produced two resolutions later
PRs would have redone); request review before that merge; keep this branch's `PCMetrics.java` at
merge - **take master's**, astubbs#57 has the better version.

**When astubbs#57, astubbs#322 and astubbs#267 have landed:** merge master once -> expect `ConsumerManagerCommitRetryBudgetTest`
to need `ThreadConfinedConsumer` wrapping and `bug-857-family.md` to conflict structurally (take
master's) -> run the deadlock probe, `ShardManagerStaleContainerTest`,
`OutForProcessingCounterDriftProbeTest`, `InstanceStallProbeIT` -> then request review + human LGTM.

**Owed at merge:** merge-strategy recommendation and squash message (90+ commits; `e81ac20fe` is
mislabelled `docs(inflight)` but carries 584 lines of detector code); a one-line `stage_detail` fix in
`docs/data/roadmap.yaml` (`known-defects-cleared` still says "drafted on astubbs#29") that no gate
will ask for.

**Unfinished:** one more `/ce-compound` run for the second learning - the four-arm measurement showing
the eager `CLASS2_STALL` is the detector meeting the workload, not a defect. The skill takes one
learning per run; the first (metrics teardown) is captured in
`docs/solutions/runtime-errors/a-throwing-meter-registry-kills-the-poll-thread-and-strands-close.md`.

**Do not re-attribute the eager `CLASS2_STALL` sightings to this PR.** Four measured arms say
otherwise; details below and in `test-857-revoke-under-work-sightings.md`.


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
