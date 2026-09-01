---
artifact_contract: "ce-handoff/v1"
created_at: "2026-09-01T03:17:05Z"
title: "God-branch decomposition: remaining work and the next god-branch survey"
summary: "Both god branches (#293 proxy, #271 Streams) are fully cut into 16 reviewable PRs; this captures the remaining decisions (reconciliation, default flip, #271 retarget, review order) and the verdict on #363 and other god candidates."
keywords: ["god-branch", "decomposition", "parallel-consumer", "proxy", "kafka-streams", "retarget", "perf-train", "363"]
cwd: "/Users/astubbs/github/parallel-consumer"
resume_focus: "Execute the remaining campaign tail: fleet review support, Wagon B sibling reconciliation + dispatch-default flip measurement, #271 retarget; then act on the god-branch survey verdicts."
repository: "astubbs/parallel-consumer"
repo_root_sha: "713c7468"
branch: "master"
head: "d1827df41 (origin/master at capture)"
---

# God-branch decomposition - remaining work, and the next god-branch survey

Session 2026-08-31 → 09-01. The campaign plan is
`docs/plans/2026-08-31-001-process-god-branch-decomposition-plan.md` on branch
`docs/god-branch-decomposition-plan` (pushed; **deliberately has no PR** - owner's decision is it
rides into master with the first campaign PR that merges). The coordination note is
`docs/inflight/process-god-branch-decomposition.md` on the same branch. Read both before anything.

## State: cutting phase COMPLETE

Every extraction is delivered, sabotage-proven, CI-settled (bar expected reds: dependency gates on
stack rungs, `claude-review`/`human LGTM` on unreviewed PRs - both documented expected states).

- **Wagon A (proxy)**: #380 build matrix → #383 protocol → #384 shell → #385 native / #386 Java
  client → #387 conformance → #390 foreign clients → **#293 retargeted onto #390's branch as the
  engine residue** (556→281 files). Descendants #328→#331→#340→#334 forward-merged (all four merge
  commits have first parents byte-identical to `backup/pre-forward-merge-<PR>` refs). The FFI
  `--shared` build is proven live on #340 post-merge. #295 (verdict-free return) reopened,
  re-cut on master, green.
- **Wagon B (Streams-on-PC)**: #379 fork machinery → #388 seam → #389 refusal envelope → #394
  lifecycle → siblings **#395 error-surfacing / #396 stream-time-punctuation / #398 evidence
  suite**, plus #391 example on #388. Dispatch default OFF with all named triggers now closed
  individually but never measured together (see reconciliation below).
- **Independents**: #378 (write-time testing + file-ref parser fix), #382 (three
  wrong-context guard fixes), #295. All green.
- Backup refs on origin: `backup/pre-recut-295`, `backup/pre-stack-merge-293`,
  `backup/pre-forward-merge-{328,331,340,334}`.

## Remaining work, in the owner's stated order of need

1. **Fleet review** (human; agents support). Bottom-up: #380 and #379 unlock the dependency-gate
   cascades. Merge-prep per PR follows `docs/merge-checklist.md`; note #293 carries a parked
   merge-prep item (`docs/inflight/pr-293-commit-subject-trailing-reference.md` - the trailing
   `(confluentinc#154)` on ~81 subjects is settled at re-cut, fix is deletion). The known-good
   reds the owner has not yet ruled on: `dups: similarity` on #295/#380/#383/#384 (required
   ArchUnit-file duplication / lexical noise - both real clone engines clean) and `dups: clones`
   on #390 (generated stubs/README banners, answered in body).
2. **Wagon B sibling reconciliation + dispatch-default flip measurement.** The three siblings
   (#395/#396/#398) have never coexisted in one tree; every default-OFF trigger is closed but only
   in isolation. `docs/inflight/streams-dispatch-default-flip-is-reserved-until-the-rungs-reconcile.md`
   (on #395's branch) reserves the flip for ONE fresh seam-on measurement on the reconciled
   module. Checklist for whoever reconciles: delete B6's two sibling-owned ledger attributions
   (`stream-time-never-advances`, `exception-type-lost-in-the-worker`) in
   `docs/inflight/test-streams-seam-on-divergence-triage.md` - then **B6's `divergence
   classification` lane is the acceptance test**: if those divergences still classify, the merge
   lost a fix. Flip ON only if the combined measurement is clean; else the failure becomes the
   next named trigger.
3. **#271 retarget**, mirroring #293's proven move: reconcile siblings into the stack top, merge
   that into `feats/ks-on-pc-spike`, retarget #271's base, rewrite body to residue framing, push
   backup ref first (`backup/pre-stack-merge-271`). Natural to combine with item 2 in one
   dispatch. The ks-streams forest branches stay untouched (merge-never-rebase, they are the
   evidence record; handover: `git show abcc811e6:docs/inflight/branch-ks-streams-handover.md`).
4. **Open investigations, recorded not chased** (do NOT re-run to green):
   - `processInKeyOrder` flake rate ROSE through 09-01 on byte-identical core (ledger in #380's
     branch sighting note + `docs/inflight/test-load-tightness-flakes.md` family) - something
     environmental/load-shaped.
   - `PcDrivenStatefulProofTest` stall, 1-in-2 CI, not reproduced locally at forkCount=4; the
     settling experiment is **vary core count**, recorded in #398's notes.
   - `PartitionStateCommittedOffsetIT.committedOffsetRemoved[1] latest` - the 2026-08-05 plan's
     prediction arrived on its own arm; every parameter now seen failing
     (`docs/inflight/test-load-tightness-flakes.md`).
5. **Small parked items**: `web/landing-page` branch (no PR by owner decision;
   `docs/inflight/branch-landing-page.md` on the plan branch names its four gate reds as PR-time
   work). `feats/native-image-sidecar` superseded-but-retained
   (`docs/inflight/branch-native-image-sidecar.md`, plan branch). The proxy pom still declares no
   `mainClass` (recorded in the rewritten `testing-evidence.d/parallel-consumer-proxy.yaml`).
   The dotnet demo's three `-Dmdep.includeScope` sites are the SAME silent-prefix defect fixed on
   #340 but deliberately ordered AFTER `docs/inflight/bug-sidecar-runtime-logging-and-address-leak.md`
   steps 1/2/4. The proxy evidence file's "118 tests" coverage figure was NOT audited.
   FFI throughput figures in `docs/inflight/perf-embedding-the-engine-over-ffi.md` predate the
   classpath fix - re-measurement deferred by owner.

## The god-branch survey (what's left, and verdicts)

**#363 `perf/engine-concurrency` (745 files shown, ~10 dependency PRs) - VERDICT: do nothing
structural now; wait for the children to merge.** Reasoning against the synthetic-base idea (merge
all dependents into a base branch built just for #363, then retarget):

- The reviewability problem the #293 retarget solved does not exist here. #293's 556-file diff had
  no map; #363's body already IS the map - it says "read this one last, expect it to shrink,
  nothing here should be reviewed on its merits if it appears in a parent". Its residue is
  campaign records, not semantics needing early review.
- Its children are the most merge-ready branches in the repo: #335/#336 already merged, #358-#362
  open and green, and this campaign back-filled the stranded residue (STRATEGY delta, tests,
  feature records, CSVs) into their owning children - so #363's true residue is already clean.
- A synthetic base is a branch someone must re-maintain on every child update, sits in the
  dependency gate's path as a non-PR ref, and buys no reviewer anything. The diff number on the PR
  page is cosmetic until someone actually needs to review the residue.
- **The trigger that flips this verdict**: children stall in review for weeks AND someone needs to
  review #363's residue before they merge. Then do the retarget move - it is now twice-rehearsed
  (see #293's execution: merge stack top in, `gh pr edit --base`, body to residue framing) and
  costs about one agent-session. Because the children are master-based siblings, the "stack top"
  would indeed be a synthetic integration branch (serial ordinary merges of the five, conflicts
  reconciled at the source) - mechanically fine, just not worth it today.
- Optional cheap tidy noticed during back-fills: #362's CONTENT stacks through #361 onto #359
  while all three PRs claim base master - their displayed diffs overlap. Retargeting #362→#361→
  #359's branches would make each diff honest. Small, cosmetic, owner's call.

**#29 (bug 857)** - already mid-self-decomposition (#375/#376 merged; its body carries the
remaining cut list). Leave it to its own plan; do not fold into a campaign.

**#269 Connect-on-PC** - the watch-item. Not yet god-sized, and its biggest ancestry burden (the
Kafka fork/patch machinery) now exists as reviewable #379. When Connect work resumes, the first
move is re-cutting it onto #379's machinery instead of the spike's - which prevents it ever
becoming a god branch. Also: `origin/feats/connect-on-pc-spike` carries a prior fix for the
classifier-cache class (cited in #379's warm-step comment).

**#333 adaptive concurrency** - god-shaped by ancestry only; when the perf train merges it needs
only a base retarget to master. No action.

**Nothing else qualifies.** #262 (transactional battle-test), #352 (commit-failure seam), #268
(dashboard) are large-but-coherent: one thesis each.

## Traps the next agent WILL hit (all live at capture)

- **The wrong-context guard bugs are fixed only on unmerged #382.** Until it merges, the
  pre-commit hook gates whatever worktree the session last touched, not yours: verify
  `bin/check-all.sh` exits 0 in YOUR worktree, then `--no-verify` and record it. The
  history-rewrite hook misnames branches the same way.
- **The classifier cache-warm fix lives only on #379's branch** (`prepare-deps` step in
  `maven.yml`). Every other streams-carrying branch still coin-flips on Maven Central fetching
  `kafka-streams:jar:sources` - a red Unit/Integration lane with ZERO tests run is that, not the
  PR (`docs/inflight/ci-streams-classifier-artifacts-escape-the-cache-warming-job.md`, now on
  #379's branch). Re-running IS legitimate there (no test outcome is hidden) but never instead of
  recording.
- **Shared scratchpad across concurrent agents**: namespace every temp file with a task slug -
  a generic `pr-body.md` was overwritten mid-use and briefly swapped PR 390's body for 391's.
- **Prose to files, never shell strings** (fish shell); JDK 17 via
  `/opt/homebrew/opt/openjdk@17` per-command (default java_home lacks 17); delete stale surefire
  report dirs before reading counts after any failed/sabotage run; `-Dtest=` silently skips the
  upstream Kafka suite.
- **Session-limit kills**: background agents die mid-task on usage limits; SendMessage to the
  dead agent's id resumes it from transcript with context intact - tell it to re-verify its
  worktree state first. Worked every time (three occurrences).
- Disk hovers low on this machine (dipped to ~15 GiB); avoid container builds locally, report
  before large ones.

## Where the deep context lives (pointer-first)

- Campaign reasoning: the plan doc (above) + `docs/inflight/process-god-branch-decomposition.md`.
- Per-rung reasoning: each PR body defends its decisions BY NAME - read the body and comments
  before touching any rung branch (the repo's inherited-record rule; hooks inject this).
- Wagon B trigger chain: `PcDispatchSwitch` javadoc on any B branch + the reserved-flip note.
- Streams forest topology/traps: `git show abcc811e6:docs/inflight/branch-ks-streams-handover.md`.
- #293 merge account: merge commit `8a7dc3ec7` body; retarget summary comment on #293.
- The repo's own AGENTS.md is the binding rule set; merge prep is `docs/merge-checklist.md`.
