---
artifact_contract: "ce-handoff/v1"
created_at: "2026-09-01T03:45:00Z"
title: "Navigator session two: from the in-process MVP to the rate limiting feature"
summary: "Handoff from the session that built, reviewed and demoed the navigator micro-MVP (astubbs/parallel-consumer#392) to the session that builds the Kafka-coordinated allocator - the rung that makes it PC's global rate limiting feature"
keywords: ["navigator", "rate-limiting", "resource-allocator", "kafka-coordination", "hasten", "handoff"]
cwd: "/Users/astubbs/github/parallel-consumer/.claude/worktrees/hasten-micro-mvp (machine-local - the origin machine's worktree; a fresh clone of the branch is equivalent)"
resume_focus: "Ship astubbs/parallel-consumer#392, then build the Kafka-coordinated ResourceAllocator behind the same seam - the end MVP is PC's global rate limiting feature"
repository: "astubbs/parallel-consumer"
branch: "feats/hasten-micro-mvp"
head: "see git log - this document is committed on the branch it describes"
---

# Navigator session two: from the in-process MVP to the rate limiting feature

Committed on `feats/hasten-micro-mvp` (PR astubbs/parallel-consumer#392) for a next agent on a
different machine. Everything below is this session-writer's account; where a decision is the
OWNER's it says so. Ground truth outranks this document - verify against the tree and the PR.

## The objective, in one line each

- **Landed (this branch):** the in-process rung - a soft credit system where PC instances in one
  JVM share a rate-limited resource through one application-supplied allocator, every wait
  attributed. Observable moment demonstrated and demoed.
- **The end MVP (owner ruling, 2026-09-01):** the finished navigator IS PC's global rate limiting
  feature (astubbs#228) - the research track and the headline feature are one deliverable.
  Recorded in `docs/inflight/core-distributed-throttling.md` under "The end MVP goal is the
  rate-limiting feature itself".
- **The next phase (this handoff's second half):** the Kafka-coordinated `ResourceAllocator` -
  swap the in-JVM stub for partition-owned credit authority behind the SAME seam, so
  `bin/demo-navigator.sh`'s storyline becomes "two processes on two machines" with nothing else
  changing.

## Orient in this order (before touching anything)

1. Repo root `AGENTS.md` - binds every session; note the IN-FLIGHT package-rename rule that
   applies before any master merge, and that `gh` must be pointed at the fork
   (`gh repo set-default astubbs/parallel-consumer`) on a fresh clone or every prior-art search
   silently answers from upstream.
2. The PR body of astubbs/parallel-consumer#392 - it defends the load-bearing decisions by name
   and carries the open decisions and the Post-Deploy Monitoring section.
3. `docs/plans/2026-08-31-2029-feat-navigator-micro-mvp-plan.md` - the implementation-ready plan
   the MVP was built against; its session-settled KTDs still bind (always-succeeds spend, no
   ledger clamps, burst budgets never caps, quantum tick before work distribution, KTD7's
   demo policy).
4. `docs/inflight/core-shared-execution-resources.md` - OWNS the next phase's design (see below).
5. `CONCEPTS.md` "Parallel consumption" + "Test reliability" clusters - the vocabulary (Navigator,
   Credit, Lane) the code and docs speak.
6. Run `bin/demo-navigator.sh` (JDK 17 + Docker) to SEE the feature - ~25s storyline;
   `docs/demos.md` owns the demo conventions.

## What exists and its maturity

All COMPLETE, committed and pushed on this branch:

- The six MVP units (declaration surface, allocator, selection-path integration, attribution,
  context view, wall-clock observable moment) - commits `2f1d74e4f..d1bc0d2a9`, each body
  carrying its unit's design record.
- The shipping tail: two simplify passes and two multi-agent code reviews with independent
  validation (review artifacts lived in the origin machine's /tmp - NOT transferable; the
  receipts and every finding's disposition are summarized on the PR body and in the commit
  bodies of `e0f249420` and `6c21331e3`). Sixteen validated findings applied in total, each
  fix sabotage-verified where it guards behavior.
- The public API split: users import `bz.stub.parallelconsumer.navigator` (view, allocator seam,
  contract, stub); engine wiring stays internal.
- Burst live as a monitored budget; fail-safe allocator guards with a spend-failure latch;
  drain-aware close with an engine-side deadline; Lincheck lane wired into CI (Unit Tests row
  tail step); `NavigatorDemo` + `bin/demo-navigator.sh`; `docs/demos.md`.
- Learning capture: `docs/solutions/workflow-issues/a-lane-nothing-runs-cannot-catch-its-own-guard-drifting.md`.

NOT started: everything in "The next phase" below. IN PROGRESS: nothing - the branch is at a
clean stopping point.

## Remaining before astubbs/parallel-consumer#392 merges (status, not orders)

- The PR is a DRAFT stacked on astubbs/parallel-consumer#333; the owner has an unsubmitted
  pending review on it, and the ready-flip and `@claude review this` request (the required
  gating review) are the owner's calls.
- **The master catch-up for the whole stack is deliberately deferred (owner call).** The branch
  base is many commits behind origin/master with ~100 files changed on both sides. When it
  happens: `bin/rename-packages.sh` BEFORE merging master (the AGENTS.md in-flight rule), and
  read the inherited commit bodies - several touch files this branch edits
  (`ProcessingShard`, `WorkContainer`, `PCModule` among them).
- The inherited `check-file-refs` failures are that deferred state, not this branch's diff (the
  PR body's base-branch note explains).
- One OPEN product decision, parked on the PR body: drain-deadline expiry semantics (currently
  redeliver-after-rebalance, matching precedents).

## The next phase: the Kafka-coordinated allocator

**`docs/inflight/core-shared-execution-resources.md` OWNS the design** - read it whole. The short
shape: do not build a distributed token bucket; delegate pieces of a budget (synchronise ownership
of capacity, spend locally). Kafka is the coordination plane: resource names hash onto partitions
of an internal control topic, the partition owner is the authority, fencing vocabulary comes from
Kafka's own generations/epochs. Failure bias: failure wastes capacity, never violates the
constraint. `docs/inflight/core-distributed-throttling.md` owns the track's still-open gating
decisions (standalone throttle vs auto-scaling-controller signal, among others) - they are the
OWNER's, not the next agent's.

What the landed rung hands the next one:

- **The seam is real:** implement `bz.stub.parallelconsumer.navigator.ResourceAllocator` (its
  javadoc now states the thread-safety and exception-posture contract an implementer needs) and
  everything else - eligibility, spend-after-claim, attribution, view, meters, lifecycle - comes
  for free. `StubResourceAllocator` is the semantic reference: quantum-indexed lazy minting,
  equal-share with remainder rotation, lease TTL, conservation identity.
- **The proof obligations transfer:** the successor-epoch no-re-mint property (a superseded
  coordinator must not re-mint a quantum), the conservation identity, and KTD4's reproducible
  reads are what the Kafka implementation must keep true under partition-authority movement.
- **The test lanes transfer:** the virtual-clock lane pattern (`PCModuleTestEnv` + shared
  `MutableClock`), the Lincheck lane (now CI-gated - bump `EXPECTED_LINCHECK_CLASSES` in
  `bin/lincheck-test.sh` when adding a harness), and `NavigatorRateShareTest` as the
  asserted twin whose two-instance storyline should gain a two-process variant.
- **The demo transfers:** `docs/demos.md` + `NavigatorDemo` - the coordination rung's demo is the
  same dashboard across two JVMs/machines, which is also the feature-announcement artifact.
- **Accepted v1 limits with named remedies** (do not "fix" silently): R17's disjoint-tag share
  dilution (remedy: demand-weighted allocation, coordination rung); the allocator-global
  spent/overdraft gauges under per-instance tags; the stub's single coarse stateLock
  (lock-splitting explicitly belongs to the coordination rung).
- **A future idea recorded, not scheduled:** implicit detected dependencies (the scaler as
  dependency prober) - in `core-shared-execution-resources.md`'s closing section, owner's idea.

## Wrong paths already closed (do not retry)

- A global mutable token bucket everyone mutates; hard-ceiling semantics or wording (R8 forbids).
- Pre-filter-then-claim on the selection path (the claim path's javadoc documents the defect
  class); the quantum tick placed after work distribution (starves tagged work - the wall-clock
  lane caught it; ordering is load-bearing and commented).
- Non-monotonic per-quantum resets (both the roster guard and the burst budget rotted this way -
  mirror `renewLease`'s monotonic merge).
- Fail-safe guards that throw from every method in tests: a spend-ONLY-throwing allocator was the
  fail-open hole; partial-failure mocks are the honest test shape.
- Verification lanes shipped "opt-in for now": the lane's guard rotted unnoticed for its whole
  unwired life (`docs/solutions/workflow-issues/a-lane-nothing-runs-cannot-catch-its-own-guard-drifting.md`).
- Cross-model review peers at xhigh effort: three timeouts at the 1200s cap in one day; pin
  the compound-engineering plugin's `cross_model_effort` key to `high` (repo-root CE config,
  uncommitted here) if corroboration is wanted.

## Machine-local notes (origin machine only - do not chase these elsewhere)

- Worktree: `.claude/worktrees/hasten-micro-mvp` under the main clone; `.worktree-owner` marker
  is local-only. On a new machine, clone fresh and work in a worktree per AGENTS.md.
- JDK 17 on the origin machine came from SDKMAN (`~/.sdkman/candidates/java/17.0.18-tem`);
  `/usr/libexec/java_home -v 17` there silently returned JDK 26. Any JDK 17 works.
- Review-run artifacts under `/tmp/compound-engineering-*` are gone with that machine; the PR
  body and commit bodies carry everything durable.
