---
artifact_contract: "ce-handoff/v1"
created_at: "2026-09-10T21:15:00Z"
title: "UX modernisation, Milestone A: implemented and simplified into the engine; the tail and the next unit"
summary: "Handoff from the session that implemented Milestone A of the UX modernisation on astubbs/parallel-consumer#502 and moved the facade's duplicated mechanisms into the engine, to the session that runs the review tail and the functions-per-topic unit"
keywords: ["ux-modernisation", "fluent-api", "park-in-place", "sandbox", "milestone-a", "ktd14", "ktd15", "handoff"]
cwd: "/Users/astubbs/github/parallel-consumer/.claude/worktrees/ux-modernisation-pr (machine-local; a fresh worktree of the branch is equivalent)"
resume_focus: "Run the ce-simplify-code and ce-code-review skills over the PR, push, watch CI, request the Codex review; then the functions-per-topic unit (plan KTD15); stop at the end of Milestone A"
repository: "astubbs/parallel-consumer"
branch: "docs/ux-modernisation"
head: "a548146b7"
worktree_path: "/Users/astubbs/github/parallel-consumer/.claude/worktrees/ux-modernisation-pr (machine-local)"
---

# UX modernisation, Milestone A: where it stands, and what is next

<!-- post-merge: checked - PR and issue numbers outlive the branch -->

**Owner intent, in the owner's words.** "Let's take this baby home, but do it properly - KISS and make
the small engine modifications required to keep the wholistic repository straightforward and
'obviously correct'." And: "priority is simplicity not blast radius, always." Stop at the end of
Milestone A; Milestone C, when reached, starts on a new branch and a stacked PR.

## Orient in this order

1. `docs/plans/2026-09-09-002-feat-ux-modernisation-plan.md` - the implementation-ready plan. Read
   the Key Technical Decisions KTD13, KTD14 and KTD15 first (grep `- KTD13.`); KTD4 and KTD6 were
   rewritten on 2026-09-11 to what actually landed.
2. `git log --oneline 78ec88156..HEAD` on the branch, and the bodies of the commits citing astubbs#504.
   The one to read whole is the simplify pass, `a548146b7`: it says what the engine now owns and why.
3. `docs/inflight/core-ux-modernisation.md` - the tracking note: producer-stack merge order, the
   compatibility-gate exclusion, and the direct-pull follow-up (the pause purge has nothing to purge
   once astubbs#361 merges).
4. The PR: `gh pr view 502 -R astubbs/parallel-consumer --comments`. **Its body predates the simplify
   pass and is stale**; syncing it is a merge-prep item below.

## What exists, and its maturity

**Complete and pushed (head `a548146b7`), all citing astubbs#504.**

- The fluent API as a package in core, `bz.stub.parallelconsumer.fluent`, entered through the static
  `connect(Properties)` on `bz.stub.parallelconsumer.ParallelConsumer` (renamed from `define` by the
  owner on 2026-09-10). Every public type carries Kafka's `@InterfaceStability.Unstable`.
- Routes with typed formats, definition-time refusals, dispatch on raw bytes, outcomes (succeeded,
  filtered, park, stop), retry limit and park in place as the default reaction, stop on exhaustion,
  the handle (close, awaitShutdown, failureCause, stopRequest, per-route parked view, instance-wide
  parked view), the route meters under the `routes` subsystem in `metrics/PCMetricsDef.java`.
- The engine additions the simplify pass made (commit `a548146b7`): `PCRetriableException` carries
  `retryAfter`, `park(reason)` and `notAnAttempt`; `WorkContainer` reads them before the retry-delay
  provider and exposes `isParked`/`getParkedReason`/`isStale`; `ShardManager.getParkedWorkContainers()`
  is the parked set; the slow-work scan and the retry queue's lowest due time skip parked containers;
  the controller purges the worker pool's queue on pause and on the dont-drain close
  (`purgeQueuedWorkNotAllowedToStart` in `AbstractParallelEoSStreamProcessor`). The facade's ledger,
  intent thread-local, reconciled parked store, rebalance listener and stop fence are deleted.
- The sandbox module `parallel-consumer-sandbox` (Java 8 target; Instancio, Datafaker, Avro on their
  last Java 8 lines) with a bound that waits for every published record to be completed or parked,
  read from the commit's offset map with the engine's codec (`SandboxConsumer#awaitEveryPublishedRecordCommitted`).
- The README, regenerated from `src/docs/README_TEMPLATE.adoc`, leads with the fluent quickstart
  (`parallel-consumer-examples/parallel-consumer-example-core/.../FluentQuickstartApp.java`, tagged
  regions), run in the sandbox by `FluentQuickstartAppTest` on every build and once against a broker by
  `FluentQuickstartIT`. Three `docs/features/` records. `CONCEPTS.md` corrected.

**Verified.** The fluent, sandbox, example and touched-engine suites are green locally under `-Pci`;
the simplify worker ran the full core suite green three times; `bin/check-all.sh` passes. CI on the
previous head `26a497d2b` was green on every test lane; CI on `a548146b7` had only started when this
handoff was written, so **check it first**: `gh pr checks 502 -R astubbs/parallel-consumer`. The
reds expected on every head until the tail runs: `claude-review` and `review: human LGTM` (the review
gates), `scan: repo` (the duplicate-code scan, see below), and `codecov/project/integration` (a known
per-flag reporting fault, `docs/inflight/ci-codecov-flags-not-like-for-like.md`).

**Not started.** The functions-per-topic unit (KTD15), the review tail, and the merge-prep items.

## Decisions made this session, and whose they are

- **Owner:** the "minimal engine changes" rule for Milestone A was a complexity bound, not a licence
  to build around the engine; the first cut got that backwards, and KTD13/KTD14 reversed it. When a
  constraint would force a workaround, say so rather than build it.
- **Owner:** any number of functions per topic, run concurrently, judged collectively first (the
  record succeeds when all return, a throw from any retries them all); isolated success (a container
  per record per function) is the later step. KTD15. A "second consumer group" answer and a
  "run in sequence" answer were both proposed by the agent and rejected by the owner.
  **Reversed by the owner on 2026-09-10** - one function per topic; see the correction
  under "The next unit" below, and the plan's KTD15.
- **Owner:** the pause takes queued batches back out of the pool on the controller, not a per-task
  check or an exception through the failure path; the window before the controller's next pass is
  accepted and documented on the purge method.
- **Owner:** the fluent record types are thin views over the engine's `RecordContext`, adding only
  decoding (and park cycles); `ProcessContext` and `ParkedRecord` are what survived.
  - **`ProcessContext` is spelled `TypedRecordContext` from 2026-09-11, owner-confirmed.** The
    decision above is unchanged - it is still a thin view adding only the decoding - and the new name
    is what says so: what it adds over the engine's record is the decoded, typed key and value. It
    cannot simply be `RecordContext`, which is the engine's own class and is what this delegates to;
    under the byte-typed engine that class's `key()` returns `byte[]`, so the two collide on return
    type and cannot share a name. `TypedRecordContext` keeps the `*Context` family. `ProcessFunction`
    keeps its name: it is named for what it is passed to, not for what it is passed.
- **Agent's call, recorded in R24:** the stopping record is parked with the reason that it asked the
  instance to stop, so it appears in the parked view; the stopped counter counts it and the parked
  counter does not. Revisit if the owner wants it unlisted.
- **Standing constraints (owner):** never name other products in the document, PR, issues or commits
  ("call it a UX modernisation"); commit and push freely; never post comments, reviews or issues
  without an explicit instruction; sub-agents on Opus; never work in the main checkout; never rename
  the PR's branch; implementation commits cite astubbs#504; API maturity via `InterfaceStability`.

## The next unit: functions per topic (KTD15)

**Corrected 2026-09-10, after this handoff was written.** The owner reversed the shape described below:
the engine holds **one** function per topic, not any number, and fan-out is the user's own composition
inside their one function ("good yes, lets keep it simple"). The widened form this section describes - N
functions per topic, run concurrently on the worker pool and judged collectively - is a **rejected shape,
not a later step**: nothing is staged behind it. Two consequences change what the unit is. The narrowed
decision is already what the code does, since the first cut binds one function to one topic and already
refuses the second, so the unit turned out to be documentation rather than an engine change; and an
engine-side per-topic function registry was designed and then declined under the simplicity gate, because
it removes one of the facade's four topic lookups while the parked view and the retry-delay provider keep
the topic-to-route map regardless. The classic API deliberately gains no per-topic verb (R26); the typed
routes are the answer to astubbs#254 (confluentinc#372). The plan's KTD15 carries the decision and its
full reasoning. What follows is left exactly as written, because a dated handoff records what was believed
when it was written.


The dispatch packet lived in the session's scratchpad (machine-local, gone with the session); what
it said, pointer-first:

- The engine holds any number of functions per topic. A record on a topic with N functions stays one
  `WorkContainer`; when it runs, its N functions run concurrently on the worker pool and the verdict
  is collective. The offset map, partition state and shard key do not change. Batches share one
  function set (group by topic in `makeBatches`); the produce path collects every function's records.
- The classic API gets the registration in its own verbs (likely `pollTopic(String, Consumer)`, repeatable
  for one topic); the single-function verbs are unchanged. An unregistered topic is a definition-time
  refusal where the subscription is known, an engine fault naming the topic otherwise.
- The facade registers each route's function against its topics at start and stops looking routes
  up; the one-route-per-topic refusal in `ParallelConsumerDefinition` goes with its test;
  `RouteDispatcher`'s remaining job (decode, run, map outcome, count) folds into `Route`/`RouteState`
  if small enough. The route meters count per route; the parked set is per record.
- Engine tests first, in the existing engine test packages: concurrency proven by a latch under key
  ordering on one key; a throw in one function retries both; unregistered topic refused; batches never
  mix topics; single-function verbs unchanged. README entry-point and classic-API sections updated,
  regenerated with `./mvnw -q -N io.whelk.asciidoc:asciidoc-template-maven-plugin:1.0.21:build`.
- After it, R2, KTD2 ("route table by topic") and any "per route" wording in R25/R28 need updating.

## The tail, in order (owner-requested)

1. `ce-simplify-code` **as the skill**, over the whole PR diff. It has not run yet; the "simplify pass"
   in the log was a general worker on the KTD14 packet, which the owner noticed.
2. `ce-code-review`, push, watch CI to completion, then request the Codex review.
3. Merge prep: sync the PR body (stale); `bin/check-pr-analysis-surfaces.sh 502`; the duplicate-code
   scan's report does not post (body too long), so read the `scan: repo` job log for clones in files
   this PR added; README metrics regeneration; `STRATEGY.md` if park in place changes a claim; the
   inflight note's "which milestones landed"; `ce-compound` before merge.

## Known defects and loose ends, with evidence

- **Drain-first close waits its whole drain timeout on records queued behind a parked record** under
  key or partition ordering (about five seconds on the quickstart, ten on a small run). Documented in
  the README's park section and in `FluentQuickstartAppTest`. The simplify worker judged the engine
  fix not plain: `ShardManager.getNumberOfWorkQueuedInShardsAwaitingSelection()` is an O(1) counter
  with a documented skew the drain depends on, and excluding records behind a parked shard head needs a
  per-shard walk per ordering mode. Owner has not decided.
- `SandboxSmokeTest`'s javadoc says the dispatching wrapper is not wired; stale since U3.
- `ParkedRunBoundTest`'s deadline is loose (25 s against a 13 to 16 s run) because the log assertion
  beside it is what pins the behaviour; `SandboxConsumer.COMMITTED_BUDGET` (20 s) is the refusal's
  patience and was left alone.
- Flakes seen once each under the loaded machine, not on CI: `ParallelEoSStreamProcessorTest.executorThreadsInterruptedOnShutdownTimeout`
  (since fixed by awaiting the function's entry) and `offsetsAreNeverCommittedForMessagesStillInFlightSimplest[1]`.
  Record a sighting before merge if either recurs.
- `ExternalEngine`'s transactional refusal has no unit test (noted during U2).

## Fragile local state (machine-local)

- Worktree `.claude/worktrees/ux-sandbox-fix` on branch `fix/504-sandbox-timing` is fully merged into
  the PR branch and safe to delete (`git worktree remove`, then `git branch -D`).
- The main checkout is one commit behind `origin/master`; the PR branch last merged master at
  `b464f7209`. Merge master before the tail if it has moved.
- Fourteen orphaned `yes` load generators from a stalled worker ran for fifteen hours at a load
  average of about 250 and were killed at the end of the session; brief every worker never to start
  load generators and never to end on a background wait.

## Wrong paths already taken, so the next agent does not retake them

- Building engine notions in the facade to honour a no-engine-change rule (the first cut; deleted).
- Answering "several functions on one topic" with a second consumer group, or with sequential
  execution: both rejected by the owner.
- A per-task pause check that throws through the failure path: replaced by the controller purge.
- A bound that refuses a run whose records park: replaced by counting parked records as accounted for.
- The parked view's `byPartition()` groups by partition number across topics; key on topic and
  partition when counting.
