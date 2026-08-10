---
title: Can Connect Sink Watermarks Compose Into a Partition Frontier - Investigation Plan
type: investigate
date: 2026-08-10
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
product_contract_source: ce-plan-bootstrap
execution: code
---

# Can Connect Sink Watermarks Compose Into a Partition Frontier - Investigation Plan

## Goal Capsule

**Objective.** Settle one question with evidence: **when one partition's records are split across several
`SinkTask` lanes, can their per-task `preCommit()` watermarks compose into a per-partition frontier that
is safe to commit?** Answer yes with a mechanism, or no with the reason. Do not design the implementation
until that answer exists.

**Why it gates everything.** Key-sharding a partition across lanes is the entire value proposition
(`STRATEGY.md`, and the reconciliation section of `docs/plans/2026-08-09-001-feat-connect-on-pc-plan.md`).
If watermarks cannot compose, the module can still run connectors correctly - partition-affine, at the
partition-count ceiling `tasks.max` already reaches - but it delivers none of the strategic claim. That
is a result worth having early rather than after an implementation.

**Authority.** `WorkerSinkTask`'s real call sites govern what a `SinkTask` may observe, not Connect's
javadoc - see `docs/solutions/integration-issues/kafka-streams-task-lifecycle-callbacks-do-not-mean-what-they-are-named.md`.
`CONCEPTS.md` owns "frontier" and "frontier semantics"; use those words.

**Stop conditions.** Stop and report if any candidate requires a `SinkTask` to observe something Connect
never gives it, or requires PC to commit an offset no lane has declared durable. Both are answers, not
blockers.

**Tail ownership.** Investigation only. No implementation units, no production code. The output is a
written verdict plus the experiment that produced it.

---

## Product Contract

### Summary

Parallel Consumer commits a **frontier** - the highest contiguously-complete offset - with completions
beyond it encoded in the commit metadata. Kafka Connect's `SinkTask.preCommit()` returns a per-partition
**watermark**: "everything below this is durably written". The two are different shapes, and the mismatch
only bites when a partition is split across lanes.

This investigation determines whether that split is survivable.

### Problem Frame

Grounded in `WorkerSinkTask` (Kafka 3.9.2), read at its call sites rather than its javadoc:

- Offsets are **staged then promoted**: `origOffsets` accumulates `offset + 1` during conversion, and
  `currentOffsets.putAll(origOffsets)` runs only *after* `put()` returns. A `RetriableException` skips the
  promotion, which is what makes redelivery non-lossy.
- `preCommit()` receives `currentOffsets` - derived from **the records that task consumed**.
- Its return is clamped: a task offset at or below consumed is honoured, above is rejected with a warning.
  An omitted partition falls back to `lastCommittedOffsets`. An empty map skips the commit and reports
  success.
- `task.close(topicPartitions)` runs in the **`finally` of the `preCommit` try** (`:449`), and `doCommit`
  runs *after* that block (`:488`). So revocation order is **preCommit, then close, then the consumer
  commit** - and work a lane flushes during `close` is **not** covered by that commit. An earlier draft of
  this plan said "close follows commit", which is backwards and would have let a composition rely on
  close-time flushes being committed.
- **The clamp offers no safety for a split partition.** `WorkerSinkTask` clamps a returned offset against
  `offsetsToCommit.get(partition)` (`:466-467`), taken from the single `currentOffsets` map covering
  everything *that WorkerSinkTask* consumed for the partition. With several lanes beneath one worker, a
  lane over-claiming past its own subset but below the partition-wide union is **honoured, not rejected**.
  Do not treat the clamp as a guard for the only case this investigation is about.

**The tension.** A watermark is a high-water mark, and
`docs/solutions/architecture-patterns/a-high-water-mark-cannot-express-out-of-order-completion.md`
establishes that a high-water mark cannot express out-of-order completion. Split partition P across lanes
A and B: each sees a subset of P's records, so each lane's watermark describes its own subset, not P.
Lane A reporting "P: 100" means "everything *I saw* below 100 is durable" - it says nothing about the
records B holds.

### Requirements

- R1. The investigation produces a written verdict, in one of **three** shapes: sound (with the mechanism);
  **sound-conditional** - sound provided the connector does not observe or depend on what the mechanism
  perturbs, naming each precondition and whether PC can detect it at runtime; or unsound (with the reason).
  Three, not two, because the likely real answer is the middle one - C2 already flags that many sinks write
  `SinkRecord.kafkaOffset()` into their output - and a binary verdict rounds that to "sound", shipping the
  precondition as a footnote instead of a gate.
- R6. Every whole-partition assumption U1 step 3 surfaces - `SinkTaskContext.offset()` rewind, `pauseAll()`
  on `RetriableException`, `assignment()` - is classified as absent, benign under split lanes, or a
  separate gate. A sound or sound-conditional verdict is not issued while any is unclassified. Offset
  composition is *necessary* for splitting a partition across lanes; R6 exists because it may not be
  *sufficient*.
- R2. Every candidate mechanism is evaluated against `WorkerSinkTask`'s actual semantics, cited by call
  site, not against the javadoc.
- R3. The verdict names the experiment that would falsify it, and that experiment is run rather than
  described.
- R4. A candidate is rejected only on a stated mechanism, never on effort. "Too many places to change" is
  not a finding.
- R5. If the verdict is "unsound", the investigation states precisely what partition-affine mode costs in
  the terms `STRATEGY.md` uses - head-of-line blocking avoided, achieved fan-out versus configured max.

### Success Criteria

- The question is answered with evidence a reader can check, not with a preference.
- A reader can tell which candidates were ruled out and on what grounds.
- If sound: a crash-restart experiment in the shape of the Streams module's
  `CommitFrontierCrashRestartTest` demonstrates the composed frontier holds under a kill.
- If unsound: the specific record interleaving that breaks it is written down and reproducible.

### Scope Boundaries

#### In scope

Reading `WorkerSinkTask`'s commit and lifecycle paths; enumerating and evaluating composition candidates;
running the experiment that settles the leading candidate.

#### Explicitly out of scope

Real `Converter` wiring, connector instantiation, SMTs, DLQ, `ConfigProvider`, and Kafka 4.x
compatibility. Also out: implementing the chosen mechanism - that is the next plan, written once this one
returns an answer.

---

## Planning Contract

### Key Technical Decisions

- KTD1. **Settle feasibility before designing the mechanism.** This plan investigates and returns a
  verdict; it does not design the implementation.
  (session-settled: user-directed — chosen over designing the composition with a falsification test
  alongside it: an implementation designed around a mechanism that turns out unsound is wasted, and the
  feasibility answer is cheap by comparison.) Governs R1.

- KTD2. **Reject candidates on a named interleaving, never on argument.** Each rejection in U2 must carry
  the concrete two-lane, one-partition completion order that breaks it. Governs R2, R4.
  Rejected: reasoning from Connect's javadoc, which
  `docs/solutions/integration-issues/kafka-streams-task-lifecycle-callbacks-do-not-mean-what-they-are-named.md`
  shows is a description of intent rather than a contract - `postCommit` is reached after a swallowed
  failure despite its name.

- KTD3. **The probe must carry a negative control that fails when composition is inverted.** A probe that
  cannot detect breakage cannot support a verdict, and this investigation's whole output is a verdict.
  Governs R3.

- KTD4. **Partition-affine is the comparison arm, not a consolation prize.** C4 is evaluated on the same
  terms as the others so that an "unsound" verdict arrives with its cost already quantified in
  `STRATEGY.md`'s metrics rather than needing a second investigation. Governs R5.

---

## Prior Art - this repository has already answered part of this

Read before U2, and reconcile against it rather than re-deriving:

- **`docs/plans/2026-08-08-001-feat-connect-sink-in-pc-plan.md`, KTD5** already rejected several tasks
  sharing a partition as *unsound*, with a mechanism: "the offsets would over-report and the sink would
  lose data on restart". The verdict must say explicitly whether it upholds or overturns that.
- **`docs/inflight/pr-connect-on-pc.md`** already describes C3's resolution as designed: complete a
  record's `WorkContainer` only when the owning task's `preCommit()` covers it, each watermark read
  against that task's own record stream. U2 should test *that stated form* rather than re-inventing it.

## Candidates To Evaluate

Recorded now so the investigation tests them rather than rediscovering them, and so a reader can see what
was considered. **None is yet endorsed.**

- C1. **Minimum across lanes owning the partition.** Commit `min(watermark)` over every lane holding
  records for P. Likely safe and likely useless: a lane whose highest *seen* offset is low pins the
  frontier there regardless of what other lanes durably wrote. Determine whether it is merely
  conservative or actually incorrect.
- C2. **Per-lane synthetic offset space.** Give each lane a contiguous private offset sequence, so its
  watermark is honest *within that space*, then map back through PC's completion tracking. This is the
  shape of `one-owner-per-metadata-field-with-an-opaque-rider.md` applied to offsets rather than metadata.
  The question is whether a connector can observe the synthetic offsets and misbehave - many sinks write
  `SinkRecord.kafkaOffset()` into their output.
- C3. **Durability barrier, not watermark translation.** Complete a record's `WorkContainer` only when
  **the lane that received it** declares a watermark covering it, read against *that lane's own record
  stream*, and let PC's existing frontier machinery do the rest. Take this form from
  `docs/inflight/pr-connect-on-pc.md`, which already specifies it - an earlier draft here said "*some*
  lane", which is trivially unsound (lane B flushing to 100 would mark lane A's unflushed 50 durable) and
  would have been rejected in one line without the real candidate ever being evaluated.
  Start from the prior art rather than re-deriving: the 2026-08-08 plan's KTD3 rejected `ExternalEngine`-
  style deferred completion on granularity and retry inversion, and
  `parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/ExternalEngine.java` is the
  in-repo analogue. Assess whether those objections still bind in the patched-runtime shape.
- C4. **Partition-affine only.** One lane per partition; watermarks are honest by construction. The
  ceiling is the partition count. Include for comparison, and to quantify R5.

---

## Assumptions

- A1. The lane primitive is fixed: one `SinkTask` per lane, a lock across `put()`, keys routed by PC's
  `ShardKey` under `KEY` ordering. This investigation does not revisit it.
- A2. The parent's commit surface (`collectCommitData`, `hasCommitDataOutstanding`, `onCommitSuccess`) is
  the integration point and is owner-thread guarded. Any mechanism must work through it.
- A3. **A constraint on the mechanism, not a property of the runtime.** PC must never treat a
  `put()`-but-not-durable record as *failed* work - it is in the sink's buffer. But Connect itself does
  redeliver already-`put()` records on two live paths, so the mechanism must stay correct when it happens:
  when `preCommit` throws, the runtime seeks back to `lastCommittedOffsets` and restores `currentOffsets`
  from them (`WorkerSinkTask.java:437-442`); and `rewind()` redelivers at the connector's own request via
  `SinkTaskContext.offset()` (`:642-659`). A candidate rejected on A3 must cite the call site, like every
  other rejection under KTD2.

  *An earlier draft stated A3 as "never retried, full stop" and used it as a bare disqualifier. That was
  false about the runtime and would have killed candidates - C3 especially - on a fact that is not one.*

---

## Investigation Units

### U1. Establish what a lane can honestly claim

**Goal.** Determine, from `WorkerSinkTask`'s call sites, exactly what a `SinkTask` knows about offsets it
did not receive - and therefore what its watermark can and cannot mean.

**Requirements.** R2.

**Files.** Read-only: `parallel-consumer-connect/target/connect-patched/org/apache/kafka/connect/runtime/WorkerSinkTask.java`
(generated at build time; run `./mvnw -pl parallel-consumer-connect -am -DskipTests generate-sources` if
absent). Findings recorded in this plan under Findings.

**Approach.**
1. Trace `origOffsets` -> `currentOffsets` -> `preCommit` -> `commitOffsets`, naming each call site.
2. Establish what `currentOffsets` contains when a task received only a subset of a partition's records.
3. Determine whether Connect anywhere assumes a task owns whole partitions in a way that a split violates
   beyond the offset question - `SinkTaskContext.assignment()`, `offset()` rewind, and pause/resume are
   the suspects.

**Verification.** Each claim in Findings carries a `WorkerSinkTask.java` call-site citation.

### U2. Falsify or survive each candidate on paper

**Goal.** Reduce C1-C4 to at most one live candidate, with a stated reason for each rejection.

**Requirements.** R1, R2, R4.

**Dependencies.** U1.

**Files.**
- `docs/plans/2026-08-10-001-investigate-connect-offset-composition.md` (modify - record each candidate's
  rejection interleaving, or the survivor's requirement, under Findings)

**Approach.**
1. For each candidate, construct the interleaving that would break it: two lanes, one partition,
   completions out of order across lanes.
2. Reject on mechanism only. Record the interleaving that does the rejecting.
3. For any survivor, state what it requires from Connect and confirm Connect provides it.

**Execution note.** Write the predicted outcome for each candidate *before* tracing it, and record
refutations as prominently as confirmations - `docs/solutions/best-practices/chase-refuted-predictions.md`.

**Verification.** Every rejected candidate has a named interleaving; the survivor has a named requirement
and the call site that satisfies it.

### U3. Run the experiment that settles the survivor

**Goal.** Move the verdict from argument to evidence.

**Requirements.** R1, R3.

**Dependencies.** U2.

**Files.**
- `parallel-consumer-connect/src/test/java/io/confluent/parallelconsumer/connect/OffsetCompositionProbeTest.java` (create - throwaway probe, deleted or promoted once the verdict lands)

**Approach.**
1. Build the smallest harness that reproduces the interleaving U2 identified as decisive: one partition,
   two lanes, a controlled completion order.
2. Include a **negative control that must fail** - a variant where the composition is deliberately wrong,
   proving the probe can detect breakage. A probe that cannot fail proves nothing
   (`docs/solutions/best-practices/control-arms-vary-exactly-one-term.md`).
3. If the survivor holds, extend to a crash-restart in the shape of the Streams module's
   `CommitFrontierCrashRestartTest`: park a record, let others complete around it, kill, restart, assert
   the resume point.

**Test scenarios.**
- Two lanes hold one partition; lane A's records complete after lane B's higher-offset records. The
  composed frontier must not exceed the lowest incomplete offset.
- A lane returns a watermark below what it consumed (legitimate per Connect). The composed frontier
  respects it.
- A lane omits the partition entirely. The composed frontier does not advance on its behalf.
- All lanes return an empty map. No commit occurs.
- Negative control: composition deliberately takes the maximum rather than the safe function. The probe
  detects the resulting over-commit.
- Crash-restart: after a kill, redelivery begins at or below the composed frontier and no durable record
  is delivered twice beyond the at-least-once contract.

**Verification.** `./mvnw -pl parallel-consumer-connect -am test` green, both existing regression arms
still running Kafka's `WorkerSinkTaskTest` unchanged, and the negative control failing when inverted.

### U4. Write the verdict

**Goal.** A reader who was not here can act on the answer.

**Requirements.** R1, R5.

**Dependencies.** U3.

**Files.**
- `docs/plans/2026-08-10-001-investigate-connect-offset-composition.md` (this file - Findings and Verdict)
- `docs/inflight/pr-connect-on-pc.md` (modify - point to the verdict)
- `parallel-consumer-connect/connector-compatibility.md` (modify only if the verdict changes which
  connectors can benefit)

**Approach.**
1. State the verdict in one sentence, then the evidence.
2. If unsound: quantify what partition-affine costs using `STRATEGY.md`'s metrics - head-of-line blocking
   avoided, achieved fan-out versus configured max - and say plainly that the module then matches
   `tasks.max`.
3. Name the next plan: implement the mechanism, or implement partition-affine and close the concurrency
   claim.

**Test expectation: none** - documentation. U3's evidence is the proof.

---

## Findings

*Populated by U1 and U2. Empty until the investigation runs - do not pre-fill.*

## Verdict

*Populated by U4. One sentence, then the evidence.*

---

## Verification Contract

| Gate | Command | Covers |
|---|---|---|
| Probe and regression arms | `./mvnw -pl parallel-consumer-connect -am test` | U3 |
| Full gate before any merge | `bin/ci-build.sh` | all |

`-am` is mandatory: without it `reactorModuleConvergence` fails, the module never recompiles, and the
result is a silent false negative. `-Dtest=` cannot be used on this module - it applies globally, and the
stock regression arm runs with an empty classes directory by design.

Java 8 API surface only (`--release 8` via Jabel), so `List.of` and `List.copyOf` are unavailable despite
the Java 17 source level.

## Open Review Findings - unapplied, from the 2026-08-10 doc review

Three reviewers (coherence, feasibility, adversarial) ran on this plan. **Six fixes were applied
in place**: the `close`/`commit` ordering correction, the clamp caveat, A3 recast as a mechanism
constraint, C3 restated in its owning-lane form, R1's third verdict shape plus new R6, and the Prior Art
section. The findings below were **not** applied and need a decision before U1 starts. Each names the
reviewer and its confidence.

Worth noting for whoever picks this up: two of the applied fixes corrected claims this plan's author had
personally verified against `WorkerSinkTask` source and still got backwards. Treat the unverified claims
below as suspect by default.

### P1 - blocking, resolve before U3

- **The crash-restart arm cannot be built at the path U3 names** (feasibility, 100).
  `CommitFrontierCrashRestartTest` extends a Testcontainers base; this module's own
  `TestConventionsArchTest` fails any such class outside an `integrationTest(s)` package; the module
  declares no testcontainers dependency (test scope is not transitive); and `mvn test` excludes those
  packages regardless. *Fix:* put the arm in
  `parallel-consumer-connect/src/test/java/io/confluent/parallelconsumer/connect/integrationTests/`, add
  the testcontainers / junit-jupiter / kafka test dependencies `parallel-consumer-streams/pom.xml` already
  carries, and change U3's Verification Contract row to `verify` so failsafe runs it.

- **The plan never fixes what map each lane's `preCommit` receives** (feasibility, 100).
  `PcSinkTaskLane` holds a bare `SinkTask`, so no per-lane `currentOffsets` exists - what a lane is handed
  is a PC design choice this plan never makes, and it decides the answer. connect-api 3.9.2's default
  `SinkTask.preCommit` returns its argument verbatim, so handing a lane the partition-wide map makes C1
  collapse to that same over-claiming value for every connector that does not override `preCommit`.
  *Fix:* add a first step to U1 naming the component that calls `preCommit` per lane and fixing the map to
  that lane's own consumed subset.

- **C3's probe needs a seam this plan forbids** (feasibility, 100). `PcTaskDispatcher` completes work the
  instant `chainExecution.run()` returns, and `WorkPreparer` hands back a bare `Runnable` with no
  completion handle - so a record is complete when `put()` returns, before durability. Deferring that
  requires a new seam in `parallel-consumer-streams`, which "no production code" prohibits, so U3 would
  test something other than what ships and KTD3's negative control would prove nothing about the real
  path. *Fix:* amend Tail ownership to permit U3 adding a deferred-completion seam behind the existing
  `WorkPreparer` interface. **Note:** a blocking `Runnable` is not a workaround - `dispatchAvailable`
  computes capacity as `poolSize - inFlight` and `inFlight` only decrements in `runOnWorker`'s finally, so
  the pump stalls at capacity.

- **An "unsound" verdict would be undiagnosable** (adversarial, 75). A1 fixes the lane primitive, so the
  verdict cannot distinguish "unsound in principle" from "unsound given *this* primitive" - different
  answers with different next moves, and the plan returns only the pessimistic one. A1 is legitimately out
  of revision this round; what is missing is the verdict recording that its negative result is conditional
  on it. *Fix:* extend R1 so an unsound verdict states which, and names the responsible property.

- **U1 step 3's findings had no consumer** (adversarial, 75). *Partly addressed* by the new R6, which
  requires each whole-partition assumption to be classified. Still open: R6 is not yet reflected in the
  Definition of Done, and U1's Requirements line still cites only R2.

### P2 - worth settling, not blocking

- **C4 is immune to KTD2's rejection test** (coherence, 75). KTD2 demands every rejection carry a two-lane
  interleaving; C4 is one-lane-per-partition, so no such interleaving exists. *Fix:* scope U2's goal to
  "reduce C1-C3", and state C4 is retained throughout as the comparison baseline per KTD4, exempt from
  KTD2.

- **Candidates survive by absence of a counterexample** (adversarial, 75). Rejections need a named
  interleaving; the survivor needs nothing. U3 then builds its probe from that same survivor's imagined
  interleaving, so a blind spot in U2 is invisible to U3 by construction. For a verdict authorising offset
  commits, "we could not break it" is materially weaker than "here is why it cannot break". *Fix:* require
  the survivor to carry a stated invariant, and add a U3 arm enumerating every completion order across two
  lanes and four offsets - a small enough space to exhaust.

- **The candidate list closed before U1 runs** (adversarial, 75). U2's goal is reduction only, with no step
  for adding candidates, yet U1 exists precisely to change what is known. At least one structurally
  distinct candidate is absent: splitting partition *identity* rather than offset space, so each lane sees
  the physical partition under a distinct `TopicPartition` and its watermark, rewind and close all stay
  honest per lane. *Fix:* add U2 step 0 - re-enumerate against U1's findings before rejecting anything -
  and require an unsound verdict to argue the space is closed rather than that the list was exhausted.

- **R5's cost figure has no measurable comparator** (adversarial, 75). R5 fires exactly when no working
  key-affine Connect arm exists to measure, and `STRATEGY.md` says neither metric is emitted as its own
  meter. The likely filler is the Streams module's 57x/8x presented as a Connect number. *Fix:* restate R5
  as an analytic claim - fan-out ceiling equals partition count, per-partition head-of-line blocking
  unaddressed - and forbid quoting the Streams figures as Connect measurements.

- **The verdict may be conditional on connector behaviour PC cannot detect** (adversarial, 75). *Partly
  addressed* by R1's new sound-conditional shape. Still open: nothing yet requires the next plan to carry
  a detection or opt-in gate where PC cannot verify the precondition at runtime.

### Residual risks the reviewers raised but did not file

- The crash-restart arm must inherit `CommitFrontierCrashRestartTest`'s *live* offset-scoped reader, not
  its described outline - the earlier phase-2-reads-from-earliest defect is written up in
  `docs/solutions/test-issues/a-restart-assertion-satisfiable-by-pre-crash-data-proves-nothing.md`.
- U1's reading of `WorkerSinkTask` and U3's probe share one model of the runtime. A misreading would be
  invisible to both arms at once, and the negative control - which inverts the composition function -
  cannot detect a modelling error.
- The verdict is pinned to Kafka 3.9.2 package-private internals read from a build-time-generated copy.
  These are not public contract and can change without deprecation, so the verdict must carry its version.
- KTD1's investigate-only boundary is thinner than it reads: U3 builds a composition function, a two-lane
  harness and a crash-restart arm, which is most of the mechanism.

### Reviewer questions worth answering in U4

- Which component owns the composed `preCommit` call - the patched `WorkerSinkTask`,
  `PcConnectDispatchBridge`, or a new composer - and on which thread does it run?
- If sound, does the composed frontier reach the broker through `WorkerSinkTask`'s own `doCommit` or
  through PC's commit path, and which owns the encoded incomplete-offset metadata?
- Does lane membership survive conversion and SMTs? A key-rewriting SMT breaks the assumption that a
  record's lane is stable, beneath any sound mechanism.
- What is the do-nothing baseline for the second persona - repartition the topic and raise `tasks.max` -
  and for which workloads does the key-affine path still beat it? The plan's comparison arm is
  partition-affine, not the option a user actually has today.

---

## Definition of Done

- The Verdict section answers R1 in one sentence, with evidence beneath it.
- Every rejected candidate names the interleaving that rejected it.
- U3's negative control demonstrably fails when the composition is inverted.
- Both existing regression arms still run 30/30, unchanged.
- The probe is deleted or promoted deliberately - not left as an orphan test.
- No test assertion was weakened to reach green.
