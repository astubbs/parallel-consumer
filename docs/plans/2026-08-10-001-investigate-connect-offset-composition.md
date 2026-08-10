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

**Tail ownership.** Investigation only. The output is a written verdict plus the experiment that produced
it - no implementation units, and no production code **except one carve-out**: U3 may add a
deferred-completion seam behind `PcTaskDispatcher.WorkPreparer` if the surviving candidate needs one.
Without that carve-out the probe cannot test what would ship. `WorkPreparer.prepare` returns a bare
`Runnable` (`PcTaskDispatcher.java:115`) and `runOnWorker` completes the work the instant it returns
(`:332-336`), so under today's seam a record is durable-by-definition the moment `put()` returns, which is
the exact claim under investigation. A probe built on that seam would confirm the composition by
construction and KTD3's negative control would prove nothing about the real path.

Blocking inside the `Runnable` is **not** an alternative to the seam: `dispatchAvailable` computes capacity
as `poolSize - inFlight` (`:288`) and `inFlight` only decrements in `runOnWorker`'s `finally` (`:340`), so
waiting for durability inside the callback stalls the pump at capacity and deadlocks the very lanes whose
flush it is waiting on.

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
  - R1a. A **sound-conditional** verdict states, for each precondition PC cannot verify at runtime, that the
    next plan must carry either a detection or an explicit connector opt-in. A precondition with neither is
    an unenforced assumption wearing a verdict's clothes.
  - R1b. An **unsound** verdict says whether it is unsound *in principle* or unsound *given A1's lane
    primitive*, and names the responsible property. These have different next moves - abandon the approach,
    versus revisit the primitive - and A1 freezes that primitive without being itself under test, so a
    verdict that does not distinguish them returns only the pessimistic reading.
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
- R5. If the verdict is "unsound", the investigation states what partition-affine mode costs **as an
  analytic claim, not a measurement**: the fan-out ceiling equals the partition count, and per-partition
  head-of-line blocking is unaddressed. R5 fires exactly when no working key-affine Connect arm exists to
  measure against, and `STRATEGY.md` emits neither quantity as its own meter, so there is nothing to
  measure. **The Streams module's figures must not be quoted as Connect numbers** - different runtime,
  different dispatch path, and the tempting filler is precisely the false one.

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
  **C4 is exempt**, and not as a courtesy: it puts one lane on a partition, so the two-lane interleaving
  this rule demands cannot exist for it. C4 is the comparison baseline under KTD4 and is retained
  throughout rather than passing a test it is structurally immune to.
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
was considered. **None is yet endorsed, and the list is not closed** - U1 exists to change what is known,
so U2 step 0 re-enumerates against U1's findings before rejecting anything.

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
  ceiling is the partition count. The comparison baseline, not a split-partition candidate: it is retained
  throughout rather than reduced in U2, and is exempt from KTD2. Include to quantify R5.
- C5. **Split the partition's *identity*, not its offset space.** Each lane sees the physical partition
  under a distinct `TopicPartition` - so its watermark, its `SinkTaskContext.offset()` rewind and its
  `close()` are all honest *for the partition it believes it owns*, with no synthetic offsets and no
  deferred completion. Structurally distinct from C1-C3, which all keep one `TopicPartition` and reconcile
  after the fact. The question is what breaks on the way back: whether PC can map lane-partitions to real
  partitions on the commit path, and whether a connector that derives output identity from
  `SinkRecord.topic()`/`kafkaPartition()` - the partition-affine sinks - is simply excluded here too.
  Added after the doc review, which found the list had closed before U1 could inform it.

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

**Requirements.** R2, R6.

**Files.** Read-only: `parallel-consumer-connect/target/connect-patched/org/apache/kafka/connect/runtime/WorkerSinkTask.java`
(generated at build time; run `./mvnw -pl parallel-consumer-connect -am -DskipTests generate-sources` if
absent). Findings recorded in this plan under Findings.

**Approach.**
0. **Decide what map each lane's `preCommit` is handed, and which component hands it over.** This is a PC
   design choice the plan cannot skip, because it decides the answer. `PcSinkTaskLane` holds a bare
   `SinkTask` and calls only `put` - there is no per-lane `currentOffsets` anywhere today, so something has
   to construct one. Name the owning component (patched `WorkerSinkTask`, `PcConnectDispatchBridge`, or a
   new composer), the thread it runs on, and fix the map to **that lane's own consumed subset**.

   Handing a lane the partition-wide map is not a neutral default. `SinkTask.preCommit`'s base
   implementation in connect-api 3.9.2 is `flush(currentOffsets); return currentOffsets;` - verified by
   disassembly, not javadoc - so for every connector that does not override `preCommit`, the partition-wide
   map both returns an over-claiming watermark *and* instructs the connector to flush up to offsets that
   lane never saw. C1 then collapses to that same over-claim for the whole default-implementation
   population, which is most connectors.
1. Trace `origOffsets` -> `currentOffsets` -> `preCommit` -> `commitOffsets`, naming each call site.
2. Establish what `currentOffsets` contains when a task received only a subset of a partition's records.
3. Determine whether Connect anywhere assumes a task owns whole partitions in a way that a split violates
   beyond the offset question - `SinkTaskContext.assignment()`, `offset()` rewind, and pause/resume are
   the suspects.

**Verification.** Each claim in Findings carries a `WorkerSinkTask.java` call-site citation, step 0's
chosen owner and map are written down, and every whole-partition assumption from step 3 is classified per
R6.

### U2. Falsify or survive each candidate on paper

**Goal.** Reduce the split-partition candidates - C1, C2, C3, C5, plus anything step 0 adds - to at most one
live candidate, with a stated reason for each rejection. **C4 is not in the reduction**: it is retained
throughout as the comparison baseline per KTD4 and is exempt from KTD2's interleaving test.

**Requirements.** R1, R2, R4.

**Dependencies.** U1.

**Files.**
- `docs/plans/2026-08-10-001-investigate-connect-offset-composition.md` (modify - record each candidate's
  rejection interleaving, or the survivor's requirement, under Findings)

**Approach.**
0. **Re-enumerate before rejecting.** Read U1's findings against the candidate list and add any candidate
   they make visible. The list above was written before the runtime was traced, and a reduction step that
   can only subtract will never recover a candidate the list was missing.
1. For each candidate, construct the interleaving that would break it: two lanes, one partition,
   completions out of order across lanes.
2. Reject on mechanism only. Record the interleaving that does the rejecting.
3. For any survivor, state what it requires from Connect and confirm Connect provides it - **and state its
   invariant**: the property that makes it safe, in a form U3 can test directly. Surviving U2 means no
   counterexample was imagined, which is weaker than the verdict needs. Rejections carry a named
   interleaving; the survivor must carry a named reason it cannot break, or U3 inherits U2's blind spot
   by construction and cannot see it.

**Execution note.** Write the predicted outcome for each candidate *before* tracing it, and record
refutations as prominently as confirmations - `docs/solutions/best-practices/chase-refuted-predictions.md`.

**Verification.** Every rejected candidate has a named interleaving; the survivor has a named requirement,
the call site that satisfies it, and a stated invariant.

### U3. Run the experiment that settles the survivor

**Goal.** Move the verdict from argument to evidence.

**Requirements.** R1, R3.

**Dependencies.** U2.

**Files.**
- `parallel-consumer-connect/src/test/java/io/confluent/parallelconsumer/connect/OffsetCompositionProbeTest.java` (create - in-memory probe, surefire; deleted or promoted once the verdict lands)
- `parallel-consumer-connect/src/test/java/io/confluent/parallelconsumer/connect/integrationTests/OffsetCompositionCrashRestartTest.java` (create - broker-backed arm, failsafe)
- `parallel-consumer-connect/pom.xml` (modify - test-scope dependencies the crash-restart arm needs)
- `parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcTaskDispatcher.java` (modify, only if the survivor needs it - the deferred-completion seam the Goal Capsule's carve-out permits)

**The crash-restart arm cannot live beside the probe**, and this is enforced three ways, so discovering it
during U3 would cost a rebuild: `TestConventionRules.integration_tests_must_live_in_an_integrationTest_package`
fails any Testcontainers-using or `BrokerIntegrationTest`-derived class outside an `integrationTest(s)`
package; surefire excludes `**/integrationTest*/**/*.java` and failsafe includes exactly that; and the
module currently declares no testcontainers dependency at all.

It also **cannot extend `BrokerStreamsIntegrationTest`** the way `CommitFrontierCrashRestartTest` does -
that class is package-private in the streams module's test sources and streams publishes no tests
classifier. Extend core's `BrokerIntegrationTest<String, String>` directly; the `parallel-consumer-core`
`tests` classifier is already a test dependency of this module.

The pom needs, all at test scope: `org.testcontainers:testcontainers`, `:junit-jupiter`, `:kafka`,
`org.awaitility:awaitility`, and `org.apache.commons:commons-lang3` - the last because
`BrokerIntegrationTest` itself uses it and test-scope dependencies are not transitive, which
`parallel-consumer-streams/pom.xml:153-159` records having learned the hard way (the container fails to
start with `NoClassDefFoundError`).

**Approach.**
1. Build the smallest harness that reproduces the interleaving U2 identified as decisive: one partition,
   two lanes, a controlled completion order.
2. Include a **negative control that must fail** - a variant where the composition is deliberately wrong,
   proving the probe can detect breakage. A probe that cannot fail proves nothing
   (`docs/solutions/best-practices/control-arms-vary-exactly-one-term.md`).
3. **Exhaust the small space rather than sampling it.** Two lanes and four offsets is a completion-order
   space small enough to enumerate in full, so enumerate it and assert the composed frontier never exceeds
   the lowest incomplete offset. This is what stops U3 from only re-testing the interleaving U2 already
   imagined - the one place a U2 blind spot would otherwise stay invisible.
4. If the survivor holds, extend to a crash-restart in the shape of the Streams module's
   `CommitFrontierCrashRestartTest`: park a record, let others complete around it, kill, restart, assert
   the resume point. Copy its **live offset-scoped reader**, not its described outline - a phase-2 read
   from earliest is satisfiable by pre-crash data and proves nothing
   (`docs/solutions/test-issues/a-restart-assertion-satisfiable-by-pre-crash-data-proves-nothing.md`).

**Test scenarios.**
- Two lanes hold one partition; lane A's records complete after lane B's higher-offset records. The
  composed frontier must not exceed the lowest incomplete offset.
- A lane returns a watermark below what it consumed (legitimate per Connect). The composed frontier
  respects it.
- A lane omits the partition entirely. The composed frontier does not advance on its behalf.
- All lanes return an empty map. No commit occurs.
- Negative control: composition deliberately takes the maximum rather than the safe function. The probe
  detects the resulting over-commit.
- Exhaustive: every completion order across two lanes and four offsets. The composed frontier never
  exceeds the lowest incomplete offset in any of them.
- Crash-restart (failsafe): after a kill, redelivery begins at or below the composed frontier and no
  durable record is delivered twice beyond the at-least-once contract.

**Verification.** `./mvnw -pl parallel-consumer-connect -am test` green for the probe, `./mvnw -pl
parallel-consumer-connect -am verify` green for the crash-restart arm (failsafe, needs Docker), both
existing regression arms still running Kafka's `WorkerSinkTaskTest` unchanged, and the negative control
failing when inverted.

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
2. If unsound: state what partition-affine costs per R5 - as the analytic claim, not a measurement - and
   say plainly that the module then matches `tasks.max`. Do not reach for the Streams module's figures.
3. Name the next plan: implement the mechanism, or implement partition-affine and close the concurrency
   claim.

**Test expectation: none** - documentation. U3's evidence is the proof.

---

## Findings

### U2 predictions - recorded before U1's trace returned

Written first on purpose, per U2's Execution note and
`docs/solutions/best-practices/chase-refuted-predictions.md`. Predictions written after the evidence are
not predictions. Each is marked HELD or REFUTED once U1 and U2 run, and refutations stay in place.

- **C1 (min across lanes): predicted SAFE, and predicted that the plan's own "likely useless" framing is
  wrong.** The reasoning the candidate was written with - a slow lane pins the frontier - is about *commit
  lag*, not throughput. Lanes keep processing at full rate regardless of where the committed frontier sits;
  the only cost of a lagging frontier is more replay after a crash. If that holds, C1 is not the consolation
  option it was filed as. Its real problem should be sought elsewhere: what a lane that has consumed
  *nothing* for P contributes to a minimum, and whether the lane set owning P is even stable.
- **C2 (synthetic offsets): predicted SOUND-CONDITIONAL, rejected in practice.** Mechanically fine,
  defeated by observability - `SinkRecord.kafkaOffset()` reaches connector output, and Connect's own error
  reporting and DLQ records carry the offset. PC cannot detect a connector that writes it.
- **C3 (durability barrier): predicted SOUND, and predicted to be the answer.** Its trick is that it never
  composes watermarks at all: it converts each lane's watermark into per-record durability facts and lets
  PC's existing frontier machinery compose them. The interleaving that defeats every watermark scheme -
  lane A completing offset 50 after lane B completed 100 - is precisely what PC's incomplete-offset
  encoding already exists to express. Predicted cost: it needs the deferred-completion seam, so it is the
  candidate that spends the Goal Capsule's carve-out.
- **C5 (split identity): predicted REFUTED on the return path.** The consumer-group commit must name the
  real `TopicPartition`, and `SinkRecord.kafkaPartition()` becomes a lie to the connector - which is C2's
  observability problem wearing a different costume, plus a mapping problem C2 does not have.

**Prediction about the shape of the answer:** sound-conditional, not sound - with the condition being
connector observability of offsets rather than anything about ordering.

## Verdict

*Populated by U4. One sentence, then the evidence.*

---

## Verification Contract

| Gate | Command | Covers |
|---|---|---|
| Probe and regression arms | `./mvnw -pl parallel-consumer-connect -am test` | U3 steps 1-3 |
| Crash-restart arm (Docker) | `./mvnw -pl parallel-consumer-connect -am verify` | U3 step 4 |
| Full gate before any merge | `bin/ci-build.sh` | all |

The crash-restart arm needs its own row because surefire never runs it: it lives in an `integrationTests`
package, which surefire excludes and failsafe includes. A `test`-phase gate would report green having
skipped the arm entirely - the exact silent false negative this module's regression design exists to
prevent.

`-am` is mandatory: without it `reactorModuleConvergence` fails, the module never recompiles, and the
result is a silent false negative. `-Dtest=` cannot be used on this module - it applies globally, and the
stock regression arm runs with an empty classes directory by design.

Java 8 API surface only (`--release 8` via Jabel), so `List.of` and `List.copyOf` are unavailable despite
the Java 17 source level.

## Review Findings - from the 2026-08-10 doc review, now applied

Three reviewers (coherence, feasibility, adversarial) ran on this plan. **Six fixes were applied on the
first pass**: the `close`/`commit` ordering correction, the clamp caveat, A3 recast as a mechanism
constraint, C3 restated in its owning-lane form, R1's third verdict shape plus new R6, and the Prior Art
section. The remaining eleven were recorded here unapplied; **all eleven are now applied**, and this
section is kept as the record of where each landed rather than as a queue.

Worth noting for whoever picks this up: two of the first-pass fixes corrected claims this plan's author had
personally verified against `WorkerSinkTask` source and still got backwards. That is why the three P1
code-claims below were re-verified against the artifacts before being applied rather than taken on the
reviewer's word - and two of them turned out to be **understated**.

| Finding | Reviewer | Landed in |
|---|---|---|
| Crash-restart arm unbuildable at U3's path | feasibility, 100 | U3 Files + the three-way enforcement note; Verification Contract `verify` row |
| No decision on what map a lane's `preCommit` gets | feasibility, 100 | U1 step 0 |
| C3's probe needs a forbidden seam | feasibility, 100 | Goal Capsule carve-out; U3 Files; DoD |
| An unsound verdict would be undiagnosable | adversarial, 75 | R1b |
| U1 step 3's findings had no consumer | adversarial, 75 | U1 Requirements (R6), U1 Verification, DoD |
| C4 immune to KTD2's rejection test | coherence, 75 | KTD2 exemption; U2 Goal |
| Candidates survive by absence of counterexample | adversarial, 75 | U2 step 3 invariant; U3 step 3 exhaustive arm |
| Candidate list closed before U1 runs | adversarial, 75 | C5; U2 step 0 |
| R5 has no measurable comparator | adversarial, 75 | R5 restated; U4 step 2 |
| Verdict conditional on undetectable behaviour | adversarial, 75 | R1a |
| Crash-restart must copy the live reader | residual | U3 step 4 |

**What re-verification changed.** Two findings were applied in a stronger form than filed:

- The `preCommit` default is not merely "returns its argument". Disassembling connect-api 3.9.2 shows
  `flush(currentOffsets); return currentOffsets;` - so handing a lane the partition-wide map also
  *instructs the connector to flush* to offsets it never saw, which is a data claim, not just an
  over-claim. In U1 step 0.
- The crash-restart arm cannot extend `BrokerStreamsIntegrationTest` at all: it is package-private in the
  streams module's test sources and streams publishes no tests classifier. It must extend core's
  `BrokerIntegrationTest<String, String>`, and needs `commons-lang3` on top of the testcontainers set -
  a dependency the streams pom documents having discovered by container-start failure. In U3 Files.

The third P1 held exactly as filed: `WorkPreparer.prepare` returns a bare `Runnable`
(`PcTaskDispatcher.java:115`), `runOnWorker` completes on return (`:332-336`), and capacity is
`poolSize - inFlight` with the decrement in `finally` (`:288`, `:340`) - so blocking in the callback
deadlocks rather than defers.

### Residual risks the reviewers raised but did not file

- U1's reading of `WorkerSinkTask` and U3's probe share one model of the runtime. A misreading would be
  invisible to both arms at once, and the negative control - which inverts the composition function -
  cannot detect a modelling error. The crash-restart arm is the only check that runs against a real
  broker rather than against the model, which is a second reason it is not optional.
- The verdict is pinned to Kafka 3.9.2 package-private internals read from a build-time-generated copy.
  These are not public contract and can change without deprecation, so the verdict must carry its version.
- KTD1's investigate-only boundary is thinner than it reads, and the P1 carve-out thinned it further: U3
  now builds a composition function, a two-lane harness, a crash-restart arm and possibly a dispatcher
  seam, which is most of the mechanism. That is accepted rather than denied - the alternative was a probe
  testing something other than what ships - but it means the DoD's "deleted or promoted deliberately" line
  applies to the seam as much as to the probe, and that an "unsound" verdict must still end with the
  carve-out reverted.

### Reviewer questions worth answering in U4

- ~~Which component owns the composed `preCommit` call, and on which thread?~~ **Promoted to U1 step 0** -
  it turned out to be a decision the investigation depends on, not a question the write-up could answer
  afterwards.
- If sound, does the composed frontier reach the broker through `WorkerSinkTask`'s own `doCommit` or
  through PC's commit path, and which owns the encoded incomplete-offset metadata?
- Does lane membership survive conversion and SMTs? A key-rewriting SMT breaks the assumption that a
  record's lane is stable, beneath any sound mechanism.
- What is the do-nothing baseline for the second persona - repartition the topic and raise `tasks.max` -
  and for which workloads does the key-affine path still beat it? The plan's comparison arm is
  partition-affine, not the option a user actually has today.

---

## Definition of Done

- The Verdict section answers R1 in one sentence, with evidence beneath it - and satisfies R1a or R1b if
  it is sound-conditional or unsound.
- Every rejected candidate names the interleaving that rejected it; the survivor names its invariant.
- Every whole-partition assumption U1 step 3 surfaced is classified per R6 - absent, benign under split
  lanes, or a separate gate. No unclassified assumption remains beneath a sound or sound-conditional
  verdict.
- U1 step 0's answer is recorded: which component calls `preCommit` per lane, on which thread, with which
  map.
- U3's negative control demonstrably fails when the composition is inverted.
- Both existing regression arms still run 30/30, unchanged.
- The probe is deleted or promoted deliberately - not left as an orphan test. If the U3 carve-out added a
  seam to `PcTaskDispatcher`, it is likewise kept deliberately or reverted, not left behind as unused
  production surface.
- No test assertion was weakened to reach green.
