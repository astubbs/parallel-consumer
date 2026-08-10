# PR astubbs/parallel-consumer#269: Kafka Connect sinks driven by Parallel Consumer

Issue: astubbs/parallel-consumer#240 (mirror of confluentinc/parallel-consumer#119)
Branch: `feats/connect-on-pc-spike`, stacked on `feats/ks-on-pc-spike` (PR base is that branch; retarget
to master when the parent lands).

## State (2026-08-10)

Active, stacked on `feats/ks-on-pc-spike` (PR astubbs/parallel-consumer#271) because it reuses that
module's patch scripts and `PcTaskDispatcher` rather than copying them.

The plan is `docs/plans/2026-08-09-001-feat-connect-on-pc-plan.md`; the superseded embed-direction plan
(`2026-08-08-001-...`) is kept for its offset analysis. U1 — the `parallel-consumer-connect` module and
its generated-`WorkerSinkTask` shadowing proof — is on the branch and green in CI. U2 (key-to-lane
dispatch, reconciled against the parent's commit surface) is next; the module README and boundary
write-up land with the plan's U3. Publication of both experimental modules is disabled — see
`release-experimental-modules-publication-disabled.md`; do not reverse before merge.

## The step after U2: frontier-composed commit — SETTLED, verdict written 2026-08-11

**The verdict is in `docs/plans/2026-08-10-001-investigate-connect-offset-composition.md`, section
`## Verdict`. Read it before planning the implementation.** Summary: **sound-conditional**. Candidate C3
holds — do not compose the watermarks at all; read each lane's `preCommit()` return against *that lane's
own* record stream, turn it into per-record durability facts, and let PC's existing frontier machinery
compose those. This upholds the 2026-08-08 plan's KTD5 rather than overturning it: the over-report KTD5
predicted is real, and is exactly what the investigation's negative control demonstrates on a real broker —
but it followed from *composing watermarks*, which C3 never does.

The conditions are about **whole-partition ownership**, not offsets: `SinkTaskContext.offset()` rewind,
`assignment()`, `onPartitionsAssigned` seeding, `RetriableException` handling, output identity derived from
`(topic, partition, offset)`, and a key-rewriting SMT ahead of the sink. The verdict says, for each, whether
PC can detect it or whether the next plan must carry an explicit connector opt-in.

Evidence: `OffsetCompositionProbeTest` (8 arms, surefire, including an exhaustive enumeration over every
completion order across two lanes and four offsets) and `OffsetCompositionCrashRestartTest` (failsafe,
Docker) — three broker-backed arms: the sound one, a trigger-removed control proving the frontier does
advance, and a negative control proving an over-commit reaches broker state as silent data loss.

**Evidence boundary, and it matters for what the next plan must build:** the broker arm runs no Connect
runtime. `PcConnectDispatchBridge.enabled()` still returns a hard-coded `false`; the poll/dispatch/commit
loop and the sink are written by the test. PC's half of the model is executed, Connect's half is still
argued from source. The arm that drives the patched `WorkerSinkTask` itself is the next plan's entry
criterion.

**One gap found and deliberately not fixed:** `PcSinkTaskLaneRouter.runDurabilityCycle()` polls each lane's
`preCommit` in a bare loop and `PcSinkTaskLane.preCommit` rethrows, so one connector throwing aborts the
cycle for *every* lane. Not a safety defect — nothing is confirmed, so nothing is over-committed — but one
lane can stall every other lane's commit progress. Per-lane fault isolation belongs to the implementation
plan; fixing it during an investigation would have been implementing the mechanism.

The original design note this section carried is now the verdict's, and the open design point it ended on —
PC must never retry a record that was `put()` but not yet durable — is settled by the
`DeferringWorkPreparer`/`CompletionHandle` seam plus the barrier's staged-then-promoted shape.

## Direction

**Rejected:** a new `parallel-consumer-connect` module on `connect-api` that reimplements a reduced
`WorkerSinkTask`. Review established two problems. Concurrency caps at the assigned partition count -
a `SinkTask` is not thread-safe and owns partitions, so `PARTITION` ordering is forced, which is the
same ceiling a Connect worker reaches at `tasks.max = <partitions>`. And every runtime feature the
module skipped (SMTs, DLQ/`errors.tolerance`, `ConfigProvider`, plugin isolation) was skipped only
because rebuilding it is expensive - which is the wrong reason.

**Adopted:** patch Connect so `WorkerSinkTask` sources records from Parallel Consumer, using the
build-time patch strategy proven in `feats/ks-on-pc-spike` - `bin/apply-patch.sh` / `bin/regen-patch.sh`,
generated classes gitignored, a shadowed-classloading test proving the patched classes win, a
stock-baseline fixture and control arm proving the harness is behaviour-neutral, and Kafka's own tests
as the regression oracle. Stack on that branch; assume it lands.

## The question the PoC exists to answer

Does key-sharding across more `SinkTask` instances than partitions preserve correctness?

`preCommit()` returns a watermark, which no task can honestly give for a partition whose records are
split across tasks. The proposed resolution: interpret each task's watermark against **that task's own
record stream** as a durability barrier, and let PC compose the sparse completion set across tasks and
encode it in commit metadata. Sparse out-of-order completion tracking is the one thing PC has that
Connect does not, and this is the first use that genuinely requires it.

Note the inversion: `ProcessingOrder.KEY` had to be *rejected* under the embed design, and becomes the
*natural* mode here. Keyed upsert sinks - JDBC upsert, Elasticsearch by document id, Mongo - are exactly
where key-level concurrency beyond partition count is worth money.

## Target: a tech demo alongside 0.6.0.0, not a production runtime

The plan's "What worth showing people means" section carries the MVP definition and the explicit
non-goals. Summary: an unmodified sink connector receiving real records through a PC-driven Connect
worker, frontier-committed with a crash-restart proof, one partition across several lanes, and a README
a stranger can follow. Publication stays gated by `release-spike-modules-publication-disabled.md`
independently of MVP readiness.

## Connector compatibility catalogue - started, all predictions

`parallel-consumer-connect/connector-compatibility.md` now exists and is seeded with clearly
labelled **untested predictions** grouped by what each connector assumes about partition ownership
(key-affine, partition-affine, unknown). Nothing is verified. A row becomes Verified only when that
connector's own published test suite passes against the patched runtime - the same bar the Streams spike
met with Kafka's 188 tests, and the only evidence an adopter should accept. Revisit before merge.

## Known constraint: not every connector tolerates losing whole-partition ownership

S3 and HDFS sinks name output files by topic-partition-offset, so two tasks each holding part of
partition 0 would collide. Same class of problem for any connector doing partition-scoped batching or
partition-keyed state.

**Follow-up to investigate (user, 2026-08-08):** because the extension owns the worker, the operating
mode does not have to be global - it can be selected **per connector, dynamically, inside one process**.
Different connectors would then impose different restrictions on PC's mode. Worth scoping: what signal
picks the mode (a registry of known connector classes, an explicit user override, or an opt-in capability
interface a connector author can implement), and what the safe default is when the connector is unknown.
Partition-affine is the obvious default - it degrades to the ceiling above, which is still
Connect-without-a-worker.

## Carried forward from the shelved plan

The offset analysis transfers unchanged and is the expensive part:

- The committed offset and its encoded incomplete-offset payload are both anchored to
  `getOffsetHighestSequentialSucceeded() + 1`, and the read side decodes against
  `OffsetAndMetadata.offset()`. Lowering one without re-encoding the other shifts the whole decoded set.
- `PartitionState.onOffsetCommitSuccess()` calls `setClean()`, and the commit gate is
  `isTimeToCommitNow() && wm.isDirty() && !isRebalanceInProgress`. A clamped partition that goes clean is
  never committed again.
- `preCommit()` runs on `pc-broker-poll`, not the controller thread, and on the revoke path inside
  `synchronized (commitCommand)` - the monitor of the open deadlock in astubbs/parallel-consumer#29.
- Connect's `preCommit()` contract: lower offsets honoured, higher rejected, an omitted partition means
  leave it where it was, an empty map means skip the commit entirely.

## Documentation owed when this ships

Not started. Two distinct audiences, and the second is the one that gets skipped:

- **End-user documentation** - how to point the module at a connector class and config, which
  `ParallelConsumerOptions` are constrained and why, what the operating modes mean and how one is chosen
  per connector, and the delivery guarantee. Goes in `src/docs/README_TEMPLATE.adoc` with `tag=`-delimited
  regions of real module source so the examples cannot rot. **Never hand-edit `README.adoc`** - regenerate
  with `./mvnw process-sources -N`.
- **Promotional material** - the README needs to say plainly what this is for and who should reach for it,
  next to the existing pitch. The honest hook is running Connect sink connectors in-process with no worker,
  no REST API and no internal topics, plus key-level concurrency beyond partition count for connectors that
  allow it. State the ceiling for connectors that do not, rather than letting a reader discover it after
  adopting.

Packaging and licensing are tracked separately in `next-patched-kafka-packaging.md`, and block publishing
either spike.

## Earmarked for a compounding run: a review severity calibrated on an unchecked premise

Not yet written up as a learning. Do it before this PR merges, while the evidence is still to hand.

The 2026-08-10 doc review filed C3's deferred-completion requirement as **P1-blocking**, on the reasoning
that deferring completion "requires a new seam in `parallel-consumer-streams`, which 'no production code'
prohibits". That was accepted and paid for with a scope carve-out in the plan's Goal Capsule.

It is a fourth instance of a pattern core already ships. `ExternalEngine.addToMailBoxOnUserFunctionSuccess`
is deliberately a no-op for async work, and `VertxParallelEoSStreamProcessor` completes the
`WorkContainer` later from the Vert.x event loop
(`VertxParallelEoSStreamProcessor.java:191-:195`); Reactor does the same. `PcTaskDispatcher`'s `completed`
queue is already a `ConcurrentLinkedQueue` drained on the owner thread, so a completion signalled from an
arbitrary thread needs no new concurrency design.

The generalisable lesson, and the reason it is worth compounding rather than just noting: **before
classifying a required seam as new production surface, check whether the codebase already implements that
shape for a sibling integration.** A novel seam and a fourth instance of an existing pattern carry
completely different risk, and the review's severity - which bought a real scope concession - was
calibrated on the novel reading without that check being run. The nearest existing learning,
`patch-the-seam-rather-than-reimplement-the-subset.md`, is about not rebuilding what the *dependency*
already does; this is the same mistake pointed inward, at our own repository.

Also worth capturing from the same review: the finding's accompanying warning that a blocking `Runnable`
would deadlock the pump (`poolSize - inFlight`, decrement in `finally`) is correct but describes a design
nobody proposed - `inFlight` tracks *pool occupancy*, not durability, so deferring the completion signal
never requires blocking a worker at all.

## Unrelated defect found while reviewing

`AGENTS.md` said `**/*IT.java` is included in failsafe. The root pom's failsafe `<includes>` lists only
`**/integrationTest*/**/*.java`, so a `*IT.java` outside an `integrationTest` package runs in neither
suite and reports nothing. `TestConventionRules` has it right. Not yet fixed.

## Open: a lane can still over-claim to `preCommit`, on staging order rather than delivery order

Found by an independent correctness review of the contiguous-prefix fix, and **reproduced twice against
the compiled classes** - once driving `PcSinkTaskLane` + `PcSinkTaskDurabilityBarrier` through a real
`PcTaskDispatcher`, once through the real `PcSinkTaskLaneRouter`. Not a hypothetical.

`PcSinkTaskDurabilityBarrier.advanceDeliveredThrough` stops the claim at the lowest offset still in
`staged`. That correctly blocks a gap the lane *knows about*. It says nothing about an offset routed to
this lane that has **not been staged yet** - and PC hands work out per shard, so two distinct keys sharing
one lane can be dispatched in any order. In the reproduction the first record the dispatcher handed over
was offset 20 of 40; the barrier immediately claimed through 21, though offsets 0-19 were routed to that
same lane and had never been staged.

The consumer-group commit is still safe - PC's frontier holds at the lowest incomplete offset regardless -
so this is not a data-loss path today. The damage is confined to the map handed to `preCommit`: for a
connector that overrides `flush`, it is an instruction to flush offsets the lane was never given, which is
the hazard `PcSinkTaskLane`'s own javadoc names.

Why it is parked rather than patched: the stop line the fix needs is "the lowest offset this lane will
*ever* be given and has not yet received", which is not derivable inside the barrier - it is a fact about
routing that only exists once the record has been routed. The two candidate shapes are (a) the router
declares a lane's owed offsets at registration time, before `prepare` runs, or (b) a lane's own records are
staged in strict offset order, buffering higher offsets until the lower ones arrive. Both change the
router's concurrency shape. `PcConnectDispatchBridge.enabled()` still returns `false`, so nothing reaches
this in a running system - but it must be settled before the router is wired to a real Connect runtime.

**Test-shape lesson worth keeping:** every existing probe arm stages every offset up front and then varies
*delivery* order. That is precisely why this survived - no arm varies *staging* order, so the whole class
of defect was invisible by construction. A regression arm has to drive staging through the real dispatcher
with several keys on one lane.
