---
title: Patch the framework's seam rather than reimplement a reduced subset of it
date: 2026-08-10
category: architecture-patterns
module: parallel-consumer-connect
problem_type: architecture_pattern
component: background_job
severity: high
applies_when:
  - You need to change how a framework hands work to its processing chain, and the framework owns that execution model
  - A design's scope section is about to defer a list of capabilities the framework already ships
  - You are choosing between building a new module against the framework's API and modifying the framework's own runtime
  - Your users arrive holding the framework's artefact - a topology, a connector, a plugin - rather than your API
  - A reduced reimplementation looks like the smaller option because it touches no third-party code
tags:
  - framework-integration
  - build-time-patching
  - kafka-connect
  - deferred-scope
  - architecture-decision
  - worker-sink-task
  - strategy-fit
  - buy-vs-build
---

# Patch the framework's seam rather than reimplement a reduced subset of it

## Context

Running Kafka Connect sink connectors on Parallel Consumer (astubbs/parallel-consumer#240,
mirror of confluentinc/parallel-consumer#119) got two plans written a day apart, and the
difference between them is the learning.

The **first** design (`docs/plans/2026-08-08-001-feat-connect-sink-in-pc-plan.md`) built a
new `parallel-consumer-connect` module against `connect-api` only, with `connect-runtime`
explicitly excluded from main scope (R14, `:134-135`). Inside it, a reduced
reimplementation of what `WorkerSinkTask` does: instantiate the connector, derive task
configs, start a pool of `SinkTask`s, convert `ConsumerRecord` to `SinkRecord`, drive
`put()`, gate commits on `preCommit()`. Eight implementation units. It was a serious,
well-argued plan - the offset analysis in it is still the best in the repo - and it looked
like the *smaller* option, because it touched no Apache Kafka source at all.

The **second** design (`docs/plans/2026-08-09-001-feat-connect-on-pc-plan.md`) keeps
Connect's runtime intact and patches its own `WorkerSinkTask` at build time so it sources
records from Parallel Consumer, using the harness proven on the Kafka Streams spike
(`docs/solutions/architecture-patterns/patch-a-dependency-at-build-time-without-vendoring-it.md`).
That is the direction under development.

**This document is about the choice, not the mechanism.** The sibling document above covers
how to patch a dependency safely - unpack, apply, shadow, prove. It assumes you have already
decided the dependency is the thing to change. The question here is upstream of that: given
a framework you need to behave differently, do you modify *it*, or do you build your own
smaller version of the part you need?

## Guidance

### The deferred list is the measurement, and you already wrote it

The rejected plan's scope section carried this, under "Deferred to Follow-Up Work" and
"Outside this product's identity" (`:156-183`):

| Deferred item | Where it was deferred | Who already implements it |
| --- | --- | --- |
| Single Message Transforms | `:168` | `WorkerSinkTask.convertAndTransformRecord` |
| DLQ and `errors.tolerance` | `:168` | `RetryWithToleranceOperator`, held by `WorkerSinkTask` itself |
| `ConfigProvider` secret resolution | `:168-169` | `org.apache.kafka.connect.runtime.WorkerConfigTransformer` |
| Plugin classloader isolation | `:182` | `runtime.isolation.Plugins` / `DelegatingClassLoader` / `PluginClassLoader` |
| `SinkTaskContext.offset(...)` rewind | `:160-163` | `WorkerSinkTask.rewind()` |
| Per-partition `pause`/`resume` | `:173-174` | `WorkerSinkTask` via `context.pausedPartitions()` |
| Per-partition batching (more than one record per `put()`) | `:171-172`, KTD11 | `WorkerSinkTask`'s `messageBatch`, amortised over `max.poll.records` |

Seven items. Every one of them is a thing Connect's runtime already does, correctly, today.
Not one of them was deferred because we wanted different behaviour - each was deferred
because rebuilding it is expensive.

**That is the tell.** A deferred list made of the framework's own *features* is evidence
that you are rebuilding the framework badly. A deferred list made of *decisions* - a policy
you want to differ on, a guarantee you are choosing not to offer, a mode you will add when
someone asks - is ordinary scoping and means nothing of the kind. Sort your deferred list
into those two buckets before you commit to the design. If the features bucket is the long
one, you have not chosen the smaller option; you have chosen a partial, subtly-different
copy of the framework with a "not supported yet" list that never shrinks.

### Watch for the moment the deferral becomes a wall the user hits

The rejected plan handled its deferrals honestly, and the honest handling is itself the
signal. R17 (`:87-91`) made construction *fail*, naming the unsupported capability, when the
connector config contained `transforms*`, `predicates*`, `errors.*`, or a `${provider:...}`
reference - because "an operator who set `errors.tolerance=all` would believe poison records
are being diverted when they are stalling a partition."

That reasoning is right, and rejecting is strictly better than silently ignoring. But look
at what the module then is: a Connect host that refuses configurations Connect accepts, and
will go on refusing them until someone reimplements each one. The rejection wall is where
"deferred" stops being an internal scheduling note and becomes a permanent, user-visible
property of the product.

### Count the framework classes you would end up rewriting

KTD10 of the rejected plan is the whole trap in miniature (`:311-313`): write a pass-through
byte-array `Converter` by hand, because Connect's own `ByteArrayConverter` lives in
`connect-runtime`, not `connect-api`, and pulling `connect-runtime` in "would cost 45 extra
jars to save roughly fifteen lines."

Locally that is a sound trade. But it is the third or fourth time in one plan that the
answer to "the framework has this" is "so write a small one." Each instance is defensible;
the accumulation is the design. When you find yourself reimplementing the framework's
utility classes because your dependency choice excluded them, the dependency choice - not
the utility class - is the thing under review.

### The correctness surface shrinks too, not just the feature surface

This is the part that is easy to miss, because correctness work does not appear on a
deferred list - it appears as requirements you are proud of.

The rejected plan derived five requirements governing `preCommit()` reconciliation, and they
are genuinely subtle: never commit past what the task declared (R9), skip a partition
`preCommit()` omitted (R10), stay dirty and drop the encoding metadata when clamping (R11),
Connect's last-consumed-plus-one convention (R12), and - the delicate one - refuse an offset
that `preCommit()` returns *above* what was consumed (R15, `:124-126`), "because a driver that
trusts the task blindly would commit offsets it never consumed."

**Four of those five are already Kafka 3.9.2's `WorkerSinkTask.commitOffsets()`.** It filters
to the partitions it asked about, calls `preCommit`, returns early when the task opts out with
an empty map, and for each returned partition takes the task's offset only when
`taskOffset <= currentOffset`, logging `Ignoring invalid task provided offset ... not yet
consumed` otherwise - plus the not-assigned and not-requested cases the plan had not yet
enumerated. R9, R10, R12 and R15 are that method. R15 in particular is a `<=` comparison
someone already wrote, tested and shipped; the two designs differ only in the *reject* branch,
where Connect falls back to the last committed offset and the rejected plan would have
committed PC's own.

**R11 is the exception, and it is the informative one.** It has no Connect counterpart:
offset-encoding metadata anchored to `getOffsetHighestSequentialSucceeded() + 1`, and a
per-partition dirty flag that stops a clamped partition ever being committed again, are
Parallel Consumer's own (KTD4, `:242-251`). Clamping one without re-anchoring the other is a
defect only this integration can have.

That residue is the actual size of the work. A reimplementation does not merely defer the
framework's features; it re-derives the framework's *invariants*, from the outside, without
its tests - and buries the one requirement that was genuinely yours inside four that were
not. Those are the expensive requirements to get right and the expensive ones to be wrong
about.

### Ask whose code changes - it may decide the question on its own

`STRATEGY.md:41-46` describes the second persona: teams already on Kafka Streams or Kafka
Connect with one slow stage, who "arrive with a topology or a sink connector, not with a
consumer loop, and they are not shopping for a consumer library at all. They hire Parallel
Consumer as the engine their existing framework runs on, and the thing they are buying is
that their code does not change" - emphasis on that last clause is added here, and is the
whole of this section.

`STRATEGY.md` landed after the first plan was written. The adopted plan's reconciliation
section states the consequence flatly (`:158-168`): "it retroactively explains why the embed
direction was the wrong shape ... Embedding asked them to write PC code."

So patch-versus-reimplement was not only a cost question. A reimplementation exposes a new
API and asks the user to write against it; a patched runtime asks them to change a
dependency version. If your audience is defined by already holding the framework's artefact,
a design that requires them to abandon it does not serve that audience at all, however
cheaply it can be built. **Run the deferred-list test and the whose-code-changes test
separately - they can disagree, and when they agree the decision is settled.**

### The honest counterweight: what patching costs, and what it does not save you from

Patching couples you to internals that carry no compatibility promise and change across
versions. That cost is real, recurring, and paid at every upgrade, in patch drift - it is the
first row of the adopted plan's risk table (`:401`), and the sibling document's "do not apply
when the target is a moving trunk" exists for the same reason.

Two further costs specific to this direction, neither of which the deferred-list argument
touches:

- **Dependency weight lands on whoever consumes the artefact.** The rejected plan's KTD2
  (`:211-216`) measured it: `connect-api` plus `connect-json` resolves to 13 jars;
  `connect-runtime` resolves to 58, adding Jetty 9.4, Jersey 2.47, HK2, and a second logging
  backend. That was a good reason to avoid `connect-runtime`, and the patched design accepts
  it - `parallel-consumer-connect/pom.xml:47-51` takes `connect-runtime` at compile scope.
  It is tolerable *because the module is development-only* (`maven.deploy.skip`,
  `maven.install.skip`, `gpg.skip` at `:35-38`). It becomes a live question the moment
  publication is on the table, and it is tracked there:
  `docs/inflight/next-patched-kafka-packaging.md`.
- **Split-package shadowing is not a distribution mechanism.** See the sibling document; the
  technique is sound inside your own build and does not survive an arbitrary downstream
  classpath.

And the counterweight has a counterweight worth stating, because it was the surprise:
**reimplementing does not escape version coupling.** The rejected plan had to defer Kafka 4.x
too (`:164-167`), because `SinkTaskContext.pluginMetrics()` became abstract in Kafka 4.1 and
returns a type absent from kafka-clients 3.9.2, so one hand-written context could not compile
against both. Building against the *public* API bought a version-coupling problem of the same
kind - just paid in a different currency, and paid alongside the feature list rather than
instead of it.

The judgement here: the reimplementation's cost is permanent and user-visible (a feature list
that never completes, and an API the target user does not want), while the patch's cost is
periodic and internal (re-derive a diff at each Kafka upgrade), and it fails at *build* time
rather than at runtime. That is why it was judged worth paying. Say the cost out loud in the
decision record rather than discovering it at the first upgrade.

### Inheriting a feature is not the same as inheriting it free

Patching removes the obligation to *rebuild* the framework's features. It does not remove the
obligation to analyse how they interact with the thing you changed.

Two open questions on the adopted design make this concrete. SMTs are inherited, but they can
change a record's identity, and this design shards by key - so "which identity governs
sharding after Connect conversion and SMTs?" is explicitly deferred to follow-up design
(`:431-433`). And key-sharding takes whole-partition ownership away from a task, which some
connectors rely on for output naming or partition-keyed state - the whole reason
`parallel-consumer-connect/connector-compatibility.md` exists.

Be accurate about the claim, in both directions. At the point of writing this, the Connect
spike's bridge is inert and no live delivery seam exists, so nothing here has yet been
*demonstrated* to work with SMTs or a DLQ. What changed is the **kind** of deferral: from
"we must build this" to "we have not wired this up yet." That is the whole benefit, and
overstating it into "it just works" would be the same overclaim in the other direction.

## Why This Matters

**The reduced runtime could not deliver the product's reason for existing.** This is the
single strongest fact in the comparison, and it is downstream of the same root cause. KTD5 of
the rejected plan (`:255-271`) established that a `SinkTask` is not thread-safe and owns
partitions, so `ProcessingOrder.PARTITION` is forced, so the maximum number of concurrent
`put()` calls equals the assigned partition count - "identical to a Connect worker running
`tasks.max = <partitions>`." The plan says so plainly and refuses to let a later change claim
otherwise. But per `STRATEGY.md`, key-level concurrency *above* the partition count is the
entire value proposition. The reimplementation was going to be correct, honest, and pointless.
Note the inversion recorded in `docs/inflight/pr-connect-on-pc.md:54-56`: `KEY` ordering had
to be *rejected* under the embed design, and is the *natural* mode under the patched one.

**The deferred list is a diagnostic you get for free.** You write it anyway, during planning,
before any code exists. Sorting it into features-versus-decisions costs one pass and can
invalidate a whole design at its cheapest moment. Nothing else in the planning artefact is as
predictive of eventual scope, and it is right there in the scope section.

**"Smaller" measured as third-party code touched is the wrong metric.** By that measure the
rejected design won outright: zero lines of Apache Kafka source. By the measure that matters -
what must be built, tested, maintained, and eventually explained to a user as unsupported -
it was far larger. Pick the metric before the metric picks the design.

**The size stays measurable afterwards.** `parallel-consumer-connect/src/main/patch/pcconnect.patch`
is 19 lines against one named class (`connect.patched.classes` in
`parallel-consumer-connect/pom.xml:30` lists `WorkerSinkTask.java` and nothing else), versus
the rejected plan's eight implementation units. That comparison is not final - the spike's
patch is only an inert bridge and a live delivery seam will grow it - but the patch stays a
`wc -l` on a tracked file, so the honest number is always available, and a list that grows
past the sibling document's roughly-a-dozen-classes threshold tells you the verdict has
flipped.

## When to Apply

- **Apply when** the framework already implements, correctly, the things your reduced version
  would have to redo - and you can name them by listing what you were about to defer.
- **Apply when** your users' code is the framework's artefact. If they arrive with a topology,
  a connector, or a plugin, a new API is a migration you are asking them to fund.
- **Apply when** the framework has a nameable seam: one place where it hands work to its
  processing chain. `WorkerSinkTask` is that place for a Connect sink;
  `PartitionGroup.nextRecord()` feeding `StreamTask.process()` is that place for Streams.
- **Do not apply when** the deferred list is made of *decisions* rather than features. Wanting
  different guarantees from the framework is a real reason to build your own; wanting the same
  guarantees more cheaply is not.
- **Do not apply when** the framework's runtime dependency weight would land on consumers you
  do not control, and you have no answer for it. Development-only is an answer; "we will sort
  it out at publication" is a deferral of exactly the kind this document is about.
- **Do not apply when** the framework is unpinned or fast-moving, or when the patched class
  set keeps growing - both are the sibling document's stop conditions, and they bound this
  decision too.

## Examples

**The deferred list that decided it** - from the rejected plan's own scope section
(`docs/plans/2026-08-08-001-feat-connect-sink-in-pc-plan.md:168-169`, `:182`):

```
- Single Message Transforms, dead-letter queues and `errors.tolerance`, and `ConfigProvider` secret
  resolution. All six comparable projects surveyed omit all three.
...
- Connect's plugin classloader isolation. A library consumer owns their own classpath.
```

Read as a list of things Connect already does, this is not a scope boundary. It is an
inventory of the framework you are about to rebuild.

**The reconciliation, once `STRATEGY.md` existed**
(`docs/plans/2026-08-09-001-feat-connect-on-pc-plan.md:167-168`):

```
the second persona "arrive with a topology or a sink connector, not with a consumer loop",
and what they are buying is that **their code does not change**. Embedding asked them to
write PC code.
```

**The requirement that was already a `<=`** - Kafka 3.9.2 `connect-runtime`,
`WorkerSinkTask.commitOffsets()`, reachable locally after
`./mvnw -pl parallel-consumer-connect process-sources` regenerates the pristine tree:

```java
long taskOffset = taskProvidedOffset.offset();
long currentOffset = offsetsToCommit.get(partition).offset();
if (taskOffset <= currentOffset) {
    committableOffsets.put(partition, taskProvidedOffset);
} else {
    log.warn("{} Ignoring invalid task provided offset {}/{} -- not yet consumed, taskOffset={} currentOffset={}",
        this, partition, taskProvidedOffset, taskOffset, currentOffset);
}
```

That is R15 of the rejected plan, plus the not-assigned and not-requested cases it had not
reached yet. The reimplementation would have specified it, argued it in a KTD, and tested it
from scratch. The one thing the two designs genuinely disagree on is the reject branch -
Connect falls back to the last committed offset, the rejected plan would have committed PC's
own - and that is a *decision*, which is exactly the kind of thing that should stay on your
side of the line.

**The whole patch, at the feasibility stage** -
`parallel-consumer-connect/src/main/patch/pcconnect.patch`, one import and one static field:

```java
+import io.confluent.parallelconsumer.connect.PcConnectDispatchBridge;
...
+    private static final boolean PC_CONNECT_DISPATCH_ENABLED = PcConnectDispatchBridge.enabled();
```

Deliberately inert - it proves the patched class wins class loading and can resolve fork code,
and nothing more (KTD3, `docs/plans/2026-08-09-001-feat-connect-on-pc-plan.md:291-303`). The
comparison to make is not "19 lines beat eight units"; it is that the growth of this file is
the honest, ongoing measure of how much of Connect this integration actually has to own.

## Related

- `docs/solutions/architecture-patterns/patch-a-dependency-at-build-time-without-vendoring-it.md` -
  the mechanics once this decision is made: unpacking, applying, proving the shadowing, the
  control arm, licensing, and the stop conditions on patch-set growth
- astubbs/parallel-consumer#240 (mirror of confluentinc/parallel-consumer#119) - the issue;
  PR astubbs#269 carries the direction and the shadowing proof and is open as of this writing,
  stacked on astubbs#271 (the Kafka Streams spike whose harness it reuses)
- `docs/plans/2026-08-08-001-feat-connect-sink-in-pc-plan.md` - the rejected design, kept for
  its offset analysis; its own header states why it was superseded (`:13-23`)
- `docs/plans/2026-08-09-001-feat-connect-on-pc-plan.md` - the adopted design, its
  `STRATEGY.md` reconciliation (`:158-184`), and the follow-up design questions inheritance
  does not answer (`:421-435`)
- `docs/inflight/pr-connect-on-pc.md` - the live decision record: rejected direction, adopted
  direction, and the `KEY`-ordering inversion between them
- `STRATEGY.md:41-46` - the second persona, and the "their code does not change" clause that
  makes this a product question rather than only a cost one
- `parallel-consumer-connect/connector-compatibility.md` - which connectors the inherited
  runtime can actually be pointed at, and the bar for calling one verified
- `docs/inflight/next-patched-kafka-packaging.md` and
  `docs/inflight/release-experimental-modules-publication-disabled.md` - the dependency-weight
  and distribution costs this decision accepts, tracked where they will be paid
