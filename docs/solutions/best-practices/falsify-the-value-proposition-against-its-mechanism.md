---
title: "Falsify a plan's value proposition against the mechanism that would deliver it, not the prose that motivates it"
date: 2026-08-10
category: best-practices
module: parallel-consumer-connect
problem_type: best_practice
component: development_workflow
severity: high
applies_when:
  - "A plan or design justifies itself with a performance, concurrency, or capability claim"
  - "The justifying sentence is a general truth about the product rather than about this design"
  - "A design's key decisions constrain a knob the product's headline claim depends on"
  - "Writing success criteria for work whose whole point is a number nobody has measured yet"
  - "Reviewing a plan and about to accept its Problem Frame without reading any source"
tags:
  - plan-review
  - value-proposition
  - falsifiability
  - success-criteria
  - inherited-premise
  - concurrency-ceiling
  - kafka-connect
related_components:
  - parallel-consumer-core
  - documentation
related:
  - "fresh-work-needs-independent-review.md - independence is a property of separate contexts; this doc is the same principle aimed at a plan's premise rather than its code"
  - "chase-refuted-predictions.md - the sibling for refutations that arrive from a measurement rather than from reading source"
  - "../documentation-gaps/competitor-comparison-docs-must-cite-the-primary-spec.md - the same failure applied to claims about someone else's system"
---

# Falsify a plan's value proposition against the mechanism that would deliver it

## Context

`AGENTS.md` (*Before you investigate anything* > *Settling it: a fix that works is not evidence of the
cause*) already tells you to state predictions before running them, and to settle a cause with a control
arm rather than a fix that appears to work. Both rules assume there is something to *run*. This document
covers the case one step earlier, where nothing has been built yet and there is nothing to measure: **a
plan whose reason to exist is a claim, and whose own design decisions already contradict it.**

The evidence is astubbs/parallel-consumer#240 (mirror of confluentinc/parallel-consumer#119), the Kafka
Connect on Parallel Consumer work. The first plan,
[`docs/plans/2026-08-08-001-feat-connect-sink-in-pc-plan.md`](../../plans/2026-08-08-001-feat-connect-sink-in-pc-plan.md),
proposed a new module embedding real `SinkTask` instances inside PC. Its stated purpose was that
Parallel Consumer decouples processing concurrency from partition count, so a Connect sink hosted in PC
would no longer be capped by partitions.

That sentence is true of Parallel Consumer in general. It is roughly the product's own headline framing -
`src/docs/README_TEMPLATE.adoc:125` (and the generated `README.adoc:125`) positions PC and Share Groups
as both solving "partitions are fixed, I need more consumers". The plan inherited it rather than checking
it, and it was never restated as anything a reader could test.

Reviewers dispatched over the plan (three, per this session's record) each went and read the sharding
code instead of the framing, and each returned the same refutation. The record survives in the plan's own
superseded-direction header (`:13-24`) and in
[`docs/inflight/pr-connect-on-pc.md`](../../inflight/pr-connect-on-pc.md) under *Direction*: "Review
established two problems. Concurrency caps at the assigned partition count".

## Guidance

**1. Name the mechanism the claim rides on, then go read it.** A capability claim is always cashed out by
some specific code: a scheduler, a lock, a key function, a pool size. Write down which one, open it, and
read the lines that decide the number. In this case the claim rested on two methods in
`parallel-consumer-core`, and they settle it in about fifteen lines:

- `ShardKey.of(ConsumerRecord, ProcessingOrder)`
  (`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/ShardKey.java:31-36`) shards
  by `TopicPartition` under `PARTITION` and `UNORDERED`, and by (topic, key) under `KEY`:

  ```java
  return switch (ordering) {
      case KEY -> ofKey(rec);
      case PARTITION, UNORDERED -> ofTopicPartition(rec);
  };
  ```

  So under `PARTITION` there is exactly one shard per assigned partition.

- `ProcessingShard.getWorkIfAvailable`
  (`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/ProcessingShard.java:127-188`)
  stops scanning a shard after considering a single container whenever ordering is restricted
  (`:149-154`), and `isOrderRestricted()` is `options.getOrdering() != UNORDERED` (`:225-227`) - so
  `PARTITION` restricts.

  ```java
  if (isOrderRestricted()) {
      // can't take any more work from this shard, due to ordering restrictions
      // processing blocked on this shard, continue to next shard
      log.trace("Processing by {}, so have cannot get more messages on this ({}) shardEntry.", this.options.getOrdering(), getKey());
      break;
  }
  ```

One shard per partition, at most one record in flight per shard: **achievable concurrency equals the
partition count.** That is the same ceiling a stock Connect worker reaches with `tasks.max` set to the
partition count. The plan's entire justification evaporates in the reading, with no build and no
benchmark.

**2. Suspect the premise hardest when it is a general truth about the product.** "PC decouples
concurrency from partition count" is true. It is simply not a statement about a design that *mandates*
`ProcessingOrder.PARTITION`. A claim inherited from a product's marketing altitude carries no evidence
about a specific design that constrains the product, and it arrives pre-approved, which is exactly what
stops anyone checking it. Treat provenance as a risk signal: a premise nobody in the plan had to derive
is a premise nobody verified.

**3. Compose the key decisions and check what they add up to.** Each decision here was individually
correct, and the refutation lives only in their product:

| Decision | Individually | Composed effect on the ceiling |
|---|---|---|
| KTD5: mandate `ProcessingOrder.PARTITION`, reject `KEY`/`UNORDERED` (`:255-262`) | Correct - the only mode giving one in-flight record per partition, which is what makes a per-partition task safe | Shards = partitions |
| KTD6: one `SinkTask` per share of the assignment, `min(maxConcurrency, assignedPartitions)` (`:273-278`) | Correct - Connect's own model; a single task owning everything delivers zero parallelism | Tasks ≤ partitions |
| KTD7: guard every `SinkTask` callback with that task's own lock | Correct - `SinkTask` is not thread-safe | One `put()` in flight per task |

Nothing in the table is wrong, and no reviewer of any single row would object. Review the *composition*
explicitly, because that is the only place this class of contradiction is visible.

**4. Restate the claim as a measurable success criterion, inside the plan.** The durable fix is not to
delete the sentence; it is to convert it into something a later change can fail. The corrected plan says
what the number is and how you would see it (`:145-147`):

> Concurrency is measured, not assumed: with P partitions and `maxConcurrency > P`, the observed maximum
> of simultaneously-executing `put()` calls is P. This criterion exists to keep the honest ceiling in
> KTD5 true over time rather than letting a later change quietly claim more.

and repeats it as a unit-level criterion with concrete numbers at `:860-862` (4 partitions,
`maxConcurrency = 16`, observed maximum 4). Compare that with the original motivating prose: no number,
no observer, nothing to fail.

**5. State the ceiling in the product-facing text too, in the same words.** The corrected plan carries a
section headed *"What this does not do: raise the concurrency ceiling"* (`:59-62`) and KTD5 ends "This
module does not decouple concurrency from partition count and must not be described as if it does"
(`:266-267`). A claim that was wrong once will be re-inherited by the next reader of the README unless
the correction lives where they will look.

**6. A falsified value proposition relocates the ambition; it does not have to kill it.** The refutation
did not end the feature. It moved the question to where the mechanism actually is, and the plan that
replaced it,
[`docs/plans/2026-08-09-001-feat-connect-on-pc-plan.md`](../../plans/2026-08-09-001-feat-connect-on-pc-plan.md),
opens with the ambition phrased as something that can come out false (`:67-69`):

> The spike therefore asks a narrower, falsifiable question: can the execution side preserve per-key
> order and per-task seriality while routing a single partition to multiple task lanes?

Same goal - concurrency above the partition count. Different mechanism - patch Connect so
`WorkerSinkTask` sources from PC, and route keys to serial task lanes, rather than mandate `PARTITION`
and accept its ceiling. Reading the mechanism is what told the work which term it actually had to change.

## Why This Matters

Left unchecked, this plan would have shipped a module whose stated benefit it did not deliver. The
failure would not have surfaced as a bug: the code would have been correct, the tests green, the
concurrency real - just numerically identical to `tasks.max = <partitions>` on a stock worker. Nothing in
a build, a test suite, or a code review of any individual decision fails when a plan's *reason* is wrong.
The only thing that catches it is someone reading the mechanism, and the motivating prose is precisely
what persuades them they do not need to.

The cost of the check is the asymmetry worth remembering. Refuting this premise took reading two methods
in one module. It happened before implementation, so the correction cost was editing a plan. The same
refutation arriving after the module shipped costs the module, its tests, its docs, and the credibility
of the claim in the README.

Note also what made the refutation trustworthy rather than merely plausible: the reviewers were separate
dispatched contexts, each reading source, and they converged on the same fifteen lines. Agreement between
lenses inside one context would not have been evidence of anything - see
[fresh-work-needs-independent-review.md](fresh-work-needs-independent-review.md), which is the doc for
that half and is not restated here.

## When to Apply

- Reviewing or writing any plan whose Problem Frame contains a performance, throughput, concurrency, or
  capability claim - before agreeing to the design, not after.
- Whenever a plan's justification is a sentence you could copy verbatim out of the project's README. That
  is the strongest available tell that it was inherited rather than derived.
- Whenever a design mandates, disables, or pins the exact knob its benefit depends on (an ordering mode, a
  pool size, a lock scope, an isolation level). The mandate is where the ceiling gets set.
- When writing success criteria: any criterion that cannot be observed at a stated number is motivation
  wearing a criterion's clothes.
- **Not** as a substitute for measurement once the mechanism permits the claim. Reading source bounds what
  is *achievable*; it does not tell you what the system *achieves*. That is
  [chase-refuted-predictions.md](chase-refuted-predictions.md)'s territory, and the two compose - read the
  mechanism to know what to predict, then measure.

## Examples

### The claim as motivation (refuted) versus the claim as criterion (checkable)

Before - product-level framing, sitting above a design that constrains it. Nothing here can fail:

> Parallel Consumer decouples processing concurrency from partition count, so hosting a Connect sink in
> PC removes the partition cap.

After - the same subject, expressed so a later change can invalidate it
(`docs/plans/2026-08-08-001-feat-connect-sink-in-pc-plan.md:145-147`):

> Concurrency is measured, not assumed: with P partitions and `maxConcurrency > P`, the observed maximum
> of simultaneously-executing `put()` calls is P.

The second form names the observer (count simultaneous `put()` entries), the condition (`maxConcurrency >
P`), and the expected value (P). It is falsifiable by a test, which is the whole difference.

### The assertion that keeps the ceiling honest

The adopted direction ships exactly that instrument. `RecordingSinkTask` in
`parallel-consumer-connect/src/test/java/io/confluent/parallelconsumer/connect/PcSinkTaskLaneRouterTest.java`
counts concurrent entries into the callback (`:42-43`, `:54-56`) and the seriality test asserts the
maximum (`:214-216`):

```java
assertThat(task.maxConcurrentEntries.get())
        .as("the lane's own re-entrancy detector - more than one means the lock did not hold")
        .isEqualTo(1);
```

Per `AGENTS.md`'s rule that a guard must be verified by negative control, the counter is itself proven
capable of failing: `negativeControlWithoutTheLockDetectsConcurrentEntry` (`:220-249`) drives the same
load straight at the task with the lane lock bypassed and asserts the count goes above one, "otherwise
the seriality test above is vacuous". A concurrency ceiling asserted by an instrument nobody has seen
move is decoration.

### The composition table, applied

Reading KTD5 alone: correct. KTD6 alone: correct. KTD7 alone: correct. Reading all three against the
claim in the Problem Frame: `PARTITION` gives one shard per partition
(`ShardKey.java:34`), the shard yields at most one record at a time (`ProcessingShard.java:149-154`),
tasks are capped at the partition count, and each task admits one `put()` - so the maximum concurrent
`put()` is the partition count, which is `tasks.max = <partitions>`. The contradiction is not in any row.
It is in the sum, and only a reviewer who computes the sum will see it.

## Related

- [`AGENTS.md`](../../../AGENTS.md), *Before you investigate anything* > *Settling it: a fix that works is
  not evidence of the cause* - the prediction and control-arm rules this extends backwards, to the stage
  before anything is built.
- [fresh-work-needs-independent-review.md](fresh-work-needs-independent-review.md) - why separate
  contexts, not separate personas, are what made the three refutations corroborating rather than
  coincidental; also its rule 4, the code-comment sibling of an unverified plan premise.
- [chase-refuted-predictions.md](chase-refuted-predictions.md) - the same discipline once a measurement
  exists to refute against.
- [../documentation-gaps/competitor-comparison-docs-must-cite-the-primary-spec.md](../documentation-gaps/competitor-comparison-docs-must-cite-the-primary-spec.md)
  - the mirror image: claims about *another* system sourced from summary awareness rather than the spec.
  This doc is that failure turned inward, on claims about our own design.
- [`docs/plans/2026-08-08-001-feat-connect-sink-in-pc-plan.md`](../../plans/2026-08-08-001-feat-connect-sink-in-pc-plan.md)
  - the superseded plan, kept deliberately; its header (`:13-24`), the honest-ceiling section (`:59-62`),
  KTD5 (`:255-269`) and the measured-concurrency criteria (`:145-147`, `:860-862`).
- [`docs/plans/2026-08-09-001-feat-connect-on-pc-plan.md`](../../plans/2026-08-09-001-feat-connect-on-pc-plan.md)
  - the adopted direction, and the falsifiable form of the same ambition (`:67-69`).
- [`docs/inflight/pr-connect-on-pc.md`](../../inflight/pr-connect-on-pc.md) - *Direction*, recording what
  review established and why the embed approach was rejected.
- astubbs/parallel-consumer#240 (mirror of confluentinc/parallel-consumer#119) - the issue this was
  learned on.
