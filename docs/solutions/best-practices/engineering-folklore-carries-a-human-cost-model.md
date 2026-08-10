---
title: "Engineering folklore carries a human cost model, and a count of edits is its tell"
date: 2026-08-10
category: best-practices
module: repository-wide
problem_type: best_practice
component: development_workflow
severity: high
applies_when:
  - "You are about to argue against a change on the basis of how much of it there is"
  - "An objection is phrased as a count - six types, four subsystems, forty call sites, a lot of files"
  - "A design is being chosen because one option looks smaller, rather than because it validates more"
  - "A scope section is about to defer a list of capabilities that already exist somewhere"
  - "You are weighing modifying a dependency against building your own smaller version of it"
  - "A plan cites maintenance burden, tedium, or keeping copies in sync as a reason not to do something"
related_components:
  - parallel-consumer-connect
  - parallel-consumer-streams
  - documentation
tags:
  - agent-cost-model
  - engineering-folklore
  - decision-hygiene
  - effort-arguments
  - buy-vs-build
  - deferred-scope
  - argument-audit
status: "The rule itself is live in AGENTS.md, added 2026-08-08 under astubbs#240. This document is its searchable companion: the worked examples, the diagnostic tell, and the operational test."
related_prs:
  - "astubbs/parallel-consumer#240 - Connect on PC; the plan that was rejected for this reason and the one that replaced it"
  - "astubbs/parallel-consumer#255 - the Kafka Streams spike; the second instance, two days after the rule was written down"
related:
  - "../architecture-patterns/patch-the-seam-rather-than-reimplement-the-subset.md - the astubbs#240 example worked through as an architecture decision"
  - "../architecture-patterns/patch-a-dependency-at-build-time-without-vendoring-it.md - the mechanism whose perceived cost is almost entirely a human one"
  - "chase-refuted-predictions.md - what to do once the effort argument is gone and an empirical question is left"
---

# Engineering folklore carries a human cost model

## Context

[`AGENTS.md`](../../../AGENTS.md) already carries the rule, under
"You are a machine running techniques written for humans" (`AGENTS.md:32-72`):

> Nearly all engineering advice you have absorbed carries an unstated cost model: human attention,
> working memory, fatigue, and the tedium of repetitive work. You inherited the advice and the cost
> model together, and you cannot easily tell them apart. **The advice is usually right. The cost
> model is usually not yours.**

**Read that section first. This document does not restate its lists.** It is the searchable
companion: what the failure actually looked like twice in this repository, the one sentence shape
that gives it away, and what to do in the minute after you catch it.

The companion is needed because the rule alone did not stop the second instance. It was written
into `AGENTS.md` on 2026-08-08, prompted by astubbs#240, and on 2026-08-10 an
agent working astubbs#255 made the same move in a different module
(`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:1205-1207`). That is the important
property of this defect: **an effort argument does not feel like an effort argument from the
inside.** It presents as a design trade-off, phrased in engineering vocabulary, and it sits inside
an argument whose other half is sound. Knowing the rule is not the same as recognising an instance
of it.

## Guidance

### The test

Ask what the technique's justification rests on.

| The justification rests on | Verdict |
|---|---|
| Correctness, a guarantee, a measured constraint, evidence you would lose | **It binds.** Argue it. |
| Human attention, fatigue, tedium, how much typing it is, how many places it touches | **It does not bind.** Drop it and see what is left. |

### The tell: an objection shaped as a count of edits

Both instances below phrased the objection the same way, as a **count of things to change**:
"you would have to patch six types"; a deferred list of the framework's own features, each an item
someone would have to rebuild.

**A count of edits is never a cost to an agent.** Treat that sentence shape as a red flag on sight,
before you have decided whether the conclusion is right. It is the cheapest detector available,
because it is syntactic: you do not need to re-litigate the design to notice that the sentence is
counting.

The same detector fires on the near-synonyms: "that is a lot of files", "we would have to keep
those in sync", "that is a two-week change", "we would have to maintain the patch", "45 extra jars
to save fifteen lines of typing".

### Replace the count with the empirical question, do not just delete it

Deleting the objection loses information. The count was standing in for a real question nobody
asked, and the useful move is to name that question and go answer it.

In astubbs#255 the count was "you would have to patch six types". The empirical question underneath
it was **"what inside Kafka Streams actually calls these methods, and what would actually have to
change?"** - one grep and a read of `ProcessorTopology` in the 3.9.2 sources. Two things came back,
and neither was a count:

1. **No new patched class was needed at all.** `StreamTask` was already in the patch set and its
   constructor holds both the topology and the config, so the topology backstop costs nothing in
   patch surface (`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:1155`, `:1168`).
2. **A real constraint the count had hidden entirely.** Kafka's own test suite calls `join`,
   `windowedBy` and `suppress` extensively, so deleting the signatures stops that suite *compiling*
   and forfeits the module's strongest behaviour-preservation evidence (`:1186-1189`).

That second finding is the point. It is about **evidence**, not effort, it survives the strip, and
it is what actually decided the design: annotate and throw, keep the signatures. The effort
argument and the real argument pointed at the same code. Stripping the effort argument did not lose
the objection; it upgraded it into one that could be checked.

### Audit the objections that travel with the count

An effort argument rarely arrives alone. In astubbs#255 two supporting objections came with it and
both dissolved under one question each:

| Objection | Why it dissolved |
|---|---|
| "It breaks source compatibility with stock Kafka Streams" | That was the intended **effect**. An objection that restates the goal as a drawback is not an objection. |
| "A compile-time check catches less than the runtime check" | True, and a false dilemma. "Which one" was never the question; the answer was **both**, and the settled design has three layers (`:1177-1184`). |

So before treating any objection as a trade-off, classify it: is it (a) the goal restated as a
cost, (b) a false dilemma between options you could simply build both of, or (c) an effort claim?
None of the three is a trade-off.

### The operational rule

**Strip every effort-based reason from an argument and see what is left. If nothing is, the
objection was never real.** If something is, it is now stated in terms that can be tested rather
than asserted, which is the whole gain.

Do this to your *own* draft before you send it, not only to arguments you receive. Both instances
here were self-authored.

### What is still genuinely expensive

The rule collapses into "nothing is hard" if this half is dropped. `AGENTS.md:64-72` is the list;
what makes it credible here is that this repository has paid each one:

- **Wall-clock.** CI, broker integration tests, and builds do not care how fast you think. The
  runner-load work in `docs/inflight/ci-disabled-jobs-and-runner-load.md` exists because of it.
- **Non-determinism.** Concurrency bugs and flakes are hard for an agent too, and
  `docs/solutions/test-flakiness/` is the evidence - a directory of write-ups, several of which
  took controlled experiments to settle. Do not get confident here.
- **Judgement about people.** `STRATEGY.md:41-46` names the second persona ("they arrive with a
  topology or a sink connector ... the thing they are buying is that their code does not change").
  That statement retroactively decided the astubbs#240 direction, and no amount of agent-cheap work
  would have produced it. It had to be asked for.
- **Anything unverifiable.** No broker, no credentials, no production data. Say so plainly instead
  of reasoning past it.
- **Irreversible or outward-facing actions.** Cheap to perform is not the same as safe to perform.
  Pushing, publishing, and posting are all one command.

When an objection survives the strip and lands in this list, it is a real constraint and should be
argued at full strength.

## Why This Matters

**It has already cost a whole plan.** astubbs#240's first design
(`docs/plans/2026-08-08-001-feat-connect-sink-in-pc-plan.md`) is implementation-ready, carefully
argued, and rejected. Its own superseding header states the reason (`:13-19`): the approach forces
a long deferred list "because each is a Connect runtime feature the module would have to rebuild".
Rebuild by whom, at what cost? The list only makes sense priced in human effort.

**Knowing the rule does not prevent the instance.** Two days separate the two examples, and the
second was made by an agent working in a repository whose `AGENTS.md` already carried the rule.
That is why the syntactic tell matters more than the principle: you will not catch this by
remembering to be vigilant, you will catch it by noticing that your sentence is counting.

**The advice being right is exactly what makes it dangerous.** "Ship the smallest thing first" is
correct. It was correctly applied in astubbs#240 and produced the wrong answer, because *smallest*
was silently measured as *least third-party code touched* - the metric that tracks human effort -
rather than *what the increment validates*. The folklore does not need discarding; it needs its
units re-measured.

**One sentence from the owner settled each case.** On astubbs#240: *"you think it's hard because
you're trained on HUMAN data."* On astubbs#255: *"you're using a false premise - removing public
APIs is trivial for an agent."* Both are corrections to the cost model, not to the engineering.
When a correction of that shape arrives, the fix is to re-run the decision with the effort terms
deleted, not to patch the conclusion.

## When to Apply

- **Apply when** an argument you are making or reading contains a count of things to change.
- **Apply when** a scope or deferred section is being written. Sort it into *features someone else
  already implements* versus *decisions you are choosing to differ on*; the first bucket is the
  effort argument in list form (see the sibling architecture doc for the full test).
- **Apply when** choosing between modifying a dependency and building your own smaller version of
  it. "We would have to maintain the patch" is not a reason.
- **Apply when** an option is being rejected as "too invasive", "too much churn", or "too many
  places to keep in sync".
- **Apply when** someone tells you a premise is false. Re-derive the decision rather than defending
  the conclusion.
- **Do not apply when** the constraint is wall-clock, non-determinism, judgement about people,
  something you cannot verify, or an irreversible action. Those bind, and treating them as folklore
  is the opposite error.
- **Do not apply** to reader cost. Code that is exhaustive rather than clever is cheap for you to
  write *and* cheap for a human to read, which is why `AGENTS.md` keeps it; but a wall of
  duplication is expensive for a reader forever. Reader attention is a human cost that is genuinely
  charged.

## Examples

### 1. astubbs#240 - the reimplementation that looked smaller

**The move.** The first Connect plan built a new module against `connect-api` and reimplemented, in
reduced form, what Connect's `WorkerSinkTask` does. It looked like the smaller option because it
touched no Apache Kafka source at all. Under "Outside this product's identity" it explicitly ruled
out the direction that was later adopted
(`docs/plans/2026-08-08-001-feat-connect-sink-in-pc-plan.md:179`):

```
- "PC inside Connect" - patching `WorkerSinkTask.pollConsumer`. That is a fork of Kafka, not a library.
```

**The tell.** The deferred list (`:168-169`, `:182`) - SMTs, DLQ and `errors.tolerance`,
`ConfigProvider` secret resolution, plugin classloader isolation. Every entry is a capability
Connect's runtime already implements correctly today. Not one was deferred because different
behaviour was wanted; each was deferred because rebuilding it is expensive **for a human**.

**Stripped.** With the effort terms deleted, what remained was: the reimplementation caps
concurrency at the partition count, which is the product's entire value proposition (`:13-19`);
and the target user arrives holding a connector, so a new API asks them to fund a migration
(`STRATEGY.md:41-46`). Both are correctness-and-product arguments, and both point the other way.

**Outcome.** Direction reversed to patching Connect's own `WorkerSinkTask` at build time. The full
decision, including the honest costs patching does carry, is worked through in
[`../architecture-patterns/patch-the-seam-rather-than-reimplement-the-subset.md`](../architecture-patterns/patch-the-seam-rather-than-reimplement-the-subset.md).

### 2. astubbs#255 - the count of types, two days later

**The move.** An earlier draft argued *against* removing the unsupported operators from Kafka
Streams' DSL surface, so that windowed operators, joins and suppression could not be reached on a
path where they silently produce wrong results.

**The tell**, quoted from the plan's own correction
(`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:1205-1207`):

```
The earlier draft's argument against the DSL layer was separately wrong on its own terms: it
priced the work as "you would have to patch six types", which is a count of edits rather than a
technical cost.
```

**Stripped, then answered empirically.** No new patched class was required - `StreamTask` was
already patched (`:1155`, `:1168`). The constraint that did survive was about evidence, not effort
(`:1186-1189`):

```
Keeping the signatures is the whole point, and an earlier draft of this plan got that wrong by
proposing deletion. Kafka's own test suite calls `join`, `windowedBy` and `suppress` extensively.
Delete those methods and that suite stops *compiling* - forfeiting the 188-test result that is
currently this module's strongest behaviour-preservation evidence ...
```

**Outcome.** A three-layer refusal that keeps the signatures: `@DoNotCall` plus `@Deprecated` for a
compile-time error, a seam-guarded `UnsupportedOperationException` in the body, and a
`ProcessorTopology` check at task construction as the backstop (`:1177-1184`). The worklist entry
is `docs/inflight/pr-ks-spike-next-work.md:64-88`. Note that the *conclusion* of the original
objection - do not delete the methods - turned out to be right. It was right for a reason the
objection never gave, and it was reached only after the effort argument was thrown away.

### 3. The anti-example - what a surviving objection looks like

Not every "that is expensive" is folklore. From the same Connect decision
(`../architecture-patterns/patch-the-seam-rather-than-reimplement-the-subset.md`): patching couples
you to internals that carry no compatibility promise, and that cost is paid at **every Kafka
upgrade**, in patch drift. That one survives the strip, because it is not about how tedious
re-deriving a diff is; it is about a dependency you do not control changing under you on a schedule
you do not set. It was accepted explicitly, written into the plan's risk table, and bounded by a
stop condition on patch-set growth.

The difference between example 1 and example 3 is the whole skill. Both are sentences about cost.
Only one of them is still true when the person doing the work does not get tired.

## Related

- [`AGENTS.md`](../../../AGENTS.md) `:32-72` - "You are a machine running techniques written for
  humans". The rule itself, with the full cheap-for-you and still-genuinely-hard lists. Read it
  first; this document does not duplicate them.
- [`../architecture-patterns/patch-the-seam-rather-than-reimplement-the-subset.md`](../architecture-patterns/patch-the-seam-rather-than-reimplement-the-subset.md)
  - example 1 worked through as an architecture decision, with the deferred-list test and the
  whose-code-changes test. That document answers "which design?"; this one answers "why did the
  wrong one look smaller?".
- [`../architecture-patterns/patch-a-dependency-at-build-time-without-vendoring-it.md`](../architecture-patterns/patch-a-dependency-at-build-time-without-vendoring-it.md)
  - the mechanism both examples turn on. Its perceived cost is almost entirely a human cost model,
  which is why it reads as heroic and is not.
- [chase-refuted-predictions.md](chase-refuted-predictions.md) - what to do once the effort
  argument is gone and an empirical question is left standing in its place.
- [control-arms-vary-exactly-one-term.md](control-arms-vary-exactly-one-term.md) - how to settle
  the surviving question with a measurement rather than another argument.
- `docs/plans/2026-08-08-001-feat-connect-sink-in-pc-plan.md` - the rejected Connect design; its
  header states why (`:13-19`), and its deferred list is the artefact (`:156-183`).
- `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:1155-1207` - the DSL surface decision, the
  correction of the "six types" pricing, and the three-layer design that replaced it.
- `docs/inflight/pr-ks-spike-next-work.md:64-88` - where that decision is tracked as live work.
- `STRATEGY.md:41-46` - the second persona; the judgement-about-people input that neither example
  could have derived for itself.
