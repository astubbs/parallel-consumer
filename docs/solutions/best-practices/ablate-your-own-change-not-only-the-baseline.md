---
title: "Ablate your own recent change, not only the baseline: two arms say whether the whole thing is better, never which part did the work"
date: 2026-08-11
category: best-practices
module: parallel-consumer-streams
problem_type: best_practice
component: testing_framework
severity: high
applies_when:
  - "A benchmark compares this project against an external baseline and the headline is a single ratio"
  - "A change landed recently in the module the benchmark exercises, and it has a kill switch"
  - "A plan argues from mechanism that one recent change cannot matter for the workload being measured"
  - "A result is about to be written up as independent of some change of your own"
related_components:
  - parallel-consumer-streams
  - PcDispatchSwitch
  - BacklogCatchUpBenchmarkTest
tags:
  - benchmarking
  - ablation
  - control-arm
  - attribution
  - kill-switch
  - wake-on-work
  - kafka-streams
status: "In flight as of 2026-08-11. PR astubbs/parallel-consumer#271 and issue astubbs/parallel-consumer#255 are both open, nothing merged. PR astubbs#271 carries the dispatch seam only; the wake-on-work fix is on branch feats/ks-streams-wake-on-work and the benchmark on branch test/ks-streams-realistic-domain-benchmark, neither of which has a PR of its own yet."
related_prs:
  - "astubbs/parallel-consumer#271 - the Kafka Streams dispatch seam that these benchmarks measure (open). Does not itself contain the wake-on-work fix that was ablated"
related:
  - "control-arms-vary-exactly-one-term.md - how to build one arm correctly; this doc is about which arms to run at all"
  - "chase-refuted-predictions.md - what to do in the hour after an ablation arm refutes a prediction"
  - "choose-the-statistic-that-states-the-claim.md - picking the number the arms are compared on"
  - "../integration-issues/kafka-streams-couples-polling-and-processing-on-one-thread.md - the poll-wait mechanism wake-on-work addresses"
---

# Ablate your own recent change, not only the baseline

> Extracted from `origin/feats/ks-streams-wake-on-work` @e735cdbc1, `docs/solutions/best-practices/ablate-your-own-change-not-only-the-baseline.md`.

## Context

`AGENTS.md:61-66` ("Settling it: a fix that works is not evidence of the cause") states the rule this
document extends:

> **Confirm a cause with a control arm, not with a fix that appears to work.** Change the one term you
> believe is responsible, hold everything else identical, and show the outcome flips. Same-magnitude,
> different-position beats bigger-hammer. [...]

That rule governs an arm you have already decided to build. It is silent on the prior question: **when a
system carries several recent changes at once, which arms are there?** The default answer, and the one
that gets written into plans, is two: the system, and the baseline it is claimed to beat.

Two arms answer "is the whole thing better". They cannot answer "which part of it is doing the work",
and the gap between those two questions is invisible while you are writing the plan, because the
two-arm result is a real, defensible, reproducible number. The failure is not in the measurement. It is
in the sentence written next to it, which attributes the entire delta to whichever mechanism the author
happens to be describing at the time.

This doc is the complement to two neighbours already in this corpus.
[control-arms-vary-exactly-one-term.md](control-arms-vary-exactly-one-term.md) is about arm **hygiene**:
once you have decided to isolate a term, how to isolate it precisely and not drag a derived second term
along. [chase-refuted-predictions.md](chase-refuted-predictions.md) is about the hour **after** a
refutation lands. This one sits before both: it is about arm **selection**, and about the mechanistic
reasoning that suppresses a measurement so effectively that no refutation ever exists to chase.

## Guidance

**1. One ablation arm per recent change, not one arm total.** If the system under measurement carries
N changes you might want to take credit for, the honest arm count is N+2, not 2: the baseline, the full
system, and the full system minus each change in turn. Credit assignment then becomes a subtraction
between two of your own arms rather than a narrative. The worked case had N=1 and therefore three arms,
with credit computed as `double attributable = withWake - withoutWake;`
(`BacklogCatchUpBenchmarkTest.java:286`, on branch `test/ks-streams-realistic-domain-benchmark`).

**2. Ablate your own most recent change first.** It is the one you understand least well, because it is
newest, and simultaneously the one you have the strongest incentive to describe as not-load-bearing
("the result stands on its own, it does not lean on our own optimisation"). Those two facts compound.
A change whose independence you would like to claim is precisely the change whose independence must be
measured rather than argued.

**3. Mechanistic reasoning about which term matters is a hypothesis. It earns a prediction line, not a
skipped arm.** "Work is always available, so the poll wait cannot matter" is a well-formed, plausible,
mechanism-level sentence. It was wrong, and its plausibility is exactly what stopped anyone measuring
it. Apply the cost asymmetry explicitly: the extra arm costs one run; the wrong attribution costs a
published claim crediting the wrong mechanism, and every downstream decision made on it.

**4. A counter is not a control arm.** Instrumenting how often a code path fires measures *frequency*,
not *contribution*. A path can fire on nearly every record and contribute nothing, or fire rarely and
carry the result. Planning to "report the counter to show the result does not lean on X" is the tell
that instrumentation is being substituted for an arm. If the claim is about contribution, the only
instrument that measures contribution is an arm with X removed.

**5. Build the kill switch, then use it as the arm.** Ablation is cheap exactly when the change already
has a runtime switch, and that is an independent reason to build one.
`PcDispatchSwitch.java:63` defines `WAKE_ON_WORK_PROPERTY = "pc.streams.wakeOnWork.enabled"`, and its
javadoc at lines 53-62 says so outright: the switch exists partly because "it is the **control arm**:
the before/after measurement for wake-on-work has to vary exactly one term, and flipping this leaves
the build, the JVM, the broker and the warm-up identical in a way that comparing against a parent
commit never can." `setWakeOnWork(boolean)` at lines 96-98 is the in-JVM equivalent for a test that
wants both arms in one process; `isWakeOnWorkEnabled()` at lines 88-90 is what the patched poll path
reads.

**6. Scope the switch's stated purpose to any measurement, not just its own before/after.** This project
had already written the kill-switch rule down: `docs/plans/2026-08-10-001-feat-ks-wake-on-work-plan.md`,
"KTD6. Ship a kill switch, and use it as the control arm.". It did not survive into the next brief one
day later, because it was scoped to *that fix's own* before/after measurement. A kill switch is the
ablation arm for every later measurement the change is present in, and saying so where the switch is
defined is what makes the next person find it.

**7. Make the ablation result travel with the headline.** Once you know a headline is the product of two
mechanisms, the number cannot be quoted alone. State what may and may not be said next to it, in the
result document and in the user-facing one, not just in the test log.

## Why This Matters

A two-arm benchmark that beats its baseline is a true result attached to an unverified attribution, and
the attribution is what people act on. Reviewers decide how hard to attack a mechanism based on how
load-bearing it is claimed to be. Documentation authors decide how prominently to feature it. Operators
decide whether a switch is safe to flip. All three read the attribution, not the ratio.

The direction of the error is not symmetric in consequence, and the harmless-sounding direction is the
dangerous one here. Over-crediting a change gets caught, because someone eventually removes it and the
number does not move. **Under-crediting is self-sealing**: nobody re-measures a mechanism that was
written up as incidental, so the claim that it is incidental never gets tested again, while the docs
under-describe it and the switch that disables it looks cheap to flip. In the worked case the
mis-attribution would have understated wake-on-work by roughly two thirds of the total benefit, and the
project's own ledger records the consequence in exactly those terms: whether it is "an optimisation or
load-bearing to the whole claim - and therefore how hard it must be defended in review and how
prominently it belongs in the README"
(`docs/inflight/next-questions-the-benchmark-raised.md:14-16`).

There is a second-order point about method. The mechanism at issue here was found **twice, by two
different arms**, and the second find was only possible because someone ablated the fix the first find
produced. The first was a negative control that came out at 0.69x on p50 when it was predicted to tie,
which exposed the StreamThread poll wait and led to wake-on-work being built at all (written up as
Example 3 in [chase-refuted-predictions.md](chase-refuted-predictions.md)). The second, described below,
is a different event: it measured how much of a *later* benchmark's headline that fix was carrying. **A
control arm that discovers a mechanism does not retire the question of how much that mechanism is
worth.** That takes its own arm, later, against the fix itself.

## When to Apply

- Any benchmark or performance claim run on a system carrying **more than one** unreleased or recent
  change. The moment the answer to "what is in this build that was not in the baseline" is a list,
  two arms are not enough.
- Whenever you are about to write, in a plan or a result, that a result "does not depend on" or "is
  independent of" some change of your own. That sentence is a claim about contribution and needs an
  arm.
- When a plan proposes reporting a **counter, a log line, or a hit rate** as evidence that a mechanism
  is not contributing. Replace it with an arm before the run, not after.
- When adding a kill switch or feature flag: note where it is defined that it is the ablation arm for
  any measurement, so the next person measuring knows the arm already exists and costs one property to
  flip.
- Before quoting a headline number in user-facing documentation. If no ablation exists, the number may
  be quoted, but the attribution sentence next to it may not.
- **Not** as a reason to delay a measurement indefinitely. The rule is one arm per recent change present
  in the build under measurement, not one per change in the module's history.

## Examples

The benchmark, its plan and its result document live on branch
`test/ks-streams-realistic-domain-benchmark` and are on neither master nor the wake-on-work branch;
read them with `git show test/ks-streams-realistic-domain-benchmark:<path>`. The plan and result are
cited by section rather than by line range, because both are still being edited. The kill switch itself
is on branch `feats/ks-streams-wake-on-work` and is cited by line.

**Where this work lives, as of writing.** `astubbs/parallel-consumer#271` (issue
`astubbs/parallel-consumer#255`) is **open** and carries the dispatch seam, but not the wake-on-work fix
being ablated here: its head branch `feats/ks-on-pc-spike` has no `PcWorkSignal` and no
`WAKE_ON_WORK_PROPERTY`. The fix is on `feats/ks-streams-wake-on-work` and the benchmark on
`test/ks-streams-realistic-domain-benchmark`, and neither branch has a PR of its own yet.

### 1. The plausible sentence that suppressed the arm

The benchmark plan chose a cold-start backlog workload partly on this reasoning
(`docs/plans/2026-08-11-001-test-ks-streams-realistic-benchmark-plan.md`, KTD1):

> **It neutralises this repo's own recent optimisation.** Wake-on-work (astubbs#255) exists to stop a
> worker completion waiting out a poll budget. With a full backlog the poll almost never has to wait,
> so a good result here cannot be attributed to that fix. A result that survives the removal of your
> own optimisation is stronger evidence than one that depends on it. The split-poll-wait counter is
> reported to demonstrate that it barely fired.

Every clause of that is reasonable. Note what the last sentence commits to: a **counter**, as evidence
of non-dependence. It became prediction P9 under "Predictions, stated before running" ("wake-on-work is
not doing the work here. Split-poll-wait counts in the backlog arms are a small fraction of records
dispatched"), and a reporting step under U4 ("Report the split-poll counters (P9) to show the result
does not lean on wake-on-work"). At no point does the plan schedule an arm with wake-on-work off, even
though the property to flip it already existed.

### 2. Both halves refuted, and the second one is the number

`docs/plans/2026-08-11-001-realistic-benchmark-result.md`, section 2.1.

**First refutation, the counter:** the split-wait branch fired on **1132 of 1200 records, 94%**. The
reason: "A backlog keeps the *broker* supplied, but Parallel Consumer's max concurrency still bounds
what the StreamThread may take in one pass, so the thread returns to the poll while its workers are
mid-flight and finds itself waiting on *them* rather than on the broker. **Saturating the topic does
not saturate the thread.**"

That killed the counter as evidence but still said nothing about contribution. So the counter was
replaced with an arm:

| Arm (1200-record backlog) | Rate | vs stock |
|---|---|---|
| Stock, seam off | 25.9/s | - |
| Seam on, **wake-on-work OFF** | 33.8/s | **1.31x** |
| Seam on, wake-on-work ON | 97.2/s | 3.76x |

**Second refutation:** "concurrent dispatch alone accounts for 1.31x of the 3.76x, and the poll fix
accounts for the remaining 2.45x". The result document states the outcome precisely: the claim "a
backlog result cannot be attributed to that optimisation" *"was not merely unproven, it was
backwards."*

The consequence is carried with the headline rather than buried in section 2.1: both mechanisms ship
and default on, so the user-facing ratio stands and the module's front door
(`parallel-consumer-streams/DEMO.md`) quotes a headline of that shape for its own scenario. But "nobody
may write 'this result does not depend on our recent poll optimisation', and anyone tempted to disable
wake-on-work should know it costs roughly two thirds of the benefit."

### 3. The test that replaced the counter

`BacklogCatchUpBenchmarkTest.howMuchOfTheAdvantageSurvivesWithoutWakeOnWork()` at line 267, javadoc at
lines 248-265. Two things to copy from it.

Its arm structure is **three arms, not two** (lines 273-286): stock is still there, unchanged; the new
thing is the middle arm, the system with one of its own changes removed. Credit is the subtraction
`withWake - withoutWake` at line 286, and the varied term is named in the report itself at line 290:
`"pc.streams.wakeOnWork.enabled, seam ON in both PC arms"` - which is the hygiene rule from
[control-arms-vary-exactly-one-term.md](control-arms-vary-exactly-one-term.md) applied to the arm this
doc tells you to select.

Its javadoc records why the counter was abandoned rather than quietly dropped (lines 261-264):

> So the counter is not evidence and this test does not use it. Instead it varies wake-on-work as its
> single term [...] That is the honest form of "does this result depend on your own optimisation": a
> measurement, not an assumption.

### 4. What made the arm cheap

Nothing in this required a build, a stash, or a comparison against a parent commit. The change already
had a switch, so the middle arm is one property flip inside the same JVM, same broker, same warm-up:
`PcDispatchSwitch.setWakeOnWork(false)` in a try/finally, restored to true afterwards, at
`BacklogCatchUpBenchmarkTest.java:276-281`. The system-property equivalent for a whole run,
`-Dpc.streams.wakeOnWork.enabled=false`, is documented for users under the module README's
"Wake on work" section (`parallel-consumer-streams/README.md`). The javadoc at
`PcDispatchSwitch.java:53-62` had already anticipated the use, and the wake-on-work plan's KTD6 had
named it. The arm was available for the price of deciding to run it, and it was not run, because a
plausible sentence said it could not matter.

## Related

- `AGENTS.md:56-90`, "Settling it: a fix that works is not evidence of the cause" - the general rule
  this doc extends; it tells you how to vary one term, not which term to pick.
- [control-arms-vary-exactly-one-term.md](control-arms-vary-exactly-one-term.md) - the complement on arm
  *hygiene*. Read it once this doc has told you which arm to build.
- [chase-refuted-predictions.md](chase-refuted-predictions.md) - what to do in the hour after a
  refutation. Its Example 3 is the *earlier, different* discovery (the 0.69x single-key control that
  found the poll wait and caused wake-on-work to be built); do not conflate it with the ablation
  described here.
- [choose-the-statistic-that-states-the-claim.md](choose-the-statistic-that-states-the-claim.md) - the
  companion failure mode: an arm can be selected correctly and still be asserted on a statistic that
  does not state the claim.
- [../test-issues/a-restart-assertion-satisfiable-by-pre-crash-data-proves-nothing.md](../test-issues/a-restart-assertion-satisfiable-by-pre-crash-data-proves-nothing.md)
  - the same ablation move in the test-validity domain ("delete or disable ONLY the mechanism under
  test and confirm red"), where the question is binary rather than an apportionment of credit.
- `parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcDispatchSwitch.java:53-98`
  - the kill switch, its javadoc justifying itself as the control arm, and the in-JVM setter.
- `parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcWorkSignal.java`
  - the mechanism being ablated: `hasActiveWorkOnCurrentThread()` (line 213) decides whether to split
  the poll wait, `awaitWorkForRemainderOf(Duration)` (line 230) performs it.
- `parallel-consumer-streams/README.md`, "Wake on work" section - the user-facing description and the
  off switch.
- `docs/plans/2026-08-10-001-feat-ks-wake-on-work-plan.md`, "KTD6. Ship a kill switch, and use it as the
  control arm" - where this project first wrote the kill-switch-as-arm rule, scoped too narrowly to
  reach the next brief.
- [../integration-issues/kafka-streams-couples-polling-and-processing-on-one-thread.md](../integration-issues/kafka-streams-couples-polling-and-processing-on-one-thread.md)
  - the poll-wait mechanism wake-on-work addresses. It carries the synthetic before/after figures and
  not yet the realistic-domain ablation above.
- `docs/inflight/next-questions-the-benchmark-raised.md:6-16` - the first of six recorded refutations,
  and the open follow-up: is wake-on-work specifically a saturation mechanism, or load-bearing
  everywhere?
- On branch `test/ks-streams-realistic-domain-benchmark` only:
  `docs/plans/2026-08-11-001-test-ks-streams-realistic-benchmark-plan.md` (KTD1 and P9, the suppressed
  arm), `docs/plans/2026-08-11-001-realistic-benchmark-result.md` (section 2.1, the measurement),
  `parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/BacklogCatchUpBenchmarkTest.java`
  (the three-arm replacement test), `parallel-consumer-streams/DEMO.md` (the user-facing front door the
  attribution attaches to).
- `astubbs/parallel-consumer#271` - the Kafka Streams dispatch PR, **open** at the time of writing.
- `astubbs/parallel-consumer#255` - the tracking issue, **open**. The wake-on-work branch and the
  benchmark branch have no PR of their own yet.
