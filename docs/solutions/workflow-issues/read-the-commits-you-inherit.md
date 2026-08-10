---
title: "Read the commits you inherit - a green build does not tell you the ground under your design moved"
date: 2026-08-10
category: workflow-issues
module: git-history
problem_type: workflow_issue
component: development_workflow
severity: medium
applies_when:
  - "Your branch has just been merged, rebased, or replayed onto a parent branch that moved while you were working"
  - "A merge conflict in a file you did not touch hints at a rename or restructure you were never told about"
  - "You are about to override, revert, or disable something an earlier commit deliberately turned on"
  - "A stacked PR's parent landed work while the child branch was in flight"
  - "You need an experiment design, benchmark, or harness for an area a sibling branch has already measured"
tags:
  - git
  - rebase
  - merge
  - stacked-prs
  - commit-messages
  - handoff
  - prior-art
---

# Read the commits you inherit

## Context

Branches in this repo are routinely stacked: the Connect work
(astubbs/parallel-consumer#240) sits on top of the Kafka Streams work
(astubbs/parallel-consumer#255), which sits on master. Each of those parents keeps moving while the
child is in flight, so a child branch takes on new commits repeatedly - by merge, by rebase, by
replay onto a moved base.

What gets checked after that is the build. The build answers a narrow question: does the code still
compile and do the tests still pass. It has nothing to say about whether the *assumptions* the branch
was designed against are still true, because assumptions do not have a compiler.

This doc is the reader-side half of history hygiene. Its neighbour,
`re-cut-history-non-interactively-and-verify-the-tree-is-identical.md`, is the writer-side half: how
to rewrite a branch's commits safely and prove nothing was lost. That doc's instruction to *"write
the commit message for the story this new commit tells"* only pays off if somebody downstream
actually reads it. This is that reader.

## Guidance

**After any merge, rebase, or replay onto a moved branch, read the commit messages you just took
on.**

```bash
git log --oneline <old-base>..<new-base>     # what arrived
git log -1 --format=%B <sha>                 # the body of anything touching your area
```

The `--oneline` pass is triage; the subject lines tell you which commits are near your work. The
payload is in the **bodies**, which `--oneline` hides by design. Every one of the findings below was
in a body, not a subject.

Three things hide there, and none of them announce themselves.

### 1. Instructions addressed to your branch

A commit body can be a handoff. The parent branch's tip commit, which settled a strategy question,
closed its argument with a sentence aimed squarely downstream:

> Connect inherits this and should extend the second persona rather than re-litigate whether one
> exists.

That sentence was never read. It was obeyed anyway - the Connect agent reached the same conclusion
independently days later by reading the finished `STRATEGY.md`. That is luck, not process, and the
luck is thinner than it looks: had the agent *disagreed*, it would have argued the opposite in
perfect good faith, never knowing the question had been deliberately settled and handed over. The
handoff had no other channel. It was not in an issue, not in a plan document, not in `STRATEGY.md`
itself - it was in the commit body, addressed to whoever came next, which was the correct place to
put it.

### 2. Decisions that reshape your work

The same movement renamed a module and its Java package: `parallel-consumer-streams-spike` became
`parallel-consumer-streams`, and `io.confluent.parallelconsumer.streamsspike` became
`io.confluent.parallelconsumer.streams`. That was discovered by *tripping over a merge conflict* - the
rename asserted itself
through a mechanical failure rather than through anyone being told.

Worse, because it failed silently: a new `STRATEGY.md` naming the project's target problem, approach
and personas had arrived, and its existence was discovered only because the owner asked whether it
had been read. Reconciling the Connect plan against it changed the plan's framing materially. Nothing
would have flagged it. A document arriving is not a build failure.

### 3. Arguments against what you are about to do

A commit you are overriding usually explains itself, and that explanation is the strongest review you
will get for free. When publication was disabled for both experimental modules, the commit it
overrode had made a real case for the opposite:

> depending on the artifact IS the opt-in, so a user who adds it should not then have to switch it on

The override was correct on other grounds - see
`redistribution-obligations-attach-at-publish-not-at-build.md`; opt-in semantics are a usability
question and redistributing modified Apache Kafka is a legal one. But the argument was worth reading
precisely because it was not wrong, and it was recorded beside the override for that reason. Had it
not been, the next contributor would have read the publication guard as an oversight and reverted it.

### And one purely positive reason

Reading is not only defensive. The inherited commits carried a directly reusable experiment design: a
head-of-line-blocking measurement whose negative control was *inverted* - with every record on one
key, the parallel path measured 0.69x, i.e. **slower**, which is exactly what it must be when key
ordering forbids concurrency and the thread-pool handoff still costs something. A control that merely
tied would have been weaker evidence. That design, its refuted predictions and the throttle it
uncovered are now three separate learnings
(`../best-practices/control-arms-vary-exactly-one-term.md`,
`../best-practices/choose-the-statistic-that-states-the-claim.md`,
`../integration-issues/kafka-streams-couples-polling-and-processing-on-one-thread.md`). The
alternative to reading the commit that produced them was reinventing the same experiment, worse.

## Why This Matters

The three failure modes above all occurred on astubbs/parallel-consumer#240, in a single session.
That is the argument: this is not a rare hazard requiring an unlucky alignment, it is what routinely
happens on a stacked branch whose parent is alive.

They share a shape. **Each one passes every automated check.** A design built on a superseded strategy
compiles. A plan that ignores a document it never saw compiles. A revert of a deliberate guard
compiles - and its tests pass, because the guard was about what is *published*, not about what is
*built*. The only detector for any of them is a human or agent reading prose that a machine cannot
evaluate.

That is also why the failure gets rationalised. The instruction *was* obeyed; the rename *was*
eventually noticed; the strategy doc *was* eventually read. Every one of those looks like a
near-miss that resolved itself, and near-misses that resolve themselves teach nothing unless someone
writes down that the mechanism was luck. Only one of the three was caught by the process (the
conflict), and it was caught in the least informative way available - as an error, after the design
work had already been done against the old names.

Finally, the cost objection. Reading a dozen commit messages takes seconds. "That is a lot of commits
to read" is an *effort* argument, and effort arguments are calibrated to a human's attention budget,
not an agent's - see `../best-practices/engineering-folklore-carries-a-human-cost-model.md` and
`AGENTS.md` > *You are a machine running techniques written for humans*. Strip the effort claim out
of the objection and check what is left. Usually nothing is.

## When to Apply

- **Immediately after** any merge, rebase, or replay - before resuming design work, not after the
  next build goes green. The point is to catch moved ground before you build more on it.
- **Before overriding, reverting or disabling anything.** Run `git log -1 --format=%B` on the commit
  that introduced what you are about to undo. If it made an argument, answer it in your own commit
  body - "the override stands on ground the argument does not address" is a complete answer; silence
  is not.
- **When a merge conflict appears in a file you did not touch.** That is a rename or restructure
  announcing itself through the wrong channel. Read the commits behind it rather than just resolving
  the conflict.
- **When you are about to design an experiment** in an area a sibling branch has worked. Check what
  it already measured, and specifically what its controls did.
- **Not** for commits far from your area. Triage on `--oneline`; read bodies selectively. The
  discipline is "read what touches your area", not "read everything".

## Examples

**The method, on a stacked branch.** After the Connect branch took on the Streams parent:

```bash
# what arrived - triage on subjects
git log --oneline <old-base>..<new-base>

# then the bodies of anything near your area
git log -1 --format=%B <sha-of-the-strategy-commit>
```

The `--oneline` line for the strategy commit read *"name the second persona, and record what the
metadata collision actually did"* - accurate, and it does not contain the handoff. The handoff was
the last sentence of the body. Triage on subjects, decide on bodies.

**What a good override looks like.** The publication-gate commit did not silently win the argument;
it stated the case it was overruling and why the overruling ground was different in kind:

> Owner decision, 2026-08-10. [...] Both jars carry compiled, modified `org.apache.kafka` classes, so a
> publish is redistribution of a modified Apache Kafka; that must be a deliberate reviewed act, never
> a side effect of a master push.

A later reader hitting that guard finds a reason, not a mystery. Write your overrides so they survive
being read by someone who only has the commit.

**What "instruction in a body" looks like, and where else to put it.** The strategy commit's handoff
worked as a design (the right reader was the next branch), and failed as delivery (nobody read it).
Both halves are the lesson: keep writing handoffs into commit bodies - there is often no better
place - *and* read them on the receiving end. If the instruction genuinely must not be missed, it
belongs in the durable artefact too (`docs/inflight/`, the plan, `STRATEGY.md`), not only in the
commit.

## Related

- `re-cut-history-non-interactively-and-verify-the-tree-is-identical.md` - the writer-side half.
  Re-cutting deliberately rewrites commit messages to tell the right story; this doc is why that
  investment is not cosmetic.
- `redistribution-obligations-attach-at-publish-not-at-build.md` - the override in example 3, and the
  ground it stood on.
- `../conventions/status-words-belong-in-status-artefacts.md` - the reasoning behind the module and
  package rename that was discovered by merge conflict rather than by reading.
- `../best-practices/engineering-folklore-carries-a-human-cost-model.md` - why "that is a lot of
  commits to read" is not an argument that binds an agent.
- `AGENTS.md` > *Read the commits you inherit* - the short-form rule; this doc is the evidence behind
  it.
- `AGENTS.md` > *Before you investigate anything* - the same principle applied to prior art. Inherited
  commits are prior art that arrived without being searched for.
- astubbs/parallel-consumer#240 (Connect on PC) and astubbs/parallel-consumer#255 (Kafka Streams on
  PC) - the stacked pair this was learned on. Cited as PR numbers rather than SHAs because both
  branches are subject to re-cutting before merge.
