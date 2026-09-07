---
title: "Keeping both sides of a merge conflict resurrects the abstraction the base deleted on purpose"
date: 2026-08-07
category: workflow-issues
module: parallel-consumer-core
problem_type: workflow_issue
component: development_workflow
severity: high
applies_when:
  - Merging master up into a branch cut before the rest of its issue family landed
  - A conflict hunk shows your added method beside a base change that removed its collaborator
  - Resolving an add/add conflict where both sides independently created the same file
  - The base branch collapsed two sources of truth into one while your branch was open
  - Reconciling pom dependency declarations that the base moved to the parent
related_components:
  - tooling
  - testing_framework
tags:
  - merge-conflict
  - long-lived-branch
  - stale-base
  - single-source-of-truth
  - dead-code
  - consumer-manager
  - shutdown-path
  - conflict-resolution
---

# Keeping both sides of a merge conflict resurrects the abstraction the base deleted on purpose

> Extracted from `origin/docs/session-learnings-857-family` @94bb98a9d, `docs/solutions/workflow-issues/keeping-both-sides-of-a-merge-conflict-resurrects-a-deleted-abstraction.md`.

## Context

A long-lived branch is not just behind master - it is behind a *changing model of the code*. The
`bugs/857-paused-consumption-multi-consumers-bug` branch (PR astubbs#29, the
`synchronized(commitCommand)` -> `ReentrantLock.tryLock()` commit-deadlock fix) was cut before the
rebrand and before the rest of the confluentinc#857 family landed. It still targeted the pinned
pre-rebrand mirror `master-confluent`. While it sat, astubbs#80 landed on `master` and *deleted a
concept* the branch's base still assumed existed: `ConsumerManager.shutdownRequested`.

Merging master up produced three conflicts. None of them was a text collision. Each one was a
question about which model of the code survives:

- `ConsumerManager` - the branch's base still had the old lifecycle flag; master had collapsed it.
- `ManagedPCInstance` (test-integration helper) - add/add, both sides invented the same class.
- `pom.xml` - two declarations of the same test dependency at different versions.

This document is about the *method* for telling those apart, because the failure mode is silent.
Git's conflict markers describe overlapping text. They say nothing about whether one side deleted an
invariant. "Keep both hunks, it compiles" is exactly how a deleted concept gets reinstated, and the
result compiles, passes the unit suite, and reintroduces the original defect.

**The collision was foreseeable and nobody joined it up (session history).** astubbs#29 and
astubbs#80 were correctly identified as different defects in the same family - the boundary was
stated at the time as "production confluentinc#857 is PR astubbs#29's territory, not this PR's" - and
astubbs#29's rebase debt was recorded repeatedly. But no session ever asked whether the two touched
the same *code*. They do: both live in `ConsumerManager`'s lifecycle area. Every fact needed to
predict this conflict was written down somewhere; none of it was ever read together. Treating two
issues as separate defects is not evidence that they are separate code.

The reconciliation merge is the commit on this branch whose subject is
`Merge master into the #857 deadlock fix, reconciling against #80`; the resolutions below are what
it recorded, and they have survived three subsequent routine master merges without recurrence.

## Guidance

### 1. Classify every conflict before resolving any of them

For each conflicted hunk, answer one question first: **is either side's change a deletion of a
concept, or are both sides adding to the same region?**

- **Text collision** - both sides edited adjacent or overlapping lines, but no shared concept was
  removed. Safe to merge by hand, keeping both intents.
- **Concept deletion** - one side removed a field, a method, a state variable, a lifecycle flag, an
  option. The other side still reads or writes it, or sits textually next to it. "Keep both" here is
  a semantic regression wearing a merge resolution's clothes.
- **Add/add** - both sides independently created the same file or the same declaration. One copy must
  win outright; a hand-merge of two independent implementations is the worst of the three outcomes.

The tell for concept deletion is asymmetry: one side of the conflict has *fewer* named things than
the base, not different ones.

### 2. Find the removing commit, and read why

Never infer intent from the diff. Ask git which commit made the symbol disappear:

```
git log -S shutdownRequested --oneline -- \
  parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/ConsumerManager.java
```

`-S` (pickaxe) reports commits where the *count* of the string changed, which is precisely "added or
removed", not "mentioned". On this repo that returned astubbs#80's squash commit, whose subject is
`fix(core): draining consumer no longer busy-spins or zombie-holds its partitions (#857 family) (#80)`,
and, further back, the upstream commit that introduced the flag.

Then read the message. astubbs#80's says, verbatim:

> Fix: collapse the duplicated lifecycle state - the flag and signalStop() are deleted;
> ConsumerManager derives "close in progress" from runState (now volatile) via an injected
> signal. One source of truth, so the desync class is structurally impossible.

That sentence is the whole resolution. It is not "the flag moved" or "the flag was renamed" - it is
"the flag was a second source of truth and the second source of truth is the bug". Any resolution
that reinstates it is wrong regardless of whether it compiles.

### 3. Check whether the surviving caller still exists

A deleted concept usually leaves orphaned writers on the stale side. Enumerate them against the
*pre-merge branch tip*, not against your working tree:

```
git grep -n "signalStop" <branch-side-parent> -- '*.java'
```

On the branch side that returned three hits: the declaration in `ConsumerManager`, and two call
sites in `BrokerPollSystem` - `drain()` and `transitionToClosing()`. Both call sites were rewritten
by astubbs#80 and neither survives on master. A method with zero surviving callers on the winning
side is dead code; carrying it forward adds an unreachable second lifecycle signal that the next
reader will assume is live.

### 4. For add/add, prove superset before you discard

When both sides added the same file, do not eyeball it and do not hand-merge. Normalise and compare
as sets, so "the loser's content is fully contained in the winner" becomes a mechanical result rather
than a judgement call:

```
git show <branch-side>:path/To/File.java | sed 's/^[[:space:]]*//' | sort -u > /tmp/a.txt
git show <master-side>:path/To/File.java | sed 's/^[[:space:]]*//' | sort -u > /tmp/b.txt
comm -23 /tmp/a.txt /tmp/b.txt     # lines ONLY on the side you are about to discard
```

Empty output means every line of the discarded copy exists somewhere in the winner: discarding it
loses nothing textual. Non-empty output is your review list, and each surviving line is a decision
you now have to make deliberately. Leading-whitespace stripping matters, because reindentation
otherwise produces a wall of false differences. This is a containment check, not a semantics check -
it cannot tell you the winner's version *behaves* the same, so use it to bound the review, not to
skip it.

### 5. Duplicate dependency declarations are a build smell, not a merge outcome

Two `<dependency>` blocks for the same `groupId:artifactId` in one `<dependencies>` section is not a
resolved conflict, it is a Maven warning waiting to happen and a version that silently depends on
declaration order. If master already declares it for all modules, drop the branch's copy entirely -
including any now-unused version property it introduced.

### 6. Record the reasoning in the merge commit

The merge commit message is the only place a future reader will look for "why was this method not
carried forward?". A merge with an empty body is a merge whose reasoning is unrecoverable. State the
conflict, the classification, the resolution, and the consequence of the obvious wrong answer.

Write it *after* doing the checks, not from memory of what you expected to find. This document's own
merge commit asserts that the branch added `signalStop()`; pickaxe shows it did not (see the
corrections below). A merge message written from the pre-investigation summary preserves the
misunderstanding instead of the finding.

## Why This Matters

Reinstating a deleted concept is the single most dangerous merge outcome available, because every
signal you normally trust says it is fine:

- It compiles. The stale side's code is internally consistent; that is why it was written.
- The unit suite passes. The reinstated flag would be a *second* source of truth, and the desync it
  causes only shows up under a specific lifecycle race - here, during `DRAINING`.
- The diff looks conservative. "I kept both sides' work" reads as caution in review.

The concrete cost in this case is documented in astubbs#80 and in
`parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/internal/BrokerPollSystemDrainTest.java:25-28`,
whose javadoc records the defect the collapsed flag caused: `{@code BrokerPollSystem.drain()} used to
call {@code ConsumerManager.signalStop()} <i>before</i> entering {@code DRAINING}, and {@code
ConsumerManager.poll()} guarded the real {@code consumer.poll()} call with a private {@code while
(!shutdownRequested)} flag — so once draining started, <b>{@code consumer.poll()} was never invoked
again</b>.` The consequences were a ~10kHz busy-spin (a full core per closing instance) and a
rebalance-unresponsive group member zombie-holding its whole assignment while consuming nothing.

So the stale method is not merely dead - **raising that flag at drain time is the defect itself**.
`BrokerPollSystem.java:247-253` states the rule where the call used to be: `drain()` deliberately
does not stop the `ConsumerManager`, because the poller must keep calling `consumer.poll()`. A
draining member has to stay a live, polling participant: polling is what services commit responses
and what stops the group coordinator seeing a zombie. Only an actual close may stop the poll loop,
which is why the surviving read side is named for *close* rather than drain. A "keep both" merge
would have re-armed exactly that defect, on the branch whose entire purpose is fixing a sibling of
the same bug.

The general shape: **collapsing duplicated state is a deletion whose value lives entirely in the
absence.** Nothing in the surviving code points at what is missing, so a merge is the one moment
where the deleted thing can walk back in unnoticed. That is why the removing commit's message, not
the resulting code, is the authority.

## When to Apply

- Merging or rebasing any branch that predates a refactor of the internals it touches - especially
  one that predates a rename, a rebrand, or a change of target branch.
- Any conflict where one side has *fewer* named members than the merge base.
- Any add/add conflict on a file both sides created independently.
- Any conflict in a build file that would leave two declarations of the same artifact.
- Reviewing someone else's merge commit: if the body does not say why each conflict resolved the way
  it did, the classification was probably never done.
- Before starting work on a branch whose issue is one of a *family*: check whether the siblings
  touched the same classes, not just whether they fixed the same bug.

It is overkill for genuine text collisions - two people editing adjacent lines of the same method
with no concept removed on either side. The cost is a `git log -S` and a `git grep`; run it whenever
the conflict is in code you did not write this week.

## Examples

### `ConsumerManager` - concept deletion, resolved by dropping the stale side

Pre-merge branch side (the branch-side first parent of the reconciliation merge - reach it with
`git rev-parse '<merge>^1'` rather than a literal SHA, which a squash or rebase will rewrite):
`ConsumerManager.java:43`
declared `private final AtomicBoolean shutdownRequested = new AtomicBoolean(false)`, read at `:92`,
`:120`, `:179`, `:247`, with `signalStop()` at `:280-285` and the branch's own new
`claimConsumerOwnership()` at `:276`. The two methods were textually adjacent, which is the only
reason they conflicted together.

There was also a *write* the reads list misses: the branch's own `close()` did
`this.shutdownRequested.set(true)` at `:290`. That single line is what master's comment below now
stands in place of - the comment sits exactly where the write used to be, which is why it reads as
an explanation of an absence.

Master side: the flag is gone. In its place,
`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/ConsumerManager.java:60`
holds `private BooleanSupplier closeInProgressSignal = () -> false;` with `@Setter` at `:59` - a
read-only signal, defaulting to "never", whose javadoc (`:46-58`) states outright that "this class
deliberately holds NO lifecycle state of its own". It is read at `:109` (poll retry loop), `:137` (a
debug log), `:196` (commitSync retry loop) and `:259` (retry backoff). It is *set* in exactly one
place:
`BrokerPollSystem.java:93`, `consumerMgr.setCloseInProgressSignal(this::isCloseInProgress)`, where
`isCloseInProgress()` (`BrokerPollSystem.java:335-337`) returns `runState == CLOSING || runState ==
CLOSED` - and its javadoc adds the load-bearing caveat: NOT true while merely `DRAINING`, because a
draining consumer must keep polling.

The clinching evidence that no stop flag should be raised is a comment master left behind exactly
where the branch wanted to raise one, at
`origin/master:parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/ConsumerManager.java:257-258`
(and, unchanged, at `:295-296` post-merge):

```
// no stop flag to raise: by the time the poll system calls this, its runState is already
// CLOSING/CLOSED, so closeInProgressSignal reports true to any in-flight retry loops
```

Its counterpart sits at the writing end, `BrokerPollSystem.java:324`: `// setting CLOSING is itself
the stop signal - ConsumerManager's closeInProgressSignal reads this runState; set it before the
wakeup so an aborted poll observes it`. A comment that explains an absence is the strongest possible
merge evidence - it exists only because someone anticipated this exact question.

**Resolution:** keep `claimConsumerOwnership()`, drop `signalStop()` as dead. Post-merge,
`ConsumerManager.java:288-290` is `void claimConsumerOwnership() { consumer.claimOwnership(); }`,
delegating to the branch's own `ThreadConfinedConsumer.claimOwnership()`
(`ThreadConfinedConsumer.java:49-52`), and its single caller is `BrokerPollSystem.java:146`, at the
top of the poll control loop. Two independent things happened to be neighbours in the file; only one
of them was still real.

**Two corrections to the informal account of this merge**, both discovered by doing the checks above
rather than trusting the summary:

1. The branch did **not** add `signalStop()`. Pickaxe shows it was introduced upstream by the commit
   `Improved offset commit retry. Add support for SaslAuthenticationException retry timeout (#819)`
   and merely inherited by the stale branch. Only `claimConsumerOwnership()` is the branch's. This
   matters: the conflict was pure textual adjacency between a genuinely new method and a
   pre-existing one master had deleted - which is the most seductive possible setup for "keep both".
2. `signalStop()` had **two** callers on the branch side, not one:
   `BrokerPollSystem.java:235` in `drain()` (the defect - the flag was raised at drain time) and
   `BrokerPollSystem.java:305` in `transitionToClosing()`. astubbs#80 rewrote both; only the second
   had a legitimate reason to exist, and it was replaced by setting `runState = CLOSING` directly.

### `ManagedPCInstance` - add/add, resolved by proving superset

`parallel-consumer-core/src/test-integration/java/bz/stub/parallelconsumer/integrationTests/utils/ManagedPCInstance.java`
was created independently on both sides. Master's copy adds MDC cleanup on pooled threads
(`org.slf4j.MDC.remove(MDC_INSTANCE_ID)` in a `finally`), an `extraConsumerProps` config field
applied last so scenario overrides win, and tighter try/finally around the run body.

The `comm -23` containment check above, run against the two pre-merge parents, returned **empty
output**: no normalised line existed on the branch's copy that was absent from master's. Master's
copy taken outright, branch's discarded, with the discard justified rather than assumed.

### `pom.xml` - duplicate declaration, resolved by deletion

Branch side declared `com.tngtech.archunit:archunit-junit5` in the root pom's `<dependencies>` at
`${archunit.version}` = 1.1.1. Master declares the same artifact in the same section at 1.4.2, with
the comment `<!-- Architecture rules (e.g. keep integration tests out of the unit suite) - all
modules -->`. Merging both would have put two declarations of one artifact in a single
`<dependencies>` block. Resolution: master's declaration only (`pom.xml:404-410`), the branch's
`archunit.version` property dropped with it - `grep -c "<artifactId>archunit" pom.xml` returns `1`.

### What this merge did *not* fix - state honestly

The reconciliation is what is solved and verified here. PR astubbs#29 is **not** fixed and **not**
merged - it is still a draft, and `Integration Tests`, `Performance Tests` and `PR Checklist` are all
failing. Two defects on the branch remain open:

- **`ThreadConfinedConsumer` thread-ownership failure on close.** `close()` and `close(Duration)` are
  guarded by `checkThread` (`ThreadConfinedConsumer.java:174-184`), and `ConsumerManager.close()`
  calls `consumer.close(defaultTimeout)` (`ConsumerManager.java:306`). Ownership is claimed by the
  poll thread (`BrokerPollSystem.java:146`), so a close arriving on any other thread throws
  `IllegalStateException`. The guard is doing its job; what it guards is not yet correct.
- **`numberRecordsOutForProcessing` double-decrement.** The counter drift analysed in
  `docs/BUG_857_INVESTIGATION.md (deleted 2026-08-18; retrieve with `git show 262629aab:docs/BUG_857_INVESTIGATION.md`)` ("Bug 2: Silent Stall") is not resolved on this branch.

Verified state after the merge: the branch compiles across main, test and test-integration sources,
and the local unit suite passed. The retarget off `master-confluent` is **done** - astubbs#29 now
targets `master` - so what remains is the deadlock fix itself and the two defects above, not the
rebase debt that older notes describe.

## Related

- `docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md` - astubbs#80's own
  write-up, and the authority on *why* the duplicated lifecycle state was a bug. Read it for the
  state-collapse rationale rather than re-deriving it here; this doc only covers not undoing it.
- `docs/solutions/workflow-issues/mechanical-issue-ref-sweep-falsified-a-verbatim-log-quote.md` -
  sibling hazard from the very same event. Merging master up also made this branch inherit the
  issue-reference gate, and the sweep written to satisfy it falsified a quoted log line.
- `docs/inflight/bug-857-family.md` - what is still open across the family.
- Caution on astubbs#80's PR body: an audit at the time found two false claims in it, both in the
  chaos-test side-narrative rather than the core fix (session history). Its *commit* message and the
  code were verified and hold up; the PR body is the less reliable of the two.
