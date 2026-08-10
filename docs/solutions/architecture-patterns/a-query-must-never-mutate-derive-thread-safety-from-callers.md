---
title: "A query must never mutate - derive a thread-safety contract from callers, not javadoc"
date: 2026-08-11
category: architecture-patterns
module: parallel-consumer-streams
problem_type: architecture_pattern
component: background_job
severity: critical
applies_when:
  - About to add a thread-ownership guard because a javadoc or a design assumption says one thread owns the object
  - A dependency calls into your code from more than one of its own threads, and its docs describe only the common path
  - You need to answer a stateful question without draining or mutating the state that answers it
  - Choosing between a cached flag republished at every mutation point and an answer derived from monotonic counters
  - A new assertion turns a previously silent race into a loud deterministic failure and you must decide whether the assertion or the caller is wrong
tags:
  - thread-safety
  - owner-thread
  - query-vs-mutation
  - race-condition
  - kafka-streams
  - state-updater
  - monotonic-counters
  - assertion-guard
related_components:
  - PcTaskDispatcher
  - WorkManager
  - kafka-streams
  - testing_framework
---

# A query must never mutate - derive a thread-safety contract from callers, not javadoc

## Context

`parallel-consumer-streams` patches Kafka Streams' `StreamTask` so that records go to a Parallel Consumer
worker pool instead of the `PartitionGroup` the StreamThread used to walk
(astubbs/parallel-consumer#271, issue astubbs/parallel-consumer#255). `WorkManager`, the PC component the
seam drives, is not thread-safe. The whole design therefore rests on knowing which thread calls what.

Unit U9 wired up the commit surface. Stock `StreamTask` gates three things on a plain `boolean commitNeeded`
field: `prepareCommit`, `validateClean`, and `maybeCheckpoint`. On the PC path that field is never written,
because workers no longer touch it, so all three gates were changed to ask the dispatcher instead, through
one helper - `pcAwareCommitNeeded()` (`parallel-consumer-streams/src/main/patch/pc-streams.patch:1728-1730`,
called at `:1423`, `:1483`, `:1494` and `:1741`). At the time that helper reached a method which drained PC's
completion mailbox and folded the outcomes back into `WorkManager`, so that the answer would be as fresh as
possible.

Three of those four call sites are on the StreamThread. The fourth is not. Kafka Streams' `DefaultStateUpdater`
calls `StreamTask.maybeCheckpoint` from its own thread, for every task it is restoring. A `boolean` field read
had been replaced by a call that mutated non-thread-safe state from a second thread, and nothing in the
signature, the name, or the javadoc said so.

What made it visible rather than silent was a guard added for a different reason: `assertOwnerThread`
(`PcTaskDispatcher.java:752-762`) throws an `IllegalStateException` naming both threads when a
`WorkManager`-touching method is called off the owning thread. The guard was justified by a javadoc asserting
"StreamThread only" on the commit methods. That assertion was false, and it is exactly what made the guard
look safe to add.

Two lessons come out of it, and the second is the durable one:

1. **A dependency's thread model is the set of its callers' threads, not its javadoc.**
2. **Once a method can be called from a second thread, the rule that keeps it safe is that a question may not
   mutate.**

## Guidance

### Derive the thread model by enumerating callers in the dependency's own source

The sources are already on disk. This is a two-command audit, and it is the same recipe as the call-site audit
in the sibling lifecycle document, pointed at a different question: that one asks *when* a hook fires, this one
asks *which thread* runs it.

```bash
unzip -o -q ~/.m2/repository/org/apache/kafka/kafka-streams/3.9.2/kafka-streams-3.9.2-sources.jar \
  'org/apache/kafka/streams/processor/internals/*' -d /tmp/ks392
grep -rn "maybeCheckpoint" /tmp/ks392/org/apache/kafka/streams/processor/internals/
```

Every citation below is pinned to `kafka-streams-3.9.2-sources.jar`, package
`org/apache/kafka/streams/processor/internals`. Follow the grep hits up their own call stacks until you reach
something that extends `Thread`. That is the answer; nothing short of it is.

**The chain that refutes "StreamThread only".** `DefaultStateUpdater.java:79` declares
`private class StateUpdaterThread extends Thread`. Its `run()` (`:145`) drives `runOnce()` (`:180`), which calls
`maybeCheckpointTasks(checkpointStartTimeMs)` at `:191`. That method (`:702-719`) iterates
`updatingTasks.values()` and calls `task.maybeCheckpoint(false)` at `:713`:

```java
measureCheckpointLatency(() -> {
    for (final Task task : updatingTasks.values()) {
        // do not enforce checkpointing during restoration if its position has not advanced much
        task.maybeCheckpoint(false);
    }
});
```

`updatingTasks` is `Map<TaskId, Task>` (`DefaultStateUpdater.java:85`) and holds the real task objects, not a
wrapper. Five further enforced calls sit at `:357`, `:548`, `:616`, `:642` and `:676`. On the other end,
`StreamTask.maybeCheckpoint` (`StreamTask.java:645-653`) opens with the field read the patch replaced:

```java
public void maybeCheckpoint(final boolean enforceCheckpoint) {
    // commitNeeded indicates we may have processed some records since last commit
    // and hence we need to refresh checkpointable offsets regardless whether we should checkpoint or not
    if (commitNeeded || enforceCheckpoint) {
```

So `StreamTask.java:648` executes on the state-updater thread, for every restoring task, on a timer. There is
no doc comment anywhere on that path that says so.

**Enumerate the negative side too, or the model is half a model.** The same audit is what establishes which
methods genuinely *are* single-threaded, and that half is what lets the guard stay where it belongs. The state
updater hands tasks out as `ReadOnlyTask`, whose `prepareCommit` throws `UnsupportedOperationException`
(`ReadOnlyTask.java:178-180`) and whose `commitNeeded` throws for an active task (`ReadOnlyTask.java:207-213`):

```java
public boolean commitNeeded() {
    if (task.isActive()) {
        throw new UnsupportedOperationException("This task is read-only");
    }
    return task.commitNeeded();
}
```

`ReadOnlyTask.maybeCheckpoint` throws too (`:118-120`), which is precisely the point: the checkpoint loop above
holds the real `Task`, so `maybeCheckpoint` is the one patched method the state updater reaches on the real
object. `collectCommitData` and `onCommitSuccess`, reached only through `StreamTask.prepareCommit` and
`StreamTask.updateCommittedOffsets`, really are owner-thread-only, and keep their guard.

**Record the residuals you find and do not fix.** The audit also turns up
`DefaultTaskExecutor.TaskExecutorThread` (`DefaultTaskExecutor.java:44` in the jar's `tasks` subpackage,
`extends Thread`), which calls
`task.process(now)` at `:158` - which would drive `dispatchAvailable` off the owner thread. It is gated behind
Streams' private `__processing.threads.enabled__` config (`StreamsConfig.java:1309-1310`), off by default and
never set here. It is written into the class javadoc as a known exception rather than silently omitted
(`PcTaskDispatcher.java:86-90`), because an unnamed residual reads as an oversight next time somebody audits.

The resulting model is stated on the class, not on the individual methods, so it can be read once:
`PcTaskDispatcher.java:50-101` splits the class into a **mutating** surface (owner-thread-only, enforced,
because `WorkManager` is not thread-safe) and a **read-only** surface (any thread, answered from atomics,
concurrent collections and volatiles, never touching `WorkManager`).

### Do not settle the question by proving the threads never overlap

There is a tempting cheaper answer, and it is worth naming because it is *true* and still wrong to rely on.
Kafka Streams transfers exclusive ownership of a task between threads rather than pinning it:
`TaskManager.handleReassignedActiveTask` (`TaskManager.java:550-558`) calls `tasks.removeTask(task)` and then
`stateUpdater.add(task)`, and a restored task returns to the StreamThread only through
`drainRestoredActiveTasks`. So while a task is in `updatingTasks` the StreamThread is not processing it, and
the state updater's `maybeCheckpoint` call does not temporally overlap StreamThread work on that same task.
This was established in an earlier session on the sibling branch, by reading `TaskManager` rather than by
assuming either way (session history).

That analysis makes the hazard a publication and visibility one - one thread mutating plain non-volatile
fields that another thread later reads with no happens-before edge between them - rather than a simultaneous
double write. It does not make it safe, and more importantly **it is the wrong thing to build on**. Whether
two threads overlap is a property of the dependency's scheduler, which is free to change in a patch release
and will not tell you. Whether your method mutates is a property of your own code, which you control and can
assert on. Prefer the guarantee you own.

Note also what the guard actually proved. `assertOwnerThread` compares thread identity, so its firing is
evidence of a **cross-thread call**, not of a concurrent one. Read a guard's failure for what it can establish
and no more.

### The rule: a question may not mutate

`PcTaskDispatcher.java:93-95` states it in one line:

> The rule that keeps the surfaces apart: **a question is not allowed to mutate.** A query that drained the
> completion mailbox "just to be accurate" is how a plain field read became a cross-thread write.

This is what makes the two surfaces stable rather than a snapshot of today's caller set. Enumerating callers
tells you the model *now*; the no-mutation rule is what survives the next release adding a caller you did not
audit. A predicate that only reads is safe from a thread nobody has discovered yet.

At HEAD the query is one comparison (`PcTaskDispatcher.java:627-629`):

```java
public boolean hasCommitDataOutstanding() {
    return successesPublished.get() > successesCommitted;
}
```

`hasUncommittedWork()` (`:666-668`) is the second question - "is it safe to walk away", which in-flight work
also answers - and inherits the property for free, because its extra term is an `AtomicInteger`. The drain stays
on the owner-thread paths where a commit actually needs it: `dispatchAvailable` (`:416`) and `collectCommitData`
(`:599`). Nothing is stranded by removing it from the query, because Streams always follows a true answer with
`prepareCommit`, which collects, and collection drains.

Be precise about which of those two is *enforced*, because the distinction is the model. `collectCommitData`
keeps the `assertOwnerThread` guard; `dispatchAvailable` does not, and the class javadoc puts it in an
explicitly **unguarded** row (`PcTaskDispatcher.java:83-90`) - single-threaded because Kafka only reaches it
from `addRecords` and `process`, which are hot-path, not because anything checks. Three methods carry the
guard in the tree: `collectCommitData` (`:598`), `updatePartitions` (`:689`) and `onCommitSuccess` (`:736`).
"Owner-thread-only" and "guarded" are not the same set, and a doc that conflates them tells the next reader
that a check exists where none does.

### Being thread-safe is not enough; the answer must also still be right

This is the part that is easy to get wrong while feeling finished. Two counters can be read safely from
anywhere and still answer the wrong question.

**Count at publication, not at drain.** `successesPublished` (`PcTaskDispatcher.java:244`) is incremented by
whichever thread completes a record, *before* the container is queued (`publishSuccess`, `:532-535`):

```java
private void publishSuccess(final WorkContainer<byte[], byte[]> work) {
    successesPublished.incrementAndGet();
    completed.add(work);
}
```

Counting at the drain instead would have been just as thread-safe, and would have meant a worker could finish
while the query still said there was nothing to commit until the owner thread next drained. The field's javadoc
puts it plainly (`:240-242`): such a count "would be thread-safe and wrong, which is worse than the crash it
replaced". A thread-safe "nothing to commit" about finished work loses records; the crash only stopped the
process.

**The other side of the comparison is pinned to what a commit actually carried.** `collectCommitData` records
`successesCollected = successesDrained` (`:604`) - the *drained* count, not the published one, so a worker that
publishes after the drain is not claimed by a commit whose map does not contain it - and `onCommitSuccess`
(`:735-739`) advances `successesCommitted` to exactly that. This is the same window PC protects internally with
`PartitionState.stateChangedSinceCommitStart`.

**Failures are deliberately not counted.** `recordFailure` (`:547-553`) queues the container but does not touch
the counter, because a failure does not make a commit worth attempting: PC leaves the offset incomplete and
`PartitionState.onFailure`
(`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/PartitionState.java:268-270`) is a
no-op, so the partition never turns dirty. That asymmetry is the reason the query counts successes rather than
the mailbox's size, which would have been the obvious cheap "is there anything pending" answer. With retries
disabled in this module, a poison pill sits in PC's accounting forever, so a size-based query would report a
commit outstanding forever - and `validateClean` would turn that into a spurious `TaskMigratedException` on an
otherwise clean close.

### Two ways to build a query that may not mutate, and their standing costs

This is the strongest part of the learning, and it only became visible because two branches solved the same
cross-thread problem independently and the answers met at a merge.

**(a) Cache and republish.** The owner thread keeps the answer in a volatile and republishes it wherever the
underlying state changes. U10 did this: a `volatile boolean pcDirty` re-read from `workManager.isDirty()` by an
owner-thread-only `publishDirtyState()`, with `hasUncommittedWork()` reading
`pcDirty || inFlight.get() > 0 || !completed.isEmpty()`. It is correct, and it is cheap to write.

Its cost is an **open-ended obligation**: every present and future place that can change the underlying answer
is a place that must republish, including places that have nothing to do with completions. U10 needed four
republish points - the completion drain, `updatePartitions` (a revoke discards that partition's
completed-but-uncommitted work, changing the answer with no completion involved), `onCommitSuccess`, and
`close()`.

The fourth was not found by review. It was found by **the dependency's own test suite**: Kafka's
`shouldClearCommitStatusesInCloseDirty` asserts `commitNeeded()` is false once a task has closed dirty, and
`close()` revokes its partitions *after* the drain that published the flag, so the flag had to be republished
after the revoke. The plan records the verdict without softening it
(`docs/plans/2026-08-11-001-feat-ks-streams-task-lifecycle-and-rebalance-plan.md:604-609`): "That is the cost of
the design and it is recorded on the field." Four points is not the number; four is the number *today*.

**(b) Derive from monotonic counters.** The answer is computed from counters that are inherently safe to read
from any thread, and that are updated by the very events which define the answer. That is the design in the
tree: `successesPublished` against `successesCommitted`, and `pcDirty` is gone -
`grep -rn pcDirty parallel-consumer-streams/src/` returns nothing. It arrived from the base branch
(astubbs/parallel-consumer#271, open as of this writing) rather than from the branch that wrote (a).

There is no per-mutation-point obligation, because there is nothing being kept in step with anything else. The
revoke case is the clean demonstration of the difference. Under (a), `updatePartitions` **had** to republish or
the cached answer went stale. Under (b) it deliberately adjusts nothing, and says why
(`PcTaskDispatcher.java:715-719`):

```java
// Deliberately NOT adjusting the success counters here. A revocation discards the revoked partition's
// completed-but-uncommitted work, so the counters briefly overstate what is outstanding - but this
// dispatcher lives on and the partitions it kept may genuinely have work, and there is no per-partition
// breakdown in a counter. Overstating costs one commit cycle that collects nothing and then marks
// covered what it collected, which self-corrects; understating would drop real work.
```

Be honest about what (b) does not remove. **A terminal state still needs a declaration.** `close()` at HEAD sets
`successesCommitted = successesPublished.get()` (`PcTaskDispatcher.java:919-926`) for the same
`shouldClearCommitStatusesInCloseDirty` reason, because a closed dispatcher owns nothing and will never commit
again. What (b) removes is the *recurring* obligation at every ordinary mutation point, not every obligation
anywhere. The distinction that matters: under (a) the volatile is a cache of a derived answer that lives
elsewhere, so it must be re-synchronised wherever that answer can move; under (b) the counters **are** the
answer.

**Prefer (b) when the answer is expressible as a difference of monotonic counts.** "Is there X outstanding" very
often is: count X at the moment it comes into existence, count X at the moment it is discharged, compare. When
it is not expressible that way, (a) is legitimate - but then treat the republish set as a documented invariant
on the field, not as something a reader will infer.

### Where this sits relative to the sibling audit

The lifecycle-callbacks document reaches the same conclusion by the same method, about a different property:
a callback's contract is the set of its call sites, not its name, so audit them before attaching correctness to
the hook. That is about **when** a hook fires. This one is about **which thread** runs it, and adds the design
rule that follows once the answer is "more than one". Same jar, same grep, same ten minutes; run the audit once
and read both answers off it.

## Why This Matters

The crash was the good outcome, and it was an accident. Without `assertOwnerThread`, the state-updater thread
would have gone on draining PC's mailbox and folding outcomes into shard and partition bookkeeping with no
exception at all. The visible failure mode of that is a commit covering work that never completed, which is
silent, unrecoverable data loss discovered later by whoever next owns the partition. The guard was added for a
different reason and repaid itself immediately by converting that into an `IllegalStateException` naming both
threads.

The premise that made it dangerous was recorded in a doc comment. The three commit methods said "StreamThread
only" in prose, and the guard turned a comment into a check. Both were written in good faith and both were
wrong, and there was no mechanism by which either would have been questioned - a comment asserting a
dependency's behaviour is a claim, and this one had no citation behind it.

**Nothing local could have caught it, and the author's verification looked complete.** The guard was pushed
after `PcTaskDispatcherTest` ran 12 of 12 green plus clean full-module runs of `parallel-consumer-core` and
`parallel-consumer-streams`. That unit suite constructs and drives the dispatcher on one thread, so it
structurally cannot exercise a cross-thread caller; the defect surfaced only on CI's integration lane, where a
real Kafka Streams runtime supplies a real state-updater thread, as `PcDrivenStatefulProofTest` failing out of
`DefaultStateUpdater:326` (session history). The reflection recorded at the time is the transferable part: the
change was verified against a test that could not fail in the interesting way. The inflight entry now states the
structural version of it (`docs/inflight/pr-streams-rebalance-coverage-gaps.md:61-64`) - a unit suite with no
second thread in it cannot falsify a claim about two, so cross-thread properties need either a hand-driven
foreign thread or an integration arm.

The standing-cost argument matters because both designs pass review. Cache-and-republish is correct on the day
it is written; its defect is a *future* omission that no reviewer of the current diff can see. That is exactly
what happened: the fourth republish point was caught by Kafka's own suite, not by anyone reading the change. And
the reason the simpler design won is worth naming, because it is not a reviewing skill that can be practised.
The inflight entry records it (`docs/inflight/pr-streams-rebalance-coverage-gaps.md:36-47`): the base branch's
counter rework "subsumed" U10's answer, "the whole apparatus is gone", and

> two branches solved the same cross-thread problem independently, and the simpler answer won at merge rather
> than at review. Neither reviewer on either side would have found it, because each only saw one design.

The generalisable part is that "is this the simplest design?" is not answerable from inside one branch. When two
independent solutions to the same problem do meet, spend the time to pick rather than to reconcile - that is the
one moment the comparison is free.

## When to Apply

- Before putting a call behind any gate in patched or subclassed dependency code where the original was a
  **field read**. A field read is thread-agnostic by construction and tells you nothing about who evaluates it,
  which is precisely why replacing one is the highest-risk edit of this shape.
- Whenever a method's javadoc asserts which thread calls it. Treat that as an unverified claim, exactly as you
  would a claim about *when* a callback fires, and settle it against the dependency's source with a version-pinned
  citation.
- Before adding a thread-affinity guard. The guard encodes the model you believe; if the model is wrong the guard
  fails on legitimate calls. Derive the model first, then guard, and expect the guard's first firing to be
  evidence about the model rather than about the caller.
- When any predicate named `isX`, `hasX`, `needsX` or `shouldX` is about to acquire a side effect "for accuracy".
  That is the moment the class of this defect is created, and it is invisible at the call site.
- When choosing between caching a cross-thread answer and deriving it. Ask what the republish set is and whether
  it is closed. If new mutation points are plausible, the cache carries a permanent obligation and the derivation
  does not.
- On any dependency **upgrade**. The thread model is a property of call sites, so it can change with no
  signature, name or javadoc change at all. That is why the citations here are pinned to 3.9.2.
- When two branches turn out to have solved the same problem differently. Compare the designs at the merge, before
  one is mechanically resolved away.

## Examples

### Before - a question that mutated

U9 replaced the plain field read at `StreamTask.java:648` with `pcAwareCommitNeeded()`, which reached a
dispatcher method that was owner-thread-guarded and drained the completion mailbox on the way to its answer:

```java
public boolean hasCommitDataOutstanding() {
    assertOwnerThread("hasCommitDataOutstanding");
    drainCompletions();
    return workManager.isDirty();
}
```

Three of the four gates using that helper are on the StreamThread. `maybeCheckpoint` is not, so on any restoring
task the state-updater thread hit the guard, and the client died with
`IllegalStateException: ... owner-thread-only, but was called from '...-StateUpdater-1'; the owner is
'...-StreamThread-1'`.

### After - a question that only reads

```java
public boolean hasCommitDataOutstanding() {
    return successesPublished.get() > successesCommitted;
}
```

`PcTaskDispatcher.java:627-629`. Its javadoc records both why it may be called from any thread and why the count
is taken at publication rather than at drain, so the next reader cannot re-introduce the drain "for accuracy"
without reading the reason it was removed.

The root-cause claim was settled with a controlled experiment carrying both arms, not merely by the fix working
(recorded on the commit message of the query rework, astubbs/parallel-consumer#271):

- **Pre-fix body** (guard plus drain): the new unit test fails on the guard, and
  `PcDrivenStatefulProofTest#pcDrivenAggregationMatchesTheStockBaseline` fails 3 of 3 repetitions with the exact
  CI exception, thrown from `...-StateUpdater-1` while the owner is `...-StreamThread-1`.
- **Wrong fix** (guard deleted, drain kept): the same unit test still fails, on "must not have drained" - so the
  test rejects deleting the assertion, not just the crash. This arm is the one that matters, because deleting the
  guard is the fix a reader reaches for first.
- **With the fix**: streams module 28 of 28, Kafka's own suites 188 of 188, streams integration suite 19 of 19.

### The regression test that pins the split

`parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/PcTaskDispatcherTest.java:743-771`
drives a thread named `StateUpdater-1` by hand and asserts the foreign thread gets the *same* answer without
throwing, then that it sees a subsequent commit rather than a stale cached one:

```java
runOnNewThread("StateUpdater-1", () -> {
    assertThat(dispatcher.hasUncommittedWork())
            .as("a foreign thread must get the SAME answer without throwing - this is the exact "
                    + "call DefaultStateUpdater makes through maybeCheckpoint")
            .isTrue();
```

Both halves are load-bearing. The first fails if a drain or a guard is reintroduced; the second fails if the
query becomes stale, which is the failure mode the cache-and-republish design would reach by omission rather
than by edit.

### The two designs, side by side

```java
// (a) U10, cache and republish. Four republish points; the fourth was found by Kafka's own suite.
private volatile boolean pcDirty;
private void publishDirtyState() { pcDirty = workManager.isDirty(); }   // owner thread only
public boolean hasUncommittedWork() { return pcDirty || inFlight.get() > 0 || !completed.isEmpty(); }

// (b) derive from counters. No per-mutation-point obligation.
public boolean hasCommitDataOutstanding() { return successesPublished.get() > successesCommitted; }
public boolean hasUncommittedWork() { return hasCommitDataOutstanding() || inFlight.get() > 0; }
```

(b) is what is in the tree (`PcTaskDispatcher.java:627-629` and `:666-668`). (a) survives only in history.

## Related

- [Kafka Streams task lifecycle callbacks do not mean what they are named](../integration-issues/kafka-streams-task-lifecycle-callbacks-do-not-mean-what-they-are-named.md) -
  the same caller-enumeration audit on the same sources jar, answering *when* a hook fires rather than *which
  thread* runs it. Run the audit once, read both answers.
- [Kafka Streams polls and processes on one thread](../integration-issues/kafka-streams-couples-polling-and-processing-on-one-thread.md) -
  the other half of this integration's threading story: what the StreamThread's single-threadedness costs once
  work is made asynchronous. That document is about the thread you knew about; this one is about the one you did
  not.
- [A high-water mark cannot express out-of-order completion](a-high-water-mark-cannot-express-out-of-order-completion.md) -
  the same commit gate one unit earlier. Its claim that `commitNeeded` is "stock-path-only, single-threaded"
  after U9 remains true of the raw *field*; the public `commitNeeded()` override was later widened to read the
  dispatcher, which is the any-thread surface this document is about.
- [A control arm must vary exactly one term](../best-practices/control-arms-vary-exactly-one-term.md) - the query
  rework's "guard deleted, drain kept" arm exists because a fix that works is not evidence of the cause.
- [Fresh work needs an independent reviewer](../best-practices/fresh-work-needs-independent-review.md) - and its
  limit, made concrete here: no reviewer of either branch could have found the simpler design, because each saw
  only one.
- `docs/inflight/pr-streams-rebalance-coverage-gaps.md` - the record of what the merge subsumed, and of why the
  module's unit suite structurally cannot catch a cross-thread defect.
- `docs/plans/2026-08-11-001-feat-ks-streams-task-lifecycle-and-rebalance-plan.md` - the regression Kafka's own
  suite caught against the cache-and-republish design, and the other predictions this work refuted.
- astubbs/parallel-consumer#271, issue astubbs/parallel-consumer#255 - the PR and issue this was learned on.
