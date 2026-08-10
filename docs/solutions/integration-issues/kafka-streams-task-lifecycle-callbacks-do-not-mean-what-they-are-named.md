---
title: "Kafka Streams task lifecycle callbacks do not mean what they are named"
date: 2026-08-10
category: integration-issues
module: parallel-consumer-streams
problem_type: integration_issue
component: service_object
severity: critical
symptoms:
  - "A `postCommit` hook fires after the commit that preceded it threw"
  - "A `postCommit` hook fires when no commit was attempted at all"
  - "Work is acknowledged as durably committed while the broker holds an older offset"
  - "A task that was closed and had its resources shut down is handed live records again"
root_cause: wrong_api
resolution_type: code_fix
applies_when:
  - Attaching correctness-critical state to a Kafka Streams task lifecycle hook
  - Deciding where a "these offsets are now durable" acknowledgement belongs
  - Reviewing a code comment that asserts what a third-party framework guarantees
  - Allocating a resource in a framework object whose lifecycle the framework drives
tags:
  - kafka-streams
  - lifecycle-callbacks
  - commit-acknowledgement
  - postcommit
  - framework-contracts
  - silent-data-loss
  - call-site-audit
related_components:
  - testing_framework
  - documentation
---

# Kafka Streams task lifecycle callbacks do not mean what they are named

## Context

While hooking Parallel Consumer's commit lifecycle into a patched `StreamTask` on
astubbs/parallel-consumer#271 (issue astubbs#255), the acknowledgement of a successful commit was placed
in `postCommit`, justified by a code comment stating that Kafka only reaches `postCommit` once a commit
has succeeded. The name says so. The prefix says so. The comment said so, confidently and specifically.

An independent reviewer sent to Apache Kafka 3.9.2's own source refuted it. `postCommit` is reached
after a swallowed commit failure, and reached again with no commit attempted at all. Had the
acknowledgement shipped there, it would have marked work durably committed that the broker had never
accepted, and released the state needed to redo it.

The same audit turned up a second lifecycle hazard in the same shape: `revive()` returns a *closed* task
to service.

The general lesson is not about Kafka. It is that **a callback's name is not its contract. Its contract
is the set of its call sites.**

## Guidance

**Before attaching correctness-critical state to any framework hook, enumerate every caller of that hook
in the framework's own source, and ask of each one what it has actually established at the moment it
calls.** A `post` prefix, an `onSuccess` name and a doc comment are all weaker evidence than a grep for
callers. That grep costs ten minutes.

The sources are on disk already, so this is a two-command audit:

```bash
unzip -o -q ~/.m2/repository/org/apache/kafka/kafka-streams/3.9.2/kafka-streams-3.9.2-sources.jar \
  'org/apache/kafka/streams/processor/internals/*' -d /tmp/ks392
grep -rn "postCommit" /tmp/ks392/org/apache/kafka/streams/processor/internals/
```

Applied to Kafka 3.9.2, that grep yields seven call sites of `Task.postCommit`, and they do not agree
with the name. All line numbers below are from
`kafka-streams-3.9.2-sources.jar`, package `org/apache/kafka/streams/processor/internals`.

Start with the declaration. `Task.java:203` is:

```java
void postCommit(boolean enforceCheckpoint);
```

There is no javadoc on it at all. The javadoc immediately above at `Task.java:198-200` belongs to
`prepareCommit()` at `Task.java:201`. Everything a reader believes about `postCommit` comes from its
name.

**Call site 1 - reached after a commit that threw.** `TaskManager.tryCloseCleanActiveTasks`
(`TaskManager.java:1580`) wraps the commit in a `try`, catches `RuntimeException`, logs it, records it
as the first exception, and moves the affected tasks onto the close-dirty list
(`TaskManager.java:1619-1640`). Then, outside that `try`, it iterates **every** active task - not just
the ones that stayed clean - and calls `postCommit(true)` on each (`TaskManager.java:1642-1644`):

```java
} catch (final RuntimeException e) {
    log.error("Exception caught while committing tasks " + consumedOffsetsAndMetadataPerTask.keySet(), e);
    ...
    // If the commit fails, everyone who participated in it must be closed dirty
    tasksToCloseClean.removeAll(tasksToCommit);
    tasksToCloseDirty.addAll(tasksToCommit);
    }
}

for (final Task task : activeTaskIterable()) {
    try {
        task.postCommit(true);
```

The commit failed; `postCommit` runs anyway.

**Call site 2 - reached with no commit attempted.** `TaskManager.closeDirtyAndRevive`
(`TaskManager.java:272`) calls `prepareCommit()` purely to flush the cache, and says so in its own
comment at `TaskManager.java:284-286`: *"we do not need to take the returned offsets since we are not
going to commit anyways"*. It discards the returned offsets, suspends, and calls `postCommit(true)`
(`TaskManager.java:296-298`). No commit exists anywhere on that path.

**The remaining five** are a mix: `TaskExecutor.java:160` does follow a returned
`commitOffsetsOrTransaction`; `TaskManager.java:1163` and `:1178` in `handleRevocation`
(`TaskManager.java:1088`) are guarded by a `dirtyTasks` set that excludes some but not all failure
shapes; and `TaskManager.java:768` (`closeAndRecycleTasks`) and `TaskManager.java:1685`
(`tryCloseCleanStandbyTasks`) call it on standby tasks purely to write a checkpoint, with no commit
involved. `postCommit` is a **"finalise the commit *attempt*, and checkpoint"** hook. It is not a
success signal, and the implementation confirms it: `StreamTask.postCommit`
(`StreamTask.java:520-551`) only ever chooses whether to checkpoint, then clears the commit flags.

**The genuinely success-only seam in 3.9.2 is `StreamTask.updateCommittedOffsets`**
(`StreamTask.java:1358-1360`). Its only caller is `TaskExecutor.updateTaskCommitMetadata`
(`TaskExecutor.java:253-265`, calling through at `:259`), which is itself called from exactly three
places, all inside `commitOffsetsOrTransaction` (`TaskExecutor.java:175-251`), and each one sits on the
line immediately after a committed transaction or a returned `commitSync`, inside the `try` and before
every `catch`:

- `TaskExecutor.java:186-187` - exactly-once alpha: `commitTransaction(...)` then
  `updateTaskCommitMetadata(taskOffsetsToCommit)`; the `catch (TimeoutException)` below marks the task
  corrupted instead.
- `TaskExecutor.java:203-204` - exactly-once v2: same shape on the thread producer.
- `TaskExecutor.java:227-228` - at-least-once: `taskManager.consumerCommitSync(allOffsets)` then
  `updateTaskCommitMetadata(allOffsets)`; the three `catch` blocks below rethrow as
  `TaskMigratedException`, `TimeoutException` or `StreamsException` without reaching it.

If the commit throws, the `updateTaskCommitMetadata` line is never executed. That is what "success-only"
has to mean, and it is a property of the three call sites, not of the method's name.

**Second hazard, same method, same audit: `revive()` returns a closed task to service.**
`AbstractTask.revive()` (`AbstractTask.java:141-149`) transitions the *same instance* from `CLOSED` back
to `CREATED`:

```java
@Override
public void revive() {
    if (state == CLOSED) {
        clearTaskTimeout();
        transitionTo(CREATED);
    } else {
        throw new IllegalStateException("Illegal state " + state() + " while reviving task " + id);
    }
}
```

Any resource an integrator attached to the task and closed on the way down is therefore silently reused
after revival. Revival is not hypothetical: `closeDirtyAndRevive` is the caller, and it is on the
corruption and revocation-timeout paths.

**When you find a hazard like that, prefer failing loudly over silently re-creating the resource.** A
silent re-create looks like a fix and hides a state loss - the new instance does not carry what the old
one held.

## Why This Matters

An acknowledgement placed in `postCommit` fires on a **failed** commit, marking work as committed that
was not. Whether that is a nuisance or data loss depends on what the acknowledgement releases - and here
it releases state.

Follow the chain in this project. The hook calls `PcTaskDispatcher.onCommitSuccess`
(`parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcTaskDispatcher.java:389-391`),
which is the only caller of PC's `setClean`. That reaches
`PartitionState.onOffsetCommitSuccess`
(`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/PartitionState.java:221-224`):

```java
public void onOffsetCommitSuccess(OffsetAndMetadata committed) { //NOSONAR
    lastCommittedOffset = committed.offset();
    setClean();
}
```

Clean means the partition stops offering that completed-offset data for commit -
`getCommitDataIfDirty()` (`PartitionState.java:402-410`) returns `empty()`. Nothing re-commits it,
because as far as PC is concerned it already was committed. The broker still holds the older offset.
Whoever next owns the partition resumes from there and re-delivers records that are marked done. There
is no error, no retry, and no log line: **the loss is silent and unrecoverable.**

Note that the correctness of the *design* is fine either way - PC deliberately does not clear anything
at collection time, precisely so a commit that fails afterwards leaves the partition dirty and the next
collection returns the same or newer data (`PcTaskDispatcher.java:365-367`). The entire safety of that
design rests on the acknowledgement being called only on success. Attaching it to a hook whose name
merely *implies* success dismantles a protection that was otherwise correctly built.

This defect class already has a confirmed instance inside this repository, found independently, in the
core module: `ConsumerManager.commitSync` caught `CommitFailedException`, logged it, and returned
normally, so `AbstractOffsetCommitter.retrieveOffsetsAndCommit()` carried straight on to
`onOffsetCommitSuccess()` and marked the offsets clean
(`parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/MockConsumerCommitFailedTest.java:9-29`).
Different framework, same shape: **something downstream of a failure took a normal return as a success
signal.** Two instances is a class, not a coincidence.

The cost asymmetry is the whole argument. The grep is ten minutes. The alternative is an acknowledgement
that does not mean anything, buried behind a comment the next reader will trust.

## When to Apply

- Before attaching **any** correctness-critical state to a framework callback - commit acknowledgements,
  durability markers, "safe to release" signals, offset advances, cache invalidations.
- Whenever a callback's name contains `post`, `after`, `onSuccess`, `onComplete`, `didX` or similar, and
  you are about to rely on the event it names having actually happened.
- Whenever you write, review or read a code comment asserting what a third-party framework guarantees.
  That is a claim, not documentation. It should carry a `file:line` citation, or be treated as unverified.
- When allocating a resource inside a framework-managed object, ask the mirror question: can this object
  be *resurrected* after teardown? If yes, decide deliberately between re-creating and failing loudly.
- On any dependency **upgrade**: the contract is the call sites, so it can change without the signature,
  the name or the javadoc changing at all. The citations above are pinned to 3.9.2 for exactly this reason.

## Examples

### Before - the acknowledgement on `postCommit`

The original placement hung PC's success acknowledgement off `postCommit`, with a comment asserting
Kafka reaches it only after a successful commit. Under
`TaskManager.tryCloseCleanActiveTasks` (`TaskManager.java:1619-1644`) that comment is false, and under
`closeDirtyAndRevive` (`TaskManager.java:283-298`) there is not even a commit to be false about.

### After - the acknowledgement on `updateCommittedOffsets`

`parallel-consumer-streams/src/main/patch/pc-streams.patch:639-651`:

```java
public void updateCommittedOffsets(final TopicPartition topicPartition, final Long offset) {
    committedOffsets.put(topicPartition, offset);

    // PC dispatch (astubbs#255, U9 review): the success acknowledgement lives HERE, not in postCommit -
    // TaskExecutor invokes this only from the success branches of commitOffsetsOrTransaction, whereas
    // Kafka reaches postCommit after swallowed commit failures (TaskManager.tryCloseCleanActiveTasks)
    // and with no commit at all (closeDirtyAndRevive). Acking a failed commit would mark work clean
    // that was never durably committed. PC's onOffsetCommitSuccess reads only the offset, so the
    // reconstructed OffsetAndMetadata needs no payload.
    if (pcDispatcher != null) {
        pcDispatcher.onCommitSuccess(Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset)));
    }
}
```

The comment names the two refuting call sites. That is the form to copy: a claim about framework
behaviour should carry the evidence that settled it, so the next reader can re-check it against a new
version instead of re-deriving it.

### The revival hazard - fail loudly, do not silently re-create

`pcDispatcher` is `final` (`pc-streams.patch:271`) and is closed on the way down through
`close(boolean)` (`pc-streams.patch:414-415`). `AbstractTask.revive()` (`AbstractTask.java:141-149`)
would hand that same closed dispatcher live records again: it would accept them and never dispatch them.
No progress, no exception, nothing in the log.

The chosen answer, at `pc-streams.patch:319-341`, is to refuse:

```java
@Override
public void revive() {
    if (pcDispatcher != null && pcDispatcher.isClosed()) {
        throw new IllegalStateException(
            "PC dispatch (astubbs#255): task " + id + " cannot be revived - its PC dispatcher was closed "
                + "with the task and would accept records without ever dispatching them. Revival under PC "
                + "dispatch is not supported yet; run with the seam off "
                + "(-Dpc.streams.dispatch.enabled=false) if this path is required.");
    }
    super.revive();
}
```

Its javadoc records the reasoning: recreating the dispatcher is the real fix and belongs with the
rebalance work, and this is *"the loud-failure floor until then"*. The trade is deliberate. A silent
re-create would restore a *working* dispatcher carrying none of the closed one's in-flight state, which
converts a diagnosable crash into another silent loss - the exact failure mode this doc is about. It is
tracked as divergence 2 of six in
`docs/inflight/pr-streams-task-lifecycle-and-rebalance.md:23-25`.

### The audit, as a checklist

For each hook you are about to depend on:

1. `grep -rn "<hookName>" <framework-sources>` - list every caller.
2. For each caller, read what precedes the call **in that method**, not in the one you had in mind.
3. Ask: is the call inside a `try` whose `catch` swallows or logs? Is it in a loop over *all* objects
   rather than the surviving ones? Is it reached from a teardown path where the operation was skipped?
4. If any caller can reach the hook without the named event having happened, the hook is not that event.
   Find one that is, or add your own guard.
5. Pin the framework version in the comment, because step 1 has a different answer next release.

## Related

- [Fresh work needs an independent reviewer](../best-practices/fresh-work-needs-independent-review.md) -
  this premise was falsified by an independent reviewer, not by the author who wrote the comment
  asserting it. The author had already re-read the diff.
- [Chase refuted predictions](../best-practices/chase-refuted-predictions.md) - what to do once a
  reviewer refutes something you believed.
- `docs/inflight/pr-streams-task-lifecycle-and-rebalance.md` - the six known lifecycle divergences under
  PC dispatch, including revival and the `prepareRecycle()` leak.
- `parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/MockConsumerCommitFailedTest.java` -
  the same defect class in the core module, where a swallowed `CommitFailedException` produced a false
  success acknowledgement.
- astubbs/parallel-consumer#271, issue astubbs#255 - the PR and issue this was learned on.
