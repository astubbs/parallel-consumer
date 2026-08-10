---
title: A high-water mark cannot express out-of-order completion
date: 2026-08-10
category: architecture-patterns
module: parallel-consumer-streams
problem_type: architecture_pattern
component: background_job
severity: critical
applies_when:
  - You are adding concurrency to a pipeline whose progress is tracked by a single offset, cursor, sequence number or watermark
  - Work is sharded by key or handed to a pool, so completion order no longer equals arrival order
  - A crash or restart resumes from the stored position, and anything skipped is never retried
  - You are about to add a lock or a concurrent map to make a shared progress counter safe
  - You need to prove a crash-safety claim rather than assert it from the design
related_components:
  - parallel-consumer-core offset accounting
  - kafka-streams StreamTask commit path
  - consumer-group commit metadata
tags:
  - frontier
  - out-of-order-completion
  - crash-safety
  - offset-commit
  - checkpointing
  - at-least-once
  - kafka-streams
  - red-then-green
---

# A high-water mark cannot express out-of-order completion

## Context

A sequential pipeline can record its progress with one number. Completion order equals
arrival order, so "the highest one I finished" is also "everything up to here is done",
and committing it is safe by construction. Every checkpoint, cursor, offset and
watermark of this shape is built on that coincidence.

Kafka Streams is such a pipeline. `StreamTask` keeps one `Long` per partition -
`private final Map<TopicPartition, Long> consumedOffsets`
(`parallel-consumer-streams/src/main/patch/pc-streams.patch:248`) - written when the
processor chain returns for a record, and read at commit time. With one record in flight
at a time, that map is correct.

`parallel-consumer-streams` replaced that dispatch with Parallel Consumer's work
selection: records on different keys run concurrently on a worker pool, which is the
entire point of the module. The map did not break in any way that announced itself. It
never threw, never logged, and every test that asked "did the committed offset advance"
still passed. What changed is that the number it held stopped meaning what its readers
assumed:

> `consumedOffsets.put(...)` fires when `doProcess` returns. Workers finish out of order,
> so Streams can commit offset N for a partition while a *lower* offset from that same
> partition is still in flight; crash at that moment and those records are gone.
>
> `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:1263-1265`

The alpha shipped with that recorded as a known shortcoming, and U9 of
astubbs/parallel-consumer#271 (issue astubbs#255) removed it. The decision that governs
the removal is KTD-S7 (`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:250-253`),
and its reasoning is the reusable part:

> chosen over repairing `consumedOffsets` with synchronisation: a single `Long` per
> partition cannot represent "12 done, 10 and 11 still in flight" under any locking, so
> the structure is the defect, not the access to it.

## Guidance

### 1. Recognise the shape before you reach for a lock

The tell is that the bug report sounds like a race - two threads, a shared map, a value
that is wrong at the moment it is read - so the reflex is `synchronized`, a
`ConcurrentHashMap`, or a compare-and-set on the maximum. All of those make the number
*consistent*. None of them make it *correct*, because the information required to be
correct is "which items below the maximum are still outstanding", and a single slot has
nowhere to put it.

Test the representation, not the code path: ask whether the state can express
`12 done, 10 and 11 still running`. If it cannot, no amount of locking will help, and
every fix you ship will be a narrowing of the window rather than a closing of it.

### 2. Commit the frontier, and encode the exceptions beside it

The **frontier** is the highest point below which everything is contiguously complete
(`CONCEPTS.md:74-80`). It advances only when the gap behind it closes: with 10 and 12
still running, completed 11 and 13 do not move it. It is the *only* position that a
crash-time commit can safely name, because by definition nothing below it is unfinished.

In `parallel-consumer-core` the frontier is one method:

```java
public long getOffsetHighestSequentialSucceeded() {
    long currentOffsetHighestSeen = offsetHighestSucceeded;
    Long firstIncompleteOffset = incompleteOffsets.keySet().ceiling(KAFKA_OFFSET_ABSENCE);
    boolean incompleteOffsetsWasEmpty = firstIncompleteOffset == null;

    if (incompleteOffsetsWasEmpty) {
        return currentOffsetHighestSeen;
    } else {
        return firstIncompleteOffset - 1;
    }
}
```

`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/PartitionState.java:455-475`.
The committed value is that plus one, the next offset to poll
(`:427-429`), and the pairing with the exception list is a three-line method
(`:413-419`): a frontier, plus an optional encoded payload.

The exceptions are the second half, and they are what turn "resume safely" into "resume
safely without redoing everything". PC encodes the *incomplete* offsets below the highest
succeeded (`:442-447`), base64 into the commit's metadata field
(`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/offsets/OffsetMapCodecManager.java:241-250`).
That is the complement of the obvious encoding, and it is deliberate: over the bounded
range between the frontier and the highest completed item, naming the holes and naming
the completions carry identical information, and the holes are the smaller set.

### 3. Say what the pattern is: cumulative ACK plus SACK

The frontier plus its holes is structurally TCP's cumulative acknowledgement plus its
selective-acknowledgement blocks (`CONCEPTS.md:82-90`, which this project names
**frontier semantics**, or **frontier plus holes**). Say so in the design note.

It is worth the sentence for two reasons. It tells a reviewer that the design is a
known-good one from a protocol that has survived thirty years of adversarial conditions,
rather than something invented at the desk that afternoon. And it imports the reader's
existing intuition for free: the cumulative half is what makes loss impossible, the
selective half is a pure optimisation, and a receiver that ignores the selective blocks
is slower but never wrong.

That last property is what lets you ship the halves separately. This module commits the
holes but does not yet read them back on assignment, recorded up front as a non-goal
(`docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:855-861`): the frontier alone
prevents loss, and replaying records already completed beyond it is a permitted
at-least-once duplicate. The follow-up turns "no loss" into "no loss and minimal replay".

### 4. Degrade to frontier-only, never to high-water

The exception list has a budget - the broker caps commit metadata at 4096 bytes
(`OffsetMapCodecManager.java:63-67`) - so a design built on it must say what happens when
the holes do not fit. The answer must never be "fall back to the single number".

PC strips the payload and commits the bare frontier, then stops accepting more records for
that partition
(`PartitionState.java:504-508` and `:514-524`). Both halves matter. Stripping trades
replay for safety, which is the correct direction. The backpressure bounds how far the
hole set can grow while it cannot be persisted, so the degraded state is self-limiting
rather than a slow slide into replaying an entire partition.

### 5. Delete the single-number path, do not guard it

The fix that landed removed the writer rather than defending it. Workers no longer touch
commit state at all:

> Since U9, workers write no commit state - `consumedOffsets` and `commitNeeded` are
> stock-path-only, single-threaded fields again, and completion reaches the commit through
> the dispatcher's mailbox alone.
>
> `parallel-consumer-streams/src/main/patch/pc-streams.patch:554-568`

Every reader was then re-pointed rather than left with a fallback: `prepareCommit` returns
PC's map wholesale (`:369-382`), `commitNeeded()` asks PC's dirty state instead of sweeping
the consumer position (`:626-633`), and the three gates that must agree about outstanding
work share one helper, because "three copies is how they would silently stop agreeing"
(`:612-624`).

Leaving the old path reachable "for the sequential case" is what makes this a recurring
defect rather than a fixed one. The evidence is in this very change: the crash test's red
run was not tripped by `consumedOffsets` at all, but by a *different* single-number answer
sitting behind it. With the partition group empty on the PC path, stock's
`committableOffsetsAndMetadata()` falls back to `consumer.position()` and commits every
polled record
(`parallel-consumer-streams/src/test/java/io/confluent/parallelconsumer/streams/integrationTests/CommitFrontierCrashRestartTest.java:57-59`).
Two independent expressions of the same defect class, one of which nobody had written
down. Any code path still capable of producing a single-number answer is a latent instance.

### 6. Clear the progress state on commit *success*, and collect before you flush

Replacing the representation moves two ordering obligations into view that a high-water
mark hid:

- **Collection must not clear anything.** PC's dirty state clears only on a successful
  commit acknowledgement
  (`parallel-consumer-streams/src/main/java/io/confluent/parallelconsumer/streams/PcTaskDispatcher.java:365-372`),
  so a commit that fails after collection leaves the partition dirty and the next cycle
  re-collects. The acknowledgement is wired to Streams' success branch specifically, not to
  `postCommit`, which Kafka also reaches after a swallowed failure and after no commit at
  all (`pc-streams.patch:639-650`). Acking a failed commit marks work clean that was never
  durably recorded, which is the original bug wearing a different hat.
- **Collect before flushing outputs, not after.** Workers complete during the flush, and
  `KafkaProducer.flush()` does not cover sends enqueued after it was invoked, so collecting
  afterwards can commit a record whose output is not yet durable (`pc-streams.patch:370-381`).
  Collecting first is safe because a worker's order is send, then success, then mailbox.

Neither obligation exists while a worker writes the number itself, which is part of why
the old design looked simpler than it was.

### 7. Prove it by crashing with work in flight

A crash-safety claim of this shape is only settled by a test that crashes the system with
work deliberately in flight and asserts that the committed position covers *nothing*
unfinished. `CommitFrontierCrashRestartTest` is built entirely around forcing that moment:
a record parks on a latch inside the chain while ten records on other keys complete around
it (`:70-83`, `:300-310`), and the commit interval is dropped to 500ms so a commit lands
while it is parked (`:76-77`).

Three details are what make it a proof rather than a demonstration:

- **The crash is a crash.** `PcTaskDispatcher.abortAllActive()` kills the workers with no
  drain, no completion feed-back and no final commit (`PcTaskDispatcher.java:479-485`). A
  clean `close()` would drain via the patched `suspend()` and commit on the way down,
  handing the simulated crash the repair pass a real one never gets - and would therefore
  pass on the broken version (`CommitFrontierCrashRestartTest.java:134-141`).
- **The restart's reader is scoped past the crash.** The output topic's end offset is
  captured between phases and the verifying consumer seeks to it (`:262-292`), so the
  post-restart assertions cannot be satisfied by run 1's durable output. Without that, the
  restart assertions are vacuous - they were, and the review caught it (`:256-261`).
- **The assertion is on the exact value, with the failure explained in its message**
  (`:118-125`). "The committed offset advanced" passes just as happily on the broken
  version.

Write it red first. Against the pre-U9 mechanism this test fails, and that red run *is*
the demonstration of the defect (`:56-60`).

## Why This Matters

**The failure is silent, and it is data loss.** Nothing throws, nothing warns, no gap
appears in the log, and the run looks healthy right up until the crash. What is lost are
records that were accepted, acknowledged upstream, and then skipped forever on restart
because the committed position claimed they were done. There is no later signal that says
this happened.

**The instinctive fix is the wrong axis.** Locking, atomics and concurrent collections all
address *when* the value is read, and the defect is *what the value can say*. Work spent
there produces code that is harder to read, no safer, and now carries the appearance of
having been dealt with.

**A guard leaves a loaded gun in the drawer.** The correct change deleted the single-number
path instead of gating it, because the first person to reuse "the sequential case" path
reintroduces the defect, and the second expression of it here was one nobody had named.
Once you can state the defect *class* - any code path that answers progress with one number
on a concurrent pipeline - you can grep for it, which is not possible while you are chasing
the symptom.

**The weak test is the dangerous one.** "The committed offset advanced" is the assertion
most people write, and it is green on the broken system. Only "the committed offset covers
nothing unfinished, observed at a crash" separates the two.

**Someone now owns the metadata field.** Commit metadata is a single field with a single
decoder, and moving the exception list into it means PC owns it on that path. That is a
deliberate consequence, not an accident, and the ownership rule and its opaque-rider escape
hatch are the subject of the companion doc linked below.

## When to Apply

- **Apply when** a pipeline that was sequential gains per-key, per-shard or per-item
  concurrency, and something downstream still resumes from a single stored position. This
  is the moment the representation becomes wrong, independent of whether anyone has
  observed a loss yet.
- **Apply when** the stored position is consumed by a *restart* rather than only by a
  progress display. Loss requires that skipped work is never revisited; a monitoring
  gauge that overstates progress is a reporting bug, not a data-loss one.
- **Apply when** you are about to make a shared progress counter thread-safe. Check first
  that thread-safety is the problem.
- **Do not apply when** each item already carries its own acknowledgement, as in a queue
  with per-message acks. The frontier is a *compression* of that state for a system that
  can only persist one position; if you can persist per-item state directly, you already
  have the general representation.
- **Do not apply the exception-encoding half** before the frontier half. Frontier-only is
  correct and merely replays more; holes-only is not a thing. Ship them in that order, and
  record the read-back as an explicit non-goal if you stop halfway, so the next reader
  knows it was a decision.

## Examples

**The commit path, before and after.** Before: a worker writes the number when it finishes.

```java
// StreamTask.process, verbatim. On the pre-U9 PC path the same line ran from
// pcRunChain, on a worker thread, and deleting it is step 3 of the unit
// (docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:822-827).
consumedOffsets.put(partition, record.offset());
```

After: the worker writes nothing, and the commit asks the component that tracks per-record
completion.

```java
if (pcDispatcher != null) {
    final Map<TopicPartition, OffsetAndMetadata> pcCommitData = pcDispatcher.collectCommitData();
    flush();
    hasPendingTxCommit = eosEnabled;
    log.debug("Prepared {} task for committing (PC frontier)", state());
    return pcCommitData;
}
```

`parallel-consumer-streams/src/main/patch/pc-streams.patch:376-382`. `collectCommitData()`
drains completions but never waits for in-flight work, "which is precisely what keeps the
frontier below them"
(`PcTaskDispatcher.java:357-372`), and reaches
`WorkManager.collectCommitDataForDirtyPartitions()`
(`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/state/WorkManager.java:201-203`),
the same method core's own committer has always used
(`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/AbstractOffsetCommitter.java:32`).

**The assertion that separates fixed from broken.** The parked record sits at offset 0 with
ten completed records behind it:

```java
assertThat(committed.offset())
        .as("THE FRONTIER PROPERTY (R10): the record at offset 0 is still IN FLIGHT - parked "
                + "inside the chain - so the committed offset must be exactly 0, the frontier. "
                + "A higher value records the parked record as done while it is running: crash "
                + "now and it is silently lost. ...")
        .isEqualTo(0L);
```

`CommitFrontierCrashRestartTest.java:118-125`.

**The measured red-then-green, which is the whole claim in one line:**

| Commit-frontier IT | red-then-green: committed 11 (consumer position) before, 0 (frontier) after |

`docs/plans/2026-08-08-002-ks-on-pc-spike-result.md:519`. Eleven records were polled, so
the broken version committed 11 and reported all of them done while one of them was still
running. The frontier arithmetic gives the fixed value directly: the lowest incomplete
offset is 0, so `getOffsetHighestSequentialSucceeded()` returns -1 and the committed
position is 0 - resume exactly where the unfinished work is.

**The end-to-end proof, and its honest limit.** `killRestartLosesNothing`
(`CommitFrontierCrashRestartTest.java:147-199`) crashes with the frontier committed,
restarts, and asserts the in-flight record is processed *by the restart*. It also asserts
each of the ten completed records is re-processed, because with metadata read-back still a
non-goal the restart replays everything at or beyond the frontier. That is the permitted
duplicate, and the test asserts presence rather than exact counts so it states the real
guarantee rather than a stronger one nobody implemented.

**The compatibility question the encoding creates.** `stockRestartOnPcCommittedGroupDegradesGracefully`
(`:207-254`) starts the same application id with the seam off, on a group whose last commit
carries PC's payload. Stock Streams reads PC's magic byte as an unsupported version, warns,
degrades to UNKNOWN partition time, and runs. Whenever you take over a shared metadata
field, some other decoder is going to read your bytes: find out what it does, and assert
the behaviour rather than the log line.

## Related

- `docs/solutions/architecture-patterns/one-owner-per-metadata-field-with-an-opaque-rider.md` -
  where the exceptions get stored, who owns that field, and how a displaced tenant gets its
  state back without becoming a second writer
- `CONCEPTS.md:74-90` - **Frontier** and **Frontier semantics**, the project's vocabulary for
  this design, and the **high-water mark** contrast
- astubbs/parallel-consumer#271 (issue astubbs#255) - the PR that removed the high-water
  mark, in unit U9
- `docs/plans/2026-08-08-001-feat-ks-on-pc-spike-plan.md:250-253` (KTD-S7, the decision and
  the rejected repair), `:788-902` (U9, the approach and its test scenarios), and `:56` (R10,
  the requirement)
- `docs/plans/2026-08-08-002-ks-on-pc-spike-result.md:478-548` - the measured outcome,
  including the predictions that were refuted
- `docs/solutions/best-practices/control-arms-vary-exactly-one-term.md` - the general form of
  the red-then-green discipline used here
