---
title: "volatile is the fix for a one-writer field, not a shared one - the two PartitionState flags, measured apart"
date: 2026-09-07
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: partition_state
root_cause: check_then_act_and_lost_update_on_a_flag_written_by_two_threads
resolution_type: code_fix
severity: medium
symptoms:
  - "A partition is marked clean over a completion the commit did not include; nothing re-dirties it until the next completion, and on a partition that then goes idle the committed offset waits for the next rebalance"
  - "The offset-encoding back-pressure flag is read stale on the control thread, and because every write to it lives inside the commit-path encode that only runs when the partition is dirty, a stale false never clears - the partition takes no new work and nothing re-dirties it"
  - "A scanner names both fields under AT_STALE_THREAD_WRITE_OF_PRIMITIVE while the ledger recording that rule names neither"
applies_when:
  - A field crosses threads unfenced and volatile looks like the obvious fix
  - Two threads write the same flag and one of them clears it
  - A clean/dirty, done/pending or seen/unseen flag is cleared by one party and set by another
  - Choosing between a modifier and a protocol change, and wanting the choice measured rather than argued
  - Building a deterministic test for a window that has no override point in the shipped code
tags:
  - concurrency
  - check-then-act
  - lost-update
  - memory-model
  - volatile
  - jcstress
  - monotone-counter
  - test-seam
  - control-arm
  - spotbugs
related_components:
  - PartitionState
  - PartitionStateManager
  - AbstractOffsetCommitter
  - WorkManager
---

# volatile is the fix for a one-writer field, not a shared one

## Context

`PartitionState` carried three flags that cross the broker-poll / control thread boundary.
astubbs#349 fenced one of them, `dirty`, with `volatile` against a jcstress FORBIDDEN arm, and
deliberately left the other two alone for want of a measurement, filing a note for each. astubbs#469
took both notes. **The two fields look identical on the page and needed opposite fixes**, and the
only thing that separated them was measurement.

## The discriminator: how many threads WRITE it

- **`allowedMoreRecords` has one writer.** The broker-poll thread sets it inside the commit path's
  encode; the control thread only reads it when deciding whether to take work. One writer, no
  read-modify-write, so the whole defect is visibility - and `volatile` is exactly the tool for
  visibility. Measured: plain, with the real neighbouring `ConcurrentSkipListMap` accesses in place,
  9.2e-6 per raced pair; with the neighbours stripped, 4.7e-4, so **the incidental fencing from a
  concurrent collection one statement away suppresses the window fiftyfold and does not close it**
  (the `dirty` pair measured the same shape at ~130x). Volatile: 0 in 201,927,188, FORBIDDEN.

- **`stateChangedSinceCommitStart` had two writers, and one of them cleared it.** The control thread
  set it on a completion; the committer thread cleared it at commit start and then read it in
  `setClean()` to decide whether to mark the partition clean. Three distinct mechanisms broke the
  invariant and only one of them is a visibility effect:

  1. **check-then-act** - `setClean()` read the flag and wrote `dirty` as two steps, so a completion
     landing between them was simply overwritten. Reachable on sequentially consistent hardware.
  2. **lost update** - two racing writes with no read-modify-write discipline. Ordering them decides
     nothing about which wins.
  3. **staleness** - the only one `volatile` addresses.

  Measured, and this is the finding: **the plain arm and the volatile arm fire at the same rate.**
  142,177 anomalies in 91,295,031 raced pairs versus 148,418 in 94,683,660 - both 1.6e-3,
  statistically indistinguishable. Mechanism (1) alone accounts for the rate, so the modifier that
  fixed the field beside it moves this one by nothing.

**The lesson generalises past this class.** "It crosses threads unfenced" names a symptom, not a
fix. Count the writers first: one writer is a modifier, two writers is a protocol.

## The protocol that replaced it

Both flags collapse into a monotone completion count plus the count the commit covered:

- the completing thread only ever increments a count;
- the committer samples that count at commit start, holds it, and publishes it on success;
- "is this partition dirty" is a comparison of the two.

Neither thread writes a value that can lose to the other, and there is no check-then-act left,
because the clean-marking step publishes a value decided earlier rather than reading one. Measured:
0 in 121,707,028 samples, FORBIDDEN, with 79.92% of pairs landing in the pessimistic corner (still
dirty, one extra commit cycle) and nothing at all in the lossy one.

**The sampling order is the part to get right, and it is counter-intuitive.** Sample the count
*before* capturing the offsets. A completion in between is then committed *and* still counted as
uncovered - one wasted commit cycle. Sampling after would mark it covered when it was not, which
loses it. Pessimistic is the safe direction.

**The release/acquire edge astubbs#349 measured survives the collapse rather than being discarded.**
An `AtomicLong` `incrementAndGet` is a strictly stronger release than the volatile store it replaced,
and the counter load on the reader is the paired acquire, so the plain `long`s written before it are
published exactly as before. A field collapse that removes a measured fence has to say where the
measurement went; this one moved the modifier tripwire rather than deleting it.

## Testing a window whose endpoints are one instruction apart

Every *other* position a completion can take in a commit window was already reachable through public
calls, and the old protocol handled all of them - which is why the existing test for a completion
landing between collect and commit-success passed throughout. The one position it lost was inside
`setClean()`, and the shipped code offered no override point between that read and that write, so
**no deterministic test that was red on unmodified master existed, and the absence was structural
rather than an oversight**.

Two things were done instead of settling for "it is a 1e-3 hardware effect, trust the probe":

- **A seam was added to the production code at exactly that instruction** - an overridable no-op the
  clean-marking step calls before it publishes. A test lands a completion there deterministically.
- **The old protocol is kept as a control arm in the same test class**, driven through the same seam
  on the same scenario, asserting that it *does* lose the completion. Only the protocol term differs
  between the two arms. That keeps the defect executable after the fix has removed it, which a
  commit message describing a build nobody can re-run does not.

The redness was then confirmed the other way round too: the production protocol was temporarily
reverted to the two booleans with the seam left in place, and the shipped arm failed on the
partition being clean. Same magnitude, different position - the control-arm discipline
[`docs/investigating.md`](../../investigating.md) asks for.

**A replica is not the original.** The control arm is nine copied lines, bound to what shipped by
nothing but a human having copied them - the same limitation the `jcstress-poc/` probes carry and
record about themselves. Its independent corroboration is that the jcstress arm modelling the same
shape measured the same loss.

## The ledger was wrong about this, and nothing went red

SpotBugs named both fields under fb-contrib's `AT_STALE_THREAD_WRITE_OF_PRIMITIVE` the whole time,
along with `PartitionState.bootstrapPhase` and `ProgressTracker.highestRoundCountSeen`. The offender
list in [`docs/refactoring.md`](../../refactoring.md) named two fields and a count, and no
`PartitionState` field at all - **while astubbs#349 was actively fencing that class**. The lane runs
with `-Dspotbugs.failOnError=false`, so the finding annotates and never blocks.

That is the worse failure of the two: **the signal was present and the record was wrong about it**,
which reads as an all-clear. The repair was not to write a better list - a list rots the same way -
but to replace it with the command that re-derives it:

```bash
./mvnw -o spotbugs:spotbugs -pl :parallel-consumer-core
# then read parallel-consumer-core/target/spotbugsXml.xml for the bug type
```
<!-- file-refs: N/A - the report path is build output, written by the command above it and absent from a clean checkout -->

## Watch for the finding your own fix introduces

Making the held count a plain `long` raised a *new* `AT_NONATOMIC_64BIT_PRIMITIVE` on a line the
change itself wrote - the kind of thing `bin/check-pr-analysis-surfaces.sh` exists to catch and
nobody looks at. Checking the premise rather than dismissing it found the real answer: the commit
cycle is mutually exclusive, but **which** thread runs it varies - the poll thread in the consumer
commit modes, the control thread in transactional mode, and the poll thread again inside the revoke
callback's `tryCommitOffsetsOnRevoke`. So it is not confined, `@ThreadConfined` would have been a
lie, and the analyser was right. One `volatile`, one store per commit cycle.
