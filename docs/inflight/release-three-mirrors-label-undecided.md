# Three closed mirrors that may belong in 0.6.0.0

<!-- inflight-type: task -->
<!-- inflight-impact: release-gate -->


astubbs#118, astubbs#233 and astubbs#252 are closed `upstream-mirror` issues carrying no `0.6.0.0`
label. Whether they should is undecided - but for two of them the *code* question is already
answered, and the remaining question is narrower than it looks.

## Already answered: astubbs#233 and astubbs#252 are fixed

`src/docs/development/upstream-map.yaml`, entry `sweep-2023-already-fixed` (`last_checked:
2026-08-07`, `status: merged`), records the code-level check for both - and opens with *"Recorded so
nobody re-investigates."*

- **astubbs#233** (`confluentinc#41`): `findCompletedEligibleOffsetsAndRemove` is gone;
  `PartitionState.incompleteOffsets` is a `ConcurrentSkipListMap` and
  `getOffsetHighestSequentialSucceeded()` is a `ceiling()` lookup - *"exactly the proposed design"*.
  The manifest notes one residue: the code's own comment still points at `confluentinc#200` for the
  complete solution, so that exact variant stays open.
- **astubbs#252** (`confluentinc#319`): the `ConcurrentModificationException` is structurally
  impossible - `PartitionMonitor` became `PartitionStateManager`, `partitionStates` is a
  `ConcurrentHashMap`, and `getAssignedPartitions()` collects into a fresh map before any stream.

**Do not re-derive this.** An earlier draft of this note did exactly that, concluded astubbs#233
"cannot be assessed by name" and astubbs#252 was "probably not fixed", and was corrected by review.
The manifest is named in [`AGENTS.md`](../../AGENTS.md) as the source of truth for fork↔upstream
mapping; it is the first place to look, not the last.

## The open question is the label, not the fix

Both were fixed **by inherited upstream code** rather than by fork work - the `PartitionStateManager`
rework for astubbs#252, and whatever removed the offset scan for astubbs#233 (`confluentinc#270`, the
PR that proposed it, was swept unmerged). So the question is the one 0.5.3.3 raises elsewhere in
[`release-0.6.0.0.md`](release-0.6.0.0.md): **was that code in an upstream release users can already
consume?**

- If yes, 0.6.0.0 delivers nothing new for these two and the label would overstate it.
- If no, 0.6.0.0 is their first delivery and the label is right, exactly as for astubbs#182.

Settling it means finding which upstream version contains those changes - not re-checking whether
the code is fixed.

## Genuinely open: astubbs#118

Not in the manifest at all. It carries `partially-fixed-in/0.5.2.6`, and nothing records which part
remained. Commit `ed78d6cc9` is on master, and its subject quotes this issue's own failure -
*"fix(core) #118: foreign offset metadata no longer kills the consumer on assignment"* <!-- issue-refs: exempt -->
- with the `RuntimeException: Unexpected magic: 1` reproduced in the body.

What is unestablished is whether that fork commit closes the part 0.5.2.6 left open, or fixes a
different symptom raising the same exception. That is a code question, and the answer belongs in a
comment on the issue - one is already posted stating exactly this, so start there.

## Do not label them on the strength of being closed

A mirror's closure describes upstream's state, never the fork's. An audit nearly labelled all three
for that reason alone, which would have asserted a delivery that may not exist.
