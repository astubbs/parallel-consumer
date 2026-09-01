# `PostCommitCheckpointGapTest`'s PC arm passes in a shared JVM and fails alone

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

`parallel-consumer-streams` (astubbs#255). **Owned by the stream-time rung,
astubbs/parallel-consumer#396**, which wrote the class. Recorded here because it was found on the
reconciliation branch, and because the first reading of it was wrong in a way worth writing down.

## What fails

`PostCommitCheckpointGapTest.pcPathAlsoRefreshesTheCheckpointUnderLoad` asserts that the changelog
offset read out of the checkpoint file lands within roughly one commit round of the records
processed. It fails with

```
Expecting actual:
  -4L
to be greater than:
  10000L
```

**`-4` is a sentinel, not a small number** - it is what a checkpoint entry carries when the offset is
unknown, so the checkpoint file exists and its changelog offset was never populated. Reading that as
"the run did not get far enough under load" is the mistake this note exists to prevent: it is a
different failure from a bound missed by a margin, and it does not move with more time.

## The three arms, and the one that inverts the obvious reading

| Arm | Result |
|---|---|
| Reconciliation branch, whole streams integration suite, local, one fork | **passes** |
| Reconciliation branch, this class alone, local | **fails 3 of 3**, always `-4` |
| astubbs/parallel-consumer#396's own tip, this class alone, local | **fails 3 of 3**, always `-4` |
| CI `Integration Tests`, whole lane at `forkCount=4` | **fails**, same `-4` |

**The control arm is the third row and it is what matters.** The reconciliation is not the cause: the
class fails identically on the branch that wrote it, with the one changed term being the merge and no
change in outcome. That was the hypothesis worth ruling out first, because a commit fence and a
checkpoint expectation meeting for the first time is exactly the kind of interaction a reconciliation
surfaces.

**What is left is an ordering dependency.** The class passes when it shares a JVM with the siblings
that ran before it and fails when it does not - which is why a single-fork local suite run is green
and a four-fork CI lane is not, since forked classes do not reliably inherit that neighbour.

## Why this is worth more than a red build

The class's own assertion message says it **refutes** a sibling class's inference - that `postCommit`
never runs on the PC path - and that "if this ever goes empty, the skip has stopped being confined to
idle windows and the sibling class's original reading becomes correct after all". A refutation that
only holds when an earlier class has warmed the JVM is a weaker refutation than the one written down,
and the green suite is what hides that. This module's own handover warns about exactly this shape:
**several tests here passed while proving nothing.**

## What settling it needs

Find what the earlier class leaves behind that this one reads - the state directory, the changelog
topic, or a store that is only initialised once per JVM are the candidates, and the class pins its own
`stateDir` - and make this class establish it for itself. **Do not widen the bound**: the bound is not
what is failing, and a `-4` will not become 11,526 by waiting longer.

Until then, a green `PostCommitCheckpointGapTest` in a full local run is not evidence about the PC
path's checkpointing, and the assertion message's refutation should be read as unproven rather than
as established.

## Delete when

The class passes run alone, or its claim is restated to what a shared JVM actually establishes.
