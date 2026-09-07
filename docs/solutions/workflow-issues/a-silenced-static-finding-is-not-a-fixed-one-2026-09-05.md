---
title: A silenced static finding is not a fixed one, and the ratchet does not need a state for it
date: 2026-09-05
category: workflow-issues
module: parallel-consumer-core
problem_type: workflow_issue
component: static_analysis
severity: medium
root_cause: tooling_blind_spot
resolution_type: code_fix
applies_when:
  - A known identity in config/infer-known-findings.txt stops firing and your diff did not touch the code it names
  - Deciding whether an analyser going quiet means the defect is gone
  - Proposing a new state for the Infer ratchet, or for any identity-keyed baseline
  - A nullable-documented boxed return is unboxed into a primitive somewhere down the call chain
symptoms:
  - "bin/infer-test.sh fails with 'these known races no longer fire' on a branch that changed nothing near the finding"
  - "A ratchet line is retired alongside genuinely fixed ones, and the defect claim it stood for is lost"
  - "A documented-nullable Long is auto-unboxed into a long, and the only guard is an NPE naming a local variable"
tags:
  - infer
  - pulse
  - ratchet
  - null-safety
  - partition-assignment
  - epoch
---

# A silenced static finding is not a fixed one, and the ratchet does not need a state for it

## What happened

Infer's Pulse reported `NULLPTR_DEREFERENCE OffsetMapCodecManager.lambda$loadPartitionStateForAssignment$2`
from the day the ratchet was written: `PartitionStateManager.getEpochOfPartition` returns `Long` and
is documented "or null if not yet assigned", and `OffsetMapCodecManager` unboxed that value into
`PartitionState`'s primitive `long` epoch at two sites. On astubbs#57 the finding stopped being
reported, and nothing in that PR touched the dereference, its javadoc, or the constructor. The
ratchet (`bin/infer-test.sh`) has exactly two signals, "new" and "no longer fires", and it reads the
second as "somebody fixed it and did not ratchet" - so the only way to a green lane was to delete the
line, alongside four `PCMetrics` identities that genuinely were fixed. A silenced finding was recorded
in the same gesture as four fixed ones.

What kept the defect claim alive was an inflight note, `bug-epoch-null-unboxes-on-partition-assignment.md`,
which said plainly that the analyser's reach had changed and the code had not, and asked two things
of whoever fixed it: cover both unbox sites, and decide whether the ratchet needs a third state.

## What settled the defect claim

Reading the only production caller, not re-running the analyser (Infer is not installed on the
machine that did this, and it would not have helped - the question was reachability in the code, not
reachability for Pulse).

`PartitionStateManager.onPartitionsAssigned` does, in order and on the calling thread:
`incrementPartitionAssignmentEpoch(assignedPartitions)`, which writes an epoch for every partition in
the collection unconditionally; then `loadPartitionStateForAssignment(assignedPartitions)` with the
same collection. Both unbox sites in the codec manager only look up partitions from that collection -
`Consumer.committed(Set)` returns keys drawn from the set it was given (the real consumer and the
mock alike), and the default-entry pass iterates the assignment itself. Nothing ever removes an
epoch: the map has one writer, `incrementPartitionAssignmentEpoch`, and no `remove` or `clear`. The
"partial-assignment error path" - the load throwing part way - cannot reach the sites either, because
the epoch write is complete before the load begins.

So null is **unreachable at both sites from production**, and the "not yet assigned" decision the
note thought it needed - fail open or fail closed for a partition decoded before its epoch exists -
turned out not to exist: no such partition reaches the decode. That is a different question from the
one `core-stale-arrival-guard-needs-a-null-safety-decision.md` carries about `getPartitionState`,
which is a poll registering against a partition the manager does not know; that one is real and is
untouched by this.

The contract was narrowed, not made lenient. `OffsetMapCodecManager.epochOfPartitionBeingAssigned`
is now the one place the nullable getter is unboxed, and it `Objects.requireNonNull`s with a message
naming the partition and the ordering that was broken - a caller reaching the codec manager for a
partition the state manager was never told about. The return type of `getEpochOfPartition` stays
`Long`, because one reader consumes the null on purpose: `EpochAndRecordsMap` skips a poll's records
for a partition whose assignment callback has not fired yet. `OffsetMapCodecManagerAssignmentEpochTest`
reaches both sites without an epoch and asserts the named failure; against the previous code both
cases fail with the JDK's unbox message instead, and the control case (epoch present) is green either
way.

## The decision: no third ratchet state

The note asked whether the ratchet needs a "silenced" state beside "fixed" and "new". It does not,
and the reason is what a ratchet line *is*.

Every line in `config/infer-known-findings.txt` is a claim the analyser can check in both directions:
present, or gone. That is what lets the gate fail both ways, and it is the property that made
identities better than a count. A "silenced" line would be a claim the analyser can check in
**neither** direction - the finding is not reported, and the line says that is expected - so it could
never go red. It would be the count-that-cannot-move problem again, wearing an identity's clothes.

What a silenced finding actually is, is a **defect claim with no instrument behind it**. Those already
have an owner: a `bug-` note in `docs/inflight/`, delivered at every session start, deleted when the
defect is fixed with a test. That is exactly the path this one took, and it worked - the note outlived
the analyser's reach and got the site guarded. The ratchet stays an instrument-reading
register; the inflight directory stays the register of claims.

So the rule, now written into the ratchet file's own header where the next person to shrink the set
will read it: **when a known identity stops firing and your diff did not change the code it names,
that is a silenced finding, not a fixed one. Retire the line - the gate gives you no choice - and in
the same change either establish by reading that the defect is gone, or open a `bug-` note carrying
the claim.** Never retire it in the same breath as genuine fixes without saying which is which.

## What is still not known, and does not need to be

Why Pulse stopped reaching the dereference. astubbs#57's only edit on the call chain was caching the
`OffsetMapCodecManager` in a `PartitionStateManager` field instead of constructing one per
assignment; the note's first explanation (a removed `@Setter` on `PCModule.workManager`) was
falsified when the setter came back and the lane stayed green. It was not demonstrated here either,
and it did not need to be: the defect claim was settled from the source, and the guard now in place
is one Pulse models directly, so whether or not it regains reach the site reads the same to it.
