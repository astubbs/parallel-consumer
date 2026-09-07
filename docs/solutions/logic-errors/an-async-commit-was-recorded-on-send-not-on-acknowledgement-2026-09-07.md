---
title: "An async commit was recorded on send, not on acknowledgement - and a failed callback had nothing left to retry"
date: 2026-09-07
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: commit-path
root_cause: state_advanced_on_request_not_on_acknowledgement
resolution_type: code_fix
severity: high
symptoms:
  - "In PERIODIC_CONSUMER_ASYNCHRONOUS - the shipped default commit mode - the broker's committed offset silently stays behind what PC believes is committed, and no error is raised"
  - "The OffsetCommitCallback logs `Error committing offsets` with a RetriableCommitFailedException and nothing retries, because the partition was already marked clean"
  - "A callback that never arrives at all leaves the same state as one that failed, with no event to hang a decision on"
  - "Commit attempts stop after the first one even while the commit is failing every time: nothing is dirty, so collectCommitDataForDirtyPartitions() returns empty"
  - "The next owner of a partition resumes from a position the broker never recorded - records are redelivered or, on a raised external offset, skipped"
applies_when:
  - Reading or changing ConsumerOffsetCommitter's async branch, or AbstractOffsetCommitter's success marking
  - Wiring any new committer whose transport does not block until the broker answers
  - Asking why the commit-failure seam (astubbs#317) excludes the async commit mode
  - Auditing a codebase for state advanced at request time rather than at acknowledgement time
  - Investigating a PERIODIC_CONSUMER_ASYNCHRONOUS stall or lag stagnation that no known defect explains
tags:
  - commit-path
  - async-commit
  - offset-management
  - data-loss
  - acknowledgement
  - fire-and-forget
  - dirty-tracking
  - issue-248
related_components:
  - ConsumerOffsetCommitter
  - AbstractOffsetCommitter
  - ConsumerManager
  - PartitionState
  - WorkManager
---

# An async commit was recorded on send, not on acknowledgement

## The one-paragraph answer

`AbstractOffsetCommitter.retrieveOffsetsAndCommit()` called `onOffsetCommitSuccess()` on the line after
`commitOffsets()` returned. For `commitSync` and for the transactional producer that is a statement about
something that has already happened - both return only once the broker has answered. For
`Consumer#commitAsync` it is not: that call returns as soon as the request is handed to the client, and
answers later through an `OffsetCommitCallback`. So under `PERIODIC_CONSUMER_ASYNCHRONOUS`, **the shipped
default commit mode**, the partition was marked clean at the moment of *sending*. A callback that then
arrived carrying an exception had nothing dirty left to retry, and a callback that never arrived was
indistinguishable from success. The fix moves the state transition into the callback's success path.

## What state was moving, and when

One commit cycle in the async mode, before the fix. The whole of it runs on the broker-poll thread.

| Step | What moves |
|---|---|
| `PartitionState.getCommitDataIfDirty()` | Resets `stateChangedSinceCommitStart` to false, so completions arriving *during* the commit keep the partition dirty. Returns the offset to commit |
| `ConsumerOffsetCommitter.commitOffsets()` | Hands the request to the client. **Returns immediately.** Nothing has reached the broker yet |
| `AbstractOffsetCommitter` | Calls `onOffsetCommitSuccess()` - **here was the defect** |
| `PartitionState.onOffsetCommitSuccess()` | `lastCommittedOffset` advances (the `pc.partition.latest.committed.offset` gauge reads this), and `setClean()` clears the dirty flag |
| *later* `OffsetCommitCallback` | Logged. On failure, `log.error` and nothing else. It carried a `// todo keep work in limbo until async response is received?` |

Nothing else moved: **incompletes are not trimmed on commit** - `PartitionState` tracks only *incomplete*
offsets and the offset committed is the lowest incomplete one, so there is nothing below it to discard
(truncation happens on the bootstrap poll instead). There is no commit metric other than that gauge. So the
whole of the damage is in two fields, and the load-bearing one is the dirty flag.

**What a failed or dropped callback then left PC believing:** that the offset was durable. And because the
partition was clean, `collectCommitDataForDirtyPartitions()` returned empty from then on, so no further
`commitAsync` was ever issued for those offsets. Not a delayed retry - **no retry**. The broker's committed
offset stayed where it was, forever, while PC ran on happily. Silent loss.

## The fix

Two small pieces, both minimal on purpose:

- `AbstractOffsetCommitter` gained `commitOffsetsReturnsOnlyOnceAcknowledged()`, true by default, and now
  only marks success inline when it holds. `onOffsetCommitSuccess` became protected so a committer that
  answers false can call it itself.
- `ConsumerOffsetCommitter` returns `isSync()` from it, and its async branch registers
  `onAsyncCommitAnswered`, which marks success when the broker acknowledges and otherwise leaves the offsets
  dirty for the next cycle to re-send.

**A failure is now a deferral, and lands where the synchronous path's rejections already landed** - logged,
not fatal, offsets still marked as needing a commit. That is the option `commitDeferringOnRebalance()`'s
javadoc argues for at length, arrived at from the other direction: there, deferral is what a thrown
exception *causes* by aborting before the success marking; here it is what the absence of a success signal
causes. Both are the same rule - **only an acknowledgement advances the state.**

### The one hazard the fix itself creates, and the one rule for it

Deferring the clean-marking is what makes **two async commits able to be in flight at once**; before, the
first send marked the partition clean so there was never a second. An answer can therefore arrive for a
request a later one has partly overtaken, and the rule that handles it lives entirely in `PartitionState`:

> **`getCommitDataIfDirty` remembers the offset it offers. `onOffsetCommitSuccess` records every
> acknowledgement, and marks the partition clean only when the offset acknowledged is that offer.**

Both halves matter, and they are separate because an acknowledgement carries two different things. **The
offset is always recorded** - the answer is true, the broker committed up to it, and
`recordCommittedOffset` keeps the higher of the two when answers arrive out of order. **The clean mark
waits**, because marking a partition clean at an offset a later offer has passed is what would leave
nothing dirty to re-send the offsets in between if that later request then failed or was dropped - this
defect, re-entered through the door the fix opened.

**The committer keeps no record of what it has in flight, and that is the point.** It passes an
acknowledgement straight through, whole. The partition offered the offset, so the partition is the thing
that can recognise the answer to its own latest offer; a map in the committer would have been a second
copy of what the partition already knew. Two earlier drafts of this fix put the decision there and were
replaced on the maintainer's call: first an `asyncCommitSequence` that ignored any answer which was not
the latest - throwing away a fact the broker had established, for every partition, including the ones
nothing had superseded - and then a per-partition `highestOffsetInFlight` map, split acknowledgements and
a second success method plumbed through four layers, to reach an outcome the partition produces on its
own.

The outcome still differs per partition, and it falls out rather than being implemented. A commit carries
every dirty partition and only the ones that completed more work move, so a request is routinely the
newest word on one partition and superseded on another - and in the second round the partition that stood
still is re-offered at exactly the same offset, so the first round's answer *is* its latest offer and
cleans it. A whole-request rule leaves that partition waiting for a re-commit of an offset the broker has
already acknowledged, every cycle, for as long as one partition of an assignment outruns another.

Leaving a partition dirty costs at most one extra commit and cannot under-report, which is why every
edge lands on the dirty side: a partition state rebuilt by a rebalance starts with no offer recorded, so
an acknowledgement addressed to the assignment before it cannot clean it either.

**A failed async commit is one `WARN`.** Nothing was lost and nothing needs an operator tonight - the
partitions were never marked clean, so they are still dirty and a later request carries the same offsets.
The astubbs#168 (confluentinc#629) bound applies to it: the offsets are summarised, never interpolated.

## The experiment

Two red tests first, one per way a sent request never becomes a durable commit: `MockConsumerAsyncCommitCallbackFailsTest`
(the callback arrives with a `RetriableCommitFailedException`) and `MockConsumerAsyncCommitCallbackDroppedTest`
(no callback at all). **The discriminating assertion is the broker's own committed offset**, because every
weaker one passes on the defect: no exception escapes, PC does not close, and the records all get processed.

The control arms flip exactly one term - the new predicate - and nothing else:

| Arm | `commitOffsetsReturnsOnlyOnceAcknowledged()` | Async tests | Sync control |
|---|---|---|---|
| pre-fix behaviour | forced `true` | **both fail**: `expected to be greater than: 3 but was: 2` | pass |
| shipped fix | `isSync()` | pass | pass |
| over-applied | forced `false` | pass | **fails**: attempts `expected: 2 but was: 54` |

The third arm is why the sync control test exists. `MockConsumerSyncCommitMarksCleanOnReturnTest` asserts that
commit attempts **stand still** once the batch is committed - not merely that the offset was reached, which is
true either way on the first commit. A sync mode that stopped marking clean would re-commit on every
`commitInterval` forever, and only the attempt count says so.

## Where the mechanism was already recorded, and what it explains

- **astubbs/parallel-consumer#248** (mirror of confluentinc/parallel-consumer#203, nioertel 2022-03) reported
  exactly this path - endless `RetriableCommitFailedException` after a broker restart - and its "Fork status"
  section names the async callback, quoting the `todo keep work in limbo` line verbatim as the part not
  addressed. Closed upstream as *completed* by an administrative sweep while labelled `wait for info`; never
  reproduced, because the three diagnostic questions went unanswered.
- **`src/docs/development/upstream-map.yaml`**, entry `sweep-2023-broker-disconnect-commit`, carried the same
  verdict from the 2026-08-07 sweep.
- **Chaos sightings 4, 5 and 6** in
  [`two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md`](../architecture-patterns/two-threads-one-consumer-why-the-commit-seam-keeps-deadlocking.md)
  are `PERIODIC_CONSUMER_ASYNCHRONOUS` stalls recorded as explained by *no known defect* - "either a fourth
  defect or the chaos harness's own teardown races". This is now a defect that operates in that mode, so it is
  a candidate the ledger did not have. **It is not an attribution**: nothing here was measured against those
  seeds, and `docs/inflight/test-857-churn-storm-async-stalls.md` names the replay as the deciding experiment.
  Record it as a hypothesis to test, not as an answer.

## How this composes with the commit-failure seam

The seam (astubbs/parallel-consumer#317) deliberately excludes the async mode, and its own note names **two**
structural gaps: no commit budget, so no exhaustion event to hand the handler; and offsets marked clean
optimistically, so a CONTINUE decision has nothing to attach to. **This closes the second and leaves the
first.** The seam's options validation still rejects a handler configured under the async mode, correctly -
a handler there would still never fire, because nothing throws the exhaustion event.

That ordering is the useful part for whoever picks the async seam up: the dirty-tracking half is the one that
had a silent-loss bug attached to it and was worth doing on its own; the budget half is a feature with no
demonstrated demand, and is where the remaining work is.

## The general rule

**Do not advance state on a request; advance it on the acknowledgement of that request.** The tell is a
success marking sitting on the line after a call whose contract is "handed to the client". Where a codebase
does this correctly it is worth saying why, because it looks the same at a glance: PC's produce path returns
the send futures and blocks on `futureSend.get(sendTimeout)` before the batch is marked succeeded, and the
Vert.x, Reactor and Mutiny engines all mark `onUserFunctionSuccess()` from the completion signal while
`ExternalEngine` suppresses the dispatch-time marking. Each of those is the same decision, made the right way.
