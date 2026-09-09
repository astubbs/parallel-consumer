---
title: "Absence of commit data was inferred from a sentinel a real commit shares - and the truncation warning fired on every new consumer group"
date: 2026-09-09
category: logic-errors
module: parallel-consumer-core
problem_type: logic_error
component: partition-bootstrap
root_cause: absence_inferred_from_a_sentinel_rather_than_recorded_at_its_source
resolution_type: code_fix
severity: medium
symptoms:
  - "`Truncating state - removing records lower than N ... Bootstrap polled N but expected 0 from loaded commit data` on a partition the broker reports with an empty CURRENT-OFFSET"
  - "The warning fires on a brand new consumer group, and on a group whose committed offset has aged out of the offsets topic"
  - "The line claims records were removed while the incompletes map is empty, so nothing was removed and nothing was loaded - false in both halves"
  - "It is emitted once per partition per assignment, so a rebalance across a large topic produces a burst of them"
applies_when:
  - Changing PartitionState#maybeTruncateBelowOrAbove or anything that reads its bootstrap expectation
  - Adding a branch that means to ask whether a partition arrived with committed offsets
  - Auditing any code that reads KAFKA_OFFSET_ABSENCE as though it meant "there was no commit"
  - Reading confluentinc#546 / astubbs#162 and wanting the three defects behind that one warning string separated
tags:
  - offset-management
  - partition-bootstrap
  - truncation
  - sentinel-values
  - misdirection
  - logging-contract
  - issue-162
  - issue-546
related_components:
  - PartitionState
  - OffsetMapCodecManager
  - EncodedOffsetPair
---

# Absence of commit data was inferred from a sentinel a real commit shares

## The one-paragraph answer

`PartitionState#maybeTruncateBelowOrAbove` took its expectation from `getOffsetToCommit()` and never asked
whether commit data existed. A partition assigned with no committed offset is built from
`OffsetMapCodecManager.HighestOffsetAndIncompletes.of()`, so `offsetHighestSucceeded` is
`KAFKA_OFFSET_ABSENCE` and the expectation computes to 0 - and any first poll above offset 0 then took the
above-expected branch, logged `Truncating state - removing records lower than ...` at WARN, and pruned
nothing, because there was nothing loaded to prune. The fix is not to test the sentinel. It is to **record
the absence where it is known** - the empty `Optional` the default entry is constructed with - and read that
one fact at the branch.

## Why the sentinel is the wrong question, which is the whole point

The cheap check reads `offsetHighestSucceeded == KAFKA_OFFSET_ABSENCE`, and it is wrong:

| | commit data | decoded highest-seen | `offsetHighestSucceeded` | bootstrap expectation | should a poll above it warn? |
|---|---|---|---|---|---|
| new group / expired offset | **none** | `Optional.empty()` | `-1` | 0 | **no** - nothing was loaded, nothing pruned |
| a real commit filed at offset 0, no payload | **yes** | `Optional.of(0 - 1)` | `-1` | 0 | **yes** - the records it was filed against are gone |

The two states are indistinguishable downstream: same sentinel, same expectation, opposite correct
behaviour. Suppressing on the sentinel would silence the true report for exactly the partition most likely
to be caught by retention - the one that has processed nothing yet. `-1` is not "no commit"; it is "nothing
succeeded", and a committed offset of 0 legitimately means nothing succeeded.

The fact that separates them exists once, at construction: every decode path produces a **present**
`HighestOffsetAndIncompletes.getHighestSeenOffset()` because every decode has a committed offset to be
relative to, and only `HighestOffsetAndIncompletes.of()` - the default entry for an assignment with no
commit history - produces an empty one. `PartitionState` now samples that into a `final boolean` before
`initStateFromOffsetData` collapses the `Optional` into the sentinel. This is the engine `AGENTS.md`'s
collapse-parallel-state rule pointed at a *fact* rather than at a cache: keep the one the source knows
rather than re-deriving a weaker one at the point of use.

## The control arm, and what refuted the shortcut

`PartitionStateAbsentCommitData162Test` is a pair differing in one term. Both arms present an expectation of
0 and a first poll above it; one arrived with commit data and one did not. Predicted before running:
implementing the guard as the sentinel shorthand fails **only** the commit-at-offset-0 arm. Run with the
shorthand substituted, it did exactly that - one failure, the control arm, with the absent-data arms and
`PartitionStateBootstrapTruncation162Test`'s two genuine branches still green. That is what makes the
distinction load-bearing rather than stylistic.

## The three defects behind one warning string in confluentinc#546

The upstream thread reads as one bug and is three. Keeping them apart is what the thread never did, and it
is why the issue outlived its own fix.

1. **The RunLength "expected 1" decode - fixed upstream, inherited here.** `857c384af`
   (confluentinc#563, upstream 0.5.2.6); the guard is `long highestSeenOffset = (baseOffset > 0)` in
   `offsets/OffsetRunLength.java`. A no-progress commit used to decode back as base offset 0. The reporter
   confirmed that form of the message stopped.
2. **The false warning on absent commit data - fixed here.** The same warning was reported on **0.5.2.7**,
   *after* that fix shipped, in the "expected 0 from loaded commit data" form, alongside a broker CLI
   screenshot showing an empty `CURRENT-OFFSET` for exactly those partitions. That is this document.
3. **`Bootstrap polled offset has been reset to an earlier offset` - refuted as a PC defect.** Not a race
   between PC's `consumer.committed()` and the fetcher's position resolution: the client runs
   `coordinator.poll` (and so the rebalance listener) before `updateFetchPositions`, so PC's read strictly
   precedes the fetcher's and a commit landing between them yields the *other* branch. A commit PC writes
   decodes back to exactly the offset it was filed under, which `PartitionStateBootstrapTruncation162Test`
   pins over four commit shapes. Every remaining way in is a genuine rewind by the broker, where replaying
   is right. Settled by astubbs#484.

## A claim this fix corrected on the way past

The fork's note said astubbs#217's foreign-metadata recovery landed on the same default entry through
`catch (OffsetDecodingError)` in `loadPartitionStateForAssignment`, and so inherited the false warning. **It
does not.** Under the runtime-default `IGNORE` policy, `EncodedOffsetPair#handleUnreadableMetadata` returns
`of(baseOffset - 1)` - commit data carrying the committed offset - so such a partition bootstraps with a
real expectation and the truncation branches stay correct for it. Under `FAIL` the typed exception
propagates instead. Neither policy reaches the default entry, and the only production caller of
`OffsetEncoding#decode` (the one site that throws `OffsetDecodingError`) is `EncodedOffsetPair#unwrap`,
which today has test callers only. Pinned by
`PartitionStateAbsentCommitData162Test#unreadableMetadataKeepsTheCommittedOffsetRatherThanBecomingAbsentCommitData`.

## The generalisable rule

**A sentinel answers the question it was defined for and no other.** `KAFKA_OFFSET_ABSENCE` was defined to
mean "no offset has succeeded", and it does that faithfully; it was then read as "no commit arrived", which
is a different question whose answer it merely correlates with. When you find yourself reading a sentinel
to recover a fact, ask where that fact was last known for certain and carry it from there - it is almost
always cheaper than the arithmetic, and unlike the arithmetic it cannot alias.

The tell that you are about to do this: the value you are testing was **derived** from the thing you care
about (here, an `Optional` flattened by `orElse`), rather than being it.
