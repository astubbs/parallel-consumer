---
title: "Transactional mode has three hang-shaped failures with different causes: tell them apart by what the logs do NOT contain"
date: 2026-09-02
category: runtime-errors
module: parallel-consumer-core
problem_type: diagnostic_method
component: transactional-commit
applies_when:
  - "A transactional instance stops making progress and the source offset stops advancing"
  - "About to attribute a transactional stall to the confluentinc#857 commit-lock deadlock"
  - "A rebalance never completes for a transactional member and the group reports it is already rebalancing"
  - "Every produced record fails with `Cannot execute transactional method because we are in an error state`"
  - "Deciding whether a transactional hang is a deadlock, a never-attempted commit, or a poisoned transaction"
related_components:
  - AbstractParallelEoSStreamProcessor
  - ProducerManager
  - ConsumerOffsetCommitter
tags:
  - concurrency
  - transactions
  - exactly-once
  - stall
  - diagnosis-method
---

# Hang-shaped failures in transactional mode, told apart

## Context

`CONCEPTS.md` already warns that a stall, a load-tightness flake and an unforceable trigger all
present as the same expired await. Transactional mode narrows that further: it carries three known
failures that a user reports identically - *"it stopped and stayed stopped"* - with genuinely
different causes, in different commit modes, with different fixes.

Confusing them has already cost one investigation, and the misattribution outlived the investigation:
the battle test's own issue map (astubbs#262) paired the reported commit-lock timeout with the wrong
defect and the wrong fix, and was still saying so weeks after the correction landed in the note that
owns it. This write-up is the discriminator, extracted so it is not restated in each of the four notes
that own a piece of it.

## The three shapes

### 1. The commit-lock hang when a second instance starts

This one splits in two, and the split is the correction worth reading before anything else. Both
present as *"second instance joins, the first stops, the group reports it is already rebalancing"*.

- **The AB-BA deadlock** - confluentinc#857 family, **fixed by astubbs#29**. The poll thread parks on
  `synchronized (commitCommand)` in the revoke callback while the control thread holds that monitor
  inside a blocking commit. The second edge of the cycle lives in `ConsumerOffsetCommitter`, which
  `BrokerPollSystem` constructs **only** for the consumer-commit modes - so **it cannot close in
  transactional mode at all**. Write-up:
  [`revoke-path-commit-deadlock-between-poll-and-control-threads.md`](revoke-path-commit-deadlock-between-poll-and-control-threads.md).
- **The unbounded revoke wait** - astubbs#44 / confluentinc#803, **still open**. In
  `AbstractParallelEoSStreamProcessor`, the revoke callback loops `while (isTransactionCommittingInProgress())`
  with no deadline. That gate is on `options.isUsingTransactionCommitMode()`, so this runs **only** in
  transactional mode, where it is the common case rather than a race. The callback runs on the poll
  thread inside `poll()`, so overrunning `max.poll.interval.ms` evicts the member. Owner:
  [`docs/inflight/bug-857-transactional-revoke-wait.md`](../../inflight/bug-857-transactional-revoke-wait.md).

**astubbs#44 was re-triaged off astubbs#29 and onto the revoke wait on 2026-08-18.** astubbs#29 has
since merged; its `tryLock` change does not touch the revoke wait, so a transactional instance that
hangs on a second start is still an open defect. Reading the merge as a fix for astubbs#44 is the
specific mistake this section exists to stop.

### 2. The batching stall - no commit was ever ATTEMPTED

**Fixed by astubbs#257.** The produce lock was acquired per poll context and released per record, so
at `batchSize >= 2` every batch failed - and because only a *success* marks a partition dirty, no
commit was ever attempted. The source offset simply froze (3 of 201 in the reproduction). Write-up:
[`../test-issues/transactional-batching-stall-produce-lock-released-per-record-2026-08-08.md`](../test-issues/transactional-batching-stall-produce-lock-released-per-record-2026-08-08.md).

### 3. The poisoned-transaction wedge

**Open - a design decision, not a defect.** After a terminal produce failure the transaction is
correctly moved to abortable-error, every subsequent send is refused, and the instance stays alive
without progressing. Settled from the code rather than by experiment: the only `abortTransaction()`
call site is `ProducerManager#close(Duration)`, and `lazyMaybeBeginTransaction` never opens a
replacement, so there is **no recovery path short of `close()`**. Owner:
[`docs/inflight/bug-wedged-after-poisoned-transaction.md`](../../inflight/bug-wedged-after-poisoned-transaction.md).

## The discriminator: read what the logs do NOT contain

This is the part that generalises, and it is the reason a thread dump is not always the first move.

| Shape | Commit-path errors | Produce-path errors | Threads |
|---|---|---|---|
| 1 - commit-lock hang | none, or a `commitLockAcquisitionTimeout` at the end | none | **blocked**, not failing - poll thread parked in the revoke callback |
| 2 - batching stall | **none at all**, because commits were never attempted rather than attempted and failing | one user-function failure per batch, blaming code that did not fail | running |
| 3 - poisoned wedge | none while nothing is dirty | **every send fails loudly**, unthrottled, once per record per retry | running |

Shape 2 is the counter-intuitive one: an empty commit-error log is normally read as "commits are
fine". Here it is the positive evidence, and the batch-failure log actively misdirects by naming the
user's function.

## What this does not settle

Two of the three underlying defects are still open (shape 1's revoke wait, shape 3's design
decision). This document tells them apart; it does not fix them, and the notes named above own their
current state. The transactional chaos scenario that would exercise all three under churn is Phase B
of [`../../plans/2026-08-07-001-test-transactional-eos-battle-test-plan.md`](../../plans/2026-08-07-001-test-transactional-eos-battle-test-plan.md).
