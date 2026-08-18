---
title: "Two threads, one consumer: why the commit seam keeps producing deadlocks, and why mechanism changes never fix it"
date: 2026-08-18
category: architecture-patterns
module: parallel-consumer-core/internal
problem_type: architecture_pattern
component: background_job
severity: high
applies_when:
  - Asking why `pc-control` and `pc-broker-poll` split responsibilities the way they do
  - About to "reconcile" the two `isResponsibleForCommits()` methods, or otherwise assuming they contradict each other
  - Adding a thread-ownership guard or confinement wrapper around the Kafka consumer
  - Proposing to replace the commit request/response queues with actors, futures, or another IPC mechanism
  - Diagnosing a deadlock, stall or unclosed consumer anywhere on the revoke or close path
  - Reading a fix that adds a flag, a cache or a sleep near `onPartitionsRevoked`
related_components:
  - BrokerPollSystem
  - AbstractParallelEoSStreamProcessor
  - ConsumerManager
  - ConsumerOffsetCommitter
  - ProducerManager
tags:
  - thread-ownership
  - consumer-confinement
  - deadlock
  - ab-ba-lock-ordering
  - commit-path
  - shared-nothing
  - archaeology
  - issue-857
---

# Two threads, one consumer

Why Parallel Consumer's commit machinery is shaped as it is, reconstructed from the commit record on
2026-08-18. Written because the reasoning existed only in the original author's head, and three
separate 2026 investigations each re-derived part of it before acting.

## The one-paragraph answer

`KafkaConsumer` is not thread-safe. PC runs two long-lived threads. Which thread may touch the
consumer was never *decided* - it is residue from extracting the blocking poll loop into its own
thread in 2020. Every incident since has happened at the seam where that residue meets the fact that
**commit initiation lives on one thread and commit execution may live on the other, depending on
commit mode.** Four times now the seam has been patched with a cache, a flag or a sleep, and never
restructured.

## How it got this way

| When | Commit | What it decided |
|---|---|---|
| pre-2020-06 | - | **Single-threaded.** The control loop called `consumer.poll()` itself. Commits were transactional-producer only. No ownership question existed. |
| 2020-06-17 | `af1fa5de4` "Event based control, integration tests, back-pressure" | **`BrokerPollSystem` became its own thread.** The consumer object went with it. The commit body gives no reason for consumer ownership - one thread cannot block on both the work mailbox and a long broker poll, so each blocking source got a thread, and the consumer travelled with the poll. **Poll-thread ownership is a side effect, not a decision.** |
| 2020-11-23 | `60e398102` "Choose between Consumer commit or Producer transactional commits" (confluentinc#25) | **The mode split.** Created `ConsumerManager`, `ConsumerOffsetCommitter`, `AbstractOffsetCommitter`, `ProducerManager`, and *both* `isResponsibleForCommits()` methods, in one 16-file change. Commit *initiation* stayed on control (it owns `WorkManager` state and cadence). Commit *execution* went to whichever component owns the needed client. |
| 2020-12-03 | `9dc92e51c` "Move consumer back to PC wrapped for thread safety, so commits are in line with control" | **The redesign was attempted three weeks later and abandoned.** On branch `move-cons-to-pc`, it comments out `BrokerPollSystem`'s `committer`, `maybeCloseConsumer` and `isResponsibleForCommits` so control commits directly. Never merged. **The record does not say why.** |
| 2020-12-15 | `c033f367e` "Replace commit request/signal locking system with far simpler message passing" | Lock + `Condition` + `commitRequested` flag replaced by `commitRequestQueue` / `commitResponseQueue`. **Same wait-for edge, cleaner mechanism.** |
| 2021-07-23 | `a3378ed58` "AK 2.7 blocks concurrent access to group metadata, so cache it" | Control's `groupMetadata()` call collided with the poll thread. Patched with the `metaCache` still in `ConsumerManager`. |
| 2023-04-03 | `2738fb3d5` (confluentinc#548) | A deadlock between these same two threads on revoke. Patched with an `AtomicBoolean isRebalanceInProgress` and a `while (isTransactionCommittingInProgress()) Thread.sleep(100)` spin. **That spin is now the transactional revoke wait behind astubbs/parallel-consumer#44.** |
| 2026 | confluentinc#857 | The same pair, deadlocked again, on the same path. |

## The two `isResponsibleForCommits()` methods do NOT contradict each other

This is the most reliable way to misread the code, and a 2026 investigation got it wrong before
checking. Both methods were created in `60e398102`, by one author, on one day. Both carry the
**identical javadoc**, still present verbatim:

> To keep things simple, make sure the correct thread which can make a commit, is the one to close
> the consumer. This way, if partitions are revoked, the commit can be made inline.

They are an **XOR over commit mode**:

| Class (thread) | Predicate | True when |
|---|---|---|
| `BrokerPollSystem` (`pc-broker-poll`) | `committer.isPresent()` | consumer-commit modes |
| `AbstractParallelEoSStreamProcessor` (`pc-control`) | `committer instanceof ProducerManager` | transactional mode |

Exactly one is true. Exactly one thread closes the consumer. Neither predicate has been edited since
2020.

**The trap is that the relationship is implicit, not that the name is wrong.** Both methods ask the
*same* question - "am I the component that commits, and therefore the one that closes the consumer?" -
and the shared javadoc says so. Only the answer differs, per component, per mode. That is
polymorphism, and it is not expressed as such: there is no shared type, no stated invariant, and
nothing that says these two are halves of one decision. A reader who finds both concludes they
disagree.

**This is a correction to an earlier draft of this document**, which described them as two different
questions with a badly overloaded name. That was wrong in a way worth recording, because it is the
mistake the code invites: differing *implementations* were read as differing *questions*.

**The fix is an interface, not a rename.** Give the two a common type - one method, one javadoc, two
implementors - so the polymorphism is official. That also makes the XOR **statable and checkable**:
"exactly one implementor returns true for a given configuration" is an invariant nobody can express
today and which can be asserted at construction, rather than left for the next archaeologist to
rediscover. Renaming alone only documents the trap more loudly.

## Why transactional mode is where things break

The design carved out an exception from day one. The comment removed by `c033f367e` reads:

> if owning thread is asking, then perform the commit directly (this is the thread that controls the
> consumer) - this can happen when the system is closing ... and partitions are revoked and we want
> to commit

That carve-out exists **only on the consumer-commit side**. In transactional mode the revoke callback
runs on the poll thread, but the committer is the `ProducerManager` on the control thread - so there
is no direct path. **Both confluentinc#548 and confluentinc#857 live in exactly that gap.**

## What has been tried, and the one thing that survives everything

The fatal edge is: **the control thread blocks on something only the poll thread can produce.**

It survived:

1. the 2020 lock + `Condition` + flag scheme (`60e398102`),
2. its replacement by request/response queues (`c033f367e`),
3. the 2022 actor draft on `improvements/commit-command-actor`, which commented out `commitAndWait()`
   and both queues and replaced them with `Future ask = commitRequestSend(); if (isSync())
   ask.get(timeout)` - **the same wait-for edge with better plumbing.**

**Changing the mechanism has never removed the deadlock class.** Only changing *who owns the consumer*
would. The 2022 draft says so itself, in a TODO on `ConsumerOffsetCommitter.commit()`:

> todo this should be package private, and should not need to expose a thread safe interface - bubble
> up to broker system instead - see Controller refactor

alongside a second TODO on the same method - `todo why? when am i the owner? audit - i think never` -
suggesting the owner-asks-directly carve-out was already suspected dead.

`refactor/extract-controller` (`25db90e38`, 2022-11-30, "extract SubscriptionHandler interface, pull
Poll up") got as far as an `internal/Controller.java`, a `RebalanceHandler` and a
`SubscriptionHandler` interface. The actor family stalled with **two unreconciled actor bases**
(`d391398f1`: "split: actor base - needs unifying of the two actor classes"). Why it stopped, the
record does not say.

The canonical tracker is **confluentinc#200 / astubbs/parallel-consumer#142, "Refactor: Consider a
shared nothing architecture, to reduce thread complexity"** - both still open. *Shared nothing* means
no object is shared between threads at all; each owns its state and they communicate by messages.
Applied here it means eliminating the separate poller thread, so one thread owns the consumer and
does both the polling and the committing.

## Constraints any fix must respect

Derived from what the history punished, not from first principles:

- **The revoke callback must be able to complete a commit inline, before truncation**, on whatever
  thread `consumer.poll()` runs on. Set in `60e398102`, re-affirmed by confluentinc#548's fix. In
  transactional mode this needs the producer - a cross-thread dependency the current design cannot
  express except via flags and sleeps.
- **Whoever closes the consumer triggers revoke callbacks on the closing thread.** That is the whole
  reason for "the committer closes". Any confinement scheme therefore needs ownership *transfer* -
  `ThreadConfinedConsumer.claimOwnership()` is claim-only, with no release or handover - or close must
  be routed to the owning thread.
- **A confinement guard must test whether anyone is still *using* the consumer, not thread identity.**
  In `innerDoClose`, `brokerPollSubsystem.closeAndWait()` returns *before* `maybeCloseConsumer()` runs,
  so at the moment the guard fires the poll loop has already finished and there is no concurrency at
  all. The guard rejects the call because the pooled `pc-broker-poll` thread object still exists and
  still holds the claim - identity, not usage. This is also why roughly three quarters of observed
  cases report a *live* owner: a pooled thread outlives the task that claimed ownership. Close is a
  clean sequential handoff point; releasing ownership when the poll loop exits makes the guard assert
  what it actually means.
- **`consumer.wakeup()` is the only thread-safe consumer call, and it is aimed poorly.** The
  `pollingBroker` guard and the `WakeupException` retry loops in `ConsumerManager` exist because a
  wakeup meant for `poll()` can land on `commitSync()`. Any design keeping cross-thread wakeups keeps
  that race.
- **Control must not read `groupMetadata()` live** - AK 2.7+ blocks concurrent access. The `metaCache`
  is load-bearing.
- **Mode-conditional thread topology is the root hazard.** Every incident happened where transactional
  mode put a required action on the thread that does not own the needed client. A fix should unify who
  commits across modes, or unify who owns the consumer. **Asserting confinement on top of the split
  just makes the 2020 design's deliberate violations throw** - which is what
  astubbs/parallel-consumer#29's `ThreadConfinedConsumer` does, 88 times in one CI run.

## Related

- [`a-query-must-never-mutate-derive-thread-safety-from-callers.md`](a-query-must-never-mutate-derive-thread-safety-from-callers.md) -
  the sibling lesson: do not add a thread-ownership guard on the strength of a javadoc or a design
  assumption. This document is the worked case of that rule being broken.
- `docs/solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md` -
  the confluentinc#857 AB-BA cycle itself, and why it closes only in `PERIODIC_CONSUMER_SYNC`.
- `docs/plans/2026-08-18-002-fix-857-revoke-path-cluster-decomposition-plan.md` - the four clusters in
  astubbs/parallel-consumer#29 and the order to take them.
- `docs/inflight/bug-857-transactional-revoke-wait.md` - the confluentinc#548 sleep-spin as it stands
  today, carrying astubbs/parallel-consumer#44.
- `docs/refactoring.md` - the Actor/IPC section and the `refactor-thread-model-god-class` entry.
