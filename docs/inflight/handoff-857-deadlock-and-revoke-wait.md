---
artifact_contract: "ce-handoff/v1"
created_at: "2026-08-13T00:00:00Z"
title: "confluentinc#857 deadlock: diagnosis landed, revoke-wait fix undecided"
summary: "Three learnings committed; the next code change is bounding an unbounded wait in onPartitionsRevoked, and the design choice for it is still open."
keywords: ["857", "deadlock", "onPartitionsRevoked", "producer-fencing", "revoke-wait", "pr-29"]
cwd: "/Users/astubbs/github/parallel-consumer/.claude/worktrees/ce-docs"
resume_focus: "Decide and implement how onPartitionsRevoked should handle an in-flight transaction"
repository: "astubbs/parallel-consumer"
branch: "docs/session-learnings-857-family"
---

# Handoff: confluentinc#857 deadlock work

## Objective

Land the last of the three defects behind confluentinc#857 ("paused consumption after rebalance").
Two siblings (astubbs#100, astubbs#80) are merged. The third is a commit-path deadlock, and PR
astubbs#29 is the long-running draft that attempts it.

## What is done

**Three solutions docs are committed on this branch** (`docs/session-learnings-857-family`), plus
eight `CONCEPTS.md` entries. Read the runtime-errors one first — it is the substantive one:

- `docs/solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md`
- `docs/solutions/workflow-issues/mechanical-issue-ref-sweep-falsified-a-verbatim-log-quote.md`
- `docs/solutions/workflow-issues/keeping-both-sides-of-a-merge-conflict-resurrects-a-deleted-abstraction.md`

**Issue astubbs#225 is filed** (`feature`, `area/reliability`): make producer fencing recoverable
instead of fatal, modelled on Kafka Streams' `TaskMigratedException`.

## The decision that is open — this is where to start

`AbstractParallelEoSStreamProcessor.onPartitionsRevoked` waits with **no deadline** for an in-flight
transaction:

```java
while (isTransactionCommittingInProgress())
    Thread.sleep(100);
```

This is on **master**, predating astubbs#29. The callback runs inside `poll()`, so it is bounded by
`max.poll.interval.ms` — overrun means eviction, which is the reported symptom.

Three constraints, all verified by reading the code:

1. **Waiting risks eviction** (unbounded wait inside a deadlined callback).
2. **Proceeding risks instance death** — `ProducerFencedException` is wrapped in
   `InternalRuntimeException` and kills the instance. This is what astubbs#225 would fix.
3. **The poll thread cannot abort the transaction.** `ProducerManager` enforces single-writer from
   the control thread and throws `ConcurrentModificationException` otherwise. This kills the obvious
   "bound the wait, then abort" design.

**The proposal I had reached, not yet agreed with the user:** put the deadline on the *holder*
rather than the waiter. The control thread holds the produce write lock across `flush()` plus a
200-retry commit loop with nothing bounding it; the revoke callback is only what notices. Bounding
the control thread is safe today (it owns the transaction, so it can abort itself), fixes more than
the revoke path, and does not depend on astubbs#225. Cost: it touches the commit path, which is more
load-bearing than a rebalance callback.

Branch `fix/bound-revoke-transaction-wait` exists off master for this, with **no code on it yet**.

## State of PR astubbs#29 — do not assume it is close

Draft, `CONFLICTING`, three checks red. Three independent defects on the branch:

1. **`RebalanceEoSDeadlockTest` cannot observe its own fix.** It counts a latch by overriding
   `commitOffsetsThatAreReady()`; the fix moved the revoke path to a private
   `tryCommitOffsetsOnRevoke()`. The latch is unreachable by construction — **it would fail 5/5
   against a perfect fix**. Also, the test runs `PERIODIC_TRANSACTIONAL_PRODUCER`, where the AB-BA
   cycle cannot occur at all. Neither arm of the `tryLock()` fix executed in CI (the skip log
   appears zero times in 741k lines).
2. **`ThreadConfinedConsumer` ownership failure on close** — two subsystems disagree on
   `isResponsibleForCommits()`. 33 of 43 instances show a *live* owner, so a dead-owner fix is
   wrong for the majority case.
3. **`numberRecordsOutForProcessing` double-decrement** — three unclamped decrement sites, only the
   revoke path clamps.

My recommendation was to **split astubbs#29**: the lock change is ~20 lines and provable; the
218-line `ThreadConfinedConsumer` refactor is where its defects come from and deserves its own PR.

## Traps that cost me time

- **A package rename `io.confluent.*` → `bz.stub.*` landed on master and on the astubbs#29 branch**
  from a concurrent session. My local `pr29` worktree is 5 commits behind its own remote. Path
  citations in the committed docs were rewritten to `bz/stub/` and each verified to exist. Anything
  written earlier against `io/confluent/` paths is stale.
- **`docs/BUG_857_INVESTIGATION.md` (on the astubbs#29 branch) is actively misleading.** It presents
  the `tryLock` fix as validated at 80-90% and lists `ThreadConfinedConsumer` under "what was
  fixed". This session disproved both. It also names three different "root causes" in three
  sections. It needs a dated status banner; that was not done.
- **The stale worktree.** `/Users/astubbs/github/parallel-consumer/.claude/worktrees/pr29`
  (machine-local) holds the same docs against pre-rename paths. It is superseded by this branch —
  do not re-commit from it.

## Verification actually performed

Read-only, against the tree and one CI log (run 31147427151, 741,161 lines). Gates run on the docs:
frontmatter, claims, issue-refs, copyright — all green. **No code was written or run**, so nothing
about the proposed fix is empirically tested.

## Suggested next step

Settle the revoke-wait design with the user (holder-deadline vs waiter-deadline), then implement on
`fix/bound-revoke-transaction-wait`. The `ce-compound` skill is worth running again once something
actually lands and is proven.
