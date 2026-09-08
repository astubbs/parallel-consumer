# The retry-queue controller purge: what it supersedes, what it leaves open

<!-- inflight-type: bug -->
<!-- inflight-impact: stall -->
<!-- inflight-labels: concurrency -->

The retry-queue purge fixes the defect that
`git show 745b1f6a5:docs/inflight/bug-retry-queue-write-lock-on-the-rebalance-path.md` tracked - the
rebalance callbacks waiting on `RetryQueue`'s unbounded fair write lock inside `consumer.poll()` -
and retires that note into
[`../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md`](../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md),
which owns the mechanism, both designs and the evidence. Do not re-derive any of that here.

## What a reader needs that no command will tell them

- **It supersedes astubbs/parallel-consumer#431, which is still OPEN and must not be merged.** That
  PR fixes the same defect by a different design (decline the lock with `tryRemove`, abandon the
  paired shard removal on refusal). It is correct; it is more machinery. Closing it is the owner's
  call, and the purge does not make it for them.
- **It collides, textually, with every open PR touching the sweep.** astubbs#468 (the stale sweep's
  by-key eviction), astubbs#410, astubbs#392 and astubbs#362 all edit `ShardManager` or
  `ProcessingShard` around the same methods; astubbs#408 edits `ArchitectureTest`. None of them
  conflicts in *design* - whichever lands second resolves the text.
- **astubbs#468 is the one worth reading first if you are picking this up**, because its finding -
  a removal keyed on an offset cannot say which container it meant - is the shape of the only real
  hazard in the purge. The purge is safe from it only because the retry queue has exactly one
  writer, and that is an invariant nothing enforces at runtime.

## What this leaves open, deliberately

- **`ShardManager.onFailure`'s post-add residency confirmation is now belt-and-braces.** The purge
  would collect every orphan it prevents, a tick later. It is kept this round because removing it is
  a separate decision with its own evidence to gather - and its javadoc says so at the site. Whoever
  takes that decision should read
  [`../solutions/runtime-errors/retry-queue-orphan-window-between-the-requeue-check-and-the-add.md`](../solutions/runtime-errors/retry-queue-orphan-window-between-the-requeue-check-and-the-add.md)
  first, including its 2026-09-08 correction.
- **The runtime controller-ownership guard is what would make the purge's premise checkable** -
  [`core-retry-queue-needs-a-runtime-controller-ownership-guard.md`](core-retry-queue-needs-a-runtime-controller-ownership-guard.md),
  whose case the purge strengthens rather than resolving.
- **The purge's scan is unmetered, and gating it on a departure counter is the open optimisation.**
  Both reviewers of the change found this independently: it is the only unconditional full scan of
  the retry queue on the control loop, because shard residency does not follow the queue's
  `retryDueAt` sort order and so cannot early-stop the way every other reader there does. The
  rejected-alternatives section of
  [`../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md`](../solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md)
  owns the shape (a monotonic `LongAdder` on departure, not a boolean flag) and the order of work:
  meter it first - `DispatchScanMeter` is the precedent - then decide. Do not adopt the counter
  without the measurement; it puts poll-thread-written state back on the rebalance path, which is
  what the change removed.
- **The shard-displacement orphan is narrowed, not fixed** -
  [`bug-shard-displacement-orphans-the-retry-queue-entry.md`](bug-shard-displacement-orphans-the-retry-queue-entry.md)
  carries what changed and what did not.

Delete this note once astubbs/parallel-consumer#431 has been dispositioned and the three open items
above have been re-read - not merely when the purge lands, since none of them does.
