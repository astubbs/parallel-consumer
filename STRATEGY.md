---
name: Parallel Consumer
last_updated: 2026-08-07
---

# Parallel Consumer Strategy

> **Stub.** The full strategy document lands with astubbs/parallel-consumer#223
> (`docs/strategy-and-share-groups-comparison`), which is expected to merge before this branch.
> This file carries only the positioning section that depends on *this* branch's test results. On
> merge, take #223's version of the file and re-apply the section below.

## Marketing

**Lead with the combination nothing else has: exactly-once, massively parallel, and optionally
key-ordered.**

Each half is unremarkable alone. Kafka has had exactly-once since KIP-98, and KIP-932 Share Groups
now scale consumers past the partition count. Having both at the same time is not available
anywhere else, and that is the line to put in talks, posts and the README's opening rather than
leaving it as a row two screens down a comparison table.

It holds because the broker-native answer to parallelism gives up exactly-once **by protocol, not by
omission**. [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka):

> "Although it is possible to read transactionally written records, the current protocol does not
> include the ability to acknowledge message delivery within an atomic transaction."

> "This means that the delivery behavior is at-least-once."

The mechanism: exactly-once processing needs the consumer's *offset* commit to join the producer's
transaction, and a share group has no offset to contribute - its state is per-record acknowledgement
state held broker-side, which nothing can enlist in a transaction. The KIP lists exactly-once only as
possible future work. Two details worth keeping straight when writing about this: isolation level is
a **group-level** setting (`share.isolation.level`), not per-consumer; and the delivery counts behind
poison-message protection are themselves not exactly-once, so the KIP says they "cannot be relied
upon to be precise".

So Share Groups decouple scaling from partitions but deliver out of order *and* cannot acknowledge
inside a transaction. Nothing else gives low latency, guaranteed per-key ordering, and exactly-once at
the same time. When #223's `## Target problem` section merges, it names only the ordering half - add
the second gap there too.

### Do not promote this yet - one half of it is currently refuted

**Say it exactly as loudly as it is verified.** This is a promise about delivery semantics, and the
README already warns that EoS does not prevent duplicate *replay*. An overstated headline here is the
kind of claim that costs trust rather than winning it.

The validation is `docs/plans/2026-08-07-001-test-transactional-eos-battle-test-plan.md`, which
enumerates every documented transactional guarantee and proves or refutes each one, with a negative
control required before any claim counts as proved. That gate has now fired against us, so this
section is written down as the finding rather than as an aspiration:

- **Non-batched transactional processing: the guarantee holds.** At `batchSize = 1` the abandoned
  transaction is invisible, the replay commits results and their source offset as one set, and the
  output topic holds each result exactly once. Proved with observed controls
  (`TransactionalCrashReplayIT`).
- **Batched transactional processing: refuted, by a stall.** At `batchSize >= 2` the source offset
  freezes - 3 of 201 in the reproduction - and never advances, so the results the README promises
  "will exist exactly once" never come to exist at all. Cause: the produce lock is taken once per
  poll context but released per record, the failed release fails the whole batch, and because only a
  *success* marks a partition dirty, no commit is ever attempted. 5/5 at `batchSize` 3, 4/4 clean at
  `batchSize` 1.

The uncomfortable part is where the defect sits. The differentiator is exactly-once *with*
parallelism, and batching is a parallelism feature - so the break is inside the combination we would
be selling, not off to one side of it. Marketing this now would be advertising precisely the
configuration that does not work.

**What has to happen first:** land the produce-lock fix (`d95a21d4`, currently unpushed), re-enable
`outputHoldsEachResultExactlyOnceAcrossTheReplayWhenBatching`, and confirm it goes green. When
`TransactionalClaim` shows `RESULTS_EXACTLY_ONCE_UNDER_FAILURE` as `PROVED` rather than `REFUTED`,
promote the section above to the headline. Until then the honest claim is narrower and still worth
saying: exactly-once with per-key ordered concurrency, batching excepted pending the fix.
