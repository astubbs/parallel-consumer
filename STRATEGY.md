---
name: Parallel Consumer
last_updated: 2026-08-07
---

# Parallel Consumer Strategy

> **Stub.** The full strategy document lands with astubbs/parallel-consumer#223
> (`docs/strategy-and-share-groups-comparison`), which is expected to merge before this branch.
> This file carries only the positioning section that depends on *this* branch's test results. On
> merge, take astubbs#223's version of the file and re-apply the section below.

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
the same time. When astubbs#223's `## Target problem` section merges, it names only the ordering half - add
the second gap there too.

### Verified, with one caveat that must travel with the claim

**Say it exactly as loudly as it is verified.** This is a promise about delivery semantics, and the
README already warns that EoS does not prevent duplicate *replay*. An overstated headline here is the
kind of claim that costs trust rather than winning it.

The validation is `docs/plans/2026-08-07-001-test-transactional-eos-battle-test-plan.md`, which
enumerates every documented transactional guarantee and proves or refutes each one, with a negative
control required before any claim counts as proved. That gate has now fired against us, so this
section is written down as the finding rather than as an aspiration:

**Crash and replay, both batch sizes: the guarantee holds.** An abandoned transaction is invisible,
the replay commits results and their source offset as one set, and the output topic holds each result
exactly once. Proved with observed controls (`TransactionalCrashReplayIT`).

That took a real defect out of the path first, which is the part worth telling honestly. At
`batchSize >= 2` the consumer used to **stall outright** - the produce lock was taken once per poll
context but released per record, the failed release failed the whole batch, and because only a
*success* marks a partition dirty, no commit was ever attempted. The source offset froze at 3 of 201.
That was found by this suite before the fix landed, so astubbs#257 is not a fix we assumed works: the
same test went from RED 5/5 to GREEN 5/5 across it.

**The caveat that must travel with the claim.** One documented guarantee is still refuted, and it is
adjacent enough that omitting it would be misleading. When a single send in a `pollAndProduceMany`
result set fails terminally, the records already accepted stay in the transaction and the next commit
publishes them - so a `read_committed` consumer sees a **partial** result set for one source offset
(observed: 2 of 5). `ProducerManager` installs a producer `Callback` that throws from `onCompletion`,
which pre-empts Kafka's own `maybeTransitionToErrorState`, so the transaction is never marked
abortable. Registered as C7 `PRODUCE_MANY_ALL_OR_NONE` and C2 `ALL_OR_NONE_PER_SOURCE_OFFSET`, both
`REFUTED`.

So the honest headline today is: **exactly-once with per-key ordered concurrency, verified across
crash and replay** - and do not yet claim all-or-none for multi-record result sets where a send can
fail terminally. Promote the unqualified version when C2 and C7 read `PROVED` in `TransactionalClaim`.
The register is the gate; this section follows it rather than leading it.
