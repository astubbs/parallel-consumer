# What would a Parallel Consumer share-groups mode look like?

<!-- inflight-type: feature -->
<!-- inflight-impact: architecture -->

**Antony's idea, 2026-08-22:** a PC mode that runs *on* share groups - PC tracks each record's
acknowledgement and issues the response, and the engine's ordering and retry machinery sits on top of
a share consumer instead of a classic one.

Recorded as a design sketch. **Two constraints below are potentially fatal and are named first**, so
nobody builds toward them.

## The trade, stated up front

Today: `KafkaConsumer` + PC's shards, ordering, retries, **offset encoding**, and **partition
assignment handling**.
Proposed: `KafkaShareConsumer` + PC's shards, ordering, retries - and **the broker keeps the
per-record state**.

## What PC gives share groups: the thing they cannot do

**Per-key ordering.** Share groups have none and no equivalent, and adding it broker-side means
per-key in-flight state that is unbounded in cardinality and must be replicated
([`next-what-survives-share-groups.md`](next-what-survives-share-groups.md)). PC already does exactly
that, in a client, where it is cheap.

So the mode's proposition is: **ordered concurrency over a share group** - which neither component
offers alone.

**And a second headline, added 2026-08-25: auto-sized concurrency over a share group.** Share
Groups deliver records; how many to process at once remains the application's problem, and it is
the deploy-time guess adaptive concurrency exists to remove. The argument and the market estimate
behind it are owned by
[`next-what-survives-share-groups.md`](next-what-survives-share-groups.md) ("Share Groups still
hand you the parallelism problem"); what matters here is that it widens the wrapper's audience to
users who never need ordering.

Plus PC's retry policy in the application's own code, rather than delivery-count archival as the
broker's policy. `RELEASE` and `REJECT` map onto redelivery and poison-pill routing, and PC's DLQ
could use `REJECT` as the signal it currently has to invent.

## What share groups give PC: two entire subsystems stop existing

This is the part that makes it more than a curiosity.

- **Offset encoding goes.** The run-length and bitset encoding, the commit-metadata size ceiling, and
  the backpressure that fires when the encoded set gets too large - all of it exists to compress
  "which records above the frontier are done" into a commit. **The share coordinator tracks exactly
  that, per record, as its normal operation.** An entire subsystem, and its hardest operational limit,
  becomes someone else's problem.
- **Partition assignment goes.** Share groups do not assign partitions; consumers acquire records.
  So revocation, the epoch machinery, commit-during-revoke, and the whole
  [`bug-857-family.md`](bug-857-family.md) - twelve sightings of stalls and offset-tracking failures
  after rebalance - **are about a mechanism that would no longer be there.**

**PC's two hardest bug families are both in the parts share groups replace.** What is left is the
shard and ordering machinery, which is the part that works.

## The two constraints that could kill it

**1. Key ordering is only enforceable within ONE consumer, and share groups exist to break that.**

Key ordering needs every record for key K to reach one place. Classic consumer groups deliver that
through partition assignment - K hashes to a partition, the partition has one owner. **Share groups
deliberately dissolve that affinity**: any consumer may acquire any record.

So a PC share mode guarantees key ordering **within a single instance** and loses it across
instances. Two PC instances in one share group can hold records for the same key simultaneously and
neither can know.

That is not necessarily fatal - a single instance with high concurrency **is** PC's pitch, and the
whole argument is that you should not need more instances to get more concurrency. **But it must be
stated as a hard limit, not discovered.** "Ordered, one instance per share group" is a coherent
product; "ordered" is not.

**2. The acquisition lock fights PC's core behaviour.**

Records are acquired for `share.record.lock.duration.ms`. Exceed it and the record is released and
redelivered **to someone else** - a duplicate delivery, which is the one thing PC exists to prevent.

**PC's entire value is holding many records outstanding for a long time.** Measured: 5,000 in flight,
and at a 100ms handler that is seconds of residence. So either the lock duration is raised to cover
PC's worst-case residence - which weakens the broker's own liveness guarantee for everyone in the
group - or PC bounds its residence, which is a constraint it does not have today.

**Measure PC's residence distribution against realistic lock durations before designing anything
else.** The residence metric that landed 2026-08-22 is exactly the instrument, and this may be the
first real use for it: if p99 residence exceeds a plausible lock duration, this idea is dead and one
sweep says so.

## And a third, smaller: in-flight is capped at a batch

Neither acknowledgement mode may poll with records unacknowledged, so PC could not fetch ahead as it
does today. Its concurrency would operate **within** an acquired batch. Batches are large - 2,606
measured - so this is a bound rather than a blocker, but it is the reason the 2.5x throughput result
inverts at 100ms, and PC-on-share-groups would inherit that inversion rather than fix it.

## The honest summary

**A trade, not an upgrade.** Give up cross-instance key ordering and accept a lock-bounded residence;
gain the deletion of offset encoding and partition-assignment handling, which is where PC's hardest
bugs live.

**Worth a spike, gated on constraint 2**, which is measurable today and cheap.

## The better question: what does PC offer someone ALREADY using share groups?

**Antony, reframing 2026-08-22: not what share groups would simplify in PC - what subsystem of PC is
still useful to a user who has a share consumer and is happy with it?**

A much cleaner product, because **everything contentious drops out.** No cross-instance ordering
problem: an acquired batch is exclusively yours. No offset encoding, no partition assignment, no
rebalance handling - the broker did those.

### What a share-groups user still writes themselves

| They need | They write today | PC already has |
|---|---|---|
| **Concurrency at all** | a thread pool and a semaphore | the worker pool, virtual threads, **self-tuning concurrency** |
| **Per-key ordering inside the batch** | nothing, or a hand-rolled key lock | the shard machinery - **and inside one acquired batch it is exactly correct** |
| **Retry with backoff** | hold the record and sleep, burning the acquisition lock | per-record retry delay, attempt limits, the retry queue |
| **A DLQ with context** | `REJECT`, which archives and routes nowhere | DLQ with headers saying what failed and how often |
| **Not acquiring more than it can finish** | guesswork against `share.record.lock.duration.ms` | residence measurement - exactly the number that decides it |
| **Visibility into what is stuck** | broker-side metrics only | residence, in-flight, per-shard state |

**Ordering inside a batch is the one to pause on.** If a batch holds three records for key K, a share
user almost certainly wants them in order and has nothing to help. PC's shards do this - and **the
cross-instance objection that sinks the full mode does not apply**, because the batch is exclusively
held.

### The product: PC's engine WITHOUT the Kafka parts

```
List<Verdict> verdicts = batchProcessor.process(records, userFunction);
// caller applies them: ACCEPT / RELEASE / REJECT
```

**The caller keeps the consumer, the acknowledgement and the lock.**

**That shape already exists here**, which is the best argument it is coherent: the language-proxy
clients receive records and return verdicts, with ordering, retries and offset tracking staying in the
engine. **This is the same seam pointed at a different caller** - and the conformance suite proving
ten clients behave identically at that boundary would apply.

### What it honestly is not

**A refactor, not a wrapper.** `WorkManager` is coupled to `PartitionStateManager`, epochs and offset
state; extracting ordering, concurrency and retry over a caller-supplied list is real work, and nobody
has traced those dependencies.

**It does not lift the batch-synchronous ceiling.** In-flight is still bounded by the batch. PC would
make the batch run better, not bigger.

**The acquisition lock stays the user's problem** - but PC would give them the number to reason about
it with, which they do not have today.

### Why it may be the better of the two ideas

**Additive rather than competitive.** It asks nobody to stop using share groups, adopt PC's consumer,
or give up broker-side state. It sells the part of PC that Kafka has not built and shows no sign of
building: ordered concurrency, retry policy, and knowing what your workers are doing.

It is [`next-what-survives-share-groups.md`](next-what-survives-share-groups.md) turned from a
defensive position into a product.
