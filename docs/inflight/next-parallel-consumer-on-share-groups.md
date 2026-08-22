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
