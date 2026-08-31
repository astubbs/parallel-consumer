# Partition virtualization: attack Kafka partition rigidity from both sides

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - a program, not a feature; caught from the handover document, absent from the transcript excerpts -->

From the compound-engineering handoff
([`docs/ideation/2026-08-29-hasten-compound-engineering-handoff.md`](../ideation/2026-08-29-hasten-compound-engineering-handoff.md),
sections 43-45, which own the detail) - the largest architecture chapter the pasted transcript
excerpts never carried. PC virtualizes the partition from the *inside* (shards remove unnecessary
head-of-line coupling); because the runtime can control both production and consumption, it can
also attack from the *outside*: optimize ownership, extract pathological keys, and evolve the
physical topic underneath a stable logical stream. The target is honest: not "partitionless
Kafka" - making the physical layout's consequences small and its mistakes reversible.

## The intervention hierarchy - always the cheapest lever that releases useful work

Internal shard execution -> move a partition/task to a less-loaded runtime -> extract only the
problematic logical shard (predictive hot-key extraction via an elastic *shadow route*, removable
later) -> merge it back -> evolve the topic generation -> only then scale infrastructure. *"Do
not scale machines because Kafka happened to hash badly."* This names a first-class
**partition-placement ceiling** - useful parallelism and spare capacity exist but placement
prevents them meeting - and unlike every other ceiling in
[`core-bottleneck-attribution.md`](core-bottleneck-attribution.md), the runtime can remove this
one itself. [`core-partition-advisor.md`](core-partition-advisor.md) is the diagnosis; this is
the remedy ladder that mostly avoids repartitioning.

## The load-bearing pieces, each with its owner

- **Scheduler-driven assignment**: a partition/task assignor fed by scheduler knowledge -
  *balanced ownership means balanced predicted useful execution pressure, not balanced partition
  counts*. For Streams, ride its task-assignment semantics so state/IQ move coherently.
- **Routing state is data, anchored to frontiers**: a globally materialized routing map
  (key/shard -> topic generation + partition, with the epoch and frontier from which it is
  authoritative). Producers never emit on an epoch they cannot resolve; consumers gate on it -
  the [`core-frontier-handover.md`](core-frontier-handover.md) protocol is the cutover mechanism.
- **Topic generations make partition count reversible**: a logical topic backed by successive
  physical generations - create N+1 with a better layout, cut over per-shard at agreed frontiers,
  retire N once resolved. Kafka cannot shrink a topic; this sidesteps rather than fixes that.
- **Partition-count optimization from real key distributions**: simulate the partitioner over the
  observed keys and look-ahead horizon - the right answer may be an irregular 19, not 16/32 - and
  close the loop by measuring whether the predicted improvement occurred. Extends the advisor
  from "partitions bind" to "here is the layout that would not".
- **Virtual records** (handoff §44): a logical record over one or more physical records -
  transparent large-message chunking where an incomplete logical record is simply *ineligible*,
  and ordering, lineage, DLQ semantics and Why Wait all operate on the logical identity. The full
  virtualization stack: virtual record -> offset -> shard -> partition -> topic -> topology
  generation, which is [`core-work-identity-model.md`](core-work-identity-model.md)'s
  identity/position split applied at every granularity.

## Boundaries kept from the handoff

Vanilla Kafka-compatible applications keep normal Kafka semantics - aggressive virtualization
only where the runtime owns both endpoints, and **internal Streams repartition/changelog topics
are the safest first target** (their physical form is already runtime-generated, not an external
contract; co-partitioning and state/task affinity must survive).
