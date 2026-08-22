# Share groups as a benchmark arm - and it does not need PC to support Kafka 4

<!-- inflight-type: next -->
<!-- inflight-impact: performance -->
<!-- inflight-labels: needs-measurement -->

**Antony, 2026-08-22: when we move to Kafka 4, add share groups as another arm. And - can we do it
now, with a standalone share-groups consumer?**

**Yes, and that is the point worth recording**: a share-groups arm is a *bare client* arm, like
`vanilla` and `franz`. It uses `KafkaShareConsumer` and no Parallel Consumer at all, so **it is not
blocked on PC supporting Kafka 4.** The only Kafka-4 dependency is the broker the whole bench runs
against.

## What it takes

| | Needed | Have |
|---|---|---|
| Broker | **4.2.0+** - KIP-932 went GA there (2026-02-17); 4.0 was preview | `apache/kafka:3.9.0`. **This is the only blocker** |
| Client | `kafka-clients` 4.2+ for `KafkaShareConsumer` | `CLIENT_PINS` already pins the client per arm, independently of PC |
| Engine | none | the arm is a bare consumer loop |

**And moving the bench broker to 4.2 does not require PC to move.** Kafka keeps client/broker
compatibility in both directions; a 3.9 client talks to a 4.x broker fine. So the sequence is: bump
the broker image, re-take the baselines on it, then add the arm - **not** wait for PC's own Kafka-4
work.

**Re-take every baseline after the bump.** Every figure in
[`perf-engine-comparison-2026-08-22.md`](perf-engine-comparison-2026-08-22.md) was taken on 3.9.0.
Comparing a share-groups arm on 4.2 against PC numbers from 3.9 is the same like-for-like error this
repository has already published once.

## Why it is the most interesting arm available

**Share groups are Kafka's own answer to part of the problem Parallel Consumer exists to solve** -
per-record acknowledgement, and concurrency not bounded by partition count. Every other arm is a
library; this one is the broker changing the rules underneath the whole category.

`STRATEGY.md` already positions against them narrowly. **A measurement would replace a position with
a number**, which is worth more than any of the engine comparisons taken so far.

## The comparison that would be dishonest, and the one that would not

**Share groups have no ordering guarantee at all.** Records go to whichever consumer is free. So a
raw throughput comparison flatters them, because they are doing strictly less work - and a table
putting them beside PC's `KEY` mode would be comparing a system that orders against one that does
not.

Two honest arms instead:

- **Share groups against PC in `UNORDERED`** - like-for-like on semantics, and the only fair
  throughput number. This is the one that could go badly for us, which is exactly why it is worth
  taking.
- **`KEY` ordering has no share-group equivalent.** That is not a benchmark row, it is the
  differentiator, and the honest way to present it is as a capability share groups do not have rather
  than as a number they lose on.

**Also measure acknowledgement cost.** Share groups acknowledge per record; PC batches offset state
into an encoded commit. At high throughput that is a real difference in broker load, and it will not
show up in a consumer-side msg/s figure at all. Whatever else the arm measures, it should record
what the broker is doing.

## Related

`franz-go` shipped full share-group support in v1.21.0 and is **the only Go client with it** -
librdkafka has a C-only preview not recommended for production and no Go binding
([`next-franz-go-as-a-client-option.md`](next-franz-go-as-a-client-option.md)). If a Go-side share
arm is ever wanted, that is the client.
