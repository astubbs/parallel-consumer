# Next: brokers other than Kafka

<!-- inflight-type: feature -->
<!-- inflight-impact: coordination -->

**Position: not any time soon, and the research supports that rather than merely asserting it.**
Opened 2026-08-21 after a competitor comparison found llingr treats the broker as pluggable from the
start, which raised the question of whether PC should.

PC is Kafka-only by construction: `ParallelConsumerOptions` takes an `org.apache.kafka.clients`
`Consumer` and `Producer`, and the offset model is Kafka's. Nothing here proposes changing that. This
note exists so the question is answered once, with evidence, instead of being reopened.

## What llingr does, for contrast

llingr's engine talks to a `BrokerPort` interface - `Subscribe`, `Unsubscribe`, `Poll`,
`ExtractEnvelope`, `CommitOffsets`, `AckRebalance`, `BrokerQuery`, `ConsumerGroup` - described as
suiting *"Kafka, Pulsar, NATS JetStream, or any system with partition/offset semantics"*.

**But only Kafka adapters exist.** `llingr-adapter-nats` and `llingr-adapter-pulsar` are marked
**planned** in the docs and neither repository exists in the GitHub org. So the abstraction is real
and the portability is aspirational - the same position PC would be in on day one of such a project.

Worth noting what that abstraction cost llingr: the engine's offset trackers are keyed by **partition
number alone**, with no topic in the key, which is precisely why it is limited to one topic per
consumer. A broker-agnostic port encouraged a lowest-common-denominator identity.

## NATS JetStream, researched 2026-08-21

The most plausible candidate, and the answer is clearer than expected.

**What it is.** A Go messaging system - one static binary, no JVM, no ZooKeeper - where JetStream is
the persistence layer (GA 2021) adding streams, durable consumers, replay, plus KV and object stores.
CNCF **incubating** since 2018, Apache-2.0, maintained by **Synadia**.

**Governance caveat worth carrying.** In April 2025 Synadia moved to withdraw NATS from the CNCF,
reclaim the donated trademark and relicense the server under the Business Source Licence. CNCF
resisted and prepared to fork; it settled on 1 May 2025 with trademarks assigned to the Linux
Foundation and NATS remaining Apache-2.0. Resolved, but it is a single-vendor-capture risk that
already materialised once - relevant to any decision to build on it, and relevant to
[`next-licensing-strategy.md`](next-licensing-strategy.md) as a worked example of a BSL move.

**Independent scrutiny.** Jepsen analysed JetStream 2.12.1 in December 2025, unpaid and
uncommissioned, and found real safety problems: default fsync every two minutes with immediate
acknowledgement, loss on minority-node corruption or truncation, and split-brain with acknowledged-
message loss. Several were unfixed at publication. Being Jepsen-tested is a maturity signal; the
findings are a caution.

### The load-bearing finding: half of PC's value does not transfer

- **Unordered parallelism is already native and uncapped.** Any number of workers can bind to one
  pull consumer and share its cursor. There is no partition-count ceiling, so the "I need more
  concurrency than partitions allow and I do not need ordering" case - a large part of PC's
  `UNORDERED` demand - **has no problem to solve on NATS.**
- **Key-ordered parallelism does hit the same wall**, and worse: the native serial mechanism is
  `MaxAckPending=1`, which serialises everything matching the consumer filter rather than per key.
- **But that gap is already filled by the vendor.** Synadia's `pcgroups` (Orbit collection) combines
  deterministic subject partitioning with `pinned_client` priority groups to give per-key ordered
  parallelism, **shipping in Go, Rust, JavaScript and Java**. Its own docs state the ceiling -
  *"effective parallelism is `min(partitions, members)`"* - and that pinning is *"affinity plus
  failover, not a distributed lock"*, so handlers must be idempotent.

### Market size, which is the decisive argument

A NATS port from this project would be a **JVM** library. NATS users are overwhelmingly not JVM users:
`nats.java` has roughly 675 GitHub stars against 6,727 for `nats.go`, and Synadia's own `orbit.java`
has 8 against 91 for `orbit.go`. Stack Overflow shows ~358 NATS questions all-time against ~33,000
for Kafka.

So the addressable set is: NATS users, who are also JVM users, who need *key-ordered* concurrency,
who are not adequately served by the vendor's own library. That is a small intersection of two small
sets, and the niche is occupied - though not well defended, since Synadia hints the functionality may
move server-side in a later release.

**Trend, for completeness:** NATS is growing in *relative* mindshare - its share of Kafka's Stack
Overflow volume rose from ~0.6% in 2019 to ~4.3% in 2025 - but from a small base, and the recent
absolute numbers are too small to carry weight.

## Other candidates, briefly

- **Pulsar** - has its own subscription types (`Key_Shared` gives key-ordered parallelism natively),
  so the problem PC solves is again largely absent. Flat-to-declining on the same relative measure.
- **RabbitMQ / SQS** - queue semantics, not a partitioned log. No partition ceiling to escape.
- **Kafka-API-compatible brokers (Redpanda, WarpStream, Confluent Cloud)** - these are not "other
  brokers" at all. They speak the Kafka protocol, so PC already works against them. **This is the
  cheap win in this space** and it is a documentation and testing question rather than an
  engineering one: state which are known to work, and test at least one in CI.

## The restart trigger

Reopen if any of these change: a Kafka-API-compatible broker becomes a large enough share of users to
justify explicit testing (already the cheapest action here); NATS's JVM community grows materially;
Synadia deprecates or fails to maintain `pcgroups`; or a user with a real workload asks. Absent
those, the answer stands.

## Related

- [`market-analysis-llingr.md`](market-analysis-llingr.md) - where the question came from.
- [`next-architecture-landscape-comparison.md`](next-architecture-landscape-comparison.md) - the
  standing comparison document; Kafka Share Groups (KIP-932) is covered there as the nearest
  competitor for the same use case.
- [`next-licensing-strategy.md`](next-licensing-strategy.md) - the NATS/CNCF/BSL episode is a live
  case study for that decision.
