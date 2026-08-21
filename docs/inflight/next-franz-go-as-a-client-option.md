# Next: franz-go as a client for the non-JVM proxies - what it buys and what it costs

<!-- inflight-type: task -->
<!-- inflight-impact: architecture -->

Opened 2026-08-21. **Prior art checked**: no existing inflight entry covers Go client selection; the
language-proxy fan-out (astubbs#242, `clients/*.md`) assumes a proxy to the JVM engine and does not
discuss which native client a proxy would use. This is the first note on the question.

## Why it is being asked now

A controlled benchmark, not a hunch. `bench/franz` drives **franz-go with no engine at all** and
`bench/` now has a matching **Java floor** (`pool`: plain `KafkaConsumer`, thread pool, semaphore,
sleep). Both floors, same broker, same dataset, all keys distinct
([`bench/results/high-concurrency-unordered.csv`](../../bench/results/high-concurrency-unordered.csv)):

| Delay / concurrency | Java floor | Go floor | Java as % of Go |
|---|---:|---:|---:|
| 0ms / 250 | 97,201 | 144,009 | **67%** |
| 2ms / 5,000 | 43,482 | 138,427 | **31%** |
| 100ms / 5,000 | 20,102 | 37,600 | **53%** |

**No engine on either side.** That gap is `kafka-clients` versus franz-go, and it is larger than
anything Parallel Consumer's engine contributes - PC sits within a few percent of the Java floor at
most points and beats it at two. See
[`bug-in-flight-ceiling-above-2000-concurrency.md`](bug-in-flight-ceiling-above-2000-concurrency.md).

**The Go floor also reaches a concurrency of 5,000 exactly, where both Java arms plateau near 2,800.**

## The capability case is stronger than expected - and share groups are the reason

**franz-go is the only Go client with KIP-932 share groups.** Kafka made them GA in 4.2.0 on
2026-02-17; franz-go shipped full support 63 days later in v1.21.0. librdkafka has a C-only preview
explicitly not recommended for production and **no Go binding**; Sarama has an unanswered feature
request from 2025.

**That matters to this project more than to most**, because share groups are Kafka's own answer to
part of the problem Parallel Consumer exists to solve, and a competitor's FAQ already routes unordered
workloads to them. Whatever the strategy there, **being able to reach them from a Go proxy is a
capability no other Go client offers at all.**

Other things it has that the Java client does not: **`kfake`**, an in-process protocol-level fake
broker with fault injection - materially better than `MockConsumer` and a serious saving on proxy test
cost; producer data-loss detection callbacks; lag-preferring fetch ordering; AWS MSK IAM SASL built
in; a schema-registry client.

Protocol coverage is close to literal: a ~200-row supported-KIP table from KIP-1 to KIP-1258, and -
unusually - an explicit *"KIPs intentionally not implemented"* section with per-item rationale, whose
exclusions are all Java-idiom or JMX-metric KIPs. Interceptors are present via hooks that **mutate**,
not merely observe, so that is genuine parity rather than a gap.

## The three risks, stated plainly

1. **The bus factor is exactly one.** Not approximately - **90.6% of all commits** are the
   maintainer's (85.5% over the last two years). Second place is 1.0%. **No CONTRIBUTING, no
   CODEOWNERS, no GOVERNANCE, no CODE_OF_CONDUCT.** Funding is one person's GitHub Sponsors.
   Copyright is held personally. He works at Redpanda and Redpanda is a listed adopter, but this is
   personal work under his own copyright and **no corporate maintenance commitment or succession plan
   was found, and it was looked for.**

   **The counterweight is real and should not be waved away**: hygiene beats every alternative -
   **2 open issues**, median time-to-close 9.3 days, against 203 open for confluent-kafka-go and 183
   for segmentio/kafka-go. Today the bus factor reads as a *benefit*: one fastidious expert with total
   context shipped share groups two months after Kafka did, while three better-resourced projects
   shipped nothing. **It is the same fact that becomes an unmaintained dependency the day he stops.**

2. **KIP-848 is gated behind judgement, not backlog.** The new consumer rebalance protocol has been
   implemented since May 2025 and is reachable only via an undocumented magic context string, because
   the maintainer distrusts the broker implementation - repeated heartbeats fencing members far more
   than they should - and says his KAFKA tickets go unanswered. His stated precondition (KIP-1251)
   shipped in Kafka 4.3.0, but the release acting on it has not. **If a supported product must offer
   KIP-848 within the year, that is a dependency on one person changing his mind**, and today only the
   cgo-bound confluent-kafka-go can serve it.

3. **Kafka 4.3 support is written but unshipped.** The PR has sat clean and untouched since
   2026-07-02 - **91 days past Kafka 4.3 GA and counting, the longest lag in a two-year sample** where
   the historical range is 44-89 days and had been *shrinking*. Whether that is bandwidth pressure or
   a deliberate hold is not established.

## Licence: the one axis where it is weaker

**BSD-3-Clause, no CLA, and no express patent grant.** `kafka-clients` is Apache-2.0, which carries an
explicit patent grant and a retaliation clause. Permissiveness is fine either way and neither is
copyleft, so this is not an adoption blocker - but for a project whose own licence posture is a stated
competitive asset ([`next-licensing-strategy.md`](next-licensing-strategy.md)), it is worth knowing
that this dependency is the weaker of the two on patents. The absent CLA also means relicensing would
require tracing 126 contributors, which is a fork-viability question, not ours.

## The alternatives, briefly - and there is no conventional choice

- **confluent-kafka-go** - the only Go client with **KIP-848 GA**, corporately funded, ~6-week
  cadence, commercial support available. Costs: cgo, so `CGO_ENABLED=0` is impossible and truly static
  glibc builds are not available; **no prebuilt binary has GSSAPI/Kerberos**; musl and
  cross-compilation both need special handling. No Go share-group binding.
- **Sarama** - most stars, clean tracker, energetic in 2026, but **also bus factor ~1**, no KIP-848
  (the API key is a commented-out line), and no share groups. It shipped a cooperative-sticky assignor
  in July 2026, i.e. it is catching up to a 2019 KIP.
- **segmentio/kafka-go** - most-imported and **effectively dead**: 2 commits in 12 months, 183 open
  issues, and a Segment employee on the record recommending franz-go instead.

**Installed base and project health point in opposite directions.** Trailing-year commits: franz-go
821, Sarama 381, confluent-kafka-go 126, kafka-go 2.

## What this does and does not argue for

- **It does not argue for rewriting Parallel Consumer's engine.** The measurements say the engine is
  close to free; the client is the large term. A rewrite aimed at the engine would target the smallest
  available saving, which is the same mistake as competing on engine microseconds
  ([`market-analysis-llingr.md`](market-analysis-llingr.md) section 5a).
- **It does argue that a native Go path is worth more than a proxy to the JVM**, for Go users
  specifically, on both throughput and reachable concurrency - and that share groups and `kfake` make
  the case on capability as well as speed.
- **The realistic hedge is a thin internal abstraction over the client**, so a later swap to
  confluent-kafka-go is bounded rather than a rewrite. Cheap to build up front, expensive to retrofit.
  Sponsoring is also cheap relative to the exposure.

## Open questions

1. **Does a Go proxy use franz-go natively, or continue to proxy the JVM engine?** That is the
   language-proxy architecture question (astubbs#242) and this note only supplies inputs to it.
2. **What is the project's position on share groups generally?** They overlap with what Parallel
   Consumer does, they are now GA, and no note here states a position. That is a strategy gap
   independent of Go.
3. **Would virtual threads close any of the Java-versus-Go floor gap?** Untested, and it is the
   pending experiment in the ceiling note. If it does, the urgency of a native Go client drops.
