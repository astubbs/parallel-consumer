# Feature data, point by point, against llingr

<!-- inflight-type: register -->

Every record in `docs/features/` (36 published features, one deprecated) cross-referenced against
llingr. Companion to [`market-analysis-llingr.md`](market-analysis-llingr.md), which holds the
narrative; this is the exhaustive pass so nothing is skipped by being uninteresting.

**Reading the verdict column.** `PC only` and `llingr only` mean the capability was confirmed present
on one side and confirmed or strongly evidenced absent on the other. `Both` means a genuine
equivalent exists, not necessarily an identical design. **`Unverified`** means our side is known and
llingr's is not established either way - those are the rows to check before anything is published.

llingr facts come from llingr's site, llingr's public Go and Rust repositories, and llingr's GitHub org
listing, verified 2026-08-21. llingr's JVM build is not inspectable, so JVM-specific claims are llingr's own.

| PC feature | llingr equivalent | Verdict |
|---|---|---|
| `parallel-processing` - concurrency beyond partition count | Yes. `ConcurrentKeys`, default 250, max 5,000 | **Both** - the shared core claim |
| `head-of-line-avoidance` | Yes, at dispatch. **But reintroduced at the commit layer**: a contiguous-commit design stalls behind one slow key | **Both, PC deeper** |
| `offset-map-acknowledgement` - per-record ack, offset-map commits | **None.** Commits the contiguous-commit design only; out-of-order completions accumulate in an **unbounded** in-memory slice | **PC only - the differentiator** |
| `ordering-modes` - partition, key, unordered | **Key only.** 13 enumerated config parameters, none an ordering selector; FAQ sends unordered workloads to Share Groups | **PC only** |
| `batch-processing` | **None.** Every handler signature in Go, Rust and JVM takes one message | **PC only** |
| `bulk-kafka-transactions` | **None**, and structurally impossible - no produce path. Transactions explicitly declined as "possible, but costly" | **PC only** |
| `result-models` - void, produce, streaming | Void only. `BrokerPort` is `Poll` + `CommitOffsets`; no produce operation exists | **PC only** |
| `commit-modes` - async, sync, transactional | One mode: engine-owned periodic contiguous commit | **PC only** |
| `transactional-lock-timeouts` | N/A - no transactions | **PC only** |
| `invalid-offset-metadata-policy` | N/A - **llingr writes no offset metadata**, so the failure mode cannot arise. A cost of our design, not a gap in llingr | **PC only, by consequence** |
| `custom-retry-delay` | **None.** No retry policy at all - a handler error goes straight to dead-letter | **PC only** |
| `controlled-retry-exception` - signal a retriable failure | **None**, same reason | **PC only** |
| `static-retry-delay` (deprecated here) | None | **PC only** (deprecated) |
| `vertx-integration` | None | **PC only** |
| `reactor-integration` | None | **PC only** |
| `mutiny-integration` | None | **PC only** |
| `managed-executor-service` - JEE managed executors | None. Goroutines, or Kotlin coroutines on the JVM | **PC only** |
| `managed-thread-factory` | None, same reason | **PC only** |
| `reflective-access-escape-hatch` | N/A - a PC-specific workaround for a PC-specific check | **PC only, by consequence** |
| `micrometer-metrics` | Pluggable `MetricsSink`, with a bundled Prometheus implementation. Micrometer is the more portable JVM abstraction; llingr's has richer *content* | **Both, different axis** |
| `client-side-work-queue` | Yes. Per-key buffered channels, `PerKeyBufferLen` default 16, max 64 | **Both** |
| `message-buffer-configuration` | Yes, and more granular: `PerKeyBufferLen`, `CommitIngestChannelLen`, `CommitPartitionSliceLen`. **But `messageBufferSize` is inert on our external engines** - see the throughput note | **Both** |
| `commit-interval` | Yes - `AutoCommitInterval` | **Both** |
| `draining-shutdown` | Yes - `DrainTimeout`, a drain coordinator, and `NotifyShutdown` in the relay protocol | **Both** |
| `operation-timeouts` | Yes - `QueryTimeout`, `PollTimeout`, `AcquireCommitGuardTimeout` and two circuit-breaker timeouts | **Both** |
| `backpressure-and-broker-liveness` | Partial. Backpressure is `ConcurrentKeys`; liveness is circuit breakers plus relay heartbeats. **No adaptive element** | **Both, PC has more machinery** |
| `load-factor-tuning` | **None** - `ConcurrentKeys` is fixed. Ours is adaptive in principle, though measured inert on external engines and reported pegged at 100/100 (astubbs#155) | **PC only, and ours is broken** |
| `last-failure-reason` | Partial. The reason reaches the dead-letter handler; there is no in-context failure history for a retry, because there are no retries | **Both, different shape** |
| `slow-record-warning` | Partial. A `process_duration` histogram and a worker circuit breaker, rather than a warning threshold | **Both, different shape** |
| `poll-context` - the processing API | `Message<T>` envelope with `WithEnrichContext` / `WithExtractEnvelope` | **Both** |
| `core-dependency-footprint` | Go engine: one dependency (llingr's own contracts module). JVM: kafka-clients, kotlinx-coroutines, slf4j. **Theirs is leaner** | **Both, llingr ahead** |
| `java-compatibility` - per-module Java baselines | **JDK 21+ only.** PC still targets Java 8 bytecode - wider reach, older floor | **Both, opposite trade** |
| `fair-partition-traversal` | **No starvation prevention exists** - fairness is incidental, falling out of single-threaded FIFO admission. Worse: **a concurrency token is acquired *before* routing**, so once all workers are live the polling loop stalls behind the oldest undispatched message regardless of its key - coarse head-of-line blocking, ending in emergency shutdown after the acquire timeout | **PC only** (confirmed from source) |
| `pause-and-resume` - global pause/resume | **None.** `BrokerPort` is a closed, enumerated interface - `Subscribe`, `Unsubscribe`, `Poll`, `ExtractEnvelope`, `CommitOffsets`, `AckRebalance`, `BrokerQuery`, `ConsumerGroup` - with no pause, resume or seek | **PC only** (confirmed from source) |
| `compacted-topic-offset-recovery` | They list "log compaction gaps" among tested Kafka edge cases, but describe no recovery mechanism | **Unverified** |
| `sasl-authentication-retry` | **None.** No auth handling anywhere in engine or adapters; a poll error is logged and the loop continues, so an unrecoverable SASL failure becomes an unbounded hot error-log loop rather than a fail-fast. OAUTHBEARER refresh is a callback whose retry timer belongs to librdkafka | **PC only** (confirmed from source) |

## The tally

**Nineteen PC-only, three of which exist only because of choices llingr made** (no offset metadata, no
transactions, no reflective check). So **sixteen substantive**, and these cluster in three groups:
retry behaviour, output/produce models, and reactive-framework integrations.

**Zero llingr-only rows in this table** - because the table is generated from *our* feature data. That
is a real limitation and not a result: llingr's DLQ, formal verification, relay heartbeat, per-message
overhead figure and snapshot endpoint have no PC feature record to hang from. The honest gap list is
in [`market-analysis-llingr.md`](market-analysis-llingr.md); this table cannot produce it by
construction.

**All four previously-unverified rows are now settled from source** (2026-08-21): pause/resume,
fair traversal, compacted-topic recovery and SASL retry. Three resolved in PC's favour; **compaction
handling resolved in llingr's, and is genuinely well done** - the note says so, because a comparison
that never concedes anything is not usable.

## Access to the underlying client, and to an admin API

Read from source, 2026-08-21.

**llingr:**

- **confluent-kafka-go adapter: a real accessor exists.**
  `func (a *Adapter) ConfluentConsumer() *kafka.Consumer` - documented *"for advanced use cases such
  as querying metadata"*, and integration-tested against a live broker.
- **franz-go adapter: no accessor at all.** Every exported method was enumerated; the `*kgo.Client`
  is an unexported field with no getter. The only route is choosing `NewCustom()` at construction and
  keeping your own reference - **a one-way door decided before the consumer exists.**
- **Rust binding: no access.** The engine is a statically linked Go blob behind an FFI ABI; broker
  config is a curated typed options builder that rejects unknown keys.
- **No admin surface of any kind.** No topic creation, no `DescribeGroups`, no `ListOffsets`, no
  partition listing, no seek, no lag query.
- **`BrokerQuery` is dead surface.** One query type is defined (`CommittedOffsets`); **both shipped
  adapters return an empty response as a documented no-op**, and whole-repo grep shows **the engine
  never calls it** - it is threaded into `subscription.New(...)`, stored as a field, and has no call
  site. Wired, reserved, unused.
- **Configuration is enforced by panic, not documentation.** franz must have `DisableAutoCommit`,
  `BlockRebalanceOnPoll` and the three rebalance callbacks; confluent must have
  `enable.auto.commit=false`, `enable.auto.offset.store=false`, a non-`read_uncommitted`
  `isolation.level`, and `go.events.channel.enable=false`. Violations panic at wiring time.

**PC, by contrast: the user always holds the client.** `ParallelConsumerOptions` requires the caller
to construct and supply the `Consumer` (and `Producer`), so the reference is never surrendered and
there is no equivalent of llingr's franz one-way door. **This is an under-appreciated PC advantage**
and it is not in the feature data - there is no feature record for it. Whether unrestricted access to
a client the engine is actively polling is *safe* is a separate question, and one PC documents less
forcefully than llingr enforces it.

**Neither exposes an admin client.** For topic creation or group description, both leave the user to
build their own `AdminClient`. So this is not a differentiator today - but it is a recurring user ask,
and llingr's `BrokerQuery` is a placeholder for exactly that surface, currently doing nothing.

## Multi-topic - correcting our own premise

**PC already supports multiple topics.** `ParallelConsumer` exposes
`subscribe(Collection<String>)`, `subscribe(Pattern)`, and both with a `ConsumerRebalanceListener`.
Pattern subscription included. An earlier belief that PC is single-topic is wrong.

**llingr is genuinely single-topic, and it is a hard architectural constraint rather than a
convention.** The engine actively rejects a foreign topic and trips the circuit breaker:

> `"rebalance rejected: topic %q does not match this consumer's topic %q - a consumer serves exactly
> one topic (offset tracking is keyed by partition alone, so a second topic would cross-contaminate
> committed offsets); use one consumer per topic"`

with three dedicated tests. **The root cause is the interesting part: llingr's offset trackers are
keyed by partition number alone**, so the topic is not part of the key, and `Message[T]` carries **no
topic field at all**. PC keys by `TopicPartition` and so has never had this constraint - a design
decision that quietly bought multi-topic support.

**What neither does is route different topics to different functions.** PC runs one user function
across every subscribed topic; llingr runs one handler per consumer, and a second topic means a
second consumer. So "multi-function to multi-topic" - per-topic handlers, or any cross-topic
join or correlation - is absent from both, and is a genuine open design space rather than a gap to
close. There is no feature record and no roadmap entry for it here.

Note one thing llingr has in that space which we do not: **`WithOverflowGuard`, a shared burst-capacity
channel between consumer instances in the same process** - so when you *do* run several consumers for
several topics, they can share a concurrency budget. Go-only. If PC ever grows per-topic handlers, a
shared budget across them is the same problem, and worth knowing the shape of an existing answer.

Two caveats on that answer, both from source. The mechanism is a plain `select` race with **no
priority and no fairness** - llingr's FAQ calls it *"an overflow channel, not a scheduler"* - so it
caps what a runaway topic can take without guaranteeing anyone else gets served. And running several
consumers in one process carries a footgun: **the default emergency-shutdown handler sends
`os.Interrupt` to the whole process**, so one consumer's circuit breaker takes down every other
consumer unless a shutdown callback is registered on each.

Also worth recording as a support signal: **llingr's GitHub org has zero issues, open or closed.**
There is no public tracker, so there is no way for a user to see known problems or for intent to be
stated in public.
