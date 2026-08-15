<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# The engine - the shipped library, and the module every other one is built on

Parallel Consumer itself: it holds the Kafka connection, hands records to your function on a worker
pool, and commits only what has actually completed. `vertx`, `reactor` and `mutiny` each extend it;
the proxy sidecar wraps it. Nothing else in the tree replaces it.

**The repo-root [`README.adoc`](../README.adoc) owns the user-facing introduction** - ordering
guarantees, retries, commit modes, shutdown modes, the metrics catalogue, and the tables that pick a
module for you (`java-version-per-module`, `Module maturity`). This file does not restate any of it;
it is the map you want when you are about to open the source. Note that `README.adoc` is generated -
the editable original is `src/docs/README_TEMPLATE.adoc`, and its own banner says so.

## What is in it

- **`bz.stub.parallelconsumer`** - the public API. `ParallelStreamProcessor` (`poll`,
  `pollAndProduce`, `pollAndProduceMany`), `ParallelConsumerOptions` (the builder: `ordering`,
  `commitMode`, `maxConcurrency`, `batchSize`, `retryDelayProvider`), `PollContext`/`RecordContext`
  (what a user function is handed, including `getNumberOfFailedAttempts`), and
  `PCRetriableException` for a retry that does not log at ERROR.
- **`internal/`** - the threading. `AbstractParallelEoSStreamProcessor` is the control loop;
  `BrokerPollSystem` is the broker poller. Alongside them: `ProducerManager` (the produce/commit
  lock pair), `ConsumerOffsetCommitter`, `DynamicLoadFactor` (how far ahead of the pool records are
  pre-fetched), and `PCModule` - a hand-rolled dependency-injection container, "modelled on how
  Dagger works" and deliberately not Dagger.
- **`internal/ExternalEngine`** - the seam the three integration modules extend. It overrides
  `setupWorkerPool` down to a single dispatch thread and `getTargetOutForProcessing` to stop
  pipelining, because their concurrency is the async engine's, not a pool's. It also **rejects
  transactional commit mode outright**, which is why none of those three modules offers
  exactly-once.
- **`state/`** - work management. `WorkManager` over `ShardManager` (the shards, keyed by
  `ShardKey`), `PartitionStateManager`/`PartitionState` (what each assigned partition has completed,
  and what it may commit), `WorkContainer` (one record's attempt count and retry-due time), and
  `RetryQueue` - a purpose-built sorted set whose uniqueness key (topic/partition/offset) is
  deliberately different from its sort key (retry-due time first).
- **`offsets/`** - the encoding written into commit metadata. `OffsetMapCodecManager` runs every
  candidate through `OffsetSimultaneousEncoder` and takes `packSmallest`; `BitSetEncoder` and
  `RunLengthEncoder` are the two families, each in a plain and a zstd-compressed form, and
  `OffsetEncoding` maps every variant to its magic byte - including the two `KafkaStreams` values,
  which exist only to recognise metadata Kafka Streams wrote into the same field and refuse it
  cleanly, via `KafkaStreamsEncodingNotSupported`.
- **`metrics/`** - `PCMetrics` and `PCMetricsDef`, the Micrometer meters. The meter-by-meter
  catalogue is in the root `README.adoc` under `== Metrics`.

## Three things to know before reading the code

- **The poller and the control loop are separate threads, and the distinction is load-bearing.** A
  stalled controller and a stalled poller are different failures with different symptoms, and the
  poller is also what keeps group membership alive while the controller is busy. `BrokerPollSystem`
  and `AbstractParallelEoSStreamProcessor` share `WorkManager` state across that boundary, which is
  why so much of `state/` is explicitly documented as thread-safe.
  [`CONCEPTS.md`](../CONCEPTS.md) **owns this vocabulary** - control loop, broker poller, shard,
  in-flight work, commit frontier, the produce/commit lock pair - and any code comment that seems to
  disagree with it should be read against it first.

- **What gets committed is the highest *sequentially* succeeded offset; everything completed above
  it lives in the commit metadata.** That is the whole reason `offsets/` exists, and it is bounded -
  `DefaultMaxMetadataSize` mirrors the broker's 4096-byte limit, so the encoding can genuinely run
  out of room. The mechanism and what it does and does not guarantee are described in `README.adoc`
  at the `[[offset_map]]` anchor, and recorded as a capability in
  [`docs/features/offset-map-acknowledgement.yaml`](../docs/features/offset-map-acknowledgement.yaml).
  Do not re-derive either here.

- **Ordering decides two separate things, and reading it as one is the commonest mistake.** First,
  *which shard a record lands in* - `ShardKey.of` switches on `ProcessingOrder`, so `KEY` shards by
  the record's key while `PARTITION` and `UNORDERED` both shard by topic-partition. Second, and
  independently, *whether a shard hands out a second record before the first resolves* -
  `ProcessingShard.isOrderRestricted`, which is false only for `UNORDERED`.

  That second axis is why **`UNORDERED` is not "`PARTITION` with a different name" even though the
  two compute the same shard key**, and why its relationship to partition count is the awkward one:

  | Mode | Shard per | Concurrency ceiling |
  |---|---|---|
  | `KEY` | key, per *topic* | one in flight per distinct key |
  | `PARTITION` | topic-partition | **the number of assigned partitions** |
  | `UNORDERED` | topic-partition | **the executor pool - shard count does not bound it** |

  So under `UNORDERED` the shard count still equals the partition count while concurrency is not
  bounded by it at all. The shards remain the unit of *accounting*, not of *parallelism*.

  A second surprise on the first axis: key-ordered shards key on the **topic name**, not the
  topic-partition, so the same key arriving on two assigned partitions still runs in one shard.

## Where the topics are really owned

| Topic | Owner |
|---|---|
| User-facing introduction, ordering, retries, commit modes, metrics catalogue | repo-root `README.adoc` (edit `src/docs/README_TEMPLATE.adoc`) |
| Per-capability records - maturity, limits, when to use | [`docs/features/`](../docs/features/), one YAML per capability, most of them `module: parallel-consumer-core` |
| Domain vocabulary | [`CONCEPTS.md`](../CONCEPTS.md) |
| Suites, quarantine lane, chaos suite, the ambient probe, shared test utilities | [`docs/testing.md`](../docs/testing.md) |
| Deferred internal refactors and the `TODO`/`FIXME` triage | [`docs/refactoring.md`](../docs/refactoring.md) |

## Two build facts that catch people out

- **This module's `src/test/java` and `src/test-integration/java` ship as a `tests`-classifier
  artifact, and eight other modules depend on it** - the three integration modules and all five
  examples. `LongPollingMockConsumer`, `KafkaTestUtils`, `BrokerIntegrationTest` and the rest of the
  shared harness live here, so a change to them is a change to eight other modules' tests. Search
  before adding a helper; [`docs/testing.md`](../docs/testing.md) owns where they live.
- **Test parallelism is configured per module, in the pom, on purpose.** The
  `junit.parallelism.configuration.parameters` property is handed to both surefire and failsafe by
  this pom; the comment above it records why it may not go back into a packaged
  `junit-platform.properties` - that file sits at the root of the tests jar and silently overrode
  all eight downstream modules.
