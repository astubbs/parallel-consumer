# RocksDB works on the JVM sidecar, first try - the blocker we recorded is stale

<!-- inflight-type: register -->

Spiked 2026-08-24, ten minutes, reverted immediately. Recorded because it **invalidates a stated
blocker**, and without this note the next person re-derives it from the same stale evidence.

## What was believed

KTD1 of the Kafka Streams PoC plan chose `Stores.inMemoryKeyValueStore` over RocksDB, citing "a
JNI-backed native library whose per-platform problems are documented and land on this project's own
platform: the container native build hardcodes a linux64 variant, the artifact ships Linux binaries
only, and an older release was not compiled for macOS arm64 at all."

## What is true

**`rocksdbjni-7.9.2`, the version already on the streams module's classpath, ships
`librocksdbjni-osx-arm64.jnilib`.** It also ships osx-x86_64, linux aarch64/ppc64le/s390x, musl
variants and win64. The "Linux binaries only" and "not compiled for macOS arm64" claims were true of
an older release and are not true of the one in use.

Flipping the store to `Stores.persistentKeyValueStore` and running the demo on macOS arm64:
**1000 keys counted exactly, exit 0, consumer group stable.** No configuration, no flags, no
troubleshooting.

**Verified it actually ran rather than silently falling back** - which matters, because a fallback
would look identical from the demo's output. Kafka Streams wrote
`kafka-streams/<application-id>/0_1/rocksdb/counts-store` directories on disk, and an in-memory
store writes none. That is the same observable the existing `theAggregationLeavesNoRocksDbBehind`
test asserts the absence of.

## What this does and does not change

**KTD1's decision was still right, and it is not being reversed here.** Keeping RocksDB out isolated
the PoC from a native-library question while the model itself was unproven, and that was the correct
order. What changed is the *cost of reversing it*, which is now approximately zero on the JVM
sidecar.

**The unknown moves, it does not disappear, and it is bigger than RocksDB.** The remaining question
is RocksDB - or Kafka Streams at all - **under a native image**, and that is further away than it
looks:

- The native-image build in [`perf-native-image-sidecar-works.md`](perf-native-image-sidecar-works.md)
  was `parallel-consumer-proxy`, **not** the streams module, which has no native profile at all. So
  Kafka Streams has never been in a native image, with or without RocksDB.
- That note is candid that its reachability config came from a run of 20 records with no failures,
  no retries, no rebalance and no transactional commit, and that missing paths "appear the first
  time a record fails in production".

## The question to settle before spending anything on that

**Does the Streams wrapper need to be a native image at all?** The native-image case is a fast start
and no JDK on the user's machine. Startup barely matters for a long-running stateful service, and
the JDK-installation argument has alternatives - a jlink'd runtime, a bundled JRE, a container -
that do not require getting Kafka Streams *and* a JNI native library through GraalVM's reachability
analysis.

If the answer is "a JVM sidecar is fine for Streams", RocksDB stops being a blocker and becomes a
packaging preference. **Cheaper to answer than to build against, and it reorders everything
downstream of it.** Nobody has asked it.

## Prior art

- [`next-kafka-streams-foreign-wrappers.md`](next-kafka-streams-foreign-wrappers.md) - the PoC's findings, whose "RocksDB under a native image" line remains accurate
- [`perf-native-image-sidecar-works.md`](perf-native-image-sidecar-works.md) - what the native build actually covered, and what it did not
