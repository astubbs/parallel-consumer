# Kafka Streams under GraalVM native-image - the ladder's named companion gap

<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->

**Branch: `perf/242-crossing-cost-ladder`.**
[`branch-crossing-cost-ladder.md`](branch-crossing-cost-ladder.md) **owns why this probe exists**:
the ladder measures call mechanics under libjvm-or-native assumptions, and it names this as the gap
that a green ladder does not close - *Kafka Streams has never run under GraalVM here*. Two routes
need one of them proven: (1) native-image including Kafka Streams, (2) libjvm embedding. This note
settles route (1) for the PoC surface only.

**Scope choice, stated up front because it changes what the result licenses.**
`parallel-consumer-proxy-streams` uses **in-memory stores only** -
`TopologyAssembler` builds every materialisation with `Stores.inMemoryKeyValueStore`. So the classic
RocksDB-JNI blocker is out of scope by construction, not by being solved. **RocksDB remains an
unprobed cliff for any durable-state future** - see
[`core-rocksdb-works-on-the-jvm-sidecar.md`](core-rocksdb-works-on-the-jvm-sidecar.md).

## Pre-registered predictions, written before the first build

Recorded 2026-08-25, before any native-image invocation. House falsification style: the point is
that the misses are visible afterwards, not that the guesses were good.

| # | Prediction | Confidence |
|---|---|---|
| P1 | The image **builds**. In-memory stores mean no `org.rocksdb` native library is needed at run time, and reachability of `RocksDBStore` costs size, not a build failure | high |
| P2 | The build **needs a new metadata capture**. The sidecar's shipped `META-INF/native-image/reachability-metadata.json` traced a PC-core session and knows nothing about Streams | high |
| P3 | The wall, if there is one, is at **run time inside `new KafkaStreams(...)` or `streams.start()`**, not at build time - Streams resolves serdes, the timestamp extractor, both exception handlers, the partition assignor and the client supplier **from configuration strings**, exactly the shape that broke the sidecar's `Configure` | high |
| P4 | **Logging is the build-time obstacle again**, not Netty or Kafka - the sidecar's five-attempt log says every blocker was logback/SAX, and gRPC-netty-shaded raised nothing | medium |
| P5 | **GraalVM 25 may reject the inherited `--initialize-at-build-time=ch.qos.logback,...` list.** That recipe was captured on Oracle GraalVM 23; strict image heap is the default from 24 onward, which is the same mechanism that produced attempt 4's `LocatorImpl` in the image heap | medium |
| P6 | **No dynamic-proxy blocker.** Kafka Streams' hot path is plain classes; the JMX/metrics surface registers concrete MBeans rather than `Proxy` instances | medium |
| P7 | Binary **110-150MB** (sidecar was 79MB; kafka-streams plus its state-store and assignor machinery is the delta), build **2-5 minutes** on this box | low |
| P8 | Startup to the `port:` line **under 200ms native vs 1-3s JVM** - the same order the sidecar showed, and the one number this probe can quote honestly at 200 records | medium |
| P9 | The demo's **assertions pass unchanged if it starts at all** - once the topology is assembled the record path is bytes in, bytes out over gRPC, and nothing on it is reflective | medium |
| P10 | **`num.stream.threads` and the state-directory lock are not a problem.** Streams' `StateDirectory` uses ordinary file locks, which Substrate supports | medium |

**The prediction I most expect to be wrong** is P3's *precision*: I expect a runtime failure, but I
expect to be wrong about *which* class it names first, and the tracing agent - not reading - is what
finds it. That is the sidecar note's transferable lesson, and it is why the trace has to run over a
**real** demo session rather than a start-and-stop.

**What would falsify the whole route** (as opposed to costing another metadata entry): a Streams
internal that needs a class name computed at run time from something the trace cannot enumerate, or
a build-time initialisation cycle that neither `--initialize-at-run-time` nor a metadata entry can
break. Anything fixable by adding entries is *cost*, not a wall.

## Result: Kafka Streams RUNS under native-image, and the Python demo passed against it

Measured 2026-08-25 on Linux/x86-64, GraalVM CE 25.0.2, box under ordinary load (`load average`
7-12 of 32 cores). **The existing Python Streams demo, unchanged in what it asserts, passed against
a native executable with no JVM in it:**

```
Keys expected           400
Keys matching exactly   400
Python invocations      400
Consumer group          STABLE/1 for all 3 samples after joining
OK - 400 keys counted correctly by a topology described entirely from Python
```

`ldd` on the binary lists `libz`, `libc` and the loader - **no `libjvm`, no `libjava`**. Combined
with `--no-fallback`, that is the check that this is a real image rather than a fallback that still
needs a JVM.

| | JVM engine (Temurin 17) | native engine | |
|---|---|---|---|
| startup to the `port:` line | 317 / 336 / 330ms | **14 / 16 / 14ms** | ~22x, 3 runs each |
| demo end to end, 400 records | 2.0s | 1.4s | startup-dominated |
| artifact | classpath of 44 jars | **78MB** executable | |
| build | - | 53s | |

**No throughput claim is available from this.** 400 records is startup-dominated, the box was
shared, and the per-invocation figures (1662us JVM against 1394us native) differ by less than the
run-to-run spread on a loaded box. The startup number is the only one worth quoting, and it is the
one that matters for an embedded engine anyway.

## The wall, and it was exactly where the sidecar's was

The **first** build - no traced metadata, only what ships on the classpath - **built fine in 41s
(41.7MB)**, started, bound gRPC, and **assembled the topology correctly** (the demo printed the
`Topologies:` description the engine produced). It then died the moment the topology was started:

```
Exception in thread "grpc-default-executor-0" java.lang.ExceptionInInitializerError
    at org.apache.kafka.streams.KafkaStreams.<init>(KafkaStreams.java:833)
Caused by: org.apache.kafka.common.config.ConfigException: Invalid value
    org.apache.kafka.streams.errors.LogAndFailExceptionHandler for configuration
    default.deserialization.exception.handler: Class ... could not be found.
    at org.apache.kafka.common.config.ConfigDef.parseType(ConfigDef.java:778)
    at org.apache.kafka.streams.StreamsConfig.<clinit>(StreamsConfig.java:921)
```

**The failing frame is `StreamsConfig.<clinit>`, and that is the transferable part.** It is not a
serde resolved from a user's configuration - it is Kafka Streams' own `ConfigDef` **defaults**,
which are class *names* validated by loading them while the config class initialises. So the very
first thing Streams does resolves a dozen classes by string, and closed-world analysis sees none of
them. Same mechanism as the sidecar's `Configure` failure, one layer earlier.

Fixed by one traced capture and one rebuild - **no flag changes, no `--initialize-at-run-time`, no
substitutions.**

## The recipe that worked

```
native-image --no-fallback -cp <streams classes>:<44 runtime jars> \
  --initialize-at-build-time=ch.qos.logback,org.slf4j,org.xml.sax,com.sun.org.apache.xerces,javax.xml \
  -H:ConfigurationFileDirectories=<traced config> \
  bz.stub.parallelconsumer.streams.StreamsMain
```

Two scripts carry it, and neither invents a build system - both wrap what the sidecar and the
`--shared` build already do:

- **`ffi/crossing-ladder/build-streams-native.sh`** - the build. The `--initialize-at-build-time`
  list is inherited verbatim from the sidecar's recipe.
- **`ffi/crossing-ladder/trace-streams-engine.sh`** - a `java` stand-in that runs the engine under
  the tracing agent, reached through the demo's own `PC_DEMO_JAVA`. `config-merge-dir`, so several
  traced sessions accumulate into one config.

The capture it produced is kept at
`ffi/crossing-ladder/streams-native/trace/reachability-metadata.json` (23KB, 179 types); the
directory's `.gitignore` keeps the 78MB binary out while keeping that. **It is the expensive half:
it needs a broker, a real demo run and the agent, and the build is a minute once you have it.**

**The demo needed one seam to launch a binary**, and it is the seam the comparison demo already
had: `streams_demo.py` now resolves `PC_DEMO_STREAMS_ENGINE` (an absolute binary) before falling
back to `PC_DEMO_STREAMS_CLASSPATH` plus `java`, mirroring `reference_demo.py`'s
`PC_DEMO_SIDECAR` / `PC_DEMO_SIDECAR_CLASSPATH` pair exactly.

## Predictions scored

| # | Prediction | Outcome |
|---|---|---|
| P1 | image builds | **confirmed** - first attempt, 41s, no flag hunting |
| P2 | needs a new metadata capture | **confirmed** - and it is the ONLY thing that was needed |
| P3 | wall at run time in `new KafkaStreams`/`start()`, from config strings | **confirmed in mechanism, wrong in target** - it is `StreamsConfig.<clinit>` resolving its own `ConfigDef` defaults, before any user serde is looked at. The shape was right; the class was not, which is why the agent found it and reading would not have |
| P4 | logging is the build-time obstacle | **refuted** - nothing blocked the build at all. Inheriting the sidecar's init list meant the logging problem was already paid for |
| P5 | GraalVM 25's strict image heap rejects the inherited list | **refuted** - CE 25.0.2 accepted it unchanged. The macOS/Oracle-23 recipe transferred to Linux/CE-25 verbatim |
| P6 | no dynamic-proxy blocker | **confirmed** (nothing surfaced) |
| P7 | 110-150MB, 2-5 min | **refuted, and by a lot** - 78MB and 53s with metadata; 41.7MB and 41s without. Kafka Streams cost ~36MB and 12s over the un-traced build. The sidecar's 79MB was for PC core alone, so **the whole Streams engine fits in the size budget the sidecar already established** |
| P8 | startup under 200ms native, 1-3s JVM | **confirmed on the native side (14ms), refuted on the JVM side (330ms)** - this engine is a gRPC listener that starts no Kafka client until a session opens, so the JVM's cost here is much lower than a sidecar's |
| P9 | demo assertions pass unchanged | **confirmed** |
| P10 | state directory and threads fine | **confirmed** for in-memory stores, one stream thread |

Wrong on 4 of 10, including both of the ones about where the difficulty would be. **The build was
the easy half and the metadata was the whole of the difficulty**, which is the sidecar note's
lesson arriving intact one module later.

## What this does NOT prove

- **In-memory stores only, and that is a PoC scope choice, not a solved problem.** `RocksDB` under
  native-image is untouched here: `rocksdbjni` is on the classpath but nothing on this path reaches
  it, so its JNI library was never loaded. **Any durable-state future re-opens this as an unprobed
  cliff** - see [`core-rocksdb-works-on-the-jvm-sidecar.md`](core-rocksdb-works-on-the-jvm-sidecar.md).
- **One happy path traced, exactly as the sidecar's capture was.** Two attempts to walk the failure
  arm (`--function-delay-ms 60` over 100 records, then 150ms over 600) produced **no reflection
  failure and no eviction** - the second simply ran out of demo timeout. So the rebalance and
  eviction paths are **unprobed under native image**, not proven. The trace also never walked a
  windowed store, a join, or an interactive query.
- **One topology shape**: source -> mapValues -> groupByKey -> count -> to. `TopologyAssembler`
  supports more than the demo describes.
- **Linux/x86-64 only**, one Kafka version (3.9.2), `num.stream.threads=1`.
- **Nothing about `--shared`.** This is an executable. The `--shared` build has its own entry-point
  surface, and the reflection-inheritance question was settled for the PC core library
  ([`perf-embedding-the-engine-over-ffi.md`](perf-embedding-the-engine-over-ffi.md)) but not for
  this classpath.

## What it means for the two embedding routes

[`branch-crossing-cost-ladder.md`](branch-crossing-cost-ladder.md) names the write-up's obligation:
say which route it assumes. **Route (1), native-image including Kafka Streams, is no longer
unproven - it is the cheaper of the two on this evidence.** The engine builds in under a minute
into a 78MB self-contained binary that starts in 14ms and counts correctly, and getting there cost
one tracing run and zero build-flag archaeology; the metadata approach the fork already uses for PC
core carried over without modification. That matters more than the size or the speed, because it
means the Streams fast path can be built on the **same** artifact pipeline as the `--shared`
library the FFI work already produces, rather than forking the toolchain. **Route (2), libjvm
embedding, is now the fallback rather than the likely answer** - its advantages (JIT retained, no
closed-world analysis, no metadata to maintain) are real but they are paid for with a JVM inside
the host process, and the only one of those advantages this probe found a use for is the metadata
maintenance, which is a real ongoing cost rather than a one-off. **The honest boundary is
durability**: everything above holds for in-memory stores. If a durable-state Streams engine is
ever in scope, RocksDB's JNI surface has to be probed before route (1) can be assumed again, and
that is exactly the point at which libjvm embedding stops being the fallback and becomes the
question again.

## Prior art this builds on

Checks run before this probe, and what each returned:

- `ls docs/plans/` + grep for `native-image` - `2026-08-22-001-feat-shared-c-transport-plan.md`
  and `2026-08-22-002-feat-kafka-streams-foreign-wrappers-plan.md`; neither had built Streams
  natively.
- `grep -rl` over `docs/solutions/` for `native-image` - **nothing**.
- `grep -rl` over `docs/inflight/` - the two notes this builds on, plus
  `core-rocksdb-works-on-the-jvm-sidecar.md`, `parked-a-c-client-and-the-ffi-question.md` and
  `next-kafka-streams-foreign-wrappers.md`.
- [`perf-native-image-sidecar-works.md`](perf-native-image-sidecar-works.md) - the build recipe,
  the five-attempt log, and the "only the agent could fix it" finding, all of which transferred.
- [`perf-embedding-the-engine-over-ffi.md`](perf-embedding-the-engine-over-ffi.md) - the `--shared`
  build and the classpath-discovered metadata that covered it unchanged.

