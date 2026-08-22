# The sidecar runs as a GraalVM native image, and a Python demo drove it end to end

<!-- inflight-type: feature -->
<!-- inflight-impact: throughput -->
<!-- inflight-labels: release-note, needs-measurement -->

**Branch: `feats/native-image-sidecar`**, stacked on `feats/polyglot-demos`
(astubbs/parallel-consumer#331) - that is where the sidecar's `Main` entry point and the eleven demos
both live, which is why the work sits there rather than on the virtual-threads research branch.

Measured 2026-08-22. **A Python application, with no JVM anywhere in
its stack, ran Parallel Consumer through a native binary it spawned itself:**

```
AK core (confluent-kafka)        20 records  20 keys
pc-python-grpc (this client)     20 records  20 keys
Big replay                       40 records  40 keys
exit 0
```

The `records`/`keys` pair is the point, not the rate: it is the deterministic oracle every demo
reports, and 20/20 then 40/40 is what the seeding predicts. **No throughput figure from this run is
quotable** - the box was busy and 20 records is startup-dominated.

This is the architecture claim made concrete. A Go, Python or Ruby team is handed **an executable**,
not a JVM dependency, and gets key-ordered concurrency from it.

## The build recipe, and why each flag is there

```
native-image --no-fallback -cp <sidecar classes>:<86 runtime deps> \
  --initialize-at-build-time=ch.qos.logback,org.slf4j,org.xml.sax,com.sun.org.apache.xerces,javax.xml \
  -H:ConfigurationFileDirectories=<agent output> \
  bz.stub.parallelconsumer.proxy.Main
```

79MB, 1m17s, on Oracle GraalVM 23.

**`--no-fallback` is not optional and is the most important flag here.** Without it native-image
quietly emits a *fallback image* that still requires a JVM at runtime. That builds green, runs fine
on the build machine, and destroys the entire proposition - the point is handing a Go team a binary.

## Five attempts, and the obstacles were not where the warnings point

| # | change | outcome |
|---|---|---|
| 1 | plain `--no-fallback` | logback: `Logger.name` not available during analysis - failed in 7.7s |
| 2 | logback at **run** time | same failure, 9.5s |
| 3 | logback at **build** time | **built**, 1m18s, 70MB |
| 4 | \+ reachability config | SAX `LocatorImpl` in the image heap - failed in 16.1s |
| 5 | \+ XML/SAX at build time | **built**, 1m17s, 79MB, and the demo passes |

**Everything that blocked the build was LOGGING.** gRPC/Netty and the Kafka client raised nothing at
build time at all - which supports the proxy pom's note that a feasibility probe already proved
`grpc-netty-shaded` under native image. Anyone starting this work braced for Netty will spend their
first hour in the wrong place.

**Attempt 4 is the transferable lesson: adding config BROKE a build that had passed.** The extra
reachability let logback's XML configurator actually run during the build - visible in the log as
`DefaultJoranConfigurator.configure() call lasted 78 millis` - stranding a SAX object in the image
heap. Native image is not a monotonic "add config until green" process; the analysis surface moves as
you feed it.

## The Kafka client failed at RUNTIME, and only the agent could fix it

Before the reachability config, the native sidecar started, bound gRPC, and completed the
handshake - then failed inside `Configure` with `org.apache.kafka.common.KafkaException`. That is the
expected shape: the Kafka client instantiates serializers and partitioners **by reflection from
configuration strings** (`key.deserializer=...ByteArrayDeserializer`), which closed-world analysis
cannot see because they are runtime values rather than constants.

No amount of reading finds those. The fix was GraalVM's tracing agent, run over **a real session** -
the JVM sidecar wrapped in a script, pointed at by `PC_DEMO_SIDECAR`, driven by the Python demo -
producing a 1,049-line `reachability-metadata.json`. An agent run that merely started and stopped the
sidecar would have recorded nothing about `Configure`, which is exactly where it failed.

## What this does NOT prove, and it is the gap that will bite

**The captured config only covers code paths the traced run executed.** That run was: 20 records,
unordered, no failures, no retries, no rebalance, no transactional commit mode. Every path it did not
walk is still invisible, and the failure will not appear at build time - it appears the first time a
record fails in production and something reaches for a class that is not in the binary.

Turning this into a shippable artifact needs the trace extended across the retry path, the failure
path, each commit mode and a rebalance - or better, a test that **asserts** reachability rather than
trusting a one-off capture.

Also untried: the other ten demos (only Python was run), Linux (this was macOS/arm64), and the
sidecar's Unix-domain-socket mode.

## Prior art this builds on

- [`next-virtual-threads-under-graalvm-native.md`](next-virtual-threads-under-graalvm-native.md) -
  virtual threads work in a native image, including the engine's reflective construction path.
  Orthogonal to this note: nothing here needed them.
- [`parked-a-c-client-and-the-ffi-question.md`](parked-a-c-client-and-the-ffi-question.md) - the
  decision to dual-ship a JVM jar and a native binary (KTD13), which this makes real.
- The proxy pom's own comment: `grpc-netty-shaded` was chosen partly *because* the feasibility probe
  proved it under a native-image build.
- `PC_DEMO_SIDECAR` already existed, documented as "the shape this will take once a native sidecar
  exists" - the seam was written before the thing that needed it.
