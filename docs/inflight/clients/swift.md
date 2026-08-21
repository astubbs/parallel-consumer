# Client: Swift (astubbs#242)

Per-language working note for the Swift client of the language-proxy plan
(`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Effort figures, divergence notes, and
anything the Swift wave learns that a later session needs go HERE - never appended to
`docs/inflight/branch-language-proxy.md` - one file per language, so concurrent waves never edit a
shared note.

**Status: wave one done.** The client, its selftest, its conformance runner and twenty unit tests
are in; the module is back in the reactor and its maturity and testing-evidence deferrals are
lifted. The `toolchain-smoke/` package is gone, as its own header said it should be once a real
target proved the same three things. What is NOT implemented, and not declared either, is every
capability but `dispatch`: no heartbeats, no manifest reconnect, no worker-death reporting, no
terminal outcomes, no proxy-initiated drain.

## What this wave learned that the next one needs

- **An actor is not a mutex, because actors are REENTRANT across a suspension.** The ordering
  scenario's sabotage - the one the conformance README says is the only shape that works, a lock
  around the whole processor - was first written as an actor whose method awaited the body. The
  suite stayed **green**: the moment the body awaits, the actor admits the next caller, so nothing
  was serialised and the sabotage proved nothing. Excluding took a flag held *across* the suspension
  with its own waiter queue. Any client author reaching for an actor as a lock should read this
  first, and any *sabotage* written with one is not a sabotage.
- **The `report-nothing` hold is not load-bearing here, and the inherited claim should not be
  repeated.** Go's runner exits fast enough to kill its own report in flight, which is why the
  contract has the three-second hold. Removing the hold from this runner *and* reporting success
  anyway still went red - the report reaches the wire before the process exits. The hold stays,
  because it is contract and this is a timing accident rather than a property of Swift, but no
  evidence from this module supports the claim.
- **`swift-log` needs its silence spelled out.** It is the right facade - Swift has one, unlike C++ -
  but a `Logger` taken from `LoggingSystem` with no bootstrap writes to **stdout at `info`**, so the
  default has to be `Logger(label:) { _ in SwiftLogNoOpLogHandler() }`. This is the §10.2 row owed
  back to the guide.
- **The dispatch queue is a locked class, not an actor, and the reason is `defer`.** §3.2 puts the
  decrement where a dying executor cannot skip it - the language's `finally`/`ensure`/`defer` - and
  a Swift `defer` body cannot `await`, so an actor's `settle()` would be unusable from the one place
  the rule names. Everything callable from `defer` is synchronous; only `take()` is `async`.
- **`swift-log` pins at 1.10.0 and a bump waits on the base image.** 1.10.1 onwards declare
  `swift-tools-version:6.2`, which the pinned Swift 6.1 toolchain cannot parse - the failure is at
  manifest load, so it reads as a resolution error rather than a version conflict.
- **Read the v2 API from source, do not recall it.** `.ipv4(host:port:)` is deprecated in favour of
  `.ipv4(address:port:)`, and with `-warnings-as-errors` a deprecation is a build failure. The
  generated code is the other authority: build a **scratch** stage that copies only the generated
  files and export that, per the C++ wave's warning below.

## The build environment (done - do not redesign it)

Swift is one of the two languages mise cannot serve here: `mise use -g swift@latest` 404s because
Swift.org publishes Linux toolchains for Ubuntu, Amazon Linux and RHEL only, and this box is Debian
13 ([`../parked-containerised-toolchains-and-runtime.md`](../parked-containerised-toolchains-and-runtime.md)).
The module therefore carries a multi-stage `Dockerfile`, and **nothing about it is speculative**:
the build generates Swift from the frozen `proxy.proto`, links grpc-swift and swift-protobuf, and
its artifacts are extracted to the host and **run there**. The `toolchain-smoke/` package this
section originally described has been replaced by `pc-swift-selftest`, which proves the same three
things and one more - that the client library links and initialises.

- **Base: the official `swift:6.1` image** (Swift 6.1.3 on Ubuntu 24.04) - Swift.org's own build,
  which is exactly what the unavailable mise install would have fetched, with no distro repackaging
  in between.
- **What a developer runs:** `bin/build-client.sh swift` (add the test flag to run the extracted
  artifact on this host). Through Maven it is
  `./mvnw -Dpc.foreignClients -pl :parallel-consumer-proxy-client-swift -am package`; **without
  `-Dpc.foreignClients` no container starts at all**, proven with a `docker` shim on `PATH` that
  records every call - zero calls with the profile off.
- **Extraction is BuildKit's, not a container run:**
  `docker buildx build --target artifact --build-context proto=parallel-consumer-proxy-protocol/src/main/proto --output type=local,dest=<module>/target/container <module>`.
  The ordinary build context is the module, so no other agent's `target/` is ever uploaded, and
  `proxy.proto` reaches the build as a **named context** instead of being copied into this module.
- **Measured** (32-core box), and **the number that governs the loop is the EDIT rebuild, which the
  first measurement never took**: a no-op rebuild is **1s** through `bin/build-client.sh` (48s
  through Maven is Maven's own startup plus two 245 MB exports, not the image build), while touching
  one source file cost **164s** - so "warm 48s" described a build that did nothing, and the earlier
  wave read it as the cycle cost. With the scratch paths separated (see the Dockerfile's `build`
  stage) the same edit costs **22-43s** and recompiles no dependency at all. Cold is **215s** with
  `swift:6.1` already pulled, unchanged by the fix; the recorded **5m27s** includes the one-time
  3.4 GB base-image pull, which is paid once per machine and never again. Static artifact **158 MB**
  (81 MB stripped); the dynamic control is **86 MB**.
- **Pins, and they move together:** `protoc-gen-swift` 1.38.1, `protoc-gen-grpc-swift-2` 2.4.1
  (built from source - no Linux release binaries, and neither project ships release assets at all),
  and in `Package.swift` swift-protobuf 1.38.1, grpc-swift-2 2.4.2, grpc-swift-protobuf 2.4.1,
  grpc-swift-nio-transport 2.9.1, swift-log 1.10.0. grpc-swift-protobuf 2.4.1 requires grpc-swift-2
  >= 2.3.0, and `Package.resolved` is committed so the image resolves what the checkout pins.

## Inherited from the C++ wave, which finished first

The two container languages share a build route, so the other one's findings arrive here rather than
being rediscovered. Full detail in [`cpp.md`](cpp.md).

- **`bin/build-client.sh --test` now keys off the PAIRING, not a filename.** Every extracted
  executable `X` with a sibling `X-dynamic` is a portability claim with its own control; an artifact
  with no sibling is skipped rather than run with no arguments, and at least one pair must exist or
  the run fails rather than reading as a pass. What ships the pair here now is `pc-swift-selftest`
  and `pc-swift-selftest-dynamic`; the conformance runner has no sibling and is skipped by that
  rule, which is what the rule intends.
- **Two guide divergences are settled and this wave inherits them.** The overflow protocol violation
  now renders the fencing token (opacity forbids deriving, not printing) - C++ was the first client
  to do it, so the guide's "no client does this yet" is stale. And the guide's logging table (§10.2)
  still has no Swift row; unlike C++, Swift *does* have an ecosystem facade in `swift-log`, so the
  row to add is almost certainly `Logger` from `swift-log` with no handler configured, not an
  injectable closure. **That prediction held, with one correction the row must carry**: "no handler
  configured" is not silence in swift-log - the default writes to stdout at `info` - so the row is
  `Logger` defaulted to `SwiftLogNoOpLogHandler`. Still owed back to the guide.
- **Do not `--output type=local` a non-scratch stage to read the generated code.** It exports that
  stage's whole filesystem - the entire Swift image - rather than the artifacts. Build the stage as
  an image and run it, or add a scratch stage that copies only what you want to look at.

## Toolchain facts that still bite

- **The v2 package identity is `grpc-swift-2.git`, not `grpc-swift.git`.** Every other grpc-swift
  package resolves against that URL, so depending on the older one forks the graph into two copies
  of the same package. The generator binary is `protoc-gen-grpc-swift-2` and its protoc flag is
  `--grpc-swift-2_out` - the v1 spellings silently do not exist.
- **`libprotobuf-dev` is required in the image even though nothing links C++**: it ships the
  well-known types (`duration.proto`, `timestamp.proto`) the frozen schema imports, and without it
  protoc fails on the import rather than on anything Swift.
- **The frozen `swift_prefix = "PCP"` is visible in every generated name** - `PCPClientMessage`,
  `PCPConfigure`, `PCPProxyService` (with `PCPProxyService.Method.descriptors`). Do not add a
  prefix option to a command line; the schema already owns the placement.
- **158 MB is what `--static-swift-stdlib` costs** (Foundation and ICU come along). `strip` halves
  it to 81 MB. If the demo container work (R72) cares about image size, that is the first lever.
- **SwiftPM needs the network at build time** - dependencies are fetched during `swift build`, so a
  fully offline build needs vendoring that nothing here does yet.
- **A missing Docker is exit 2, not a pass** (`bin/build-client.sh`). Through Maven that surfaces as
  `Exit value: 2` in the exec failure message, while Maven's own exit stays 1 - so read the message,
  not just the code, when a CI row goes red.

## The demo (astubbs#242, plan unit U35, R72)

**Status: written, in `parallel-consumer-proxy-clients/parallel-consumer-proxy-client-swift/demo/`.**
Two arms - `AK core` over `swift-kafka-client`, and `swift-grpc` over the client library in this
module - plus the contract's flags, environment variables, fingerprint and two tables. It is its own
SwiftPM package with a `.package(path: "..")` dependency on the client, its own `Dockerfile` and its
own `docker-compose.yml`; there is **no Maven module** and it is not in the reactor.

### What was actually run, and what it does NOT establish

Both arms were run end to end in the container, twice, on a machine shared with nine other agent
sessions:

- `--records 30 --replay-factor 1 --partitions 3` - fingerprint, both arms, small table, exit 0.
- `--records 20 --replay-factor 3 --partitions 2` - both replays, both tables and the across-replays
  footnote, exit 0.
- The image's own entry point with **no arguments** (parses the defaults and fails on the absent
  broker, exit 1), with `--help` (exit 0) and with a misspelled flag (exit 2).
- `run.sh` with no arguments, with flags, with an inherited `PC_DEMO_PARTITIONS`, `--help`,
  `--native`, an unknown flag and a flag missing its value - against a stubbed `docker`.
- No line the demo printed contained the bootstrap address; the only occurrences in the whole
  transcript are the broker's own logs.

**These runs prove the machinery, not any number.** Volumes that small are dominated by start-up:
the sidecar arm came out at 0.9x of AK core over 20 records and 2.7x over 60, which is the same
demo saying two different things about the same code. Nothing here should be quoted as a
measurement, and a default-scale run on an unloaded machine has not been done.

**One observation that a real measurement will have to account for**, seen in both runs and not yet
explained: the AK core arm's wall clock is far larger than its simulated work (30 records at 2 ms
took 1.79 s). `swift-kafka-client` feeds its message `AsyncSequence` from a poll loop whose
`pollInterval` defaults to 100 ms, so at these volumes the arm is timing that loop rather than the
work. Whether it amortises at contract scale is unknown - it was not tested, and the demo does not
change the default.

### What is owed

- **`bin/ci-demo-test.sh` runs the Java demo only.** The contract says a per-language demo inherits
  "both entry points are tested", and this one is tested by neither. It is a shared script, so
  wiring Swift in is a separate change to a file this wave did not own.
- **The demo package has no committed `Package.resolved`.** Generating one needs a toolchain that
  does not exist on this box outside the image. The image copies the resolved file it produced to
  `/app/Package.resolved` so a reader can at least see what was built; committing one properly means
  extracting it from a build, which is a `bin/build-client.sh`-shaped job rather than a demo one.
- **The blocking-sleep divergence is reasoned, not measured.** See below.
- **The big replay's title says "would take 0s+" at tiny volumes.** It is the Java seed's own
  expression, `total * delayMs / 1000` in integer arithmetic, mirrored faithfully; at the contract's
  defaults it reads correctly. Left as-is rather than diverging for a cosmetic case, but it is the
  seed's wart rather than Swift's.

### The three divergences

- **`Task.sleep`, not a blocking sleep, and the contract's "a blocking sleep is fine in Swift" is
  wrong for the sidecar arm.** `poll` starts `executorCount` Swift concurrency **tasks** on the
  cooperative pool, whose width is the core count. A blocking sleep in the user function occupies a
  pool thread for its whole duration, so an arm asking for 100 in-flight records could only ever
  have core-count many running - the table would report the pool's ceiling while appearing to report
  the engine's. This is the same mechanism this module already recorded for the conformance
  runner's ceiling barrier ("a waiter that blocked its thread would take one of those threads out of
  the pool"), so it is an established property of this client rather than a new guess.
  **It has not been measured here** - no blocking-sleep control arm has been run, and no figure for
  the gap should be quoted until one has.
- **`--partitions` reaches the broker's `num.partitions`, not a `CreateTopics` call.**
  `swift-kafka-client` has no admin client and no public metadata API, so the demo cannot create a
  topic with a partition count, cannot describe an existing one, and cannot do what the Java seed
  does when a topic already exists with the wrong count. The topic is auto-created by the first
  produce and `docker-compose.yml` carries the count. Consequence: with `--bootstrap` pointing at
  someone else's cluster the flag has no effect - pass `--topic` for a topic you made yourself.
- **No native mode.** There is no Swift toolchain on a developer box here, so `run.sh --native`
  refuses with the reason, and the demo can never start its own broker (a demo container gets no
  host Docker socket). The compose sibling is the only broker it meets.

### Things the next demo wave should not rediscover

- **The demo has to be its OWN SwiftPM package.** Its AK core arm needs `swift-kafka-client`, which
  vendors librdkafka and drags in zstd, OpenSSL and SwiftNIO. Putting that in the client library's
  manifest would add all of it to every consumer's graph, and to the committed `Package.resolved`
  the clients workflow keys its cache on, to serve a demo none of them build. A path dependency is
  also what makes it *legal*: the library's targets use `unsafeFlags`, which bars a package from
  being depended on by version - local paths are exempt.
- **`swift-kafka-client` publishes only `1.0.0-alpha.N` tags, and `main` is unusable here.** `main`
  declares `swift-tools-version:6.2.3`, which the pinned Swift 6.1 toolchain cannot parse, so the
  failure arrives at manifest load and reads as a resolution error rather than a version conflict.
  The demo pins `exact: "1.0.0-alpha.9"` - the newest tag whose tools version (5.9) the image can
  read. A base-image bump is what unblocks a move, exactly as it is for swift-log 1.10.1.
- **A Dockerfile cannot inherit a stage from another Dockerfile**, and `docker compose up` - the
  path a reader with only Docker takes - cannot build one image before another. So the demo image
  repeats the client module's `toolchain`, `plugins` and `codegen` stages **byte-identically**, which
  makes BuildKit's cache key match and pays for the protoc-plugin compile once per machine instead
  of once per Dockerfile. Only the codegen stage's schema COPY differs, because this build's context
  is already the repository. **Edit those stages in one file and you must edit the other.**
- **`maven:` as a base is usable after all, if you clear `MAVEN_CONFIG`.** The Java demo's Dockerfile
  rejects that image because it sets `MAVEN_CONFIG=/root/.m2` and the wrapper appends it to its own
  command line, so `./mvnw package` dies with `Unknown lifecycle phase "/root/.m2"`. `ENV MAVEN_CONFIG=`
  removes the conflict and keeps the wrapper, which saves pulling a second JDK base - worth knowing
  on a machine where ten agents share a disk.
- **The sidecar's jars are COPIED OUT rather than the Maven repository baked in.** The Java demo
  bakes `~/.m2` into its image because it computes a classpath pointing there; naming an output
  directory instead (`dependency:build-classpath`, then copying each entry) lets the download cache
  stay a BuildKit cache mount, which is not part of any image.
