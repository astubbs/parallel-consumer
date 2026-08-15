# Client: Swift (astubbs#242)

Per-language working note for the Swift client of the language-proxy plan
(`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Effort figures, divergence notes, and
anything the Swift wave learns that a later session needs go HERE - never appended to
`docs/inflight/branch-language-proxy.md` - one file per language, so concurrent waves never edit a
shared note.

**Status: wave not started, but the BUILD ENVIRONMENT is built and proven.** The module skeleton is
seeded at `parallel-consumer-proxy-clients/parallel-consumer-proxy-client-swift/`; its maturity and
testing-evidence deferrals live in `docs/data/module-maturity.d/` and
`docs/data/testing-evidence.d/` under the module's artifact name, and the wave that starts this
client lifts them from its own fragment files.

## The build environment (done - do not redesign it)

Swift is one of the two languages mise cannot serve here: `mise use -g swift@latest` 404s because
Swift.org publishes Linux toolchains for Ubuntu, Amazon Linux and RHEL only, and this box is Debian
13 ([`../parked-containerised-toolchains-and-runtime.md`](../parked-containerised-toolchains-and-runtime.md)).
The module therefore carries a multi-stage `Dockerfile` and a `toolchain-smoke/` package, and
**nothing about it is speculative**: the smoke generates Swift from the frozen `proxy.proto`, links
grpc-swift and swift-protobuf, is extracted to the host and **runs there**.

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
  (built from source - no Linux release binaries), and in `toolchain-smoke/Package.swift`
  swift-protobuf 1.38.1, grpc-swift-2 2.4.2, grpc-swift-protobuf 2.4.1, grpc-swift-nio-transport
  2.9.1. grpc-swift-protobuf 2.4.1 requires grpc-swift-2 >= 2.3.0.

## Inherited from the C++ wave, which finished first

The two container languages share a build route, so the other one's findings arrive here rather than
being rediscovered. Full detail in [`cpp.md`](cpp.md).

- **`bin/build-client.sh --test` now keys off the PAIRING, not a filename.** Every extracted
  executable `X` with a sibling `X-dynamic` is a portability claim with its own control; an artifact
  with no sibling is skipped rather than run with no arguments, and at least one pair must exist or
  the run fails rather than reading as a pass. `toolchain-smoke/` here is unaffected and still
  checked - but when this wave deletes it, whatever replaces it must ship the same pair.
- **Two guide divergences are settled and this wave inherits them.** The overflow protocol violation
  now renders the fencing token (opacity forbids deriving, not printing) - C++ was the first client
  to do it, so the guide's "no client does this yet" is stale. And the guide's logging table (§10.2)
  still has no Swift row; unlike C++, Swift *does* have an ecosystem facade in `swift-log`, so the
  row to add is almost certainly `Logger` from `swift-log` with no handler configured, not an
  injectable closure.
- **Do not `--output type=local` a non-scratch stage to read the generated code.** It exports that
  stage's whole filesystem - the entire Swift image - rather than the artifacts. Build the stage as
  an image and run it, or add a scratch stage that copies only what you want to look at.

## What will bite this wave

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
