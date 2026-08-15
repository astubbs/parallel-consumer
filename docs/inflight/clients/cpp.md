# Client: C++ (astubbs#242)

Per-language working note for the C++ client of the language-proxy plan
(`docs/plans/2026-08-14-001-feat-language-proxy-plan.md`). Effort figures, divergence notes, and
anything the C++ wave learns that a later session needs go HERE - never appended to
`docs/inflight/branch-language-proxy.md` - one file per language, so concurrent waves never edit a
shared note.

**Status: wave not started, but the BUILD ENVIRONMENT is built and proven.** The module skeleton is
seeded at `parallel-consumer-proxy-clients/parallel-consumer-proxy-client-cpp/`; its maturity and
testing-evidence deferrals live in `docs/data/module-maturity.d/` and `docs/data/testing-evidence.d/`
under the module's artifact name, and the wave that starts this client lifts them from its own
fragment files.

## The build environment (done - do not redesign it)

C++ is one of the two languages mise cannot serve, because gRPC and protobuf arrive as system dev
packages rather than as a versioned toolchain
([`../parked-containerised-toolchains-and-runtime.md`](../parked-containerised-toolchains-and-runtime.md)).
The module therefore carries a multi-stage `Dockerfile` and a `toolchain-smoke/` project, and
**nothing about it is speculative**: the smoke generates C++ from the frozen `proxy.proto`, links
gRPC and protobuf, is extracted to the host and **runs there**.

- **Base: `debian:trixie-slim`**, because it is what the development box runs - so the image's glibc
  is the host's, and any portability failure the extracted artifact shows is about gRPC and protobuf
  rather than libc. It ships gRPC **1.51.1** and protobuf **3.21.12** with the static archives as
  well as the shared objects, which is what makes static linking possible without compiling gRPC
  from source.
- **What a developer runs:** `bin/build-client.sh cpp` (add the test flag to run the extracted
  artifact on this host). Through Maven it is
  `./mvnw -Dpc.foreignClients -pl :parallel-consumer-proxy-client-cpp -am package`; **without
  `-Dpc.foreignClients` no container starts at all**, proven with a `docker` shim on `PATH` that
  records every call - zero calls with the profile off, `docker info` with it on.
- **Extraction is BuildKit's, not a container run:**
  `docker buildx build --target artifact --build-context proto=parallel-consumer-proxy-protocol/src/main/proto --output type=local,dest=<module>/target/container <module>`.
  The ordinary build context is the module, so no other agent's `target/` is ever uploaded, and
  `proxy.proto` reaches the build as a **named context** instead of being copied into this module.
- **Measured** (32-core box): cold image build **52s**; a rebuild with nothing changed **~0.7s**, or
  **4.1s** through Maven. Static artifact **22.1 MB** (17.8 MB stripped); the dynamic control is
  **0.5 MB**.

## What will bite this wave

- **The static link needs more than `pkg-config --static`.** Debian's `grpc++.pc` under-declares it:
  `libre2.a` calls absl's LOG machinery, whose archives appear in no `Requires` line, so the link
  fails on `absl::log_internal` symbols. `toolchain-smoke/CMakeLists.txt` resolves it by putting
  every `libabsl_*.a` in a `-Wl,--start-group`, and needs `libssl-dev`, `zlib1g-dev` and
  `libzstd-dev` present for their `.a` files. Copy that pattern rather than rediscovering it.
- **gRPC 1.51 / protobuf 3.21 are the versions you get.** Both predate the abseil-based protobuf
  API, so examples written against protobuf 4.x/5.x will not compile here. Building a newer gRPC
  from source inside the image is the escape hatch and costs tens of minutes per cold build - weigh
  that before reaching for a newer API.
- **`CMakeLists.txt` at the module root is still `LANGUAGES NONE`** and belongs to this wave;
  `toolchain-smoke/` is deliberately a separate project so the two never collide. Delete the smoke
  once a real target proves the same three things (generates, links, runs off-image).
- **A missing Docker is exit 2, not a pass** (`bin/build-client.sh`). Through Maven that surfaces as
  `Exit value: 2` in the exec failure message, while Maven's own exit stays 1 - so read the message,
  not just the code, when a CI row goes red.
