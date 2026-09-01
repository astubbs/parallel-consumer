# Copyright (C) 2026 Antony Stubbs and contributors
#
# CONTAINERISED BUILD ENVIRONMENT for the Swift proxy client (astubbs#242). Swift is one of the two
# toolchains mise cannot serve here, and the reason is not preference: `mise use -g swift@latest`
# 404s on this box because Swift.org publishes Linux toolchains for Ubuntu, Amazon Linux and RHEL
# only, and the development box is Debian 13 (trixie)
# (docs/inflight/parked-containerised-toolchains-and-runtime.md).
#
# The base is therefore the OFFICIAL swift image - Swift.org's own Ubuntu 24.04 build, the same
# artifact the unavailable mise install would have fetched, with no distro repackaging in between.
#
# Nothing here runs a container: `docker buildx build --output type=local` exports the final scratch
# stage straight to a host directory. bin/build-client.sh drives it and owns the invocation. The
# frozen proxy.proto arrives as a BuildKit NAMED CONTEXT (`--build-context proto=...`), so the build
# context stays this one module and the schema is never copied into it.
#
# THE TESTS AND THE FORMATTER RUN IN HERE, not on the host, and that is not a convenience: there is
# no Swift toolchain on this host and there will not be, so a check that only ran on the host would
# only ever run in CI. A red test or a formatting finding fails the IMAGE BUILD, which fails
# `bin/build-client.sh swift`, which fails the Maven module.

FROM swift:6.1 AS toolchain

# protoc itself is a system package; the two protoc PLUGINS are Swift source and are built below.
# libprotobuf-dev is not optional despite nothing here linking C++: it ships the well-known types
# (google/protobuf/duration.proto, timestamp.proto) that the frozen schema imports, and without it
# protoc fails on the import rather than on anything Swift.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && printf 'Binary::apt::APT::Keep-Downloaded-Packages "true";\n' > /etc/apt/apt.conf.d/keep-cache \
    && apt-get update \
    && apt-get install --yes --no-install-recommends protobuf-compiler libprotobuf-dev

FROM toolchain AS plugins

# Pinned, and each is a real released tag rather than a branch: grpc-swift-protobuf 2.4.1 requires
# grpc-swift-2 >= 2.3.0, so these move together with the pins in Package.swift. Declared in this
# stage, not in `toolchain`: a build ARG is scoped to the stage that declares it and is NOT inherited
# by stages built FROM it, so a shared declaration would expand to empty here and clone a branch
# named "".
ARG SWIFT_PROTOBUF_VERSION=1.38.1
ARG GRPC_SWIFT_PROTOBUF_VERSION=2.4.1

# protoc-gen-swift (messages) and protoc-gen-grpc-swift-2 (service stubs). Both are Swift packages
# with no release binaries for Linux - neither project ships ANY release assets, and the request has
# been open on apple/swift-protobuf since 2021 - so they are compiled here once and cached as a
# layer that nothing but a version bump invalidates. The alternative the ecosystem offers is the
# SwiftProtobufPlugin/GRPCProtobufGenerator build plugins, which is worse for exactly that reason:
# it moves compiling both plugins into the same build graph as the edit loop.
RUN --mount=type=cache,target=/root/.cache/org.swift.swiftpm,sharing=locked \
    git clone --quiet --depth 1 --branch "${SWIFT_PROTOBUF_VERSION}" \
        https://github.com/apple/swift-protobuf.git /src/swift-protobuf \
    && swift build --package-path /src/swift-protobuf -c release --product protoc-gen-swift \
    && cp "$(swift build --package-path /src/swift-protobuf -c release --show-bin-path)/protoc-gen-swift" \
        /usr/local/bin/protoc-gen-swift

RUN --mount=type=cache,target=/root/.cache/org.swift.swiftpm,sharing=locked \
    git clone --quiet --depth 1 --branch "${GRPC_SWIFT_PROTOBUF_VERSION}" \
        https://github.com/grpc/grpc-swift-protobuf.git /src/grpc-swift-protobuf \
    && swift build --package-path /src/grpc-swift-protobuf -c release --product protoc-gen-grpc-swift-2 \
    && cp "$(swift build --package-path /src/grpc-swift-protobuf -c release --show-bin-path)/protoc-gen-grpc-swift-2" \
        /usr/local/bin/protoc-gen-grpc-swift-2

# ── Codegen, from the FROZEN schema and NOTHING ELSE ────────────────────────────────────────────
# Its own stage, whose only input is the named proto context, so editing a hand-written source file
# does not re-materialise 124 KB of generated Swift with fresh timestamps. Public visibility because
# the generated code is its own SwiftPM target - see Package.swift for why it is split out.
FROM plugins AS codegen

COPY --from=proto . /proto
RUN mkdir -p /gen \
    && protoc \
        --proto_path=/proto \
        --plugin=protoc-gen-swift=/usr/local/bin/protoc-gen-swift \
        --plugin=protoc-gen-grpc-swift-2=/usr/local/bin/protoc-gen-grpc-swift-2 \
        --swift_out=/gen \
        --swift_opt=Visibility=Public \
        --grpc-swift-2_out=/gen \
        --grpc-swift-2_opt=Client=true,Server=false,Visibility=Public \
        /proto/parallelconsumer/proxy/v1/proxy.proto

FROM plugins AS build

WORKDIR /work/module

# Ordered least-likely-to-change first, so an ordinary edit re-materialises only what it touched.
# The generated code arrives BESIDE the one committed file of its target rather than instead of it -
# COPY merges into a directory.
# Package.resolved is COMMITTED and copied in, so the image resolves the graph the checkout pins
# rather than whatever the exact-version constraints happen to admit today. It is also what the
# clients workflow's swift row keys its dependency cache on.
COPY Package.swift Package.resolved .swift-format ./
COPY --from=codegen /gen Sources/ParallelConsumerProxyProtocol
COPY Sources Sources
COPY Tests Tests

# THREE SCRATCH PATHS, ONE PER BUILD CONFIGURATION, AND THAT IS THE EDIT LOOP'S WHOLE COST. SwiftPM
# keeps one build database per scratch directory and does not partition it by configuration, so two
# builds differing only in --static-swift-stdlib invalidate each other's ENTIRE dependency graph -
# all of SwiftNIO and grpc-swift - every time they alternate
# (swiftlang/swift-package-manager#5926, closed as not planned, so this is the workaround rather
# than a bug to wait out). Measured on this module, 32 cores, sharing one path: an edit to one source
# file cost 164s, of which 151s was the builds recompiling the graph for each other. Separated, an
# edit costs tens of seconds and recompiles no dependency at all.
#
# The measurement that pins the cause rather than merely correlating with it: with the configuration
# held CONSTANT the same edit rebuilt in 7s and recompiled nothing of NIOHTTP2, in the same container
# and across the same layer boundary - so this is the configuration alternating, not container
# mtimes, not the cache mount, and not the COPY above.
#
# The paths are outside the package directory deliberately: a .build inside it would be
# re-materialised by the COPY of the sources, and its name would then mean two different things.
#
# --static-swift-stdlib is the portability claim: without it the artifact needs the image's Swift
# runtime shared objects, which no host outside this image has. glibc stays dynamic - see the note in
# the C++ Dockerfile; the same reasoning applies.
#
# The static build names NO --product, deliberately: `swift build` takes only ONE, so a repeated flag
# silently builds the last one and the earlier binaries are simply missing at the copy. Building
# every product is both correct and what is wanted here, since both extracted binaries are products.
RUN --mount=type=cache,target=/root/.cache/org.swift.swiftpm,sharing=locked \
    --mount=type=cache,id=pc-swift-scratch-static,target=/scratch/static,sharing=locked \
    --mount=type=cache,id=pc-swift-scratch-dynamic,target=/scratch/dynamic,sharing=locked \
    mkdir -p /out \
    && swift build --scratch-path /scratch/static -c release --static-swift-stdlib \
    && STATIC_BIN="$(swift build --scratch-path /scratch/static -c release --static-swift-stdlib --show-bin-path)" \
    && cp "$STATIC_BIN/pc-swift-selftest" /out/pc-swift-selftest \
    && cp "$STATIC_BIN/pc-swift-conformance-runner" /out/pc-swift-conformance-runner \
    && swift build --scratch-path /scratch/dynamic -c release --product pc-swift-selftest \
    && DYNAMIC_BIN="$(swift build --scratch-path /scratch/dynamic -c release --show-bin-path)" \
    && cp "$DYNAMIC_BIN/pc-swift-selftest" /out/pc-swift-selftest-dynamic \
    && cp Package.resolved /out/Package.resolved

# The module's OWN tests, in their own scratch path for the same reason: a debug build is a third
# configuration. They are the layer the shared conformance suite is blind to - the in-flight ceiling
# counting the wrong thing, a credential in a rendering, a port line missed among the harness's log
# chatter.
#
# A separate RUN from the build above so that a red test does not force the artifacts to be rebuilt
# on the next attempt.
RUN --mount=type=cache,target=/root/.cache/org.swift.swiftpm,sharing=locked \
    --mount=type=cache,id=pc-swift-scratch-test,target=/scratch/test,sharing=locked \
    swift test --scratch-path /scratch/test

# The same formatter invocation the clients workflow's swift row runs, so there is one command rather
# than two that drift. swift-format ships inside the Swift 6 toolchain, so there is nothing to pin.
#
# IT IS A FORMATTER AND NOT A BUG-FINDER, and that is a recorded result rather than an omission:
# Swift has no mature standalone analyser in the SpotBugs/staticcheck class. The real defect
# detection is the compiler - the Swift 6 language mode that Package.swift's tools version turns on,
# whose strict concurrency checking makes a data race a compile error rather than a finding.
RUN swift format lint --strict --recursive Sources

# Extracted with the binaries: what the static artifact still depends on, and what the dynamic
# control depends on that it does not. Read it before believing either portability claim.
RUN { echo '# static artifact - ldd'; ldd /out/pc-swift-selftest; \
      echo; echo '# conformance runner - ldd'; ldd /out/pc-swift-conformance-runner; \
      echo; echo '# dynamic control - ldd'; ldd /out/pc-swift-selftest-dynamic; \
      echo; echo '# toolchain'; swift --version; protoc --version; \
    } > /out/link-report.txt

# Artifacts only - no toolchain, nothing to run. `--output type=local` exports this stage's whole
# filesystem, so anything added here lands in the developer's output directory.
FROM scratch AS artifact
COPY --from=build /out/pc-swift-selftest /
COPY --from=build /out/pc-swift-selftest-dynamic /
COPY --from=build /out/pc-swift-conformance-runner /
COPY --from=build /out/Package.resolved /
COPY --from=build /out/link-report.txt /

# ─────────────────────────────────────────────────────────────────────────────────────────────────
# The stage the CONFORMANCE SUITE runs inside, for hosts that cannot execute what `artifact` exports.
#
# Same reasoning as the C++ module's identical stage, and Swift has a second reason of its own: it
# has no native path here at all, because SwiftPM refuses a dependency's unsafe build flags outside
# a container. `artifact` above is a Linux binary; on macOS the shell answers 126 for every scenario.
#
# The whole run comes in here - this suite, the engine in its own JVM, the shim it writes and the
# runner it spawns - so the loopback they must share is this container's own. See
# bin/run-conformance-in-container.sh for why containerising only the runner cannot work.
#
# The runner is COPIED from the same `build` stage CI extracts from, so this stage and CI drive one
# binary rather than two builds that could differ.
FROM toolchain AS conformance

# A JDK, not a JRE: the suite is built and run by Maven inside this container.
COPY --from=eclipse-temurin:17-jdk /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"

COPY --from=build /out/pc-swift-conformance-runner /opt/pc-runners/pc-swift-conformance-runner

# Read by LanguageRunners.prebuiltRunner: with this set, the Swift entry carries NO build command,
# because its binary is already here.
ENV PC_CONFORMANCE_PREBUILT_RUNNERS=/opt/pc-runners

WORKDIR /repo
