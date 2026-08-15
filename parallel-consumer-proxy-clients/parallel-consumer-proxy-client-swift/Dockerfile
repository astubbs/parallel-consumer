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
# grpc-swift-2 >= 2.3.0, so these move together with the pins in toolchain-smoke/Package.swift.
# Declared in this stage, not in `toolchain`: a build ARG is scoped to the stage that declares it
# and is NOT inherited by stages built FROM it, so a shared declaration would expand to empty here
# and clone a branch named "".
ARG SWIFT_PROTOBUF_VERSION=1.38.1
ARG GRPC_SWIFT_PROTOBUF_VERSION=2.4.1

# protoc-gen-swift (messages) and protoc-gen-grpc-swift-2 (service stubs). Both are Swift packages
# with no release binaries for Linux, so they are compiled here once and cached as a layer.
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

FROM plugins AS codegen

WORKDIR /work
COPY --from=proto . /work/proto
COPY toolchain-smoke /work/toolchain-smoke

# Generated into the package's own Sources tree, from the FROZEN schema and nothing else. The
# generated type names carry the PCP prefix the schema's `swift_prefix` option names, which is what
# makes that option's placement in proxy.proto - rather than on nine command lines - observable here.
RUN mkdir -p /work/toolchain-smoke/Sources/Smoke/gen \
    && protoc \
        --proto_path=/work/proto \
        --plugin=protoc-gen-swift=/usr/local/bin/protoc-gen-swift \
        --plugin=protoc-gen-grpc-swift-2=/usr/local/bin/protoc-gen-grpc-swift-2 \
        --swift_out=/work/toolchain-smoke/Sources/Smoke/gen \
        --swift_opt=Visibility=Internal \
        --grpc-swift-2_out=/work/toolchain-smoke/Sources/Smoke/gen \
        --grpc-swift-2_opt=Client=true,Server=false \
        /work/proto/parallelconsumer/proxy/v1/proxy.proto

FROM codegen AS build

# --static-swift-stdlib is the portability claim: without it the artifact needs the image's Swift
# runtime shared objects, which no host outside this image has. glibc stays dynamic - see the note
# in the C++ Dockerfile; the same reasoning applies.
#
# EACH LINK MODE GETS ITS OWN SCRATCH PATH, AND THAT IS THE EDIT LOOP'S WHOLE COST. SwiftPM keeps
# one build database per scratch directory and does not partition it by link mode, so two builds
# that differ only in --static-swift-stdlib invalidate each other's ENTIRE dependency graph - all of
# SwiftNIO and grpc-swift - every time they alternate (swiftlang/swift-package-manager#5926, closed
# as not planned, so this is the workaround rather than a bug to wait out). Measured on this module,
# 32 cores, one shared scratch path: an edit to one source file cost 164s, of which 151s was the two
# builds recompiling the graph for each other. Separated, the same edit costs 54s and recompiles no
# dependency at all.
#
# The measurement that pins the cause rather than merely correlating with it: with the flag held
# CONSTANT the same edit rebuilt in 7s and recompiled nothing of NIOHTTP2, in the same container and
# across the same layer boundary - so this is the flag alternating, not container mtimes, not the
# cache mount, and not the COPY above.
#
# The paths are outside the package directory deliberately: .build inside it would be re-materialized
# by the COPY of the sources, and its name would then mean two different things in two stages.
RUN --mount=type=cache,target=/root/.cache/org.swift.swiftpm,sharing=locked \
    --mount=type=cache,id=pc-swift-scratch-static,target=/scratch/static,sharing=locked \
    --mount=type=cache,id=pc-swift-scratch-dynamic,target=/scratch/dynamic,sharing=locked \
    mkdir -p /out \
    && swift build --package-path /work/toolchain-smoke --scratch-path /scratch/static \
        -c release --static-swift-stdlib --product pc-swift-toolchain-smoke \
    && cp "$(swift build --package-path /work/toolchain-smoke --scratch-path /scratch/static -c release --static-swift-stdlib --show-bin-path)/pc-swift-toolchain-smoke" \
        /out/pc-swift-toolchain-smoke \
    && swift build --package-path /work/toolchain-smoke --scratch-path /scratch/dynamic \
        -c release --product pc-swift-toolchain-smoke \
    && cp "$(swift build --package-path /work/toolchain-smoke --scratch-path /scratch/dynamic -c release --show-bin-path)/pc-swift-toolchain-smoke" \
        /out/pc-swift-toolchain-smoke-dynamic

# Extracted with the binaries: what the static artifact still depends on, and what the dynamic
# control depends on that it does not. Read it before believing either portability claim.
RUN { echo '# static artifact - ldd'; ldd /out/pc-swift-toolchain-smoke; \
      echo; echo '# dynamic control - ldd'; ldd /out/pc-swift-toolchain-smoke-dynamic; \
      echo; echo '# toolchain'; swift --version; protoc --version; \
    } > /out/link-report.txt

# Artifacts only - no toolchain, nothing to run. `--output type=local` exports this stage's whole
# filesystem, so anything added here lands in the developer's output directory.
FROM scratch AS artifact
COPY --from=build /out/pc-swift-toolchain-smoke /
COPY --from=build /out/pc-swift-toolchain-smoke-dynamic /
COPY --from=build /out/link-report.txt /
