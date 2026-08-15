# Copyright (C) 2026 Antony Stubbs and contributors
#
# CONTAINERISED BUILD ENVIRONMENT for the C++ proxy client (astubbs#242). Every other toolchain in
# this fan-out comes from mise; C++ is one of the two that cannot, because gRPC and protobuf arrive
# as system dev packages rather than as a versioned language toolchain
# (docs/inflight/parked-containerised-toolchains-and-runtime.md).
#
# Debian trixie-slim is the base for one reason: it is what the development box runs, so the image's
# glibc matches the host's exactly and any portability failure the extracted artifact shows is about
# gRPC and protobuf, not about libc. Debian 13 ships gRPC 1.51.1 and protobuf 3.21.12 with BOTH the
# shared objects and the static archives, which is what makes the static link below possible without
# compiling gRPC from source (measured: a from-source gRPC build is tens of minutes; this image is
# minutes, most of it apt).
#
# Nothing here runs a container: `docker buildx build --output type=local` exports the final scratch
# stage straight to a host directory. bin/build-client.sh drives it and owns the invocation.
#
# The frozen schema arrives as a BuildKit NAMED CONTEXT (`--build-context proto=...`) rather than by
# a build context that spans the repository: the module directory is the only ordinary context, so
# the build never uploads other agents' target/ output, and proxy.proto is never copied into this
# module.

FROM debian:trixie-slim AS toolchain

# apt's caches are what the cache mounts below preserve, so the image must stop deleting them.
RUN rm -f /etc/apt/apt.conf.d/docker-clean \
    && printf 'Binary::apt::APT::Keep-Downloaded-Packages "true";\n' > /etc/apt/apt.conf.d/keep-cache

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update \
    && apt-get install --yes --no-install-recommends \
        build-essential \
        cmake \
        pkg-config \
        libgrpc++-dev \
        libprotobuf-dev \
        protobuf-compiler \
        protobuf-compiler-grpc \
        libssl-dev \
        zlib1g-dev \
        libzstd-dev

FROM toolchain AS build

WORKDIR /work
COPY --from=proto . /work/proto
COPY toolchain-smoke /work/toolchain-smoke

RUN cmake -S /work/toolchain-smoke -B /work/build \
        -DCMAKE_BUILD_TYPE=Release \
        -DPC_PROTO_ROOT=/work/proto \
    && cmake --build /work/build --parallel "$(nproc)"

# Recorded in the image and extracted with the binaries: what the static artifact still depends on,
# and what the dynamic control depends on that the static one does not. Read it before believing
# either portability claim.
RUN { echo '# static artifact - ldd'; ldd /work/build/pc-cpp-toolchain-smoke; \
      echo; echo '# dynamic control - ldd'; ldd /work/build/pc-cpp-toolchain-smoke-dynamic; \
      echo; echo '# toolchain'; cmake --version | head -1; protoc --version; \
      dpkg-query -W -f='${Package} ${Version}\n' libgrpc++-dev libprotobuf-dev; \
    } > /work/build/link-report.txt

# Artifacts only - no base image, no toolchain, nothing to run. `--output type=local` exports this
# stage's whole filesystem, so anything added here lands in the developer's output directory.
FROM scratch AS artifact
COPY --from=build /work/build/pc-cpp-toolchain-smoke /
COPY --from=build /work/build/pc-cpp-toolchain-smoke-dynamic /
COPY --from=build /work/build/link-report.txt /
