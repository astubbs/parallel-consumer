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
#
# THE TESTS AND THE STATIC ANALYSIS RUN IN HERE, not on the host, and that is not a convenience: the
# host has no gRPC dev packages and no C++ toolchain for this module, so a check that only ran on
# the host would only ever run in CI. A red test or a cppcheck finding fails the IMAGE BUILD, which
# fails `bin/build-client.sh cpp`, which fails the Maven module.

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
        libzstd-dev \
        cppcheck

FROM toolchain AS build

WORKDIR /work
COPY --from=proto . /work/proto
COPY CMakeLists.txt /work/module/CMakeLists.txt
COPY scripts /work/module/scripts
COPY src /work/module/src
COPY tests /work/module/tests

RUN cmake -S /work/module -B /work/build \
        -DCMAKE_BUILD_TYPE=Release \
        -DPC_PROTO_ROOT=/work/proto \
    && cmake --build /work/build --parallel "$(nproc)"

# The module's own tests. They are the layer the shared conformance suite is blind to - the in-flight
# ceiling counting the wrong thing, a credential in a rendering, a port line missed among the
# harness's log chatter.
RUN ctest --test-dir /work/build --output-on-failure

# The module's OWN static-analysis recipe - the same file the clients workflow's cpp row runs, so
# there is one invocation rather than two that drift.
RUN /work/module/scripts/analyse.sh

# Recorded in the image and extracted with the binaries: what the static artifact still depends on,
# and what the dynamic control depends on that the static one does not. Read it before believing
# either portability claim.
RUN { echo '# static artifact - ldd'; ldd /work/build/pc-cpp-selftest; \
      echo; echo '# conformance runner - ldd'; ldd /work/build/pc-cpp-conformance-runner; \
      echo; echo '# dynamic control - ldd'; ldd /work/build/pc-cpp-selftest-dynamic; \
      echo; echo '# toolchain'; cmake --version | head -1; protoc --version; cppcheck --version; \
      dpkg-query -W -f='${Package} ${Version}\n' libgrpc++-dev libprotobuf-dev; \
    } > /work/build/link-report.txt

# Artifacts only - no base image, no toolchain, nothing to run. `--output type=local` exports this
# stage's whole filesystem, so anything added here lands in the developer's output directory.
FROM scratch AS artifact
COPY --from=build /work/build/pc-cpp-selftest /
COPY --from=build /work/build/pc-cpp-selftest-dynamic /
COPY --from=build /work/build/pc-cpp-conformance-runner /
COPY --from=build /work/build/link-report.txt /

# ─────────────────────────────────────────────────────────────────────────────────────────────────
# The stage the CONFORMANCE SUITE runs inside, for hosts that cannot execute what `artifact` exports.
#
# `artifact` above is a Linux binary. On a Linux host that runs natively and this stage is unused; on
# macOS it cannot be executed at all - the shell answers 126, "found but cannot execute" - so C++ and
# Swift simply could not be driven from a developer's laptop.
#
# Containerising only the runner does not fix it. The client spawns the sidecar as its own child and
# talks to it over loopback, and the suite's shim announces a bare port with no host, deliberately, so
# the client exercises its real spawn-and-reap path. Client, shim and engine must therefore share ONE
# loopback - and measured on Docker Desktop, a container cannot reach the host's 127.0.0.1 even with
# --network host. So the whole run comes in here instead, where that loopback is this container's own.
#
# The runner is COPIED from the same `build` stage CI extracts from, so this stage and CI drive the
# identical binary rather than two builds that could differ. Nothing is compiled here.
FROM toolchain AS conformance

# A JDK, not a JRE: the suite is built and run by Maven inside this container.
COPY --from=eclipse-temurin:17-jdk /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"

COPY --from=build /work/build/pc-cpp-conformance-runner /opt/pc-runners/pc-cpp-conformance-runner

# Read by LanguageRunners.prebuiltRunner: with this set, the C++ entry carries NO build command,
# exactly as the JVM languages do, because its binary is already here.
ENV PC_CONFORMANCE_PREBUILT_RUNNERS=/opt/pc-runners

# The repository is mounted here by bin/run-conformance-in-container.sh rather than copied: the suite
# is driven from the working tree the developer is actually editing.
WORKDIR /repo
