# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE SWIFT DEMO IMAGE (astubbs#242, plan unit U35, R72).
#
# It is a RUNTIME image, unlike the client module's Dockerfile beside it - that one is a
# containerised BUILD environment that exports artifacts to the host and runs nothing. This one is
# something a person runs, so it keeps a toolchain rather than exporting past it. Read that file
# first: it explains why Swift is built in a container here at all (Swift.org publishes Linux
# toolchains for Ubuntu, Amazon Linux and RHEL only, and this project's box is Debian 13).
#
# THREE THINGS HAVE TO MEET IN ONE IMAGE, which is what makes this the largest demo image in the
# repository:
#
#   1. the Swift demo binary, which needs the Swift toolchain to exist at all;
#   2. the client library it drives, whose protocol target is GENERATED from the frozen schema by
#      two protoc plugins that ship no Linux binaries and are therefore compiled from source here;
#   3. the sidecar, which is a JVM program - so the runtime carries a JRE and the proxy's jars.
#
# THE BUILD CONTEXT IS THE REPOSITORY, and that is a deliberate departure from the rule the C++
# Dockerfile states ("the module directory is the only ordinary context, so the build never uploads
# other agents' target/ output"). A Maven reactor module cannot be built without its parents and
# siblings, so this build needs the whole tree - exactly as the Java demo's does. The hazard is
# addressed by the repository-root .dockerignore, which excludes every target/ directory, .git and
# .claude. Those two files must be read together.
#
# THE FIRST THREE SWIFT STAGES ARE BYTE-IDENTICAL TO THE CLIENT MODULE'S, on purpose. A Dockerfile
# cannot inherit a stage from another Dockerfile, so the alternative to repeating them is building
# the module image first and naming it here - which `docker compose up`, the path a reader with only
# Docker takes, cannot do. Keeping the commands identical means BuildKit's cache key matches and the
# expensive part (compiling swift-protobuf and grpc-swift-protobuf) is paid once per machine rather
# than once per Dockerfile. IF YOU EDIT THOSE STAGES THERE, EDIT THEM HERE.

# ── The sidecar: the engine, as the binary the client library spawns ────────────────────────────
#
# `maven:...` RATHER THAN A PLAIN JDK, WHICH IS THE OPPOSITE OF WHAT THE JAVA DEMO CHOSE, and the
# reason is this machine rather than a preference. That image sets MAVEN_CONFIG=/root/.m2 and the
# Maven WRAPPER appends it to its own command line, so `./mvnw <phase>` there dies with
# `Unknown lifecycle phase "/root/.m2"`. Clearing the variable removes the conflict and keeps the
# repo's rule that builds go through the wrapper (AGENTS.md, "Build Requirements") - and it saves
# pulling a second half-gigabyte JDK base onto a disk this demo has to share.
FROM maven:3.9.9-eclipse-temurin-17 AS sidecar
ENV MAVEN_CONFIG=

WORKDIR /src
COPY . .

# THE JARS ARE COPIED OUT, NOT THE MAVEN REPOSITORY BAKED IN. The Java demo bakes ~/.m2 into its
# image because it computes a classpath that points there; naming an output directory instead lets
# the download cache stay a BuildKit cache mount, which is not part of any image. What lands in
# /opt/parallel-consumer/lib is exactly the proxy's runtime graph and nothing else.
#
# -Dcopyright.skip=true because the header check needs git history to find its fork point and
# .dockerignore excludes .git. It is a source-tree gate that runs in every ordinary build and in
# CI's own copyright lane; re-running it here would prove nothing about the sources it just copied.
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    ./mvnw --batch-mode -q -DskipTests -Dcopyright.skip=true \
        -pl :parallel-consumer-proxy -am \
        package dependency:build-classpath \
        '-Dmdep.outputFile=${project.build.directory}/sidecar-classpath.txt' \
 && mkdir -p /opt/parallel-consumer/lib \
 && tr ':' '\n' < parallel-consumer-proxy/target/sidecar-classpath.txt \
      | while read -r jar; do [ -n "$jar" ] && cp "$jar" /opt/parallel-consumer/lib/; done \
 && find parallel-consumer-proxy/target -maxdepth 1 -name '*.jar' \
      ! -name '*-sources.jar' ! -name '*-javadoc.jar' \
      -exec cp {} /opt/parallel-consumer/lib/ \;

# ── The Swift toolchain ─────────────────────────────────────────────────────────────────────────
FROM swift:6.1 AS toolchain

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && printf 'Binary::apt::APT::Keep-Downloaded-Packages "true";\n' > /etc/apt/apt.conf.d/keep-cache \
    && apt-get update \
    && apt-get install --yes --no-install-recommends protobuf-compiler libprotobuf-dev

FROM toolchain AS plugins

ARG SWIFT_PROTOBUF_VERSION=1.38.1
ARG GRPC_SWIFT_PROTOBUF_VERSION=2.4.1

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

# ── Codegen, from the FROZEN schema and nothing else ────────────────────────────────────────────
# The schema arrives by path rather than as the module build's named context, because this build's
# context is already the repository. That is the ONE line of these stages that differs from the
# module's, and it costs only this stage's cache entry.
FROM plugins AS codegen

COPY parallel-consumer-proxy-protocol/src/main/proto /proto
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

# ── The demo binary ─────────────────────────────────────────────────────────────────────────────
FROM plugins AS build

# swift-kafka-client VENDORS librdkafka and compiles it from source, so these are its build-time
# dependencies rather than the demo's. libssl-dev and libsasl2-dev are what its `COpenSSL` system
# library and its `-lsasl2` linker setting resolve against; pkg-config is how SwiftPM finds the
# former. Without them the failure is a linker error naming a symbol, not a missing package.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update \
    && apt-get install --yes --no-install-recommends \
        libssl-dev libsasl2-dev zlib1g-dev pkg-config

# THE DIRECTORY NAME IS LOAD-BEARING, not decoration. A SwiftPM PATH dependency takes its package
# IDENTITY from the last component of the path, not from the `name:` in its manifest - so the demo's
# `.product(name:..., package: "parallel-consumer-proxy-client-swift")` only resolves if the client
# module sits in a directory of that name here, exactly as it does in the repository. Call it
# /work/module and the build fails with "unknown package ... valid packages are: 'module'".
WORKDIR /work/parallel-consumer-proxy-client-swift

# The client module, then the demo package inside it - the demo's manifest reaches the client with
# `.package(path: "..")`, so the two have to keep this relationship on disk.
COPY parallel-consumer-proxy-clients/parallel-consumer-proxy-client-swift/Package.swift \
     parallel-consumer-proxy-clients/parallel-consumer-proxy-client-swift/Package.resolved \
     parallel-consumer-proxy-clients/parallel-consumer-proxy-client-swift/.swift-format ./
COPY --from=codegen /gen Sources/ParallelConsumerProxyProtocol
COPY parallel-consumer-proxy-clients/parallel-consumer-proxy-client-swift/Sources Sources
COPY parallel-consumer-proxy-clients/parallel-consumer-proxy-client-swift/demo demo

# ITS OWN NAMED SCRATCH PATH, and the module's Dockerfile explains why at length: SwiftPM keeps one
# build database per scratch directory and does not partition it by configuration, so two builds
# that differ in anything invalidate each other's ENTIRE dependency graph. This one is a fourth
# configuration - a different root package - and sharing a path with the module's three would make
# every alternation recompile SwiftNIO and grpc-swift.
#
# NOT `--static-swift-stdlib`, unlike the module's artifacts, and that is a claim not being made
# rather than one being dropped: those binaries are extracted and run on a host with no Swift
# runtime, so the static link is the whole point of them. This one never leaves the image, and the
# image's own base is where its runtime comes from.
RUN --mount=type=cache,target=/root/.cache/org.swift.swiftpm,sharing=locked \
    --mount=type=cache,id=pc-swift-demo-scratch,target=/scratch/demo,sharing=locked \
    mkdir -p /out \
    && swift build --package-path demo --scratch-path /scratch/demo -c release \
    && cp "$(swift build --package-path demo --scratch-path /scratch/demo -c release --show-bin-path)/pc-swift-demo" \
        /out/pc-swift-demo \
    && cp demo/Package.resolved /out/demo-Package.resolved

# The same formatter the clients workflow runs on the module's own sources, pointed at the demo's.
# A separate RUN from the build above so a finding does not force the binary to be rebuilt on the
# next attempt. It is a FORMATTER and not a bug-finder - the Swift 6 language mode the demo's tools
# version turns on is where defect detection actually lives.
RUN swift format lint --strict --recursive demo/Sources

# ── What a reader runs ──────────────────────────────────────────────────────────────────────────
#
# BACK TO THE PLAIN SWIFT IMAGE, not a slim runtime base and not a JRE image. The demo binary is
# dynamically linked against the Swift runtime, and this base is where that runtime came from - so
# there is no version of the "built on one distro, run on another" question to get wrong. A JRE
# arrives by package instead, because the sidecar is a JVM program.
FROM swift:6.1 AS demo

# libssl-dev/libsasl2-dev rather than the runtime packages they depend on: the runtime package
# names moved under Ubuntu's time_t transition (libssl3 -> libssl3t64) and the -dev names did not,
# so this is the spelling that does not depend on which Ubuntu the base image is today.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update \
    && apt-get install --yes --no-install-recommends \
        libssl-dev libsasl2-dev zlib1g-dev \
    && (apt-get install --yes --no-install-recommends openjdk-17-jre-headless \
        || apt-get install --yes --no-install-recommends default-jre-headless)

COPY --from=sidecar /opt/parallel-consumer/lib /opt/parallel-consumer/lib
COPY parallel-consumer-proxy-clients/parallel-consumer-proxy-client-swift/demo/sidecar.sh \
     /opt/parallel-consumer/sidecar
RUN chmod 0755 /opt/parallel-consumer/sidecar

COPY --from=build /out/pc-swift-demo /app/pc-swift-demo
# The resolved dependency graph the image actually built, kept where a reader can see it: the demo
# package has no committed Package.resolved, because resolving needs a toolchain no developer box
# here has.
COPY --from=build /out/demo-Package.resolved /app/Package.resolved

# PC_DEMO_ARGS is unquoted on purpose: it carries the caller's flags through compose as a single
# string and must word-split into separate arguments.
ENTRYPOINT ["/bin/sh", "-c", "exec /app/pc-swift-demo $PC_DEMO_ARGS"]
