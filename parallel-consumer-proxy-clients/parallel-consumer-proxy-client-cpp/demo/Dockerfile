# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE DEMO CONTAINER for the C++ client (astubbs#242, plan unit U35, governed by KTD40).
#
# THIS IS A RUNTIME IMAGE, AND THE OTHER C++ DOCKERFILE IS NOT. ../Dockerfile is a containerised
# BUILD environment: it exports static artifacts to a host directory with `--output type=local` and
# runs nothing. This one is something a person runs. Read that file first anyway - it explains why
# C++ builds in a container at all (gRPC and protobuf arrive as system dev packages rather than as
# a versioned toolchain, so mise cannot serve this language), and its Debian trixie base is the
# reason this one is Debian trixie too.
#
# FOR C++ THE CONTAINER IS THE ONLY WAY IN, which is the one place this demo diverges from the
# reference. Java's run.sh picks native or container; there is no native path here, because there is
# no host C++ toolchain and a demo that told a reader to install gRPC dev packages first would not
# be a one-command demo. demo/README.md records it.
#
# THE BUILD CONTEXT IS THE REPOSITORY, and that is the same deliberate departure the Java demo's
# Dockerfile makes. Two things here come from outside this module and cannot come any other way: the
# FROZEN proxy.proto, which is never copied into a client module, and the SIDECAR, which is a Maven
# reactor module that cannot be built without its parents and siblings. The hazard the
# module-directory rule protects against - uploading other agents' target/ output - is addressed by
# the repository-root .dockerignore instead. Those two files must be read together.
#
# THE SIDECAR IS BUILT IN, NOT DEPLOYED. It is a child process the client library spawns and
# supervises, so the reader installs, deploys and operates nothing (KTD41). It is emphatically not a
# compose service; see docker-compose.yml.

# ── The C++ toolchain, shared by the build stage and the runtime stage ───────────────────────────
# The runtime stage is built FROM this rather than from a slimmer base, so the demo's dynamically
# linked binary is guaranteed the exact libraries it was linked against. The image is large as a
# result, which is the trade a runtime image makes and a build image does not - the Java demo's
# Dockerfile records the same trade for the same reason.
FROM debian:trixie-slim AS cpp-toolchain

# apt's caches are what the cache mounts below preserve, so the image must stop deleting them.
RUN rm -f /etc/apt/apt.conf.d/docker-clean \
    && printf 'Binary::apt::APT::Keep-Downloaded-Packages "true";\n' > /etc/apt/apt.conf.d/keep-cache

# librdkafka-dev is the one package this image needs that ../Dockerfile does not: it is C++'s own
# Kafka client, and the AK core arm is the whole reason the demo has a comparison to make.
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
        librdkafka-dev \
        libssl-dev \
        zlib1g-dev \
        libzstd-dev \
        ca-certificates

# ── The demo binary ──────────────────────────────────────────────────────────────────────────────
FROM cpp-toolchain AS cpp-build

WORKDIR /work
# The frozen schema, from the module that owns it. Never copied into a client module - there is
# exactly one proxy.proto in this repository.
COPY parallel-consumer-proxy-protocol/src/main/proto /work/proto
# The client library, its test harness, and the demo. The demo links the library from source rather
# than from an artifact, because that is what proves the demo exercises THIS library.
COPY parallel-consumer-proxy-clients/parallel-consumer-proxy-client-cpp/src /work/module/src
COPY parallel-consumer-proxy-clients/parallel-consumer-proxy-client-cpp/tests /work/module/tests
COPY parallel-consumer-proxy-clients/parallel-consumer-proxy-client-cpp/demo /work/module/demo

RUN cmake -S /work/module/demo -B /work/build \
        -DCMAKE_BUILD_TYPE=Release \
        -DPC_PROTO_ROOT=/work/proto \
    && cmake --build /work/build --parallel "$(nproc)"

# THE DEMO'S OWN TESTS, IN HERE, for the same reason the client library's run inside its image: this
# host has no C++ toolchain, so a check that only ran on the host would only ever run in CI. They
# cover the entry path - the no-argument case, the flag/environment precedence, the fingerprint's
# silence about the broker - which is where every failure this fan-out's demos have actually had
# lived. A red test fails the IMAGE BUILD, which fails the demo.
RUN ctest --test-dir /work/build --output-on-failure

# ── The sidecar ──────────────────────────────────────────────────────────────────────────────────
# A JDK image builds it; only its jars and a JRE reach the final stage.
FROM eclipse-temurin:17-jdk AS sidecar-build

WORKDIR /src
COPY . .

# THE JARS ARE COPIED OUT, NOT LEFT IN A MAVEN REPOSITORY, and that is what lets the local
# repository be a BuildKit cache mount here where the Java demo's Dockerfile could not have one. Its
# classpath file points into /root/.m2, so a cache mount - which is not part of the resulting image -
# would leave it naming jars the running container does not have. Real copies plus a wildcard
# classpath have no such coupling, and the Maven downloads stay out of the image.
#
# `sharing=locked` because ten sibling demos may build at once and a Maven local repository is not
# safe for concurrent writers.
#
# -Dcopyright.skip=true because the header check needs the repository's git history to find its fork
# point, and .dockerignore excludes .git from the context. It is a source-tree gate that already runs
# in every ordinary build and in CI's own copyright lane.
#
# AND THEN AN SLF4J BINDING, WHICH THE RUNTIME SCOPE DOES NOT CARRY. The proxy logs through the SLF4J
# API and every binding in this reactor is test-scoped, so the sidecar spawned by this image would
# print `No SLF4J providers were found` and then nothing at all - measured, not assumed. That
# silences exactly the diagnostics the client library goes out of its way to preserve: it inherits
# the sidecar's stderr by default so that a misconfigured broker explains itself instead of becoming
# an unexplained hang. slf4j-simple writes INFO to stderr with no configuration file, which is what a
# demo wants. Its version is READ OFF the api jar already copied rather than pinned here, so it
# cannot drift from the root pom's.
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    ./mvnw --batch-mode -q -DskipTests -Dcopyright.skip=true \
        -pl :parallel-consumer-proxy -am \
        package dependency:copy-dependencies \
        -DincludeScope=runtime -DoutputDirectory=/app/sidecar/lib \
    && cp -r parallel-consumer-proxy/target/classes /app/sidecar/classes \
    && if [ -z "$(ls -A /app/sidecar/lib)" ]; then \
           echo 'the sidecar build produced no dependencies - the demo would fail at spawn' >&2; \
           exit 1; \
       fi \
    && SLF4J_VERSION="$(ls /app/sidecar/lib/slf4j-api-*.jar | sed 's|.*/slf4j-api-\(.*\)\.jar|\1|')" \
    && ./mvnw --batch-mode -q --non-recursive -Dcopyright.skip=true \
        dependency:copy "-Dartifact=org.slf4j:slf4j-simple:${SLF4J_VERSION}" \
        -DoutputDirectory=/app/sidecar/lib

# ── The demo image ───────────────────────────────────────────────────────────────────────────────
FROM cpp-toolchain AS demo

# A JRE, not the JDK that built the sidecar: the demo runs the proxy, it does not compile it.
COPY --from=eclipse-temurin:17-jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"

COPY --from=sidecar-build /app/sidecar /app/sidecar
COPY parallel-consumer-proxy-clients/parallel-consumer-proxy-client-cpp/demo/sidecar-launcher.sh \
     /app/sidecar/sidecar
RUN chmod +x /app/sidecar/sidecar

COPY --from=cpp-build /work/build/pc-cpp-demo /app/pc-cpp-demo

# The absolute path the client library is handed. It is an environment variable rather than a flag
# because the demo's flag list is fixed at seven by the contract, and this is a property of the image
# rather than of the run.
ENV PC_DEMO_SIDECAR=/app/sidecar/sidecar

# PC_DEMO_ARGS is unquoted on purpose: it carries the caller's flags through compose as a single
# string and must word-split into separate arguments. `exec` so the demo is pid 1's own process and
# a `docker compose down` reaches it.
ENTRYPOINT ["/bin/sh", "-c", "exec /app/pc-cpp-demo $PC_DEMO_ARGS"]
