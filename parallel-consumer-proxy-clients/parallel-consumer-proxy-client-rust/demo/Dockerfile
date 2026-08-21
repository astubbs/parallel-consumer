# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE DEMO CONTAINER for the Rust client (astubbs#242, plan unit U35, governed by KTD40).
#
# The reader should need Docker and nothing else: no Rust toolchain, no JDK, no protoc, no Maven.
#
# TWO TOOLCHAINS IN ONE IMAGE, AND THAT IS THE HONEST SHAPE OF THIS DEMO RATHER THAN BLOAT. The
# application is Rust; the sidecar it spawns is a Java program, because that is what the proxy is.
# A Rust user installs neither - the client library spawns the sidecar as a child process and the
# product ships it - but a container that BUILDS both from source needs both. Splitting this into a
# Rust builder stage and a JDK runtime stage was rejected: the Rust binary links glibc, so it would
# only run on a final stage whose glibc is at least as new as the builder's, and pinning that
# relationship across two independently-updated base images is a silent breakage waiting to happen.
# One base, one libc, no version arithmetic.
#
# WHY A RUST BASE WITH A JDK INSTALLED, RATHER THAN THE REVERSE. Debian ships a current JDK and a
# `rustc` several releases behind the 1.75 this crate's Cargo.toml requires, so the JDK is the
# apt-installable half and Rust is not.
#
# THE BUILD CONTEXT IS THE REPOSITORY, AND THAT IS A DELIBERATE DEPARTURE. The C++ Dockerfile states
# the rule - "the module directory is the only ordinary context, so the build never uploads other
# agents' target/ output" - and this build cannot keep it twice over: a Maven reactor module cannot
# be built without its parents and siblings, and the client crate generates from the frozen .proto
# in `parallel-consumer-proxy-protocol`. The hazard the rule protects against is addressed directly
# instead, by the repository-root .dockerignore that excludes every target/ directory, .git and
# .claude/worktrees. Those two files must be read together; if you add a build output directory to
# this repo, add it there too.
#
# THIS IS A RUNTIME IMAGE. The C++ and Swift client Dockerfiles are containerised BUILD environments
# that export artifacts and run nothing. This one is something a person runs, so it keeps its
# toolchains rather than exporting past them, and it bakes the Maven downloads in for the same
# reason the Java demo's does: the sidecar's classpath is computed at build time and points into
# /root/.m2, so a BuildKit cache mount - which is not part of the resulting image - would leave that
# classpath naming jars the running container does not have.
FROM rust:1-bookworm

# protobuf-compiler because the client crate generates its bindings in build.rs on every consumer's
# machine, deliberately, so they cannot drift from the schema. A JDK rather than a JRE because the
# image builds the sidecar from source with Maven.
#
# libprotobuf-dev IS NOT OPTIONAL, AND LEAVING IT OUT FAILS IN A MISLEADING WAY. Debian's
# `protobuf-compiler` ships the protoc BINARY only; the well-known types the frozen schema imports -
# `google/protobuf/duration.proto` and `timestamp.proto` - are in `libprotobuf-dev`, which installs
# them under /usr/include. Without it the build fails with "File not found" for those imports while
# a perfectly good protoc is installed, which reads as a broken schema rather than a missing
# package. Measured here, not guessed: that is exactly how this image failed first time.
RUN apt-get update \
 && apt-get install --yes --no-install-recommends \
        protobuf-compiler libprotobuf-dev openjdk-17-jdk-headless \
 && rm -rf /var/lib/apt/lists/*

# STATED RATHER THAN DISCOVERED. The client crate's build.rs resolves protoc and its include
# directory through a chain of fallbacks for a developer's machine, one of which is the copy Maven
# downloads into ~/.m2 during the sidecar build below - and Maven stores that copy without the
# executable bit. A container is the case its own header says should set these explicitly, so the
# search never runs here and this image cannot break because a fallback further down the chain
# changed.
ENV PROTOC=/usr/bin/protoc \
    PROTOC_INCLUDE=/usr/include

WORKDIR /src
COPY . .

# THE SIDECAR, and its classpath computed at BUILD time into a file. Computing it at run time would
# put Maven in the startup path of a demo whose whole promise is one command.
#
# -Dcopyright.skip=true because the header check needs the repository's git history to find its fork
# point, and .dockerignore excludes .git from the context. Skipping it here loses nothing: it is a
# source-tree gate that already runs in every ordinary build and in CI's own copyright lane, and
# re-running it inside a demo image would prove nothing new about the sources it just copied.
RUN ./mvnw --batch-mode -q -DskipTests -Dcopyright.skip=true \
        -pl :parallel-consumer-proxy -am \
        package dependency:build-classpath \
        '-Dmdep.outputFile=${project.build.directory}/demo-proxy-classpath.txt' \
 && mkdir -p /app \
 && printf '%s:/src/parallel-consumer-proxy/target/classes' \
        "$(cat /src/parallel-consumer-proxy/target/demo-proxy-classpath.txt)" > /app/proxy-classpath.txt

# THE APPLICATION. Release, not debug: the demo's entire output is throughput, and a debug-profile
# number would be a measurement of the optimiser being switched off.
RUN cd parallel-consumer-proxy-clients/parallel-consumer-proxy-client-rust/demo \
 && cargo build --release \
 && cp ../target/demo/release/demo /app/demo

# PC_DEMO_ARGS is unquoted on purpose: it carries the caller's flags through compose as a single
# string and must word-split into separate arguments. The classpath is read from the file rather
# than baked into an ENV so that a very long one cannot run into an environment-size limit.
#
# THE SIDECAR IS SPAWNED BY THE CLIENT LIBRARY, not by this entrypoint and not by compose. All the
# image does is tell the demo where the JVM and the classpath are; the library does the spawning,
# the port line and the supervision, exactly as it would in a user's own application.
ENTRYPOINT ["/bin/sh", "-c", \
    "PC_DEMO_PROXY_CLASSPATH=\"$(cat /app/proxy-classpath.txt)\" exec /app/demo $PC_DEMO_ARGS"]
