# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE DEMO CONTAINER for the TypeScript client (astubbs#242, plan unit U35, governed by KTD40).
#
# R72: every language's demo ships a container, so a reader with only Docker can run it. That is a
# stronger claim here than in Java, because this demo needs TWO toolchains - Node to run the demo,
# and a JVM to run the sidecar the client library spawns as a child process - and a reader is
# unlikely to have both.
#
# THE BUILD CONTEXT IS THE REPOSITORY, AND THAT IS THE SAME DELIBERATE DEPARTURE THE JAVA DEMO
# MAKES. The rule is stated in the C++ client's Dockerfile - the module directory is the only
# ordinary context, so a build never uploads other agents' target/ output - and this build cannot
# keep it: the sidecar is a Maven reactor module and cannot be built without its parents and
# siblings. The hazard is addressed directly instead, by the repository-root .dockerignore that
# excludes every target/, node_modules/, .git and .claude/worktrees. Read the two files together.
#
# TWO STAGES, BECAUSE THE SIDECAR IS BUILT AND THE DEMO IS RUN. The first stage is a JDK that
# builds the proxy module and materialises a flat directory of jars; the second is the image a
# person actually runs. Only the jars cross the boundary, so the Maven distribution, the reactor's
# sources and the ~/.m2 cache stay out of the shipped image.

FROM eclipse-temurin:17-jdk AS sidecar

WORKDIR /src
COPY . .

# A BUILDKIT CACHE MOUNT ON ~/.m2 IS CORRECT HERE AND WRONG IN THE JAVA DEMO, and the difference is
# worth stating because the two files disagree on purpose. A cache mount is not part of the
# resulting image, so it cannot be used by an image whose classpath points INTO ~/.m2 - which is
# what the Java demo's does, and why it bakes the downloads in. This stage copies the jars it needs
# out to /sidecar/lib in the same RUN, so nothing in the final image refers to the cache and the
# downloads never have to be shipped.
#
# `install` rather than `package` so the second invocation can resolve the reactor's own artifacts:
# Maven resolves a sibling module's jar from the reactor only within the session that built it.
#
# -Dcopyright.skip=true because the header check needs the repository's git history to find its
# fork point, and .dockerignore excludes .git from the context. It is a source-tree gate that runs
# in every ordinary build and in CI's own copyright lane; re-running it here would prove nothing
# new about the sources it just copied.
#
# The test jar is excluded by name. It is attached because other modules depend on it, it carries
# TestModeMain - the MOCK sidecar - and a demo that ran against a mock would report throughput for
# a broker that was never there.
RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw --batch-mode -q -DskipTests -Dcopyright.skip=true \
        -pl :parallel-consumer-proxy -am install \
 && ./mvnw --batch-mode -q -pl :parallel-consumer-proxy \
        dependency:copy-dependencies -DoutputDirectory=/sidecar/lib -DincludeScope=runtime \
 && find parallel-consumer-proxy/target -maxdepth 1 -name 'parallel-consumer-proxy-*.jar' \
        ! -name '*-tests.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' \
        -exec cp {} /sidecar/lib/ \;

# A NODE IMAGE WITH A JRE COPIED IN, rather than a JDK image with Node installed into it. Node is
# the demo's own runtime and its base image is the one that has to be right; the JVM is only ever
# `exec`d as the sidecar, so a JRE laid down beside it is enough. jammy's glibc is older than
# bookworm's, which is the direction that works - a noble-based JRE would not run here.
FROM node:22-bookworm-slim AS demo

COPY --from=eclipse-temurin:17-jre-jammy /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"

COPY --from=sidecar /sidecar/lib /sidecar/lib
# The `*` is a JAVA classpath wildcard, expanded by the JVM itself. It is never expanded by a
# shell: demo/src/sidecar.ts passes it straight through to spawn() as one argv element, and the
# client library launches the binary directly with no shell between - which is also what makes the
# inherited stdin pipe a working parent-death signal.
ENV PC_PROXY_SIDECAR_CLASSPATH="/sidecar/lib/*"

WORKDIR /app/client
COPY parallel-consumer-proxy-clients/parallel-consumer-proxy-client-typescript/ ./

# `clean` first, and it is not superstition: .dockerignore excludes node_modules but not dist, so a
# developer's local build output is in the context. tsc would keep its incremental state and the
# image would ship whatever the host last compiled.
#
# The library is built BEFORE the demo installs, because the demo depends on it through `file:..`
# and npm resolves that to a symlink pointing at dist/ - the built package a user would install,
# not the sources beside it.
RUN npm run clean && npm ci --no-audit --no-fund && npm run compile \
 && cd demo && npm run clean && npm ci --no-audit --no-fund && npm run compile

# PC_DEMO_ARGS is unquoted on purpose: it carries the caller's flags through compose as a single
# string and must word-split into separate arguments. With nothing set the demo runs its own
# defaults, which is what `docker compose up` on its own must do.
ENTRYPOINT ["/bin/sh", "-c", "exec node /app/client/demo/dist/src/main.js $PC_DEMO_ARGS"]
