# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE DEMO CONTAINER for the Go client (astubbs#242, plan unit U35, governed by KTD40).
#
# TWO TOOLCHAINS, WHICH IS WHY THIS IS A MULTI-STAGE BUILD AND THE JAVA ONE IS NOT. The demo itself
# is a Go binary, and it needs no Go at run time - Go links statically. The SIDECAR it spawns is a
# JVM application, and that toolchain has to be in the final image because the sidecar is a child
# process of the running demo, not a build artifact. So: Go builds and is thrown away, the JDK
# stays.
#
# THE BUILD CONTEXT IS THE REPOSITORY, AND THAT IS A DELIBERATE DEPARTURE. The C++ Dockerfile states
# the rule - "the module directory is the only ordinary context, so the build never uploads other
# agents' target/ output" - and it is a good rule that this build cannot keep, for the same reason
# the Java demo cannot: a Maven reactor module cannot be built without its parents and its sibling
# modules, and the sidecar is one. The hazard the rule protects against is addressed directly
# instead, by the repository-root .dockerignore that excludes every target/ directory, .git and
# .claude/worktrees. Those two files must be read together; if you add a build output directory to
# this repo, add it there too.
#
# WHY THE MAVEN DEPENDENCIES ARE BAKED IN RATHER THAN CACHE-MOUNTED. A BuildKit cache mount on
# /root/.m2 is the obvious way to keep the downloads out of the image, and it is wrong here: a cache
# mount is not part of the resulting image, so the sidecar classpath computed below would point at
# jars the running container does not have. The image is large as a result. That is the trade a
# runtime image makes and a build image does not.

# ---------------------------------------------------------------------------------------------
# Stage 1: the demo binary.
#
# ONLY THE GO CLIENT MODULE IS COPIED IN, even though the context is the whole repository - the
# demo module's `replace` directive points at ../, so the client library beside it is the entire
# rest of what this stage needs. Copying less also means a change anywhere else in the repo does not
# invalidate this layer.
#
# CGO_ENABLED=0 because the binary is copied into a different base image below; a dynamically linked
# one would be looking for the golang image's libc. Nothing here needs cgo - franz-go and the client
# library are pure Go.
# ---------------------------------------------------------------------------------------------
FROM golang:1.25-bookworm AS demo-build

WORKDIR /src
COPY parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go ./parallel-consumer-proxy-client-go
RUN cd parallel-consumer-proxy-client-go/demo \
 && CGO_ENABLED=0 go build -o /out/pc-go-demo .

# ---------------------------------------------------------------------------------------------
# Stage 2: the image that runs, carrying the JVM the spawned sidecar needs.
#
# A PLAIN JDK, NOT THE `maven:` IMAGE, and that is a bug fix rather than a preference. The official
# Maven image sets MAVEN_CONFIG=/root/.m2, and the Maven WRAPPER appends $MAVEN_CONFIG to its own
# command line - so `./mvnw <phase>` inside that image dies with `Unknown lifecycle phase
# "/root/.m2"`. Using the image's own `mvn` would dodge it and break the repo's rule that builds go
# through the wrapper (AGENTS.md, "Build Requirements"). A JDK base keeps the wrapper and removes
# the conflict: .mvn/wrapper/maven-wrapper.jar is committed, so the wrapper needs nothing but Java.
# ---------------------------------------------------------------------------------------------
FROM eclipse-temurin:17-jdk

WORKDIR /src
COPY . .

# ONE Maven invocation, and the sidecar's classpath computed at BUILD time into a file. Computing it
# at run time would put Maven in the startup path of a demo whose whole promise is one command.
#
# -Dcopyright.skip=true because the header check needs the repository's git history to find its fork
# point, and .dockerignore excludes .git from the context. Skipping it here loses nothing: it is a
# source-tree gate that already runs in every ordinary build and in CI's own copyright lane, and
# re-running it inside a demo image would prove nothing new about the sources it just copied.
#
# target/classes is appended rather than the built jar, so nothing has to guess the jar's name.
RUN ./mvnw --batch-mode -q -DskipTests -Dcopyright.skip=true \
        -pl :parallel-consumer-proxy -am \
        package dependency:build-classpath \
        '-Dmdep.outputFile=${project.build.directory}/sidecar-classpath.txt' \
 && mkdir -p /app \
 && cp parallel-consumer-proxy/target/sidecar-classpath.txt /app/sidecar-classpath.txt \
 && printf ':/src/parallel-consumer-proxy/target/classes' >> /app/sidecar-classpath.txt

COPY --from=demo-build /out/pc-go-demo /app/pc-go-demo

# Where the demo reads the sidecar's classpath from. It is plumbing rather than an eighth flag -
# there is no --sidecar-classpath and no default a user would set - and it exists only because the
# sidecar ships as a JVM application rather than a binary. run.sh sets the same variable natively.
ENV PC_DEMO_SIDECAR_CLASSPATH=/app/sidecar-classpath.txt

# PC_DEMO_ARGS is unquoted on purpose: it carries the caller's flags through compose as a single
# string and must word-split into separate arguments. Empty is the ordinary case - `docker compose
# up` with no arguments must run the demo with its own defaults.
ENTRYPOINT ["/bin/sh", "-c", "exec /app/pc-go-demo $PC_DEMO_ARGS"]
