# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE DEMO CONTAINER for the Java client (astubbs#242, plan unit U35, governed by KTD40).
#
# R72 is explicit that "Java is included, not exempted": a visitor evaluating "does this work in my
# language" reads an exempted Java as an admission that the other ten are the hard case. So this
# image exists for the same reason the C++ and Swift ones do - the reader should need Docker and
# nothing else - even though a JDK is the one toolchain most people reading this repo already have.
#
# THIS IS A RUNTIME IMAGE, WHICH MAKES IT THE ODD ONE OUT HERE. The C++ and Swift Dockerfiles are
# containerised BUILD environments that export artifacts to the host and run nothing. This one is
# something a person runs, so it keeps its toolchain rather than exporting past it.
#
# THE BUILD CONTEXT IS THE REPOSITORY, AND THAT IS A DELIBERATE DEPARTURE. The C++ Dockerfile states
# the rule - "the module directory is the only ordinary context, so the build never uploads other
# agents' target/ output" - and it is a good rule that this build cannot keep: a Maven reactor
# module cannot be built without its parents and its six sibling modules. The hazard the rule
# protects against is addressed directly instead, by the repository-root .dockerignore that excludes
# every target/ directory, .git and .claude/worktrees. Those two files must be read together; if you
# add a build output directory to this repo, add it there too.
#
# WHY THE DEPENDENCIES ARE BAKED IN RATHER THAN CACHE-MOUNTED. A BuildKit cache mount on /root/.m2
# is the obvious way to keep the Maven downloads out of the image, and it is wrong here: a cache
# mount is not part of the resulting image, so the classpath computed below would point at jars the
# running container does not have. The image is large as a result. That is the trade a
# runtime image makes and a build image does not.
# A PLAIN JDK, NOT THE `maven:` IMAGE, and that is a bug fix rather than a preference. The official
# Maven image sets MAVEN_CONFIG=/root/.m2, and the Maven WRAPPER appends $MAVEN_CONFIG to its own
# command line - so `./mvnw <phase>` inside that image dies with `Unknown lifecycle phase
# "/root/.m2"`. Using the image's own `mvn` would dodge it and break the repo's rule that builds go
# through the wrapper (AGENTS.md, "Build Requirements"). A JDK base keeps the wrapper and removes
# the conflict: .mvn/wrapper/maven-wrapper.jar is committed, so the wrapper needs nothing but Java
# to fetch the Maven distribution it pins.
FROM eclipse-temurin:17-jdk

WORKDIR /src
COPY . .

# ONE Maven invocation, and the classpath computed at BUILD time into a file. Computing it at run
# time would put Maven in the startup path of a demo whose whole promise is one command.
#
# The ${project.build.directory} expression is evaluated per module, so the demo's file holds the
# demo's classpath rather than whichever module Maven happened to build last. Its entries point at
# /root/.m2 and at the reactor's own target directories, both of which are in this image - which is
# the reason the Maven downloads are baked in above rather than cache-mounted.
#
# -Dcopyright.skip=true because the header check needs the repository's git history to find its fork
# point, and .dockerignore excludes .git from the context. Skipping it here loses nothing: it is a
# source-tree gate that already runs in every ordinary build and in CI's own copyright lane, and
# re-running it inside a demo image would prove nothing new about the sources it just copied.
RUN ./mvnw --batch-mode -q -DskipTests -Dcopyright.skip=true \
        -pl :parallel-consumer-proxy-client-java-demo -am \
        package dependency:build-classpath \
        '-Dmdep.outputFile=${project.build.directory}/demo-classpath.txt' \
 && mkdir -p /app \
 && MODULE_DIR=parallel-consumer-proxy-clients/parallel-consumer-proxy-client-java/parallel-consumer-proxy-client-java-demo \
 && cp "$MODULE_DIR/target/demo-classpath.txt" /app/cp.txt \
 && printf ':/src/%s/target/classes' "$MODULE_DIR" >> /app/cp.txt

# A REAL FORKED JVM, not `mvn exec:java`, and this is load-bearing rather than stylistic. The demo
# spawns the sidecar as a child process and hands it System.getProperty("java.class.path"). Under
# exec:java the main class runs inside MAVEN's JVM, so that property is Maven's own classpath and
# the child sidecar would be launched without the engine on it. The native path in run.sh forks for
# exactly the same reason.
#
# PC_DEMO_ARGS is unquoted on purpose: it carries the caller's flags through compose as a single
# string and must word-split into separate arguments.
ENTRYPOINT ["/bin/sh", "-c", \
    "exec java -cp \"$(cat /app/cp.txt)\" bz.stub.parallelconsumer.client.demo.ReferenceDemo $PC_DEMO_ARGS"]
