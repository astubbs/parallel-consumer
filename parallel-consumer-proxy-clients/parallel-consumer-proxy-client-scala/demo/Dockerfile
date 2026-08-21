# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE DEMO CONTAINER for the Scala client (astubbs#242, plan unit U35).
#
# The contract's rule is that a reader with only Docker can run any language's demo, and Scala is no
# more exempt than Java is - "a demo with one tested entry point has an untested entry point".
#
# THIS IS A RUNTIME IMAGE. It keeps its toolchain rather than exporting an artifact to the host, the
# way the containerised BUILD environments for C++ and Swift do, because it is something a person
# runs rather than something that produces a file.
#
# THE BUILD CONTEXT IS THE REPOSITORY, which departs from the rule the C++ Dockerfile states - "the
# module directory is the only ordinary context, so the build never uploads other agents' target/
# output". A Maven reactor module cannot be built without its parents and siblings, so this build
# cannot keep that rule; the hazard it protects against is addressed directly instead, by the
# repository-root .dockerignore that excludes every target/ directory, .git and .claude. Read the
# two together, and if you add a build output directory to this repo, add it there too.
#
# WHY THE DEPENDENCIES ARE BAKED IN RATHER THAN CACHE-MOUNTED. A BuildKit cache mount on /root/.m2
# is the obvious way to keep the Maven downloads out of the image, and it is wrong here: a cache
# mount is not part of the resulting image, so the classpath computed below would point at jars the
# running container does not have. The image is large as a result - that is the trade a runtime
# image makes and a build image does not.
#
# A PLAIN JDK, NOT THE `maven:` IMAGE. The official Maven image sets MAVEN_CONFIG=/root/.m2 and the
# Maven WRAPPER appends $MAVEN_CONFIG to its own command line, so `./mvnw <phase>` there dies with
# `Unknown lifecycle phase "/root/.m2"`. Using that image's own `mvn` would dodge it and break this
# repository's rule that builds go through the wrapper (AGENTS.md, "Build Requirements").
FROM eclipse-temurin:17-jdk

WORKDIR /src
COPY . .

# ONE Maven invocation, and the classpath computed at BUILD time into a file. Computing it at run
# time would put Maven in the startup path of a demo whose whole promise is one command.
#
# -Dpc.scalaDemo activates the module's scala-demo profile: the demo's sources are a test source
# root it adds, and the engine jar the spawned sidecar needs is a dependency it adds. Without it
# this image would build the client library and nothing else.
#
# -Dcopyright.skip=true because the header check needs the repository's git history to find its fork
# point, and .dockerignore excludes .git from the context. Skipping it here loses nothing: it is a
# source-tree gate that already runs in every ordinary build and in CI's own copyright lane.
RUN ./mvnw --batch-mode -q -DskipTests -Dcopyright.skip=true -Dpc.scalaDemo \
        -pl :parallel-consumer-proxy-client-scala -am test-compile \
 && mkdir -p /app \
 && MODULE_DIR=parallel-consumer-proxy-clients/parallel-consumer-proxy-client-scala \
 && cp "$MODULE_DIR/target/demo-classpath.txt" /app/cp.txt \
 && printf ':/src/%s/target/classes:/src/%s/target/test-classes' "$MODULE_DIR" "$MODULE_DIR" >> /app/cp.txt \
 && cp "$MODULE_DIR/demo/logback.xml" /app/logback.xml

# A REAL FORKED JVM, not `mvn exec:java`, and this is load-bearing rather than stylistic. The client
# library spawns the sidecar as a child process and this demo hands it
# System.getProperty("java.class.path"). Under exec:java the main class runs inside MAVEN's JVM, so
# that property would be Maven's own classpath and the child sidecar would come up without the
# engine on it. The native path in run.sh forks for exactly the same reason.
#
# -Dlogback.configurationFile is what lets the two tables be seen: logback's fallback with no
# configuration on the classpath is root at DEBUG, which buries a small run's own output under
# thousands of lines of Netty frames. demo/logback.xml says what it turns down and why.
#
# PC_DEMO_ARGS is unquoted on purpose: it carries the caller's flags through compose as a single
# string and must word-split into separate arguments.
ENTRYPOINT ["/bin/sh", "-c", \
    "exec java -Dlogback.configurationFile=/app/logback.xml -cp \"$(cat /app/cp.txt)\" bz.stub.parallelconsumer.client.scaladsl.demo.ScalaDemo $PC_DEMO_ARGS"]
