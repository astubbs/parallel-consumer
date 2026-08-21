# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE DEMO CONTAINER for the Kotlin client (astubbs#242, plan unit U35, governed by KTD40).
#
# The contract requires one per language so that a reader with only Docker can run the demo - and
# R72 is explicit that a JVM language is included, not exempted: a visitor evaluating "does this
# work in my language" reads an exemption as an admission that the other languages are the hard
# case.
#
# THIS IS A RUNTIME IMAGE. It keeps its toolchain rather than exporting past it, because it is
# something a person runs rather than a containerised build that hands artifacts back to the host.
#
# THE BUILD CONTEXT IS THE REPOSITORY, which departs from the ordinary rule that a module directory
# is the only context. A Maven reactor module cannot be built without its parents and the modules
# it depends on. The hazard that rule protects against - uploading other agents' target/ output -
# is addressed directly by the repository-root .dockerignore, which excludes every target/
# directory, .git and .claude/worktrees. Read the two together; if you add a build output directory
# to this repo, add it there too.
#
# WHY THE DEPENDENCIES ARE BAKED IN RATHER THAN CACHE-MOUNTED. A BuildKit cache mount on /root/.m2
# is the obvious way to keep the Maven downloads out of the image, and it is wrong here: a cache
# mount is not part of the resulting image, so the classpath computed below would point at jars the
# running container does not have. The image is large as a result - that is the trade a runtime
# image makes and a build image does not.
#
# A PLAIN JDK, NOT THE `maven:` IMAGE. That image sets MAVEN_CONFIG=/root/.m2 and the Maven WRAPPER
# appends $MAVEN_CONFIG to its own command line, so `./mvnw <phase>` inside it dies with
# `Unknown lifecycle phase "/root/.m2"`. Using the image's own `mvn` would dodge that and break the
# repo's rule that builds go through the wrapper (AGENTS.md, "Build Requirements").
FROM eclipse-temurin:17-jdk

WORKDIR /src
COPY . .

# ONE Maven invocation, with the classpath computed at BUILD time into a file. Computing it at run
# time would put Maven in the startup path of a demo whose whole promise is one command.
#
# -Dpc.kotlinDemo activates the kotlin-demo profile: it compiles demo/src and puts the engine on
# the classpath, which the demo hands to the sidecar it spawns. test-compile rather than package,
# because the demo compiles into this module's test output tree - it is an application that uses
# the client, not part of the published library surface. -DskipTests rather than
# -Dmaven.test.skip, because the latter would skip the very compilation the demo needs.
#
# -Dcopyright.skip=true because the header check needs the repository's git history to find its
# fork point, and .dockerignore excludes .git from the context. Skipping it here loses nothing: it
# is a source-tree gate that already runs in every ordinary build and in CI's own copyright lane.
RUN ./mvnw --batch-mode -q -DskipTests -Dcopyright.skip=true -Dpc.kotlinDemo \
        -pl :parallel-consumer-proxy-client-kotlin -am test-compile \
 && mkdir -p /app \
 && MODULE_DIR=parallel-consumer-proxy-clients/parallel-consumer-proxy-client-kotlin \
 && cp "$MODULE_DIR/target/demo-classpath.txt" /app/cp.txt \
 && printf ':/src/%s/target/classes:/src/%s/target/test-classes' "$MODULE_DIR" "$MODULE_DIR" >> /app/cp.txt

# A REAL FORKED JVM, not `mvn exec:java`, and this is load-bearing rather than stylistic. The client
# library spawns the sidecar as a child process and hands it this JVM's java.class.path. Under
# exec:java that property would be Maven's own classpath and the child would come up without the
# engine on it. The native path in run.sh forks for exactly the same reason.
#
# -Dlogback.configurationFile is not decoration: parallel-consumer-core's test output is on this
# classpath and carries a logback-test.xml, which logback prefers over any logback.xml whatever the
# classpath order - so the demo's own configuration is ignored unless it is named. run.sh passes
# the same flag.
#
# PC_DEMO_ARGS is unquoted on purpose: it carries the caller's flags through compose as a single
# string and must word-split into separate arguments.
ENTRYPOINT ["/bin/sh", "-c", \
    "exec java -cp \"$(cat /app/cp.txt)\" -Dlogback.configurationFile=pc-kotlin-demo-logback.xml bz.stub.parallelconsumer.client.coroutines.demo.KotlinDemoKt $PC_DEMO_ARGS"]
