# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE DEMO CONTAINER for the Ruby client (astubbs#242, plan unit U35, governed by KTD40), so that a
# reader with only Docker can run the demo - no Ruby, no JDK, no Maven.
#
# TWO TOOLCHAINS, WHICH IS WHAT MAKES THIS IMAGE DIFFERENT FROM THE JAVA SEED'S. The application is
# Ruby and the sidecar it spawns is a JVM program, so the image is built in two stages: a JDK stage
# that builds the sidecar out of the repository, and a Ruby stage that carries a JRE to run it. Only
# the jars cross the boundary.
#
# THE BUILD CONTEXT IS THE REPOSITORY, AND THAT IS A DELIBERATE DEPARTURE from the rule the C++
# client's Dockerfile states - "the module directory is the only ordinary context, so the build never
# uploads other agents' target/ output". This build cannot keep it: a Maven reactor module cannot be
# built without its parents and its siblings. The hazard is addressed directly instead, by the
# repository-root .dockerignore that excludes every target/ directory, .git and .claude. Those two
# files must be read together; if you add a build output directory to this repo, add it there too.

# ── Stage 1: the sidecar ────────────────────────────────────────────────────────────────────────
#
# A PLAIN JDK, NOT THE `maven:` IMAGE, and that is a bug fix rather than a preference: the official
# Maven image sets MAVEN_CONFIG=/root/.m2 and the Maven WRAPPER appends $MAVEN_CONFIG to its own
# command line, so `./mvnw <phase>` inside that image dies with `Unknown lifecycle phase
# "/root/.m2"`. A JDK base keeps the wrapper, which is what this repo's AGENTS.md requires builds to
# go through.
FROM eclipse-temurin:17-jdk AS sidecar

WORKDIR /src
COPY . .

# THE MAVEN REPOSITORY IS A CACHE MOUNT HERE, WHICH THE JAVA SEED'S IMAGE COULD NOT DO. That image
# runs Maven's classpath at run time, so its dependencies have to be IN the image; this one copies
# the jars it needs out of the cache in the same RUN and never looks at /root/.m2 again. The id is
# this module's own, so ten concurrent client builds do not write one local repository at once.
#
# -Dcopyright.skip=true because the header check needs the repository's git history to find its fork
# point, and .dockerignore excludes .git from the context. It is a source-tree gate that already runs
# in every ordinary build and in CI's own copyright lane; re-running it here would prove nothing new
# about the sources it just copied.
#
# EVERY CLASSPATH ENTRY BECOMES A JAR IN /sidecar/lib, including the reactor's own target/classes
# directories - Maven resolves a sibling module to a directory or to a jar depending on how far the
# lifecycle got, and a directory cannot be copied into an image that does not have the tree it came
# from. Jarring it up removes the difference rather than depending on which one happened.
RUN --mount=type=cache,id=pc-ruby-demo-m2,target=/root/.m2/repository \
    ./mvnw --batch-mode -q -DskipTests -Dcopyright.skip=true \
        -pl :parallel-consumer-proxy -am \
        package dependency:build-classpath \
        '-Dmdep.outputFile=${project.build.directory}/sidecar-classpath.txt' \
 && mkdir -p /sidecar/lib \
 && n=0 \
 && for entry in $(tr ':' '\n' < parallel-consumer-proxy/target/sidecar-classpath.txt); do \
        if [ -f "$entry" ]; then cp -n "$entry" /sidecar/lib/ ; \
        elif [ -d "$entry" ]; then (cd "$entry" && jar cf "/sidecar/lib/reactor-$n.jar" .) ; fi ; \
        n=$((n + 1)) ; \
    done \
 && find parallel-consumer-proxy/target -maxdepth 1 -name '*.jar' \
        -not -name '*-tests.jar' -not -name '*-sources.jar' -not -name '*-javadoc.jar' \
        -exec cp {} /sidecar/lib/ \;

# ── Stage 2: the demo ───────────────────────────────────────────────────────────────────────────
#
# The Ruby the CI matrix pins for this module. `-bookworm` rather than `-slim`: `rdkafka` ships
# precompiled for linux-gnu, but a platform without a precompiled gem builds librdkafka from source,
# and a base image with no compiler turns that into a build failure a reader cannot act on.
FROM ruby:3.4-bookworm

# A JRE, NOT A JDK: this image runs the sidecar, it does not build one. Copied from the Temurin
# image rather than installed with apt, so the JVM here and the JVM the sidecar was built with are
# the same distribution and major version.
COPY --from=eclipse-temurin:17-jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk

COPY --from=sidecar /sidecar/lib /opt/pc-sidecar/lib

# THE SIDECAR, AS THE CLIENT LIBRARY WANTS IT: an absolute path to a binary, and `java` itself
# rather than a wrapper script. Parent death is detected by EOF on the inherited pipe (KTD19), and a
# shell wrapper would inherit the write end and hold it open - which defeats the signal and leaks a
# JVM that still holds Kafka group membership. The `/*` is expanded by the JVM, not by a shell.
ENV PC_DEMO_SIDECAR_JAVA=/opt/java/openjdk/bin/java
ENV PC_DEMO_SIDECAR_CLASSPATH="/opt/pc-sidecar/lib/*"

# The Gemfile first and on its own, so that changing the demo's Ruby does not reinstall its gems.
# BUNDLE_WITHOUT repeats what demo/.bundle/config says, because the official Ruby image points
# BUNDLE_APP_CONFIG at /usr/local/bundle and that file is therefore not read in here.
ENV BUNDLE_WITHOUT="development:test"
WORKDIR /app/demo
COPY parallel-consumer-proxy-clients/parallel-consumer-proxy-client-ruby/Gemfile /app/Gemfile
COPY parallel-consumer-proxy-clients/parallel-consumer-proxy-client-ruby/demo/Gemfile /app/demo/Gemfile
RUN bundle install

# The client library the demo exercises, and the demo itself. Nothing is published to RubyGems in
# this wave, so the library is the one in this checkout or it is nothing.
COPY parallel-consumer-proxy-clients/parallel-consumer-proxy-client-ruby/lib /app/lib
COPY parallel-consumer-proxy-clients/parallel-consumer-proxy-client-ruby/demo /app/demo

# PC_DEMO_ARGS is unquoted on purpose: it carries the caller's flags through compose as a single
# string and must word-split into separate arguments.
#
# `"$@"` AND THE TRAILING "demo" ARE WHAT MAKE `docker compose run demo --help` WORK. `sh -c
# <script> <name> <args...>` binds $0 to the name and "$@" to the rest, so anything a caller appends
# to the image's entrypoint reaches the demo's own parser. Without them the flags land in $0 and are
# silently discarded - the demo would answer `--help` by running a full default-scale comparison.
ENTRYPOINT ["/bin/sh", "-c", "exec bundle exec ruby demo.rb $PC_DEMO_ARGS \"$@\"", "demo"]
