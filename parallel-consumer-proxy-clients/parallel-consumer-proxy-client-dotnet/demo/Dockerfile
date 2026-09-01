# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE DEMO CONTAINER for the .NET client (astubbs#242, plan unit U35, governed by KTD40), so that a
# reader with only Docker can run the demo - no .NET SDK, no JDK, nothing to install.
#
# IT CARRIES TWO TOOLCHAINS, WHICH NO OTHER CLIENT'S IMAGE HAS TO. The demo is .NET, and the sidecar
# it spawns is a JVM program in this repository rather than a shipped binary - so this image needs a
# .NET SDK to build the demo AND a JDK to build and run the proxy. That is scaffolding rather than
# the product model: the client library still spawns the sidecar itself, as a child process.
#
# THE BUILD CONTEXT IS THE REPOSITORY, AND THAT IS A DELIBERATE DEPARTURE from the rule the C++
# client's Dockerfile states - the module directory is the only ordinary context, so a build never
# uploads other agents' target/ output. A Maven reactor module cannot be built without its parents
# and siblings, so this build cannot keep it. The hazard is addressed directly instead, by the
# repository-root .dockerignore that excludes every target/ directory, .git and .claude; the Java
# demo's Dockerfile makes the same argument, and those two files must be read together.
#
# .dockerignore DOES NOT COVER .NET's BUILD OUTPUT, and the RUN below is why that is not a hole:
# MSBuild writes bin/ and obj/ beside each .csproj rather than under target/, and obj/ holds a
# NuGet asset file full of absolute host paths that do not exist in this image. Left in place they
# make `dotnet build` here fail with a restore error about the developer's own home directory. The
# root .dockerignore is not edited to add them because it is shared by every client module and by
# ten concurrent worktrees; a rule this image can enforce for itself belongs in this image.
#
# WHY THE DEPENDENCIES ARE BAKED IN RATHER THAN CACHE-MOUNTED. A BuildKit cache mount on /root/.m2
# is the obvious way to keep the Maven downloads out of the image, and it is wrong here: a cache
# mount is not part of the resulting image, so the sidecar classpath computed below would point at
# jars the running container does not have. The image is large as a result. That is the trade a
# runtime image makes and a build image does not.
#
# A .NET SDK BASE WITH A JDK ADDED, rather than a JDK base with .NET added. The .NET SDK has no
# apt package on Debian that tracks a pinned version cleanly, and the SDK image is the one that also
# pins the runtime the demo is built against. openjdk-17 is what the repository's own build
# requires, and it is available for both architectures this image is built on.
FROM mcr.microsoft.com/dotnet/sdk:8.0

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# See the header: .dockerignore covers target/, not bin/ and obj/.
RUN find /src/parallel-consumer-proxy-clients -type d \( -name bin -o -name obj \) -prune -exec rm -rf {} +

# THE SIDECAR. -Dcopyright.skip=true because the header check needs the repository's git history to
# find its fork point and .dockerignore excludes .git from the context; skipping it here loses
# nothing, since it is a source-tree gate that already runs in every ordinary build and in CI's own
# copyright lane.
#
# The ${project.build.directory} expression is SINGLE-QUOTED so the shell leaves it for Maven, which
# evaluates it per module - without that, -am has every module in the reactor overwrite one file.
RUN ./mvnw --batch-mode -q -DskipTests -Dcopyright.skip=true \
        -pl :parallel-consumer-proxy -am package \
        dependency:build-classpath -Dmdep.includeScope=runtime \
        '-Dmdep.outputFile=${project.build.directory}/sidecar-classpath.txt'

# THE DEMO, and its sidecar classpath resolved ONCE at build time. Computing it at run time would
# put Maven in the startup path of a demo whose whole promise is one command. The entries point at
# /root/.m2 and at the reactor's own target/classes, both of which are in this image - which is the
# reason the Maven downloads are baked in above rather than cache-mounted.
RUN dotnet publish --nologo -c Release -o /app \
        parallel-consumer-proxy-clients/parallel-consumer-proxy-client-dotnet/demo/Bz.Stub.ParallelConsumer.Proxy.Client.Demo/Bz.Stub.ParallelConsumer.Proxy.Client.Demo.csproj \
 && printf '%s:%s' /src/parallel-consumer-proxy/target/classes \
        "$(cat /src/parallel-consumer-proxy/target/sidecar-classpath.txt)" > /app/sidecar-cp.txt

# PC_DEMO_SIDECAR_CLASSPATH is read from the file rather than baked into an ENV because a Dockerfile
# cannot read a file into ENV, and the demo published under /app has no repository above it to
# discover one from. PC_DEMO_ARGS is unquoted on purpose: it carries the caller's flags through
# compose as a single string and must word-split into separate arguments.
ENTRYPOINT ["/bin/sh", "-c", \
    "PC_DEMO_SIDECAR_CLASSPATH=\"$(cat /app/sidecar-cp.txt)\" exec dotnet /app/pc-dotnet-demo.dll $PC_DEMO_ARGS"]
