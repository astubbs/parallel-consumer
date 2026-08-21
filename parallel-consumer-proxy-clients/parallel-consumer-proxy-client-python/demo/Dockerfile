# Copyright (C) 2026 Antony Stubbs and contributors
#
# THE DEMO CONTAINER for the Python client (astubbs#242, plan unit U35, governed by KTD40).
#
# It exists so a reader needs Docker and nothing else - no Python, no JDK, no Maven. That is the
# same reason the Java demo's container exists, and R72's "Java is included, not exempted" is the
# statement that no language gets to skip this.
#
# IT CARRIES TWO TOOLCHAINS, WHICH THE SEED'S DOES NOT. The application is Python; the sidecar it
# spawns is a JVM binary built out of this repository's Maven reactor. So the image is a JDK base
# with a Python interpreter added, rather than either alone. When a native sidecar binary exists,
# the JDK half of this file is what disappears - and the demo already reads PC_DEMO_SIDECAR for
# exactly that case (see reference_demo.resolve_sidecar).
#
# THE BUILD CONTEXT IS THE REPOSITORY, AND THAT IS A DELIBERATE DEPARTURE. The C++ Dockerfile
# states the rule - "the module directory is the only ordinary context, so the build never uploads
# other agents' target/ output" - and it is a good rule that this build cannot keep: it builds a
# Maven reactor module, which needs its parents and its siblings. The hazard the rule protects
# against is addressed directly instead, by the repository-root .dockerignore that excludes every
# target/ directory, every .venv, .git and .claude/worktrees. Those two files must be read
# together; if you add a build output directory to this repo, add it there too.
#
# WHY THE DEPENDENCIES ARE BAKED IN RATHER THAN CACHE-MOUNTED, on both sides. A BuildKit cache
# mount on /root/.m2 (or on pip's cache) is the obvious way to keep the downloads out of the image,
# and it is wrong here: a cache mount is not part of the resulting image, so the classpath computed
# below would point at jars the running container does not have, and the venv would be missing the
# wheels it installed. The image is large as a result. That is the trade a RUNTIME image makes and
# a build image does not.
#
# A PLAIN JDK, NOT THE `maven:` IMAGE. The official Maven image sets MAVEN_CONFIG=/root/.m2, and
# the Maven WRAPPER appends $MAVEN_CONFIG to its own command line - so `./mvnw <phase>` inside that
# image dies with `Unknown lifecycle phase "/root/.m2"`. Using the image's own `mvn` would dodge it
# and break the repo's rule that builds go through the wrapper (AGENTS.md, "Build Requirements").
FROM eclipse-temurin:17-jdk

# python3-venv is separate from python3 on Debian and Ubuntu, and `python3 -m venv` fails with a
# message about ensurepip without it. The venv brings its own pip, so python3-pip is not needed.
RUN apt-get update \
 && apt-get install --yes --no-install-recommends python3 python3-venv \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# THE SIDECAR, and its classpath computed at BUILD time into a file. Computing it at run time would
# put Maven in the startup path of a demo whose whole promise is one command.
#
# -DincludeScope=runtime is a bug fix rather than tidiness: the default scope drags in
# parallel-consumer-core's TEST jar, whose logback-test.xml then configures the sidecar's logging
# and prints logback's own status report to stdout, ahead of the `port: <n>` line the client
# library scans for.
#
# -Dcopyright.skip=true because the header check needs the repository's git history to find its
# fork point, and .dockerignore excludes .git from the context. Skipping it here loses nothing: it
# is a source-tree gate that already runs in every ordinary build and in CI's own copyright lane.
RUN ./mvnw --batch-mode -q -DskipTests -Dcopyright.skip=true -DincludeScope=runtime \
        -pl :parallel-consumer-proxy -am \
        package dependency:build-classpath \
        '-Dmdep.outputFile=${project.build.directory}/proxy-classpath.txt' \
 && mkdir -p /app \
 && cp parallel-consumer-proxy/target/proxy-classpath.txt /app/cp.txt \
 && printf ':/src/parallel-consumer-proxy/target/classes' >> /app/cp.txt

# THE APPLICATION. A venv rather than the system interpreter, matching what `make build` does on a
# developer's machine, so the demo runs against the same installed package either way. The `demo`
# extra is what adds confluent-kafka: the AK core arm's Kafka client, and what seeds the backlog.
WORKDIR /src/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-python
RUN python3 -m venv .venv \
 && .venv/bin/pip install --quiet --upgrade pip \
 && .venv/bin/pip install --quiet --editable '.[demo]'

# PC_DEMO_SIDECAR_CLASSPATH IS READ FROM THE FILE HERE, not baked in with ENV: a Dockerfile's ENV
# cannot read a file, and the classpath is one. The demo then launches `java` directly with it -
# never through a shell, because a shell wrapper holds the write end of the sidecar's stdin and
# defeats the parent-death watch that stops a leaked JVM holding Kafka group membership. This
# entrypoint shell exec's away before that matters; it is the demo's parent, not the sidecar's.
#
# PC_DEMO_ARGS is unquoted on purpose: it carries the caller's flags through compose as a single
# string and must word-split into separate arguments.
ENTRYPOINT ["/bin/sh", "-c", \
    "export PC_DEMO_SIDECAR_CLASSPATH=\"$(cat /app/cp.txt)\"; exec .venv/bin/python demo/reference_demo.py $PC_DEMO_ARGS"]
