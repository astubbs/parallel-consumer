#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Builds the Kafka Streams engine (parallel-consumer-proxy-streams) as a GraalVM native-image
# EXECUTABLE, to settle whether Kafka Streams can run under native-image at all - the companion gap
# docs/inflight/branch-crossing-cost-ladder.md names beside the crossing-cost ladder.
#
# The recipe is inherited from the sidecar's, not invented: the same --no-fallback and the same
# --initialize-at-build-time list, both of which docs/inflight/perf-native-image-sidecar-works.md
# records as having been added to fix a build that had actually failed. What differs is the entry
# point, and that reachability metadata is passed EXPLICITLY here - the metadata that ships at
# META-INF/native-image/reachability-metadata.json was traced over a PC-core session and knows
# nothing about Kafka Streams.
#
#   ./build-streams-native.sh                       # build with whatever metadata is on hand
#   PC_STREAMS_NI_CONFIG=<dir> ./build-streams-native.sh   # ... plus a traced config directory
#
# Cross-platform: macOS and Linux. Missing tools are reported by name rather than worked around.

set -euo pipefail

PROBE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$PROBE_DIR/../.." && pwd)"
BUILD_DIR="$PROBE_DIR/streams-native"
IMAGE_NAME="pc-streams-engine"
MAIN_CLASS="bz.stub.parallelconsumer.streams.StreamsMain"

case "$(uname -s)" in
    Darwin|Linux) ;;
    *) echo "unsupported platform $(uname -s): this script knows macOS and Linux only" >&2
       exit 1 ;;
esac

# GraalVM. GRAALVM_HOME wins; otherwise mise, then sdkman. Never fall back to the default JDK -
# native-image would simply be absent and the error would point at the wrong thing.
if [ -z "${GRAALVM_HOME:-}" ]; then
    for candidate in "$HOME"/.local/share/mise/installs/java/graalvm-community-* \
                     "$HOME"/.sdkman/candidates/java/*-graal; do
        [ -x "$candidate/bin/native-image" ] && GRAALVM_HOME="$candidate" && break
    done
fi
if [ -z "${GRAALVM_HOME:-}" ] || [ ! -x "$GRAALVM_HOME/bin/native-image" ]; then
    echo "no GraalVM with native-image found. Set GRAALVM_HOME, or: mise install java@graalvm-community-25" >&2
    exit 1
fi

# The repo builds on JDK 17 (Jabel), which is NOT the JDK that runs native-image. Set it per
# command rather than exporting it, so this script cannot change the JDK for anything else.
JDK17="${PC_JDK17_HOME:-$HOME/.local/share/mise/installs/java/temurin-17}"
if [ ! -x "$JDK17/bin/java" ]; then
    echo "no JDK 17 at $JDK17 - set PC_JDK17_HOME. The repo's Maven build requires 17." >&2
    exit 1
fi

echo "==> GraalVM:  $("$GRAALVM_HOME/bin/native-image" --version | head -1)"
echo "==> JDK 17:   $JDK17"

mkdir -p "$BUILD_DIR"

CP_FILE="$REPO_ROOT/parallel-consumer-proxy-streams/target/streams-classpath.txt"
if [ ! -f "$CP_FILE" ] || [ -n "${PC_STREAMS_NI_REBUILD:-}" ]; then
    echo "==> resolving the streams module's runtime classpath"
    # -am is not optional: the enforcer's ReactorModuleConvergence rule fails the build without it.
    # -DincludeScope=runtime keeps core's TEST jar - and its logback-test.xml - out of the image.
    (cd "$REPO_ROOT" && JAVA_HOME="$JDK17" ./mvnw --batch-mode -q \
        -pl :parallel-consumer-proxy-streams -am -DskipTests -Dcopyright.skip=true \
        -DincludeScope=runtime package dependency:build-classpath \
        '-Dmdep.outputFile=${project.build.directory}/streams-classpath.txt' \
        >"$BUILD_DIR/maven.log" 2>&1) || {
            echo "maven failed; see $BUILD_DIR/maven.log" >&2; exit 1; }
fi

STREAMS_CLASSES="$REPO_ROOT/parallel-consumer-proxy-streams/target/classes"
CLASSPATH="$STREAMS_CLASSES:$(cat "$CP_FILE")"

config_args=()
if [ -n "${PC_STREAMS_NI_CONFIG:-}" ]; then
    [ -d "$PC_STREAMS_NI_CONFIG" ] || { echo "no config directory at $PC_STREAMS_NI_CONFIG" >&2; exit 1; }
    config_args+=("-H:ConfigurationFileDirectories=$PC_STREAMS_NI_CONFIG")
    echo "==> reachability config: $PC_STREAMS_NI_CONFIG"
else
    echo "==> no traced config passed; only the metadata shipped on the classpath applies"
fi

# --no-fallback is not optional and is the most important flag here: without it native-image quietly
# emits an image that still needs a JVM at run time, which builds green and proves nothing.
echo "==> native-image ($IMAGE_NAME)"
cd "$BUILD_DIR"
start=$(date +%s)
"$GRAALVM_HOME/bin/native-image" \
    --no-fallback \
    -cp "$CLASSPATH" \
    --initialize-at-build-time=ch.qos.logback,org.slf4j,org.xml.sax,com.sun.org.apache.xerces,javax.xml \
    "${config_args[@]+"${config_args[@]}"}" \
    -H:Name="$IMAGE_NAME" \
    "$MAIN_CLASS" \
    2>&1 | tee "$BUILD_DIR/native-image.log"
status=${PIPESTATUS[0]}
end=$(date +%s)

echo
if [ "$status" -ne 0 ]; then
    echo "==> native-image FAILED after $((end - start))s; see $BUILD_DIR/native-image.log" >&2
    exit "$status"
fi
echo "==> built $BUILD_DIR/$IMAGE_NAME in $((end - start))s"
ls -l "$BUILD_DIR/$IMAGE_NAME"
