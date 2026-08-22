#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Builds Parallel Consumer as a GraalVM --shared library, so a non-JVM host process can embed the
# engine directly instead of talking to the sidecar over gRPC.
#
# Cross-platform: macOS and Linux. Every dependency it cannot satisfy is reported by name rather
# than worked around, because a silent fallback here produces a library that is missing the thing
# you were trying to test.
#
#   ./build-shared-library.sh probe     # Probe 0: is the ABI callable from Go at all?
#   ./build-shared-library.sh session   # the pc_session_* surface over the proxy's session core

set -euo pipefail

TARGET="${1:-session}"
FFI_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$FFI_DIR/../../.." && pwd)"
BUILD_DIR="$FFI_DIR/build"

case "$(uname -s)" in
    Darwin) LIB_EXT="dylib" ;;
    Linux)  LIB_EXT="so" ;;
    *)      echo "unsupported platform $(uname -s): this script knows macOS and Linux only" >&2
            exit 1 ;;
esac

# GraalVM. GRAALVM_HOME wins; otherwise look where sdkman puts it. Never fall back to the default
# JDK - native-image would simply be absent and the error would point at the wrong thing.
if [ -z "${GRAALVM_HOME:-}" ]; then
    for candidate in "$HOME"/.sdkman/candidates/java/*-graal; do
        [ -x "$candidate/bin/native-image" ] && GRAALVM_HOME="$candidate" && break
    done
fi
if [ -z "${GRAALVM_HOME:-}" ] || [ ! -x "$GRAALVM_HOME/bin/native-image" ]; then
    echo "no GraalVM with native-image found. Set GRAALVM_HOME, or install one:" >&2
    echo "  sdk install java 23-graal" >&2
    exit 1
fi

# The repo builds on JDK 17 (Jabel), which is NOT the JDK that runs native-image. Set it per
# command rather than exporting it, so this script cannot change the JDK for anything else.
JDK17="${PC_JDK17_HOME:-$HOME/.sdkman/candidates/java/17.0.18-tem}"
if [ ! -x "$JDK17/bin/java" ]; then
    echo "no JDK 17 at $JDK17 - set PC_JDK17_HOME. The repo's Maven build requires 17." >&2
    exit 1
fi

echo "==> GraalVM:  $("$GRAALVM_HOME/bin/native-image" --version | head -1)"
echo "==> JDK 17:   $JDK17"
echo "==> platform: $(uname -s) $(uname -m), building .$LIB_EXT"

mkdir -p "$BUILD_DIR"

# The proxy module's classes plus its full runtime classpath. Built with JDK 17.
echo "==> resolving the proxy module's runtime classpath"
CP_FILE="$BUILD_DIR/proxy-classpath.txt"
JAVA_HOME="$JDK17" "$REPO_ROOT/mvnw" -q -pl parallel-consumer-proxy -am \
    -DskipTests -Dcopyright.skip=true \
    package dependency:build-classpath "-Dmdep.outputFile=$CP_FILE" -Dmdep.includeScope=runtime \
    >"$BUILD_DIR/maven.log" 2>&1 || { echo "maven failed; see $BUILD_DIR/maven.log" >&2; exit 1; }

PROXY_CLASSES="$REPO_ROOT/parallel-consumer-proxy/target/classes"
CLASSPATH="$PROXY_CLASSES:$(cat "$CP_FILE")"

# Compile the FFI entry points against that classpath, with GraalVM's own javac so the class file
# version cannot outrun the native-image that has to read it.
echo "==> compiling the FFI entry points"
mkdir -p "$BUILD_DIR/classes"
"$GRAALVM_HOME/bin/javac" -nowarn -d "$BUILD_DIR/classes" -cp "$CLASSPATH" "$FFI_DIR"/java/*.java

case "$TARGET" in
    probe)   IMAGE_NAME="libpcffi" ;;
    session) IMAGE_NAME="libpc" ;;
    *)       echo "unknown target '$TARGET': expected 'probe' or 'session'" >&2; exit 1 ;;
esac

# --shared is what makes this a library rather than an executable. The build-time initialisation
# list is inherited from the native sidecar's recipe, where every one of those entries was added to
# fix a build that had actually failed - see docs/inflight/perf-native-image-sidecar-works.md.
echo "==> native-image --shared ($IMAGE_NAME)"
cd "$BUILD_DIR"
"$GRAALVM_HOME/bin/native-image" \
    --shared \
    --no-fallback \
    -cp "$BUILD_DIR/classes:$CLASSPATH" \
    --initialize-at-build-time=ch.qos.logback,org.slf4j,org.xml.sax,com.sun.org.apache.xerces,javax.xml \
    -H:Name="$IMAGE_NAME" \
    -H:ConfigurationFileDirectories="$FFI_DIR/native-image-config" \
    2>&1 | tee "$BUILD_DIR/native-image-$TARGET.log"

echo
echo "==> built $BUILD_DIR/$IMAGE_NAME.$LIB_EXT"
ls -l "$BUILD_DIR/$IMAGE_NAME.$LIB_EXT"
