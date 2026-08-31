#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Builds the parallel-consumer-proxy sidecar as a GraalVM native executable, then drives the result
# through the sidecar's whole lifecycle (astubbs#242).
#
# Usage: bin/native-image-sidecar.sh [--output <path>] [--build-only]
#
# NOTHING IN THE ORDINARY BUILD REACHES THIS SCRIPT, and that is deliberate. There is no Maven
# profile, no plugin and no reactor module that runs native-image, so `./mvnw install`,
# bin/build.sh and every CI lane except the one named below work exactly as they did on a machine
# that has never heard of GraalVM. A native image is a packaging question asked on demand; making it
# a build step would put a 1GB toolchain in the path of every developer who wants to run a unit test.
#
# WHAT AN ABSENT TOOLCHAIN MEANS - the same three-way answer, and the same exit codes, as
# bin/foreign-client-step.sh, whose header owns the reasoning:
#
#   - native-image present -> build it, verify it; the verification's verdict is this script's
#   - absent               -> print a banner naming what is missing and how to get it, and exit 0
#   - absent, and PC_NATIVE_IMAGE_STRICT is set -> exit 2
#
# STRICT IS WHAT KEEPS THE LENIENT DEFAULT HONEST: .github/workflows/native-image.yml installs the
# toolchain in the row itself, so absence there is a provisioning bug and must be red rather than a
# green skip. The lenient default is for the developer box, where not having GraalVM is normal.
#
# --no-fallback IS THE LOAD-BEARING FLAG. Without it, native-image quietly emits a FALLBACK IMAGE:
# a launcher that still requires a JVM at run time. It builds green, runs fine on the build machine,
# and destroys the entire proposition, which is handing a Go or Python team an executable. With it,
# a build that cannot close the world FAILS instead - which is the outcome you want, because it is
# the one you can see.
#
# THE CLASSPATH IS RUNTIME SCOPE, AND GETTING THAT WRONG IS SILENT. `dependency:build-classpath`
# takes -DincludeScope, NOT -Dmdep.includeScope (that spelling is accepted and ignored), and the
# ignored spelling yields the TEST classpath: JUnit, Mockito, ArchUnit and - worse - the core
# test-jar's logback.xml all get compiled into the shipped binary, which then carries this
# repository's test logging configuration as its own. The build stays green either way. Assert the
# jar count if you ever change this line.
#
# WHAT THIS DOES NOT PROVE, kept short here because the verifier's javadoc owns it: the lifecycle
# exercise establishes that the artifact behaves like the sidecar and needs nothing from its
# environment. It does NOT establish that the artifact is a native image - a shell wrapper around a
# JVM sidecar passes every arm, which was measured rather than assumed. --no-fallback is what rules
# a JVM out, at build time, and the "is it a real executable" check below is what notices a wrapper.
#
# Exit codes:
#   0  built and verified, or the toolchain is absent and strict mode is off
#   1  the Maven build, the native-image build, or the lifecycle exercise failed
#   2  the toolchain is absent and PC_NATIVE_IMAGE_STRICT is set
#   3  usage error

set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
MODULE_DIR="$REPO_ROOT/parallel-consumer-proxy"
MAIN_CLASS="bz.stub.parallelconsumer.proxy.Main"
VERIFIER_CLASS="bz.stub.parallelconsumer.proxy.NativeSidecarLifecycle"

OUTPUT="$MODULE_DIR/target/pc-sidecar"
BUILD_ONLY=0

die() { # <exit code> <message...>
    local code="$1"
    shift
    printf 'native-image-sidecar: %s\n' "$*" >&2
    exit "$code"
}

usage() {
    printf 'Usage: bin/native-image-sidecar.sh [--output <path>] [--build-only]\n'
}

strict_mode() {
    case "${PC_NATIVE_IMAGE_STRICT:-}" in
        "" | 0 | false | FALSE | no | NO) return 1 ;;
        *) return 0 ;;
    esac
}

# Bracketed and multi-line for the same reason bin/foreign-client-step.sh's is: it has to survive
# being read in a log where every other line is also progress output, and it has to name the fix.
report_absent() {
    local verdict
    if strict_mode; then
        verdict='FAILING, because PC_NATIVE_IMAGE_STRICT is set - a CI row provisions its own toolchain, so absence here is a provisioning bug'
    else
        verdict='SKIPPED - set PC_NATIVE_IMAGE_STRICT=1 to make this red instead'
    fi
    printf '\n'
    printf '========================================================================\n'
    printf '  NATIVE SIDECAR BUILD %s\n' "$verdict"
    printf '  missing : native-image (not on PATH, not under GRAALVM_HOME or JAVA_HOME)\n'
    printf '  get it  : install a GraalVM JDK (sdkman: `sdk install java 23-graal`), then\n'
    printf '            re-run, or point PC_NATIVE_IMAGE at the executable directly\n'
    printf '  nothing was built, compiled or asserted for the native sidecar\n'
    printf '========================================================================\n'
    printf '\n'
    # `printf --` is load-bearing: the format string starts with a hyphen and bash printf would
    # read it as an option. bin/foreign-client-step.sh's header records the CI row this cost.
    if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
        printf -- '- **native sidecar**: `native-image` not on PATH - %s\n' "$verdict" >> "$GITHUB_STEP_SUMMARY"
    fi
}

# PC_NATIVE_IMAGE first so a machine with several GraalVMs can name one; then the two homes GraalVM
# itself sets, then PATH. JAVA_HOME is checked LAST and never exported anywhere - the Maven build
# below must run on the project's JDK 17, and a GraalVM JAVA_HOME exported for the whole build is
# how an unrelated module's delombok step starts failing.
resolve_native_image() {
    local candidate
    if [ -n "${PC_NATIVE_IMAGE:-}" ]; then
        [ -x "$PC_NATIVE_IMAGE" ] || die 3 "PC_NATIVE_IMAGE is set to '$PC_NATIVE_IMAGE', which is not executable"
        printf '%s' "$PC_NATIVE_IMAGE"
        return 0
    fi
    for candidate in "${GRAALVM_HOME:-}/bin/native-image" "${JAVA_HOME:-}/bin/native-image"; do
        if [ -x "$candidate" ]; then
            printf '%s' "$candidate"
            return 0
        fi
    done
    if command -v native-image > /dev/null 2>&1; then
        command -v native-image
        return 0
    fi
    return 1
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --output)
            [ "$#" -ge 2 ] || { usage >&2; die 3 "--output needs a path"; }
            OUTPUT="$2"
            shift 2
            ;;
        --build-only)
            BUILD_ONLY=1
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            usage >&2
            die 3 "unexpected argument '$1'"
            ;;
    esac
done

NATIVE_IMAGE=""
if ! NATIVE_IMAGE="$(resolve_native_image)"; then
    report_absent
    # Spelled as an `if` rather than an AND-list, for the reason bin/foreign-client-step.sh states:
    # under `set -e` the list's status can make the lenient path exit 1, the outcome it exists to
    # avoid.
    if strict_mode; then
        exit 2
    fi
    exit 0
fi

printf 'native-image-sidecar: using %s\n' "$NATIVE_IMAGE"
"$NATIVE_IMAGE" --version

# The Maven half runs on whatever JDK the caller has - the project's JDK 17 - because compiling the
# module is an ordinary build. Only the image step uses GraalVM. test-compile rather than compile:
# the lifecycle verifier lives in the module's test sources, since it is a verification tool and not
# something the sidecar ships.
printf 'native-image-sidecar: building the module and its runtime classpath\n'
CLASSPATH_FILE="$MODULE_DIR/target/native-image-runtime-classpath.txt"
(
    cd "$REPO_ROOT"
    ./mvnw --batch-mode -q -pl parallel-consumer-proxy -am -DskipTests test-compile
    # -DincludeScope, NOT -Dmdep.includeScope. See the header: the wrong spelling is ignored and
    # yields the test classpath, which compiles JUnit and a test logback.xml into the binary.
    ./mvnw --batch-mode -q -pl parallel-consumer-proxy dependency:build-classpath \
        -DincludeScope=runtime "-Dmdep.outputFile=$CLASSPATH_FILE"
)

RUNTIME_CP="$(cat "$CLASSPATH_FILE")"
IMAGE_CP="$MODULE_DIR/target/classes:$RUNTIME_CP"

# The recipe is deliberately this short, and the shortness is a finding rather than an omission.
# The same build against astubbs/parallel-consumer#293's tree needed logback initialised at build
# time and a vendored reachability metadata file, because that tree's sidecar carries an engine and
# the Kafka client. This module carries neither: slf4j-api is its only logging dependency, no
# provider is on its runtime classpath, and nothing here builds an object from a configuration
# string. Adding configuration "just in case" is not free - on that branch, adding reachability
# metadata BROKE a build that had already passed, by letting logback's XML configurator run during
# the build and strand a SAX object in the image heap. If a logging backend or the engine later
# joins the runtime classpath, expect the Logger.name analysis failure back, and reach for
# --initialize-at-build-time=ch.qos.logback,org.slf4j,org.xml.sax,com.sun.org.apache.xerces,javax.xml
# then rather than now.
printf 'native-image-sidecar: building %s\n' "$OUTPUT"
"$NATIVE_IMAGE" --no-fallback -cp "$IMAGE_CP" -o "$OUTPUT" "$MAIN_CLASS"

[ -x "$OUTPUT" ] || die 1 "native-image reported success but produced no executable at $OUTPUT"

# The cheapest thing that separates an executable image from a launcher script wrapping a JVM. It is
# not a proof of nativeness - nothing short of inspecting the binary's linkage is - but a text file
# starting with `#!` is the failure mode a hand-rolled "native build" actually has, and it costs one
# read to rule out.
if [ "$(head -c 2 "$OUTPUT")" = '#!' ]; then
    die 1 "$OUTPUT is a script, not a native executable - --no-fallback should have prevented this"
fi

printf 'native-image-sidecar: built %s\n' "$OUTPUT"
ls -lh "$OUTPUT"

if [ "$BUILD_ONLY" -eq 1 ]; then
    printf 'native-image-sidecar: --build-only, so the lifecycle exercise was NOT run\n'
    exit 0
fi

# The verifier deliberately uses nothing but the JDK, gRPC and the generated stubs, so the module's
# test-classes plus the RUNTIME classpath is enough to run it - no test dependency resolution, and
# no second classpath to keep in step with the first.
#
# The verifier runs on the project's JDK, never on GraalVM's - $JAVA_HOME when the caller set one,
# so the JDK that compiled the class is the JDK that loads it.
printf 'native-image-sidecar: driving the executable through the sidecar lifecycle\n'
JAVA="java"
if [ -x "${JAVA_HOME:-}/bin/java" ]; then
    JAVA="$JAVA_HOME/bin/java"
fi
"$JAVA" -cp "$MODULE_DIR/target/test-classes:$IMAGE_CP" "$VERIFIER_CLASS" "$OUTPUT"
