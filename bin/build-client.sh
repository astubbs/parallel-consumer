#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Builds one proxy client (astubbs#242, confluentinc#154), whichever way that language builds.
#
# Usage: bin/build-client.sh <language> [--test]
#        bin/build-client.sh --list
#
# THE POINT OF THIS SCRIPT IS THAT THE CALLER DOES NOT HAVE TO KNOW WHICH ROUTE RAN.
#
#   - CONTAINER route: swift and cpp. These are the only two toolchains mise cannot serve here -
#     Swift.org publishes no Debian 13 build, and C++ needs gRPC/protobuf DEV packages rather than a
#     versioned toolchain. Each module carries a multi-stage Dockerfile whose final stage is nothing
#     but artifacts, and BuildKit exports that stage straight to <module>/target/container with
#     `--output type=local`. No container is ever run, nothing is `docker cp`-ed out, and no volume
#     permissions are involved. Reasoning:
#     docs/inflight/parked-containerised-toolchains-and-runtime.md.
#   - NATIVE route: everything else. It re-enters Maven with -Dpc.foreignClients on that module
#     alone, so the language's own build and test commands stay declared in ONE place - the four
#     pc.foreign.* properties in the module's pom - instead of being restated here and drifting.
#
# EXIT CODES, and the reason they are distinguished at all: this repo has a documented history of
# checks that passed without having run, so "the toolchain is absent" must never look like "the
# build passed".
#
#   0  the build (and --test, when asked) succeeded
#   1  the build or the test FAILED - a real, reported failure
#   2  CANNOT RUN: Docker is not installed, its daemon is unreachable, or buildx is missing
#   3  usage error: unknown language, bad flag
#
# --test on the container route runs the EXTRACTED artifact on the host, and that is the portability
# assertion, not a formality: each container module also builds a dynamically linked control, which
# is expected to fail on a host that has no gRPC or Swift runtime installed. A run where both
# binaries work means the host happens to look like the image and the static link proved nothing, so
# the script says so and fails.

set -euo pipefail

readonly CLIENTS_DIR="parallel-consumer-proxy-clients"
readonly MODULE_PREFIX="parallel-consumer-proxy-client-"
readonly PROTO_CONTEXT="parallel-consumer-proxy-protocol/src/main/proto"

# The two container languages. Everything else is native - see the header.
readonly CONTAINER_LANGUAGES="cpp swift"

die() { # <exit code> <message...>
    local code="$1"
    shift
    printf 'bin/build-client.sh: %s\n' "$*" >&2
    exit "$code"
}

usage() {
    printf 'Usage: bin/build-client.sh <language> [--test]\n'
    printf '       bin/build-client.sh --list\n'
}

list_languages() {
    local dir
    for dir in "$CLIENTS_DIR/$MODULE_PREFIX"*/; do
        [ -f "${dir}pom.xml" ] || continue
        printf '%s\n' "$(basename "$dir")" | sed "s/^$MODULE_PREFIX//"
    done
}

is_container_language() { # <language>
    case " $CONTAINER_LANGUAGES " in
        *" $1 "*) return 0 ;;
        *) return 1 ;;
    esac
}

require_docker() {
    command -v docker >/dev/null 2>&1 \
        || die 2 "CANNOT RUN: docker is not installed, and $LANGUAGE only builds in a container. Nothing was built - this is NOT a pass."
    docker info >/dev/null 2>&1 \
        || die 2 "CANNOT RUN: the Docker daemon is not reachable (docker info failed), and $LANGUAGE only builds in a container. Nothing was built - this is NOT a pass."
    docker buildx version >/dev/null 2>&1 \
        || die 2 "CANNOT RUN: docker buildx is missing; artifact extraction needs BuildKit's local exporter (docker buildx build --output type=local)."
}

build_in_container() {
    require_docker

    local module_dir="$CLIENTS_DIR/$MODULE_PREFIX$LANGUAGE"
    local out_dir="$module_dir/target/container"
    [ -f "$module_dir/Dockerfile" ] || die 3 "no Dockerfile in $module_dir"

    printf '==> %s: building in a container, extracting to %s\n' "$LANGUAGE" "$out_dir"
    rm -rf "$out_dir"
    mkdir -p "$out_dir"

    # The ordinary build context is the MODULE, never the repository: a repo-wide context would
    # upload every other module's target/ output on every build. The frozen schema arrives as a
    # BuildKit named context instead, so proxy.proto stays in exactly one place in the tree.
    docker buildx build \
        --target artifact \
        --build-context "proto=$PROTO_CONTEXT" \
        --output "type=local,dest=$out_dir" \
        "$module_dir" \
        || die 1 "$LANGUAGE: container build failed"

    printf '==> %s: extracted artifacts\n' "$LANGUAGE"
    ls -l "$out_dir"
}

# Runs the EXTRACTED artifacts on the host.
#
# THE PAIRING IS THE CONTRACT, not any particular filename. Every extracted executable X that has a
# sibling X-dynamic is a portability claim with its own control: X must run here, X-dynamic must
# NOT. A language names those binaries whatever suits it - a module whose wave has not started ships
# a toolchain smoke, one that has started ships a self-test built from its real client library - and
# an artifact with no -dynamic sibling (a conformance runner, say) is not a portability claim and is
# skipped rather than run with no arguments.
#
# At least one pair must exist: an extraction that produced no claim at all must never read as a
# pass, which is the same rule as "a missing toolchain is exit 2, not a pass".
test_in_container() {
    local module_dir="$CLIENTS_DIR/$MODULE_PREFIX$LANGUAGE"
    local out_dir="$module_dir/target/container"
    local pairs=0 static_binary dynamic_binary

    for dynamic_binary in "$out_dir"/*-dynamic; do
        [ -x "$dynamic_binary" ] || continue
        static_binary="${dynamic_binary%-dynamic}"
        [ -x "$static_binary" ] || die 1 "$LANGUAGE: $dynamic_binary was extracted without its statically linked counterpart $static_binary"
        pairs=$((pairs + 1))

        printf '==> %s: running %s ON THE HOST\n' "$LANGUAGE" "$(basename "$static_binary")"
        "$static_binary" || die 1 "$LANGUAGE: the statically linked artifact failed on the host - the extracted build is not portable"

        printf '==> %s: control - %s is expected to FAIL here\n' "$LANGUAGE" "$(basename "$dynamic_binary")"
        if "$dynamic_binary" >/dev/null 2>&1; then
            die 1 "$LANGUAGE: the dynamic control RAN on this host, so this run is no evidence that static linking is what makes the artifact portable. Check whether the image's runtime libraries are installed here."
        fi
        printf '==> %s: control failed as expected\n' "$LANGUAGE"
    done

    [ "$pairs" -gt 0 ] || die 1 "$LANGUAGE: nothing in $out_dir is a statically linked artifact with a -dynamic control, so this run proves no portability claim - it is NOT a pass. Build first, and check the Dockerfile's artifact stage."
}

# Native languages re-enter Maven rather than restating their build command here. -pl names the
# module; an AGGREGATOR pom's children are named explicitly, because `-pl :<aggregator>` builds the
# aggregator pom and nothing beneath it (recorded in the language-proxy plan's U12 verification).
maven_module_list() { # <module dir> -> comma-separated :artifactId list
    local module_dir="$1"
    local list child
    list=":$(basename "$module_dir")"
    for child in "$module_dir"/*/; do
        [ -f "${child}pom.xml" ] || continue
        list="$list,:$(basename "$child")"
    done
    printf '%s' "$list"
}

build_natively() { # <maven phase>
    local phase="$1"
    local module_dir="$CLIENTS_DIR/$MODULE_PREFIX$LANGUAGE"
    local projects
    projects="$(maven_module_list "$module_dir")"

    printf '==> %s: native toolchain via Maven (%s), projects %s\n' "$LANGUAGE" "$phase" "$projects"
    # No `clean`, deliberately: several agents and worktrees share this tree, and a clean here would
    # delete output nobody asked this script to rebuild.
    ./mvnw --batch-mode -Dpc.foreignClients -pl "$projects" "$phase" \
        || die 1 "$LANGUAGE: native build failed"
}

LANGUAGE=""
RUN_TEST="false"

while [ "$#" -gt 0 ]; do
    case "$1" in
        --list) cd "$(git rev-parse --show-toplevel)" && list_languages && exit 0 ;;
        -h | --help) usage && exit 0 ;;
        --test) RUN_TEST="true" ;;
        -*) usage >&2; die 3 "unknown flag: $1" ;;
        *)
            [ -z "$LANGUAGE" ] || { usage >&2; die 3 "one language at a time (got '$LANGUAGE' and '$1')"; }
            LANGUAGE="$1"
            ;;
    esac
    shift
done

[ -n "$LANGUAGE" ] || { usage >&2; die 3 "no language given"; }

cd "$(git rev-parse --show-toplevel)"

[ -d "$CLIENTS_DIR/$MODULE_PREFIX$LANGUAGE" ] \
    || die 3 "unknown language '$LANGUAGE'; known: $(list_languages | paste -sd' ' -)"

# Re-entry guard. The container languages must never reach the Maven branch, which would re-enter
# the same pom that called this script; a stack of nested Maven builds is a confusing way to
# discover a mis-wired pom, and this names it instead.
if [ "${PC_BUILD_CLIENT_ACTIVE:-}" = "$LANGUAGE" ]; then
    die 3 "re-entered for '$LANGUAGE' - its pom routes back into this script. Check pc.foreign.* in that module's pom."
fi
export PC_BUILD_CLIENT_ACTIVE="$LANGUAGE"

if is_container_language "$LANGUAGE"; then
    if [ "$RUN_TEST" = "true" ]; then
        # The image build is cached and cheap on a second run, so --test is self-contained: it
        # cannot report a pass against an artifact left behind by an older build of other sources.
        build_in_container
        test_in_container
    else
        build_in_container
    fi
else
    if [ "$RUN_TEST" = "true" ]; then
        build_natively test
    else
        build_natively compile
    fi
fi

printf '==> %s: done\n' "$LANGUAGE"
