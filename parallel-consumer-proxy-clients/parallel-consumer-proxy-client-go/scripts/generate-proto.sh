#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Regenerates the Go protobuf/gRPC stubs under gen/ from the FROZEN schema at
# parallel-consumer-proxy-protocol/src/main/proto/parallelconsumer/proxy/v1/proxy.proto.
#
# The generated code is COMMITTED (plan U12 step 6): Go has no codegen step at `go get` time,
# so a consumer of this module must find the stubs already there. Re-running this script on an
# unchanged .proto must leave `git status` clean - that is the regeneration check.
#
# TOOLCHAIN, and why it is assembled rather than assumed:
#   - protoc: NOT a Go tool and not installable here (system packages are Ansible-managed), so we
#     reuse the binary the protocol module's protobuf-maven-plugin already downloads into the local
#     Maven repository. Build that module once (bin/build.sh -pl :parallel-consumer-proxy-protocol
#     -am -DskipTests) and the binary is there.
#   - the well-known types (google/protobuf/duration.proto, timestamp.proto): the Maven protoc
#     artifact is the bare executable with no include/ directory, so the imports resolve from the
#     protobuf-java jar in the same local repository, which ships them as resources.
#   - protoc-gen-go / protoc-gen-go-grpc: plain Go, pinned by the `tool` directives in go.mod (NOT
#     @latest - plan U12 step 6), built here into a temp dir so nothing is installed globally.

set -euo pipefail

MODULE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$MODULE_DIR" && git rev-parse --show-toplevel)"
PROTO_ROOT="$REPO_ROOT/parallel-consumer-proxy-protocol/src/main/proto"
PROTO_FILE="parallelconsumer/proxy/v1/proxy.proto"
GO_MODULE="github.com/astubbs/parallel-consumer/parallel-consumer-proxy-clients/parallel-consumer-proxy-client-go"
M2="${MAVEN_REPO_LOCAL:-$HOME/.m2/repository}"

die() { echo "generate-proto: $*" >&2; exit 1; }

# newest protoc the local Maven repository holds for this platform
case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) PROTOC_CLASSIFIER="linux-x86_64" ;;
    Linux-aarch64) PROTOC_CLASSIFIER="linux-aarch_64" ;;
    Darwin-x86_64) PROTOC_CLASSIFIER="osx-x86_64" ;;
    Darwin-arm64) PROTOC_CLASSIFIER="osx-aarch_64" ;;
    *) die "unsupported platform $(uname -s)-$(uname -m)" ;;
esac
PROTOC="$(find "$M2/com/google/protobuf/protoc" -name "protoc-*-${PROTOC_CLASSIFIER}.exe" 2>/dev/null | sort -V | tail -1)"
[ -n "$PROTOC" ] || die "no protoc in $M2 - build the protocol module first (see the header)"
chmod +x "$PROTOC"

# the well-known types, from the protobuf-java jar beside that protoc
PROTOBUF_JAR="$(find "$M2/com/google/protobuf/protobuf-java" -name 'protobuf-java-*.jar' ! -name '*-sources.jar' 2>/dev/null | sort -V | tail -1)"
[ -n "$PROTOBUF_JAR" ] || die "no protobuf-java jar in $M2 - build the protocol module first"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/include" "$WORK/bin"
(cd "$WORK/include" && unzip -qo "$PROTOBUF_JAR" 'google/protobuf/*.proto')

# plugins at the versions go.mod pins
(cd "$MODULE_DIR" && go build -o "$WORK/bin/protoc-gen-go" google.golang.org/protobuf/cmd/protoc-gen-go)
(cd "$MODULE_DIR" && go build -o "$WORK/bin/protoc-gen-go-grpc" google.golang.org/grpc/cmd/protoc-gen-go-grpc)

# THE .proto CARRIES NO go_package OPTION (it is frozen, and only java_* options were set), so the
# mapping has to be supplied on the command line with M<file>=<import path>. Recorded as a
# specification finding in docs/inflight/clients/go.md - every Go author hits this.
GO_IMPORT="$GO_MODULE/gen/parallelconsumer/proxy/v1;proxyv1"

rm -rf "$MODULE_DIR/gen"
mkdir -p "$MODULE_DIR/gen"
"$PROTOC" \
    -I "$PROTO_ROOT" \
    -I "$WORK/include" \
    --plugin=protoc-gen-go="$WORK/bin/protoc-gen-go" \
    --plugin=protoc-gen-go-grpc="$WORK/bin/protoc-gen-go-grpc" \
    --go_out="$MODULE_DIR/gen" \
    --go_opt=module="$GO_MODULE/gen" \
    --go_opt=M"$PROTO_FILE=$GO_IMPORT" \
    --go-grpc_out="$MODULE_DIR/gen" \
    --go-grpc_opt=module="$GO_MODULE/gen" \
    --go-grpc_opt=M"$PROTO_FILE=$GO_IMPORT" \
    "$PROTO_FILE"

echo "generate-proto: wrote $(cd "$MODULE_DIR" && git ls-files --others --cached gen | wc -l) file(s) under gen/ using $(basename "$PROTOC")"
